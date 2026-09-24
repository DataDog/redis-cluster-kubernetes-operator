package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/kube"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"golang.org/x/sync/errgroup"
)

const (
	backupManifestVersion = "redis-cluster-backup/v1"
	backupArtifactRDB     = "rdb"
	backupArtifactNodes   = "nodes-conf"
	restoreStageLimit     = 8
)

type podBackendCommandExecutor struct {
	toolbox   *redis_cluster.Toolbox
	node      *redis_cluster.ClusterNode
	container string
}

func (e podBackendCommandExecutor) Execute(ctx context.Context, command []string, input []byte) (BackendCommandResult, error) {
	rc, stdout, stderr, err := e.toolbox.RunBackupCommand(ctx, e.node, e.container, command, input)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return BackendCommandResult{ExitCode: rc, Stdout: stdout, Stderr: stderr}, err
	}
	if err != nil {
		kind := BackendFailure
		var execErr *kube.ExecError
		if errors.As(err, &execErr) {
			switch execErr.Kind {
			case kube.ExecAuthentication:
				kind = BackendAuthentication
			case kube.ExecNetwork:
				kind = BackendNetwork
			}
		}
		err = &BackendError{Kind: kind, Operation: "execute"}
	}
	return BackendCommandResult{ExitCode: rc, Stdout: stdout, Stderr: stderr}, err
}

func (r *RedisClusterReconciler) bindBackupStore(cluster *redis_cluster.Cluster, backend BackupProvider, node *redis_cluster.ClusterNode) BackupStore {
	return backend.Bind(podBackendCommandExecutor{toolbox: cluster.Toolbox, node: node, container: r.backupContainer(cluster)})
}

type BackupManifest struct {
	Version     string           `json:"version"`
	Timestamp   int64            `json:"timestamp"`
	ClusterName string           `json:"cluster_name"`
	NodeCount   int              `json:"node_count"`
	Primaries   int              `json:"primaries"`
	Artifacts   []BackupArtifact `json:"artifacts"`
	BackupPath  string           `json:"backup_path"`
	FailedNodes []int            `json:"failed_nodes,omitempty"`
	DurationMs  int64            `json:"duration_ms"`
}

type BackupArtifact struct {
	Kind              string            `json:"kind"`
	Role              string            `json:"role"`
	Ordinal           int               `json:"ordinal"`
	ObjectKey         string            `json:"object_key"`
	SHA256            string            `json:"sha256"`
	SizeBytes         int64             `json:"size_bytes"`
	PrimarySlotRanges []BackupSlotRange `json:"primary_slot_ranges,omitempty"`
}

type BackupSlotRange struct {
	First int `json:"first"`
	Last  int `json:"last"`
}

func (r *RedisClusterReconciler) backupLocation() string {
	return os.Getenv("BACKUP_BUCKET")
}

func (r *RedisClusterReconciler) backupBackend(cluster *redis_cluster.Cluster) (BackupProvider, error) {
	location := r.backupLocation()
	factory := r.BackupBackendFactory
	if factory == nil {
		factory = NewBackupBackendRegistry()
	} else if optionalProviderIsNil(factory) {
		return nil, errors.New("backup backend factory is nil")
	}
	backend, err := factory.NewBackupBackend(cluster.RedisCluster.Spec.BackupBackend, location)
	if err != nil {
		return nil, err
	}
	if optionalProviderIsNil(backend) {
		return nil, errors.New("backup backend factory returned a nil backend")
	}
	return backend, nil
}

// backupContainer returns the container to use for cloud storage and file operations.
// Pods with a dedicated "backup" sidecar (e.g. cloudstore-cli) use it directly since
// it ships the required cloud CLIs. Otherwise fall back to the redis container, which
// must have the CLI mounted via an init container.
func (r *RedisClusterReconciler) backupContainer(cluster *redis_cluster.Cluster) string {
	for _, c := range cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers {
		if c.Name == constants.BackupContainerName {
			return constants.BackupContainerName
		}
	}
	return constants.RedisContainerName
}

func (r *RedisClusterReconciler) backup(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	if !cluster.RedisCluster.Spec.BackupEnabled {
		r.Log.Info("Backup is disabled for this Redis cluster")
		return false, nil
	}

	bgPhase := cluster.RedisCluster.Status.BlueGreen.Phase
	if bgPhase != "" {
		r.Log.Info("Skipping backup during blue-green deployment", "phase", bgPhase)
		return false, nil
	}

	now := time.Now().Unix()
	lastBackupTime := cluster.RedisCluster.Status.LastBackupTime
	timeSinceLastBackup := now - lastBackupTime
	backupIsNeeded := cluster.RedisCluster.Status.LastBackupTime == 0 || timeSinceLastBackup > cluster.RedisCluster.Spec.BackupFrequency

	if !backupIsNeeded {
		r.Log.Info("Backup is enabled for this Redis cluster, but not needed now", "now", now, "lastBackupTime", lastBackupTime,
			"timeSinceLastBackup", timeSinceLastBackup, "backupFrequency", cluster.RedisCluster.Spec.BackupFrequency)
		return false, nil
	}

	r.Log.Info("Backup is enabled for this Redis cluster, and needed now", "now", now, "lastBackupTime", lastBackupTime,
		"timeSinceLastBackup", timeSinceLastBackup, "backupFrequency", cluster.RedisCluster.Spec.BackupFrequency)

	_ = r.Metrics.CountToolboxOperation("create_backup")

	backend, err := r.backupBackend(cluster)
	if err != nil {
		return false, err
	}
	location := r.backupLocation()

	latestPath := r.latestBackupPath(cluster)
	previousPath := r.previousBackupPath(cluster)
	inProgressPath := fmt.Sprintf("%s/in-progress", cluster.RedisCluster.Name)

	// Upload to a temporary in-progress prefix first; promote to latest only when complete
	// (below), so a partial backup can never overwrite the last good one (which the restore
	// path depends on). Named "in-progress" rather than "staging" to avoid confusion with the
	// staging environment in the bucket name.
	if err := r.clearInProgressBackup(ctx, cluster, backend, inProgressPath); err != nil {
		return false, fmt.Errorf("failed to clear interrupted backup: %w", err)
	}

	backupStart := time.Now()

	r.Log.Info("Starting backup", "clusterName", cluster.RedisCluster.Name, "location", location,
		"backend", cluster.RedisCluster.Spec.BackupBackend, "path", inProgressPath, "nodeCount", len(cluster.AllNodes))

	var eg errgroup.Group
	var mu sync.Mutex
	var failedNodes []int
	var artifacts []BackupArtifact
	successCount := 0

	for _, node := range cluster.AllNodes {
		eg.Go(func() error {
			nodeStart := time.Now()
			nodePrefix := fmt.Sprintf("%s/%d", inProgressPath, node.PodIndex)

			if err := r.waitForBGSave(cluster, node); err != nil {
				r.Log.Error(err, "Backup failed for node", "podName", node.PodName, "podIndex", node.PodIndex, "ip", node.IP, "duration_ms", time.Since(nodeStart).Milliseconds())
				mu.Lock()
				failedNodes = append(failedNodes, node.PodIndex)
				mu.Unlock()
				return nil
			}

			if err := r.triggerBGSave(cluster, node); err != nil {
				r.Log.Error(err, "Backup failed for node", "podName", node.PodName, "podIndex", node.PodIndex, "ip", node.IP, "duration_ms", time.Since(nodeStart).Milliseconds())
				mu.Lock()
				failedNodes = append(failedNodes, node.PodIndex)
				mu.Unlock()
				return nil
			}

			nodeArtifacts, err := r.uploadNodeBackup(cluster, node, backend, nodePrefix)
			if err != nil {
				r.Log.Error(err, "Backup failed for node", "podName", node.PodName, "podIndex", node.PodIndex, "ip", node.IP, "duration_ms", time.Since(nodeStart).Milliseconds())
				mu.Lock()
				failedNodes = append(failedNodes, node.PodIndex)
				mu.Unlock()
				return nil
			}

			r.Log.Info("Backup succeeded for node", "podName", node.PodName, "podIndex", node.PodIndex, "ip", node.IP, "duration_ms", time.Since(nodeStart).Milliseconds())
			mu.Lock()
			successCount++
			artifacts = append(artifacts, nodeArtifacts...)
			mu.Unlock()
			return nil
		})
	}

	_ = eg.Wait()
	totalDuration := time.Since(backupStart)

	cluster.RedisCluster.Status.LastBackupDurationMs = totalDuration.Milliseconds()
	cluster.RedisCluster.Status.LastBackupFailedNodes = failedNodes
	cluster.RedisCluster.Status.LastBackupNodeCount = len(cluster.AllNodes)

	// Incomplete backup: not safely restorable. Discard the in-progress upload, keep the last
	// good latest/previous, return false (LastBackupTime not advanced → retried next cycle).
	if len(failedNodes) > 0 {
		_ = r.Metrics.CountBadSituation("backup_incomplete_not_promoted")
		_ = r.Metrics.Gauge(metrics.GaugeBackupFailedNodeCount, float64(len(failedNodes)))
		cleanupErr := r.clearInProgressBackup(ctx, cluster, backend, inProgressPath)
		err := fmt.Errorf("backup incomplete: %d of %d node(s) failed (pod indexes: %v); previous good backup left intact",
			len(failedNodes), len(cluster.AllNodes), failedNodes)
		if cleanupErr != nil {
			err = fmt.Errorf("%w; failed to clean incomplete backup: %v", err, cleanupErr)
		}
		r.Log.Error(err, "Backup not promoted", "successCount", successCount, "duration_ms", totalDuration.Milliseconds())
		return false, err
	}

	// Complete: write manifest into the in-progress prefix, then promote to latest (rotating
	// old latest to previous). Only a complete backup becomes latest.
	if err := r.writeBackupManifest(cluster, backend, inProgressPath, artifacts, failedNodes, totalDuration); err != nil {
		_ = r.Metrics.CountBadSituation("backup_manifest_write_failed")
		r.Log.Error(err, "Backup manifest not written; backup will not be promoted",
			"duration_ms", totalDuration.Milliseconds())
		return false, err
	}
	if err := r.promoteInProgressBackup(ctx, cluster, backend, inProgressPath, latestPath, previousPath); err != nil {
		_ = r.Metrics.CountBadSituation("backup_promote_failed")
		r.Log.Error(err, "Failed to promote in-progress backup to latest; staged backup retained",
			"duration_ms", totalDuration.Milliseconds())
		return false, err
	}

	cluster.RedisCluster.Status.LastBackupPath = latestPath

	r.Log.Info("Backup completed", "totalNodes", len(cluster.AllNodes), "duration_ms", totalDuration.Milliseconds(), "location", location, "path", latestPath)
	_ = r.Metrics.Count(counters.ClusterBackedUp)
	_ = r.Metrics.Timing(metrics.TimingBackupDuration, totalDuration.Milliseconds())
	_ = r.Metrics.Gauge(metrics.GaugeBackupNodeCount, float64(len(cluster.AllNodes)))
	_ = r.Metrics.Gauge(metrics.GaugeBackupFailedNodeCount, 0)

	return true, nil
}

// clearInProgressBackup removes any leftover in-progress prefix from a prior interrupted backup.
func (r *RedisClusterReconciler) clearInProgressBackup(ctx context.Context, cluster *redis_cluster.Cluster, backend BackupProvider, inProgressPath string) error {
	if len(cluster.AllNodes) == 0 {
		return nil
	}
	execNode := cluster.AllNodes[0]
	err := r.bindBackupStore(cluster, backend, execNode).DeletePrefix(ctx, inProgressPath)
	if IsBackendNotFound(err) {
		return nil
	}
	return err
}

// replaceLatestWithInProgress keeps the staged backup when its copy fails and, when
// latest was successfully rotated, restores that generation after clearing any partial copy.
func replaceLatestWithInProgress(ctx context.Context, store BackupStore, inProgressPath, latestPath, previousPath string, latestWasRotated bool) error {
	if err := store.DeletePrefix(ctx, latestPath); err != nil && !IsBackendNotFound(err) {
		return fmt.Errorf("failed to clear latest before promotion: %w", err)
	}
	if err := store.CopyPrefix(ctx, inProgressPath, latestPath); err != nil {
		promotionErr := fmt.Errorf("failed to copy in-progress to latest: %w", err)
		if !latestWasRotated {
			return promotionErr
		}

		if cleanupErr := store.DeletePrefix(ctx, latestPath); cleanupErr != nil && !IsBackendNotFound(cleanupErr) {
			return fmt.Errorf("%w; failed to clear partial latest before rollback: %v", promotionErr, cleanupErr)
		}
		if rollbackErr := store.CopyPrefix(ctx, previousPath, latestPath); rollbackErr != nil {
			return fmt.Errorf("%w; failed to restore previous to latest: %v", promotionErr, rollbackErr)
		}
		return fmt.Errorf("%w; restored previous backup to latest", promotionErr)
	}
	if err := store.DeletePrefix(ctx, inProgressPath); err != nil && !IsBackendNotFound(err) {
		return fmt.Errorf("promoted backup but failed to clean in-progress prefix: %w", err)
	}
	return nil
}

func rotateLatestToPrevious(ctx context.Context, store BackupStore, latestPath, previousPath string) (bool, error) {
	latestExists, err := store.Exists(ctx, latestPath)
	if err != nil {
		return false, fmt.Errorf("failed to determine whether latest backup exists: %w", err)
	}
	if !latestExists {
		return false, nil
	}
	if err := store.DeletePrefix(ctx, previousPath); err != nil && !IsBackendNotFound(err) {
		return false, fmt.Errorf("failed to clear previous backup before rotation: %w", err)
	}
	if err := store.CopyPrefix(ctx, latestPath, previousPath); err != nil {
		return false, fmt.Errorf("failed to rotate latest backup to previous: %w", err)
	}
	return true, nil
}

// promoteInProgressBackup rotates previous<-latest then latest<-in-progress. Called only for
// a complete backup. If promotion fails after rotating latest, the previous generation is
// restored; the staged in-progress backup is retained for diagnosis or retry.
func (r *RedisClusterReconciler) promoteInProgressBackup(ctx context.Context, cluster *redis_cluster.Cluster, backend BackupProvider, inProgressPath, latestPath, previousPath string) error {
	if len(cluster.AllNodes) == 0 {
		return fmt.Errorf("no nodes available to promote backup")
	}
	execNode := cluster.AllNodes[0]
	store := r.bindBackupStore(cluster, backend, execNode)

	// Rotate latest -> previous. A confirmed absence is the only first-backup case.
	latestWasRotated, err := rotateLatestToPrevious(ctx, store, latestPath, previousPath)
	if err != nil {
		return err
	}
	if latestWasRotated {
		r.Log.Info("Rotated latest backup to previous", "from", latestPath, "to", previousPath)
	}

	if err := replaceLatestWithInProgress(ctx, store, inProgressPath, latestPath, previousPath, latestWasRotated); err != nil {
		return err
	}
	r.Log.Info("Promoted in-progress backup to latest", "from", inProgressPath, "to", latestPath)
	return nil
}

func (r *RedisClusterReconciler) writeBackupManifest(cluster *redis_cluster.Cluster, backend BackupProvider, backupPath string, artifacts []BackupArtifact, failedNodes []int, duration time.Duration) error {
	if len(cluster.AllNodes) == 0 {
		return fmt.Errorf("no nodes available to write backup manifest")
	}

	primaryCount := 0
	for _, node := range cluster.AllNodes {
		if node.Role == redis_cluster.PrimaryNode {
			primaryCount++
		}
	}

	manifest := BackupManifest{
		Version:     backupManifestVersion,
		Timestamp:   time.Now().Unix(),
		ClusterName: cluster.RedisCluster.Name,
		NodeCount:   len(cluster.AllNodes),
		Primaries:   primaryCount,
		Artifacts:   artifacts,
		BackupPath:  backupPath,
		FailedNodes: failedNodes,
		DurationMs:  duration.Milliseconds(),
	}
	if _, err := validateBackupManifest(&manifest, backupPath); err != nil {
		return fmt.Errorf("refusing to publish invalid backup manifest: %w", err)
	}

	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		r.Log.Error(err, "Failed to marshal backup manifest")
		return fmt.Errorf("failed to marshal backup manifest: %w", err)
	}

	execNode := cluster.AllNodes[0]
	manifestRemotePath := backupPath + "/manifest.json"
	if err := r.bindBackupStore(cluster, backend, execNode).UploadBytes(cluster.Toolbox.Context(), manifestJSON, manifestRemotePath); err != nil {
		uploadErr := fmt.Errorf("manifest upload failed: %w", err)
		r.Log.Error(uploadErr, "Failed to write backup manifest")
		return uploadErr
	}
	r.Log.Info("Backup manifest written", "path", manifestRemotePath, "primaries", primaryCount, "nodeCount", len(cluster.AllNodes))
	return nil
}

func (r *RedisClusterReconciler) latestBackupPath(cluster *redis_cluster.Cluster) string {
	return fmt.Sprintf("%s/latest", cluster.RedisCluster.Name)
}

func (r *RedisClusterReconciler) previousBackupPath(cluster *redis_cluster.Cluster) string {
	return fmt.Sprintf("%s/previous", cluster.RedisCluster.Name)
}

func (r *RedisClusterReconciler) readBackupManifest(cluster *redis_cluster.Cluster, backupPath string) (*BackupManifest, error) {
	if len(cluster.AllNodes) == 0 {
		return nil, fmt.Errorf("no nodes available to read manifest")
	}
	backend, err := r.backupBackend(cluster)
	if err != nil {
		return nil, err
	}

	execNode := cluster.AllNodes[0]
	manifestBytes, err := r.bindBackupStore(cluster, backend, execNode).DownloadBytes(cluster.Toolbox.Context(), backupPath+"/manifest.json")
	if err != nil {
		return nil, fmt.Errorf("failed to download manifest at %s/manifest.json: %w", backupPath, err)
	}

	manifest, err := decodeBackupManifest(manifestBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}
	if _, err := validateBackupManifest(manifest, backupPath); err != nil {
		return nil, fmt.Errorf("invalid backup manifest: %w", err)
	}
	return manifest, nil
}

func decodeBackupManifest(data []byte) (*BackupManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest BackupManifest
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateBackupManifest(manifest *BackupManifest, restorePath string) (map[int]BackupArtifact, error) {
	if manifest == nil {
		return nil, fmt.Errorf("manifest is missing")
	}
	if manifest.Version != backupManifestVersion {
		return nil, fmt.Errorf("unsupported manifest version %q", manifest.Version)
	}
	if err := validateRemotePath(restorePath); err != nil {
		return nil, fmt.Errorf("invalid restore path: %w", err)
	}
	if manifest.NodeCount <= 0 || manifest.Primaries <= 0 || manifest.Primaries > manifest.NodeCount {
		return nil, fmt.Errorf("invalid manifest topology")
	}
	if manifest.Timestamp <= 0 || manifest.ClusterName == "" {
		return nil, fmt.Errorf("manifest provenance is incomplete")
	}
	if err := validateRemotePath(manifest.BackupPath); err != nil {
		return nil, fmt.Errorf("manifest backup path is invalid: %w", err)
	}
	if len(manifest.FailedNodes) != 0 {
		return nil, fmt.Errorf("manifest records failed backup nodes: %v", manifest.FailedNodes)
	}
	if len(manifest.Artifacts) != manifest.NodeCount*2 {
		return nil, fmt.Errorf("artifact inventory has %d entries for %d nodes", len(manifest.Artifacts), manifest.NodeCount)
	}

	rdbByOrdinal := make(map[int]BackupArtifact, manifest.NodeCount)
	kindsByOrdinal := make(map[int]map[string]bool, manifest.NodeCount)
	roleByOrdinal := make(map[int]string, manifest.NodeCount)
	primaryCount := 0
	covered := make([]bool, constants.TotalSlots)
	coveredCount := 0
	for _, artifact := range manifest.Artifacts {
		if artifact.Ordinal < 0 || artifact.Ordinal >= manifest.NodeCount {
			return nil, fmt.Errorf("artifact has invalid ordinal %d", artifact.Ordinal)
		}
		if artifact.Kind != backupArtifactRDB && artifact.Kind != backupArtifactNodes {
			return nil, fmt.Errorf("artifact %d has unsupported kind %q", artifact.Ordinal, artifact.Kind)
		}
		expectedKey := fmt.Sprintf("%d/%s", artifact.Ordinal, map[string]string{
			backupArtifactRDB: "dump.rdb", backupArtifactNodes: "nodes.conf",
		}[artifact.Kind])
		if err := validateRemotePath(artifact.ObjectKey); err != nil || artifact.ObjectKey != expectedKey {
			return nil, fmt.Errorf("artifact %d has invalid object key %q", artifact.Ordinal, artifact.ObjectKey)
		}
		if _, err := hex.DecodeString(artifact.SHA256); err != nil || len(artifact.SHA256) != 64 {
			return nil, fmt.Errorf("artifact %d has invalid SHA-256 digest", artifact.Ordinal)
		}
		if artifact.SizeBytes <= 0 {
			return nil, fmt.Errorf("artifact %d has invalid size %d", artifact.Ordinal, artifact.SizeBytes)
		}
		if artifact.Role != redis_cluster.PrimaryNode.String() && artifact.Role != redis_cluster.ReplicaNode.String() {
			return nil, fmt.Errorf("artifact %d has invalid role %q", artifact.Ordinal, artifact.Role)
		}
		if role, ok := roleByOrdinal[artifact.Ordinal]; ok && role != artifact.Role {
			return nil, fmt.Errorf("artifacts for ordinal %d disagree on role", artifact.Ordinal)
		}
		roleByOrdinal[artifact.Ordinal] = artifact.Role
		if kindsByOrdinal[artifact.Ordinal] == nil {
			kindsByOrdinal[artifact.Ordinal] = make(map[string]bool)
		}
		if kindsByOrdinal[artifact.Ordinal][artifact.Kind] {
			return nil, fmt.Errorf("duplicate %s artifact for ordinal %d", artifact.Kind, artifact.Ordinal)
		}
		kindsByOrdinal[artifact.Ordinal][artifact.Kind] = true

		if artifact.Kind == backupArtifactNodes {
			if len(artifact.PrimarySlotRanges) != 0 {
				return nil, fmt.Errorf("nodes.conf artifact %d contains slot ownership", artifact.Ordinal)
			}
			continue
		}
		if artifact.Role == redis_cluster.ReplicaNode.String() {
			if len(artifact.PrimarySlotRanges) != 0 {
				return nil, fmt.Errorf("replica artifact %d contains slot ownership", artifact.Ordinal)
			}
		} else {
			primaryCount++
			if len(artifact.PrimarySlotRanges) == 0 {
				return nil, fmt.Errorf("primary artifact %d has no slot ranges", artifact.Ordinal)
			}
			for _, slots := range artifact.PrimarySlotRanges {
				if slots.First < 0 || slots.Last >= constants.TotalSlots || slots.First > slots.Last {
					return nil, fmt.Errorf("primary artifact %d has invalid slot range %d-%d", artifact.Ordinal, slots.First, slots.Last)
				}
				for slot := slots.First; slot <= slots.Last; slot++ {
					if covered[slot] {
						return nil, fmt.Errorf("slot %d has multiple owners", slot)
					}
					covered[slot] = true
					coveredCount++
				}
			}
		}
		rdbByOrdinal[artifact.Ordinal] = artifact
	}
	if len(kindsByOrdinal) != manifest.NodeCount || len(rdbByOrdinal) != manifest.NodeCount {
		return nil, fmt.Errorf("artifact inventory does not cover every node")
	}
	for ordinal, kinds := range kindsByOrdinal {
		if !kinds[backupArtifactRDB] || !kinds[backupArtifactNodes] {
			return nil, fmt.Errorf("artifact inventory for ordinal %d is incomplete", ordinal)
		}
	}
	if primaryCount != manifest.Primaries {
		return nil, fmt.Errorf("manifest records %d primaries but inventories %d", manifest.Primaries, primaryCount)
	}
	if coveredCount != constants.TotalSlots {
		return nil, fmt.Errorf("primary slot coverage is incomplete")
	}
	return rdbByOrdinal, nil
}

func validateStagedArtifact(artifact BackupArtifact, size int64, digest string) error {
	if size != artifact.SizeBytes {
		return fmt.Errorf("artifact %q size mismatch: got %d, want %d", artifact.ObjectKey, size, artifact.SizeBytes)
	}
	if !strings.EqualFold(digest, artifact.SHA256) {
		return fmt.Errorf("artifact %q digest mismatch", artifact.ObjectKey)
	}
	return nil
}

func manifestSlotCoverage(manifest *BackupManifest) (map[int]string, error) {
	if manifest == nil {
		return nil, fmt.Errorf("manifest is missing")
	}
	coverage := make(map[int]string, manifest.Primaries)
	for _, artifact := range manifest.Artifacts {
		if artifact.Kind != backupArtifactRDB || artifact.Role != redis_cluster.PrimaryNode.String() {
			continue
		}
		ranges := make([]string, 0, len(artifact.PrimarySlotRanges))
		for _, slots := range artifact.PrimarySlotRanges {
			ranges = append(ranges, fmt.Sprintf("%d-%d", slots.First, slots.Last))
		}
		coverage[artifact.Ordinal] = strings.Join(ranges, ",")
	}
	return coverage, nil
}

// backupExists probes every pod and returns (true,nil) if a backup is present,
// (false,nil) if definitively absent, (false,err) if existence is unknown. Callers
// must treat the error case as "unknown", never "absent": empty-assigning over a
// live backup loses data.
func (r *RedisClusterReconciler) backupExists(cluster *redis_cluster.Cluster, path string) (bool, error) {
	if len(cluster.AllNodes) == 0 {
		return false, fmt.Errorf("no nodes available to probe backup existence at %s", path)
	}
	backend, err := r.backupBackend(cluster)
	if err != nil {
		return false, err
	}
	var lastErr error
	probed := false
	for _, node := range cluster.AllNodes {
		if node.PodName == "" {
			continue
		}
		exists, execErr := r.bindBackupStore(cluster, backend, node).Exists(cluster.Toolbox.Context(), path)
		if execErr != nil {
			lastErr = execErr
			continue // pod-specific exec failure — try another pod
		}
		probed = true
		if exists {
			return true, nil
		}
	}
	if !probed {
		if lastErr == nil {
			lastErr = fmt.Errorf("no reachable pod to probe backup existence")
		}
		return false, fmt.Errorf("could not determine backup existence at %s: %w", path, lastErr)
	}
	// At least one pod's backend ran and none reported the object: definitively absent.
	return false, nil
}

func (r *RedisClusterReconciler) latestBackupExists(cluster *redis_cluster.Cluster) (bool, error) {
	return r.backupExists(cluster, r.latestBackupPath(cluster))
}

func (r *RedisClusterReconciler) restoreNodes(ctx context.Context, cluster *redis_cluster.Cluster, restorePath string, nodes []*redis_cluster.ClusterNode) (resultErr error) {
	if len(nodes) == 0 {
		return nil
	}

	manifest, err := r.readBackupManifest(cluster, restorePath)
	if err != nil {
		return fmt.Errorf("failed to read and validate backup manifest: %w", err)
	}
	r.Log.Info("Backup manifest loaded",
		"restorePath", restorePath,
		"backupTimestamp", manifest.Timestamp,
		"backupNodeCount", manifest.NodeCount,
		"backupPrimaries", manifest.Primaries,
		"durationMs", manifest.DurationMs)

	backend, err := r.backupBackend(cluster)
	if err != nil {
		return err
	}
	staged, err := r.stageRestoreArtifacts(ctx, cluster, backend, restorePath, manifest)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, r.cleanupStagedArtifacts(cluster, staged))
	}()
	return r.restoreStagedNodes(cluster, restorePath, nodes, staged)
}

func (r *RedisClusterReconciler) restoreStagedNodes(cluster *redis_cluster.Cluster, restorePath string, nodes []*redis_cluster.ClusterNode, staged map[stagedArtifactKey]stagedBackupArtifact) error {
	podNames := make([]string, len(nodes))
	for i, node := range nodes {
		podNames[i] = node.PodName
	}

	r.Log.Info("Restoring nodes from backup", "restoreFrom", restorePath, "pods", podNames, "count", len(nodes))
	_ = r.Metrics.CountToolboxOperation("restore_backup")

	restoreStart := time.Now()
	var eg errgroup.Group
	var mu sync.Mutex
	var failedNodes []int
	successCount := 0

	for _, node := range nodes {
		eg.Go(func() error {
			nodeStart := time.Now()
			if err := r.activateStagedNodeBackup(cluster, node, staged); err != nil {
				r.Log.Info("Restore failed for node", "podIndex", node.PodIndex, "podName", node.PodName, "err", err)
				mu.Lock()
				failedNodes = append(failedNodes, node.PodIndex)
				mu.Unlock()
				return nil
			}

			r.Log.Info("Restore succeeded for node", "podIndex", node.PodIndex, "podName", node.PodName, "ip", node.IP, "duration_ms", time.Since(nodeStart).Milliseconds())
			mu.Lock()
			successCount++
			mu.Unlock()
			return nil
		})
	}

	_ = eg.Wait()
	totalDuration := time.Since(restoreStart)

	// All-or-nothing: any unrestored node means a shard comes back empty while the
	// cluster reports healthy. Fail so the caller requeues instead of proceeding.
	if len(failedNodes) > 0 {
		return fmt.Errorf("restore incomplete: %d of %d node(s) failed (failed pod indexes: %v) from %s",
			len(failedNodes), len(nodes), failedNodes, restorePath)
	}

	_ = r.Metrics.Timing(metrics.TimingRestoreDuration, totalDuration.Milliseconds())
	r.Log.Info("Restore completed",
		"restored", successCount, "failed", len(failedNodes),
		"targetedNodes", len(nodes), "duration_ms", totalDuration.Milliseconds(),
		"restorePath", restorePath, "location", r.backupLocation())
	return nil
}

func (r *RedisClusterReconciler) restore(ctx context.Context, cluster *redis_cluster.Cluster, restorePath string) error {
	return r.restoreNodes(ctx, cluster, restorePath, cluster.AllNodes)
}

func (r *RedisClusterReconciler) backupSlotCoverage(cluster *redis_cluster.Cluster, path string) (map[int]string, *BackupManifest, error) {
	manifest, err := r.readBackupManifest(cluster, path)
	if err != nil {
		return nil, nil, err
	}
	coverage, err := manifestSlotCoverage(manifest)
	return coverage, manifest, err
}

// slotCoverageIsComplete reports whether the per-pod ranges cover every slot 0..TotalSlots-1
// exactly once. Gaps, overlaps, out-of-range, or malformed ranges all return false so the
// caller falls back to a keys-only restore rather than re-owning from a suspect manifest.
func slotCoverageIsComplete(coverage map[int]string) bool {
	if len(coverage) == 0 {
		return false
	}
	covered := make([]bool, constants.TotalSlots)
	count := 0
	for _, ranges := range coverage {
		for _, part := range strings.Split(ranges, ",") {
			lo, hi, ok := parseSlotRange(part)
			if !ok || lo < 0 || hi >= constants.TotalSlots {
				return false
			}
			for s := lo; s <= hi; s++ {
				if covered[s] {
					return false // overlapping ownership — suspect manifest
				}
				covered[s] = true
				count++
			}
		}
	}
	return count == constants.TotalSlots
}

// parseSlotRange parses a "first-last" range (as written into the backup manifest).
func parseSlotRange(part string) (int, int, bool) {
	part = strings.TrimSpace(part)
	lo, hi, found := strings.Cut(part, "-")
	if !found {
		return 0, 0, false
	}
	first, err1 := strconv.Atoi(strings.TrimSpace(lo))
	last, err2 := strconv.Atoi(strings.TrimSpace(hi))
	if err1 != nil || err2 != nil || first > last {
		return 0, 0, false
	}
	return first, last, true
}

// redisCliError returns a non-nil error if a redis-cli call surfaced any failure
// indicator — Go error, non-zero exit code, or a Redis-protocol "(error) ..."
// response printed to stdout. redis-cli exits rc=0 even for command-level errors
// (NOPERM, WRONGTYPE, MOVED, etc.) — the error text goes to stdout — so checking
// err+rc alone silently passes through those cases.
func redisCliError(rc int, stdout, stderr string, err error) error {
	if err != nil {
		return fmt.Errorf("exec failed: %v", err)
	}
	if rc != 0 {
		return fmt.Errorf("rc=%d stdout=%q stderr=%q", rc, stdout, stderr)
	}
	if trimmed := strings.TrimSpace(stdout); strings.HasPrefix(trimmed, "(error)") {
		return fmt.Errorf("%s", trimmed)
	}
	return nil
}

func (r *RedisClusterReconciler) waitForBGSave(cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode) error {
	for i := 0; i < 120; i++ {
		rc, stdout, stderr, err := cluster.Toolbox.PersistenceInfo(node)
		if e := redisCliError(rc, stdout, stderr, err); e != nil {
			return fmt.Errorf("failed to check persistence info: %v", e)
		}
		if !strings.Contains(stdout, "rdb_bgsave_in_progress:1") {
			return nil
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("BGSAVE still in progress after 10 minutes")
}

func (r *RedisClusterReconciler) triggerBGSave(cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode) error {
	rc, origSave, stderr, err := cluster.Toolbox.LastSave(node)
	if e := redisCliError(rc, origSave, stderr, err); e != nil {
		return fmt.Errorf("failed to get lastsave: %v", e)
	}
	origSave = strings.TrimSpace(origSave)

	rc, stdout, stderr, err := cluster.Toolbox.BGSave(node)
	if e := redisCliError(rc, stdout, stderr, err); e != nil {
		return fmt.Errorf("failed to trigger bgsave: %v", e)
	}

	for i := 0; i < 120; i++ {
		time.Sleep(5 * time.Second)
		rc, lastSave, stderr, err := cluster.Toolbox.LastSave(node)
		if e := redisCliError(rc, lastSave, stderr, err); e != nil {
			continue
		}
		if strings.TrimSpace(lastSave) != origSave {
			return nil
		}
	}
	return fmt.Errorf("lastsave not updated after 10 minutes")
}

func (r *RedisClusterReconciler) uploadNodeBackup(cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode, backend BackupProvider, nodePrefix string) ([]BackupArtifact, error) {
	if err := cluster.Toolbox.PrepareNodesConfBackup(node); err != nil {
		return nil, fmt.Errorf("failed to sanitize nodes.conf: %w", err)
	}
	cleanupNodesConf := func(cause error) error {
		cleanupErr := cluster.Toolbox.RemoveBackupFiles(node, redis_cluster.BackupFileNodesConfBackup)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("failed to clean up nodes.conf backup: %w", cleanupErr)
		}
		return errors.Join(cause, cleanupErr)
	}
	dumpSize, dumpDigest, err := cluster.Toolbox.BackupFileMetadata(cluster.Toolbox.Context(), node, redis_cluster.BackupFileDumpRDB)
	if err != nil {
		return nil, cleanupNodesConf(fmt.Errorf("failed to inspect dump.rdb: %w", err))
	}
	nodesSize, nodesDigest, err := cluster.Toolbox.BackupFileMetadata(cluster.Toolbox.Context(), node, redis_cluster.BackupFileNodesConfBackup)
	if err != nil {
		return nil, cleanupNodesConf(fmt.Errorf("failed to inspect nodes.conf backup: %w", err))
	}

	// Uploads run on the backup container — it has the cloud CLI.
	store := r.bindBackupStore(cluster, backend, node)
	nodesPath, err := redis_cluster.BackupFileNodesConfBackup.Path()
	if err != nil {
		return nil, cleanupNodesConf(err)
	}
	if err := store.UploadFile(cluster.Toolbox.Context(), nodesPath, nodePrefix+"/nodes.conf"); err != nil {
		return nil, cleanupNodesConf(fmt.Errorf("failed to upload nodes.conf: %w", err))
	}

	dumpPath, err := redis_cluster.BackupFileDumpRDB.Path()
	if err != nil {
		return nil, cleanupNodesConf(err)
	}
	if err := store.UploadFile(cluster.Toolbox.Context(), dumpPath, nodePrefix+"/dump.rdb"); err != nil {
		return nil, cleanupNodesConf(fmt.Errorf("failed to upload dump.rdb: %w", err))
	}

	if err := cleanupNodesConf(nil); err != nil {
		return nil, err
	}
	role := node.Role.String()
	ranges := make([]BackupSlotRange, 0)
	if node.Role == redis_cluster.PrimaryNode {
		slots := node.SlotsRanges
		if len(slots) == 0 {
			for _, view := range node.Views {
				slots = view.SlotsRanges
				break
			}
		}
		for _, slotRange := range slots {
			ranges = append(ranges, BackupSlotRange{First: slotRange.First, Last: slotRange.Last})
		}
	}
	return []BackupArtifact{
		{Kind: backupArtifactRDB, Role: role, Ordinal: node.PodIndex, ObjectKey: fmt.Sprintf("%d/dump.rdb", node.PodIndex), SHA256: dumpDigest, SizeBytes: dumpSize, PrimarySlotRanges: ranges},
		{Kind: backupArtifactNodes, Role: role, Ordinal: node.PodIndex, ObjectKey: fmt.Sprintf("%d/nodes.conf", node.PodIndex), SHA256: nodesDigest, SizeBytes: nodesSize},
	}, nil
}

type stagedBackupArtifact struct {
	artifact BackupArtifact
	node     *redis_cluster.ClusterNode
	file     redis_cluster.BackupFile
}

type stagedArtifactKey struct {
	ordinal int
	kind    string
}

func (r *RedisClusterReconciler) stageRestoreArtifacts(ctx context.Context, cluster *redis_cluster.Cluster, backend BackupProvider, restorePath string, manifest *BackupManifest) (map[stagedArtifactKey]stagedBackupArtifact, error) {
	nodesByOrdinal := make(map[int]*redis_cluster.ClusterNode, len(cluster.AllNodes))
	for _, node := range cluster.AllNodes {
		nodesByOrdinal[node.PodIndex] = node
	}
	staged := make(map[stagedArtifactKey]stagedBackupArtifact, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		node := nodesByOrdinal[artifact.Ordinal]
		if node == nil {
			return nil, fmt.Errorf("restore target has no node for backup ordinal %d", artifact.Ordinal)
		}
		file := redis_cluster.BackupFileStagedDump
		if artifact.Kind == backupArtifactNodes {
			file = redis_cluster.BackupFileStagedNodesConf
		}
		entry := stagedBackupArtifact{artifact: artifact, node: node, file: file}
		staged[stagedArtifactKey{ordinal: artifact.Ordinal, kind: artifact.Kind}] = entry
	}

	eg, stageCtx := errgroup.WithContext(ctx)
	eg.SetLimit(restoreStageLimit)
	for _, entry := range staged {
		artifact := entry.artifact
		node := entry.node
		eg.Go(func() error {
			remotePath := restorePath + "/" + artifact.ObjectKey
			localPath, err := entry.file.Path()
			if err != nil {
				return fmt.Errorf("failed to resolve local path for artifact %q: %w", artifact.ObjectKey, err)
			}
			if err := r.bindBackupStore(cluster, backend, node).DownloadFile(stageCtx, remotePath, localPath); err != nil {
				return fmt.Errorf("failed to stage artifact %q: %w", artifact.ObjectKey, err)
			}
			size, digest, err := cluster.Toolbox.BackupFileMetadata(stageCtx, node, entry.file)
			if err != nil {
				return fmt.Errorf("failed to inspect staged artifact %q: %w", artifact.ObjectKey, err)
			}
			if err := validateStagedArtifact(artifact, size, digest); err != nil {
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		cleanupErr := r.cleanupStagedArtifacts(cluster, staged)
		return nil, errors.Join(fmt.Errorf("restore validation failed before Redis mutation: %w", err), cleanupErr)
	}
	return staged, nil
}

func (r *RedisClusterReconciler) activateStagedNodeBackup(cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode, staged map[stagedArtifactKey]stagedBackupArtifact) error {
	_, ok := staged[stagedArtifactKey{ordinal: node.PodIndex, kind: backupArtifactRDB}]
	if !ok {
		return fmt.Errorf("manifest has no RDB artifact for ordinal %d", node.PodIndex)
	}
	if err := cluster.Toolbox.ActivateStagedDump(node); err != nil {
		return fmt.Errorf("failed to activate staged dump.rdb: %w", err)
	}
	if _, _, _, err := cluster.Toolbox.ShutdownNoSave(node); err != nil {
		r.Log.Info("SHUTDOWN NOSAVE returned error (expected — Redis exits)", "podName", node.PodName, "err", err)
	}
	return nil
}

func (r *RedisClusterReconciler) cleanupStagedArtifacts(cluster *redis_cluster.Cluster, staged map[stagedArtifactKey]stagedBackupArtifact) error {
	filesByNode := make(map[*redis_cluster.ClusterNode][]redis_cluster.BackupFile)
	for _, entry := range staged {
		filesByNode[entry.node] = append(filesByNode[entry.node], entry.file)
	}
	var cleanupErr error
	for node, files := range filesByNode {
		if err := cluster.Toolbox.RemoveBackupFiles(node, files...); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("failed to clean up staged backup files on pod %q: %w", node.PodName, err))
		}
	}
	return cleanupErr
}
