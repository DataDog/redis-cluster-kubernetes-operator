// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func clusterWithSpec(spec cachev1.RedisClusterSpec) *redis_cluster.Cluster {
	return &redis_cluster.Cluster{RedisCluster: &cachev1.RedisCluster{Spec: spec}}
}

func TestWaitForBackupPollStopsWhenReconciliationIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err := waitForBackupPoll(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(started), time.Second)
}

func TestRedisInfoString(t *testing.T) {
	info := "# Server\r\nredis_version:8.6.0\r\nrun_id:abc123\r\n"
	assert.Equal(t, "abc123", redisInfoString(info, "run_id"))
	assert.Empty(t, redisInfoString(info, "missing"))
}

func TestRestoreStagedNodesRejectsMissingTargetArtifactBeforeMutation(t *testing.T) {
	r := &RedisClusterReconciler{}
	cluster := &redis_cluster.Cluster{Toolbox: &redis_cluster.Toolbox{}}
	targets := []*redis_cluster.ClusterNode{
		{PodName: "cache-0", PodIndex: 0},
		{PodName: "cache-1", PodIndex: 1},
	}
	staged := map[stagedArtifactKey]stagedBackupArtifact{
		{ordinal: 0, kind: backupArtifactRDB}: {},
	}

	err := r.restoreStagedNodes(context.Background(), cluster, "default/cache/latest", targets, staged)

	require.EqualError(t, err, "manifest has no RDB artifact for restore target ordinal 1")
}

func TestPlanMappedRDBRestoreCarriesSourceArtifactToSelectedTarget(t *testing.T) {
	target := &redis_cluster.ClusterNode{PodIndex: 3, PodName: "cache-3"}
	manifest := &BackupManifest{Artifacts: []BackupArtifact{
		{Ordinal: 0, Kind: backupArtifactRDB, ObjectKey: "0/dump.rdb", SHA256: "source-digest"},
		{Ordinal: 3, Kind: backupArtifactRDB, ObjectKey: "3/dump.rdb", SHA256: "target-digest"},
	}}

	staged, err := planMappedRDBRestore(manifest, []restoreArtifactTarget{{
		sourceOrdinal: 0,
		target:        target,
	}})

	require.NoError(t, err)
	entry, exists := staged[stagedArtifactKey{ordinal: 3, kind: backupArtifactRDB}]
	require.True(t, exists)
	assert.Equal(t, 0, entry.artifact.Ordinal)
	assert.Equal(t, "0/dump.rdb", entry.artifact.ObjectKey)
	assert.Same(t, target, entry.node)
	assert.Equal(t, redis_cluster.BackupFileStagedDump, entry.file)
}

func TestPlanMappedRDBRestoreRejectsMissingSourceArtifact(t *testing.T) {
	_, err := planMappedRDBRestore(&BackupManifest{}, []restoreArtifactTarget{{
		sourceOrdinal: 2,
		target:        &redis_cluster.ClusterNode{PodIndex: 3, PodName: "cache-3"},
	}})
	require.ErrorContains(t, err, "no RDB artifact for source ordinal 2")
}

func TestBlueGreenSafetyBackupRequiresWaitingSyncAndBypassesSchedule(t *testing.T) {
	r := &RedisClusterReconciler{ClusterClient: redis_cluster.ClusterClient{Metrics: metrics.CreateNoop()}}
	cluster := clusterWithSpec(cachev1.RedisClusterSpec{
		BackupEnabled:   true,
		BackupFrequency: 3600,
	})
	cluster.AllNodes = []*redis_cluster.ClusterNode{{PodIndex: 0, PodName: "cache-0", IP: "10.0.0.1"}}
	cluster.RedisCluster.Status.LastBackupTime = time.Now().Unix()
	cluster.RedisCluster.Status.BlueGreen.Phase = cachev1.BlueGreenPhasePromoting

	_, err := r.backupForBlueGreen(context.Background(), cluster)
	require.ErrorContains(t, err, "requires phase")

	cluster.RedisCluster.Status.BlueGreen.Phase = cachev1.BlueGreenPhaseWaitingSync
	_, err = r.backupForBlueGreen(context.Background(), cluster)
	require.ErrorContains(t, err, "no backup bucket configured")
}

func TestBlueGreenBackupClusterExcludesGossipOnlyMembers(t *testing.T) {
	blue := &redis_cluster.ClusterNode{PodIndex: 0, PodName: "cache-0", IP: "10.0.0.1"}
	gossipOnly := &redis_cluster.ClusterNode{PodIndex: -1}
	cluster := &redis_cluster.Cluster{AllNodes: []*redis_cluster.ClusterNode{blue, gossipOnly}}

	backupCluster, err := blueGreenBackupCluster(cluster)

	require.NoError(t, err)
	require.Equal(t, []*redis_cluster.ClusterNode{blue}, backupCluster.AllNodes)
	require.Len(t, cluster.AllNodes, 2, "filtering must not mutate the reconciliation topology")
}

func TestBlueGreenBackupClusterRequiresPodBackedNode(t *testing.T) {
	_, err := blueGreenBackupCluster(&redis_cluster.Cluster{AllNodes: []*redis_cluster.ClusterNode{{PodIndex: -1}}})
	require.ErrorContains(t, err, "no pod-backed blue nodes")
}

func TestBackupBucketIsNotSanitizedIntoAnotherResource(t *testing.T) {
	for _, value := range []string{"s3://my-bucket", "gs://my-bucket", "my-bucket/", " my-bucket "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("BACKUP_BUCKET", value)
			r := &RedisClusterReconciler{}
			_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "s3"}))
			require.Error(t, err)
		})
	}
}

func TestBackupLocationUnsetReturnsEmpty(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "")
	assert.Empty(t, (&RedisClusterReconciler{}).backupLocation())
}

func TestBackupBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		kind, bucket string
		typ          any
	}{
		{"", "my-bucket", &s3Backend{}}, {"s3", "my-bucket", &s3Backend{}}, {"gcs", "my-bucket", &gcsBackend{}}, {"azure", "my-container", &azureBackend{}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Setenv("BACKUP_BUCKET", tc.bucket)
			backend, err := (&RedisClusterReconciler{}).backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: tc.kind}))
			require.NoError(t, err)
			assert.IsType(t, tc.typ, backend)
		})
	}
}

func TestBackupBackendRejectsMissingAndUnknown(t *testing.T) {
	r := &RedisClusterReconciler{}
	t.Setenv("BACKUP_BUCKET", "")
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "s3"}))
	require.Error(t, err)
	t.Setenv("BACKUP_BUCKET", "my-bucket")
	_, err = r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "nfs"}))
	require.Error(t, err)
}

func TestBackupBackendUsesInjectedFactory(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "")
	var gotProvider, gotLocation string
	r := &RedisClusterReconciler{BackupBackendFactory: BackupBackendFactoryFunc(func(provider, location string) (BackupProvider, error) {
		gotProvider, gotLocation = provider, location
		return stubBackupBackend{}, nil
	})}

	backend, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.NoError(t, err)
	assert.IsType(t, stubBackupBackend{}, backend)
	assert.Equal(t, "custom", gotProvider)
	assert.Empty(t, gotLocation)
}

func TestBackupBackendRejectsTypedNilFactory(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "provider-location")
	var factory *BackupBackendRegistry
	r := &RedisClusterReconciler{BackupBackendFactory: factory}
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.ErrorContains(t, err, "factory is nil")
}

func TestBackupBackendRejectsNilFactoryResult(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "provider-location")
	r := &RedisClusterReconciler{BackupBackendFactory: BackupBackendFactoryFunc(func(string, string) (BackupProvider, error) {
		return nil, nil
	})}
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.ErrorContains(t, err, "returned a nil backend")
}

type backupProviderFunc func(BackendCommandExecutor) BackupStore

func (f backupProviderFunc) Bind(executor BackendCommandExecutor) BackupStore {
	return f(executor)
}

func TestBindBackupStoreRejectsNilStore(t *testing.T) {
	var typedNil *promotionStore
	tests := []struct {
		name     string
		provider BackupProvider
	}{
		{
			name: "nil",
			provider: backupProviderFunc(func(BackendCommandExecutor) BackupStore {
				return nil
			}),
		},
		{
			name: "typed nil",
			provider: backupProviderFunc(func(BackendCommandExecutor) BackupStore {
				return typedNil
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := (&RedisClusterReconciler{}).bindBackupStore(
				clusterWithSpec(cachev1.RedisClusterSpec{}),
				tc.provider,
				&redis_cluster.ClusterNode{PodName: "redis-0"},
			)
			require.ErrorContains(t, err, "returned a nil store")
			assert.Nil(t, store)
		})
	}
}

func TestBackupContainerSelection(t *testing.T) {
	r := &RedisClusterReconciler{}
	withSidecar := cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis"}, {Name: "backup"}}}}}}}
	without := cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis"}}}}}}}
	assert.Equal(t, "backup", r.backupContainer(clusterWithSpec(withSidecar)))
	assert.Equal(t, "redis", r.backupContainer(clusterWithSpec(without)))
}

func TestBackupPathsDistinguishLatestAndPrevious(t *testing.T) {
	r := &RedisClusterReconciler{}
	c := &redis_cluster.Cluster{RedisCluster: &cachev1.RedisCluster{}, StatefulSet: &appsv1.StatefulSet{}}
	c.RedisCluster.Namespace = "cache-team"
	c.RedisCluster.Name = "rco-test"
	assert.Equal(t, "cache-team/rco-test/latest", r.latestBackupPath(c))
	assert.Equal(t, "cache-team/rco-test/previous", r.previousBackupPath(c))
	assert.Equal(t, "cache-team/rco-test/in-progress", r.inProgressBackupPath(c))

	otherNamespace := &redis_cluster.Cluster{RedisCluster: c.RedisCluster.DeepCopy(), StatefulSet: &appsv1.StatefulSet{}}
	otherNamespace.RedisCluster.Namespace = "other-team"
	assert.NotEqual(t, r.latestBackupPath(c), r.latestBackupPath(otherNamespace))
}

func TestSelectBackupGenerationFallsBackBeforeRestoreMutation(t *testing.T) {
	latestErr := errors.New("latest manifest is corrupt")
	validated := make([]string, 0, 2)
	selected, err := selectBackupGeneration(
		[]string{"cache/latest", "cache/previous"},
		func(string) (bool, error) { return true, nil },
		func(path string) error {
			validated = append(validated, path)
			if path == "cache/latest" {
				return latestErr
			}
			return nil
		},
	)

	require.NoError(t, err)
	assert.Equal(t, "cache/previous", selected)
	assert.Equal(t, []string{"cache/latest", "cache/previous"}, validated)
}

func TestSelectBackupGenerationReturnsEmptyOnlyWhenBothAreAbsent(t *testing.T) {
	selected, err := selectBackupGeneration(
		[]string{"cache/latest", "cache/previous"},
		func(string) (bool, error) { return false, nil },
		func(string) error { t.Fatal("validate called for absent backup"); return nil },
	)
	require.NoError(t, err)
	assert.Empty(t, selected)

	_, err = selectBackupGeneration(
		[]string{"cache/latest", "cache/previous"},
		func(string) (bool, error) { return true, nil },
		func(path string) error { return fmt.Errorf("%s corrupt", path) },
	)
	require.ErrorContains(t, err, "no valid backup generation")
}

func TestPlanBackupValidationDoesNotRequireHistoricalOrdinals(t *testing.T) {
	nodes := []*redis_cluster.ClusterNode{
		{PodName: "cache-0", PodIndex: 0},
		{PodName: "cache-1", PodIndex: 1},
	}
	artifacts := []BackupArtifact{
		{Ordinal: 0, Kind: backupArtifactRDB},
		{Ordinal: 1, Kind: backupArtifactRDB},
		{Ordinal: 6, Kind: backupArtifactRDB},
		{Ordinal: 7, Kind: backupArtifactRDB},
	}

	assignments, err := planBackupValidation(nodes, artifacts)
	require.NoError(t, err)
	require.Len(t, assignments, 2)
	assert.Equal(t, []int{0, 6}, []int{
		assignments[0][0].artifact.Ordinal,
		assignments[0][1].artifact.Ordinal,
	})
	assert.Equal(t, []int{1, 7}, []int{
		assignments[1][0].artifact.Ordinal,
		assignments[1][1].artifact.Ordinal,
	})
	assert.Same(t, nodes[0], assignments[0][1].node)
	assert.Same(t, nodes[1], assignments[1][1].node)
}

type promotionStore struct {
	calls     []string
	failures  map[string]error
	exists    bool
	existsErr error
}

func (s *promotionStore) UploadFile(context.Context, string, string) error      { return nil }
func (s *promotionStore) DownloadFile(context.Context, string, string) error    { return nil }
func (s *promotionStore) UploadBytes(context.Context, []byte, string) error     { return nil }
func (s *promotionStore) DownloadBytes(context.Context, string) ([]byte, error) { return nil, nil }
func (s *promotionStore) CopyPrefix(_ context.Context, src, dst string) error {
	key := "copy:" + src + ":" + dst
	s.calls = append(s.calls, key)
	return s.failures[key]
}
func (s *promotionStore) DeletePrefix(_ context.Context, p string) error {
	key := "delete:" + p
	s.calls = append(s.calls, key)
	return s.failures[key]
}
func (s *promotionStore) Exists(context.Context, string) (bool, error) { return s.exists, s.existsErr }

func TestReplaceLatestPropagatesPromotionAndRollbackFailures(t *testing.T) {
	promotionErr := errors.New("promotion failed")
	rollbackErr := errors.New("rollback failed")
	store := &promotionStore{failures: map[string]error{
		"copy:cluster/in-progress:cluster/latest": promotionErr,
		"copy:cluster/previous:cluster/latest":    rollbackErr,
	}}
	err := replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", true)
	require.Error(t, err)
	assert.ErrorIs(t, err, promotionErr)
	assert.Contains(t, err.Error(), "rollback failed")
	assert.NotContains(t, store.calls, "delete:cluster/in-progress")
}

func TestReplaceLatestPropagatesCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	store := &promotionStore{failures: map[string]error{"delete:cluster/in-progress": cleanupErr}}
	err := replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", false)
	require.ErrorIs(t, err, cleanupErr)
}

func TestReplaceLatestSuccessCleansInProgress(t *testing.T) {
	store := &promotionStore{failures: map[string]error{}}
	require.NoError(t, replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", false))
	assert.Contains(t, store.calls, "delete:cluster/in-progress")
}

func TestConfirmedNotFoundIsTheOnlyAbsenceCategory(t *testing.T) {
	notFound := &BackendError{Kind: BackendNotFound, Operation: "exists"}
	auth := &BackendError{Kind: BackendAuthentication, Operation: "exists"}
	assert.True(t, IsBackendNotFound(notFound))
	assert.False(t, IsBackendNotFound(auth))
}

func TestRotateLatestFirstBackupDoesNotCopyAbsentLatest(t *testing.T) {
	store := &promotionStore{failures: map[string]error{}, exists: false}
	rotated, err := rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.NoError(t, err)
	assert.False(t, rotated)
	assert.Empty(t, store.calls)
}

func TestRotateLatestPropagatesProbeAndRotationFailures(t *testing.T) {
	authErr := &BackendError{Kind: BackendAuthentication, Operation: "exists"}
	store := &promotionStore{failures: map[string]error{}, existsErr: authErr}
	_, err := rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.ErrorIs(t, err, authErr)
	assert.Empty(t, store.calls)

	copyErr := errors.New("rotation copy failed")
	store = &promotionStore{exists: true, failures: map[string]error{"copy:cluster/latest:cluster/previous": copyErr}}
	_, err = rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.ErrorIs(t, err, copyErr)
}

func TestReplaceLatestFirstBackupClearsPartialCopy(t *testing.T) {
	promotionErr := errors.New("copy interrupted")
	store := &promotionStore{failures: map[string]error{"copy:cluster/in-progress:cluster/latest": promotionErr}}
	err := replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", false)
	require.ErrorIs(t, err, promotionErr)
	assert.Equal(t, []string{
		"delete:cluster/latest",
		"copy:cluster/in-progress:cluster/latest",
		"delete:cluster/latest",
	}, store.calls, "partial latest must be cleared when there is nothing to roll back to")
	assert.NotContains(t, store.calls, "delete:cluster/in-progress")
}

// objectStore is an in-memory BackupStore. When failCopyTo is set, a copy into
// that prefix writes one object and then fails, like an interrupted
// `aws s3 cp --recursive`.
type objectStore struct {
	objects      map[string][]byte
	failCopyTo   string
	downloadErrs map[string]error
}

func (s *objectStore) UploadFile(context.Context, string, string) error   { return nil }
func (s *objectStore) DownloadFile(context.Context, string, string) error { return nil }
func (s *objectStore) UploadBytes(_ context.Context, data []byte, key string) error {
	s.objects[key] = data
	return nil
}
func (s *objectStore) DownloadBytes(_ context.Context, key string) ([]byte, error) {
	if err := s.downloadErrs[key]; err != nil {
		return nil, err
	}
	data, ok := s.objects[key]
	if !ok {
		return nil, &BackendError{Kind: BackendNotFound, Operation: "download"}
	}
	return data, nil
}
func (s *objectStore) CopyPrefix(_ context.Context, src, dst string) error {
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, src+"/") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for i, key := range keys {
		if dst == s.failCopyTo && i == 1 {
			return &BackendError{Kind: BackendNetwork, Operation: "copy"}
		}
		s.objects[dst+"/"+strings.TrimPrefix(key, src+"/")] = s.objects[key]
	}
	return nil
}
func (s *objectStore) DeletePrefix(_ context.Context, prefix string) error {
	for key := range s.objects {
		if strings.HasPrefix(key, prefix+"/") {
			delete(s.objects, key)
		}
	}
	return nil
}
func (s *objectStore) Exists(_ context.Context, prefix string) (bool, error) {
	for key := range s.objects {
		if strings.HasPrefix(key, prefix+"/") {
			return true, nil
		}
	}
	return false, nil
}

func newObjectStoreReconciler(t *testing.T, store *objectStore) (*RedisClusterReconciler, *redis_cluster.Cluster, BackupProvider) {
	t.Helper()
	r := &RedisClusterReconciler{
		ClusterClient: redis_cluster.ClusterClient{Log: logr.Discard()},
		BackupBackendFactory: BackupBackendFactoryFunc(func(string, string) (BackupProvider, error) {
			return backupProviderFunc(func(BackendCommandExecutor) BackupStore { return store }), nil
		}),
	}
	cluster := &redis_cluster.Cluster{
		RedisCluster: &cachev1.RedisCluster{},
		AllNodes:     []*redis_cluster.ClusterNode{{PodName: "cache-0", PodIndex: 0}},
	}
	cluster.RedisCluster.Namespace, cluster.RedisCluster.Name = "ns", "cache"
	backend, err := r.backupBackend(cluster)
	require.NoError(t, err)
	return r, cluster, backend
}

// An interrupted first promotion used to leave a partial latest behind. Every
// later promotion then refused to run, so no backup was ever promoted again.
func TestPromoteRecoversAfterInterruptedFirstPromotion(t *testing.T) {
	store := &objectStore{objects: map[string][]byte{}}
	r, cluster, backend := newObjectStoreReconciler(t, store)
	ctx := context.Background()
	inProgress, latest, previous := r.inProgressBackupPath(cluster), r.latestBackupPath(cluster), r.previousBackupPath(cluster)
	stage := func(generation string) {
		store.objects[inProgress+"/0/dump.rdb"] = []byte("rdb-" + generation)
		store.objects[inProgress+"/0/nodes.conf"] = []byte("nodes-" + generation)
		store.objects[inProgress+"/manifest.json"] = []byte("{}")
	}

	stage("1")
	store.failCopyTo = latest
	require.Error(t, r.promoteInProgressBackup(ctx, cluster, backend, inProgress, latest, previous))
	exists, err := store.Exists(ctx, latest)
	require.NoError(t, err)
	assert.False(t, exists, "partial latest must not be left behind")

	store.failCopyTo = ""
	require.NoError(t, r.clearInProgressBackup(ctx, cluster, backend, inProgress))
	stage("2")
	require.NoError(t, r.promoteInProgressBackup(ctx, cluster, backend, inProgress, latest, previous))
	assert.Equal(t, []byte("rdb-2"), store.objects[latest+"/0/dump.rdb"])
}

// An invalid latest (e.g. partial copy from an older operator, or an expired
// object) must be replaced by the next complete backup, without being rotated
// over a good previous.
func TestPromoteReplacesInvalidLatestWithoutTouchingPrevious(t *testing.T) {
	store := &objectStore{objects: map[string][]byte{}}
	r, cluster, backend := newObjectStoreReconciler(t, store)
	ctx := context.Background()
	inProgress, latest, previous := r.inProgressBackupPath(cluster), r.latestBackupPath(cluster), r.previousBackupPath(cluster)

	store.objects[previous+"/0/dump.rdb"] = []byte("rdb-good-previous")
	store.objects[latest+"/0/dump.rdb"] = []byte("rdb-partial") // no manifest: invalid
	store.objects[inProgress+"/0/dump.rdb"] = []byte("rdb-new")
	store.objects[inProgress+"/manifest.json"] = []byte("{}")

	require.NoError(t, r.promoteInProgressBackup(ctx, cluster, backend, inProgress, latest, previous))
	assert.Equal(t, []byte("rdb-new"), store.objects[latest+"/0/dump.rdb"])
	assert.Equal(t, []byte("rdb-good-previous"), store.objects[previous+"/0/dump.rdb"], "invalid latest must not overwrite previous")
	exists, err := store.Exists(ctx, inProgress)
	require.NoError(t, err)
	assert.False(t, exists)
}

// A latest whose manifest cannot be parsed is invalid in the same way as one
// with no manifest, and is replaced without touching previous.
func TestPromoteReplacesLatestWithMalformedManifest(t *testing.T) {
	store := &objectStore{objects: map[string][]byte{}}
	r, cluster, backend := newObjectStoreReconciler(t, store)
	ctx := context.Background()
	inProgress, latest, previous := r.inProgressBackupPath(cluster), r.latestBackupPath(cluster), r.previousBackupPath(cluster)

	store.objects[previous+"/0/dump.rdb"] = []byte("rdb-good-previous")
	store.objects[latest+"/0/dump.rdb"] = []byte("rdb-corrupt")
	store.objects[latest+"/manifest.json"] = []byte("not json")
	store.objects[inProgress+"/0/dump.rdb"] = []byte("rdb-new")
	store.objects[inProgress+"/manifest.json"] = []byte("{}")

	require.NoError(t, r.promoteInProgressBackup(ctx, cluster, backend, inProgress, latest, previous))
	assert.Equal(t, []byte("rdb-new"), store.objects[latest+"/0/dump.rdb"])
	assert.Equal(t, []byte("rdb-good-previous"), store.objects[previous+"/0/dump.rdb"])
}

// A transport error while reading latest's manifest says nothing about
// whether latest is valid. Promotion must refuse and leave latest, previous
// and the staged backup exactly as they were.
func TestPromoteRefusesWhenLatestCannotBeInspected(t *testing.T) {
	store := &objectStore{objects: map[string][]byte{}}
	r, cluster, backend := newObjectStoreReconciler(t, store)
	ctx := context.Background()
	inProgress, latest, previous := r.inProgressBackupPath(cluster), r.latestBackupPath(cluster), r.previousBackupPath(cluster)

	store.objects[previous+"/0/dump.rdb"] = []byte("rdb-previous")
	store.objects[latest+"/0/dump.rdb"] = []byte("rdb-latest")
	store.objects[latest+"/manifest.json"] = []byte("{}")
	store.objects[inProgress+"/0/dump.rdb"] = []byte("rdb-new")
	store.objects[inProgress+"/manifest.json"] = []byte("{}")
	store.downloadErrs = map[string]error{
		latest + "/manifest.json": &BackendError{Kind: BackendNetwork, Operation: "download"},
	}

	err := r.promoteInProgressBackup(ctx, cluster, backend, inProgress, latest, previous)
	require.ErrorContains(t, err, "refusing to replace previous with invalid latest backup")
	assert.Equal(t, []byte("rdb-latest"), store.objects[latest+"/0/dump.rdb"], "latest must not be deleted")
	assert.Equal(t, []byte("rdb-previous"), store.objects[previous+"/0/dump.rdb"], "previous must not be overwritten")
	assert.Equal(t, []byte("rdb-new"), store.objects[inProgress+"/0/dump.rdb"], "staged backup must be retained")
}

func TestIsBackupGenerationInvalid(t *testing.T) {
	notFound := &BackendError{Kind: BackendNotFound, Operation: "download"}
	network := &BackendError{Kind: BackendNetwork, Operation: "download"}
	badManifest := &invalidBackupManifestError{err: errors.New("failed to parse manifest: EOF")}

	cases := map[string]struct {
		err     error
		invalid bool
	}{
		"manifest not found":    {fmt.Errorf("failed to download manifest: %w", notFound), true},
		"artifact not found":    {fmt.Errorf("backup generation validation failed: %w", fmt.Errorf("failed to stage artifact: %w", notFound)), true},
		"malformed manifest":    {badManifest, true},
		"network error":         {fmt.Errorf("failed to download manifest: %w", network), false},
		"toolbox or exec error": {errors.New("failed to inspect artifact"), false},
		"context deadline":      {context.DeadlineExceeded, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.invalid, isBackupGenerationInvalid(tc.err))
		})
	}
}
