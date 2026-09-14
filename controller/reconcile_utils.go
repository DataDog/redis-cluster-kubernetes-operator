package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// getChartVersions gets and compares the RedisCluster and StatefulSet chart versions.
func getChartVersions(redisCluster *cachev1.RedisCluster, statefulSet *appsv1.StatefulSet, log logr.Logger) (string, string, error) {
	clusterChartVersion, ok := redisCluster.Labels["chart_version"]
	if !ok {
		return "", "", fmt.Errorf("failed to read chart_version label from the custom resource %s/%s", redisCluster.Namespace, redisCluster.Name)
	}
	statefulSetChartVersion, ok := statefulSet.Labels["chart_version"]
	if !ok {
		return "", "", fmt.Errorf("failed to read chart_version label from the statefulset %s/%s", redisCluster.Namespace, redisCluster.Name)
	}
	if clusterChartVersion == statefulSetChartVersion {
		log.Info("Chart versions are matching", "redisCluster", clusterChartVersion, "statefulSet", statefulSetChartVersion)
	} else {
		log.Info("Chart versions are not matching", "redisCluster", clusterChartVersion, "statefulSet", statefulSetChartVersion)
	}

	return clusterChartVersion, statefulSetChartVersion, nil
}

// detectSpecChanges compares rollout-relevant desired state with the active
// StatefulSet baseline. Topology is handled separately by handleScaling.
func (r *RedisClusterReconciler) detectSpecChanges(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	if cluster.StatefulSet == nil {
		r.Log.Info("detectSpecChanges: StatefulSet is nil, skipping")
		return false, nil
	}

	desiredHash := computeRolloutSpecHash(cluster)
	currentHash := cluster.RedisCluster.Status.LastAppliedRolloutSpecHash

	if currentHash == "" {
		// Migrate from the legacy full-spec hash using what is actually running.
		// Initializing from the desired CR would hide a simultaneous scale + image
		// or config change because scaling only updates StatefulSet replicas.
		currentHash = computeRolloutSpecHashForTemplate(cluster.StatefulSet.Spec.Template)
		r.Log.Info("detectSpecChanges: initializing rollout hash from active StatefulSet",
			"hash", truncateChecksum(currentHash))
		if err := r.updateRedisClusterStatusWithRetry(ctx, cluster.RedisCluster,
			func(status *cachev1.RedisClusterStatus) {
				status.LastAppliedRolloutSpecHash = currentHash
			}); err != nil {
			return false, utils.WrapErr(err, "failed to initialize rollout spec hash")
		}
	}

	if desiredHash != currentHash {
		r.Log.Info("Spec changes detected, rollout needed",
			"currentHash", truncateChecksum(currentHash),
			"desiredHash", truncateChecksum(desiredHash))
		r.logSpecDiff(cluster)
		return true, nil
	}
	return false, nil
}

// stampSpecHash updates both compatibility and rollout hashes when the active
// StatefulSet matches the CR.
func (r *RedisClusterReconciler) stampSpecHash(ctx context.Context, cluster *redis_cluster.Cluster) error {
	fullHash := computeSpecHash(cluster)
	rolloutHash := computeRolloutSpecHash(cluster)
	if cluster.RedisCluster.Status.LastAppliedSpecHash == fullHash &&
		cluster.RedisCluster.Status.LastAppliedRolloutSpecHash == rolloutHash {
		return nil
	}

	r.Log.Info("Stamping spec hashes",
		"oldHash", truncateChecksum(cluster.RedisCluster.Status.LastAppliedSpecHash),
		"newHash", truncateChecksum(fullHash),
		"oldRolloutHash", truncateChecksum(cluster.RedisCluster.Status.LastAppliedRolloutSpecHash),
		"newRolloutHash", truncateChecksum(rolloutHash))
	return r.updateRedisClusterStatusWithRetry(ctx, cluster.RedisCluster,
		func(status *cachev1.RedisClusterStatus) {
			status.LastAppliedSpecHash = fullHash
			status.LastAppliedRolloutSpecHash = rolloutHash
		})
}

// stampScalingSpecHash acknowledges only the topology checkpoint. A combined
// topology + rollout change must retain the old rollout hash so blue/green runs
// after scaling converges.
func (r *RedisClusterReconciler) stampScalingSpecHash(ctx context.Context, cluster *redis_cluster.Cluster) error {
	fullHash := computeSpecHash(cluster)
	if cluster.RedisCluster.Status.LastAppliedSpecHash == fullHash {
		return nil
	}

	r.Log.Info("Stamping scaling spec hash",
		"oldHash", truncateChecksum(cluster.RedisCluster.Status.LastAppliedSpecHash),
		"newHash", truncateChecksum(fullHash))
	return r.updateRedisClusterStatusWithRetry(ctx, cluster.RedisCluster,
		func(status *cachev1.RedisClusterStatus) {
			status.LastAppliedSpecHash = fullHash
		})
}

// computeSpecHash hashes the CR spec fields that we control and that require
// a BG rollout. Avoids K8s-defaulted fields that change between API server reads.
func computeSpecHash(cluster *redis_cluster.Cluster) string {
	spec := cluster.RedisCluster.Spec

	h := struct {
		Primaries       int32             `json:"p"`
		Replicas        int32             `json:"r"`
		BackupEnabled   bool              `json:"be"`
		BackupFrequency int64             `json:"bf"`
		BackupRetention int32             `json:"br"`
		Annotations     map[string]string `json:"a"`
		Labels          map[string]string `json:"l"`
		Images          []string          `json:"i"`
	}{
		Primaries:       spec.Primaries,
		Replicas:        spec.Replicas,
		BackupEnabled:   spec.BackupEnabled,
		BackupFrequency: spec.BackupFrequency,
		BackupRetention: spec.BackupRetention,
		Annotations:     spec.StatefulsetTemplate.Spec.Template.Annotations,
		Labels:          spec.StatefulsetTemplate.Spec.Template.Labels,
	}

	podSpec := spec.StatefulsetTemplate.Spec.Template.Spec
	for _, c := range podSpec.InitContainers {
		h.Images = append(h.Images, c.Name+"="+c.Image)
	}
	for _, c := range podSpec.Containers {
		h.Images = append(h.Images, c.Name+"="+c.Image)
	}

	data, _ := json.Marshal(h)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

// computeRolloutSpecHash tracks only the pod template. Topology is applied by
// handleScaling, while backup controls are consumed live from the CR; neither
// requires replacing Redis pods.
func computeRolloutSpecHash(cluster *redis_cluster.Cluster) string {
	return computeRolloutSpecHashForTemplate(cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.Template)
}

func computeRolloutSpecHashForTemplate(template corev1.PodTemplateSpec) string {
	labels := make(map[string]string, len(template.Labels))
	for key, value := range template.Labels {
		if key == constants.BlueGreenRoleLabel || key == constants.RedisClusterReadyPodLabel {
			continue
		}
		labels[key] = value
	}

	h := struct {
		Annotations map[string]string `json:"a"`
		Labels      map[string]string `json:"l"`
		Images      []string          `json:"i"`
	}{
		Annotations: template.Annotations,
		Labels:      labels,
	}

	for _, c := range template.Spec.InitContainers {
		h.Images = append(h.Images, c.Name+"="+c.Image)
	}
	for _, c := range template.Spec.Containers {
		h.Images = append(h.Images, c.Name+"="+c.Image)
	}

	data, _ := json.Marshal(h)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum)
}

// truncateChecksum returns first 8 chars of checksum for logging (or "empty" if empty)
func truncateChecksum(checksum string) string {
	if checksum == "" {
		return "empty"
	}
	if len(checksum) > 8 {
		return checksum[:8]
	}
	return checksum
}

// logSpecDiff logs which specific fields differ in the spec to help diagnose
// unexpected BG triggers (e.g. after operator redeployment).
func (r *RedisClusterReconciler) logSpecDiff(cluster *redis_cluster.Cluster) {
	spec := cluster.RedisCluster.Spec
	podSpec := spec.StatefulsetTemplate.Spec.Template.Spec

	var images []string
	for _, c := range podSpec.InitContainers {
		images = append(images, c.Name+"="+c.Image)
	}
	for _, c := range podSpec.Containers {
		images = append(images, c.Name+"="+c.Image)
	}

	r.Log.Info("Spec diff details",
		"primaries", spec.Primaries,
		"replicas", spec.Replicas,
		"backupEnabled", spec.BackupEnabled,
		"backupFrequency", spec.BackupFrequency,
		"backupRetention", spec.BackupRetention,
		"annotations", spec.StatefulsetTemplate.Spec.Template.Annotations,
		"labels", spec.StatefulsetTemplate.Spec.Template.Labels,
		"images", images)
}

// updateRedisClusterStatusWithRetry updates the RedisCluster status with a read-modify-write
// retry loop. On conflict, it re-reads the latest CR and re-applies the mutation, so no
// stale fields are sent to the API server.
func (r *RedisClusterReconciler) updateRedisClusterStatusWithRetry(ctx context.Context, redisCluster *cachev1.RedisCluster, mutate func(*cachev1.RedisClusterStatus)) error {
	isRetryable := func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err)
	}

	updateFn := func() error {
		rc := &cachev1.RedisCluster{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}, rc); err != nil {
			return err
		}
		mutate(&rc.Status)
		// The preserved v1 CRD requires status.nodes to be an array. A partial
		// subsystem status update can run before topology discovery, so encode an
		// empty array instead of null during bootstrap.
		if rc.Status.Nodes == nil {
			rc.Status.Nodes = []cachev1.RedisClusterNode{}
		}
		err := r.Status().Update(ctx, rc)
		if err == nil {
			redisCluster.ResourceVersion = rc.ResourceVersion
			redisCluster.Status = rc.Status
		}
		return err
	}

	return utils.RetryWithBackoff(updateFn, isRetryable, 3, time.Second, r.Log)
}

// updateRedisCluster updates the RedisCluster resource with retry logic for transient errors.
func (r *RedisClusterReconciler) updateRedisCluster(ctx context.Context, redisCluster *cachev1.RedisCluster) error {
	desired := redisCluster.Status.DeepCopy()
	return r.updateRedisClusterStatusWithRetry(ctx, redisCluster, func(s *cachev1.RedisClusterStatus) {
		*s = *desired
	})
}

// updateRedisClusterSpec persists Spec + annotations (not Status) with conflict retry —
// used for STS-name rewrites and clearing blue/green trigger annotations.
func (r *RedisClusterReconciler) updateRedisClusterSpec(ctx context.Context, redisCluster *cachev1.RedisCluster) error {
	isRetryable := func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err)
	}

	desiredSpec := redisCluster.Spec.DeepCopy()

	updateFn := func() error {
		rc := &cachev1.RedisCluster{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}, rc); err != nil {
			return err
		}
		rc.Spec = *desiredSpec
		// Clear the BG trigger annotations (metadata, not spec) on the freshly-fetched
		// object. Deleting only these keys — instead of overwriting the whole annotation
		// map with a pre-retry snapshot — preserves annotations written concurrently
		// (e.g. by Helm) during the conflict-retry window.
		delete(rc.Annotations, constants.BlueGreenForceCutover)
		delete(rc.Annotations, constants.BlueGreenForceReset)
		err := r.Update(ctx, rc)
		if err == nil {
			*redisCluster = *rc
		}
		return err
	}

	return utils.RetryWithBackoff(updateFn, isRetryable, 3, time.Second, r.Log)
}

// shouldReconcile determines whether the cluster should be reconciled.
func (r *RedisClusterReconciler) shouldReconcile(ctx context.Context, redisCluster *cachev1.RedisCluster, counting bool) bool {

	disableReconciliation := redisCluster.Spec.DisableReconciliation

	// Abort if reconciliation is not enabled for this cluster
	if disableReconciliation {
		r.Log.Info("Reconciliation is disabled for this cluster")
		return false
	}

	// Count reconciliations attempts
	if counting {
		if redisCluster.Annotations == nil {
			redisCluster.Annotations = make(map[string]string)
		}
		reconciliationCount, err := strconv.Atoi(redisCluster.Annotations["operator/reconcile_count"])
		if err != nil {
			reconciliationCount = 0
		}
		reconciliationCount++
		redisCluster.Annotations["operator/reconcile_count"] = strconv.Itoa(reconciliationCount)
	}

	return true
}

func resetReconcileCount(redisCluster *cachev1.RedisCluster) {
	if redisCluster.Annotations == nil {
		redisCluster.Annotations = make(map[string]string)
	}
	redisCluster.Annotations["operator/reconcile_count"] = "0"
}

// downShard reorganizes the nodes so that the nodes that need to be terminated can be terminated without breaking the cluster.
func (r *RedisClusterReconciler) downShard(ctx context.Context, cluster *redis_cluster.Cluster) error {

	// note: the cluster is assumed to be OK ie have only PrimaryNode or ReplicaNode.

	spec := cluster.RedisCluster.Spec

	totalPodCount := len(cluster.Nodes)
	targetPodCount := int(cluster.RedisCluster.EffectivePodCount())
	targetPrimaryCount := int(cluster.RedisCluster.EffectivePrimaries())
	targetReplicaCount := int(spec.Replicas)
	greenPrimaryCount := countGreenPrimaries(cluster, targetPodCount)

	r.Log.Info("Down-shard cluster",
		"targetPrimaries", targetPrimaryCount,
		"targetReplicas", targetReplicaCount,
		"targetTotal", targetPodCount,
		"currentTotal", totalPodCount,
		"greenPrimaries", greenPrimaryCount,
	)

	// adjust the number of green primaries
	for greenPrimaryCount != targetPrimaryCount {

		// find what to failover
		var replicaNode *redis_cluster.ClusterNode
		var primaryNode *redis_cluster.ClusterNode
		if greenPrimaryCount > targetPrimaryCount {
			// too many green primaries: find a red replica replicating a green primary and failover
			var found bool
			found, replicaNode, primaryNode = getReplicaPrimaryPair(cluster, targetPodCount, false, true)
			if !found {
				return fmt.Errorf("failed to find a replica to failover") // should never happen?
			}
			r.Log.Info("Down-shard cluster: too many green primaries, failover.", "redReplica", replicaNode.PodName, "greenPrimary", primaryNode.PodName)
		} else {
			// not enough green primaries: find a green replica replicating a red primary and failover
			var found bool
			found, replicaNode, primaryNode = getReplicaPrimaryPair(cluster, targetPodCount, true, false)
			if !found {
				return fmt.Errorf("failed to find a replica to failover") // should never happen?
			}
			r.Log.Info("Down-shard cluster: not enough green primaries, failover.", "greenReplica", replicaNode.PodName, "redPrimary", primaryNode.PodName)
		}

		// failover
		err := cluster.Toolbox.Failover(replicaNode, false, false)
		if err != nil {
			return utils.WrapErr(err, "failed to failover")
		}

		// observe failover
		var success bool
		cluster, success = r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
			return rolesHaveBeenSwapped(c, primaryNode, replicaNode)
		}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)
		if !success {
			return fmt.Errorf("failed to observe the failover after %v", cluster.Timeouts.OperationTimeout)
		}

		// recount
		greenPrimaryCount = countGreenPrimaries(cluster, targetPodCount)
	}

	// ensure that green primaries have an OK number of green replicas
	// Build green replica map: primaryID → green replicas (O(n) instead of O(n²))
	greenReplicaMap := make(map[string][]*redis_cluster.ClusterNode)
	for _, replicaNode := range cluster.Nodes[redis_cluster.ReplicaNode] {
		if !replicaNode.IsRed(targetPodCount) {
			greenReplicaMap[replicaNode.PrimaryID] = append(greenReplicaMap[replicaNode.PrimaryID], replicaNode)
		}
	}

	// Find free replicas (green primaries with more than needed)
	freeReplicas := make([]*redis_cluster.ClusterNode, 0)
	for _, primaryNode := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primaryNode.IsRed(targetPodCount) {
			continue
		}
		replicas := greenReplicaMap[primaryNode.ID]
		if len(replicas) > targetReplicaCount {
			freeReplicas = append(freeReplicas, replicas[targetReplicaCount:]...)
		}
	}
	// Reassign non-draining replicas first; draining ones only as a fallback.
	freeReplicas = orderReplicasNonDrainingFirst(freeReplicas)

	// Assign free replicas to primaries that need them
	for _, primaryNode := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primaryNode.IsRed(targetPodCount) {
			continue
		}
		greenReplicaCount := len(greenReplicaMap[primaryNode.ID])
		for greenReplicaCount < targetReplicaCount {
			if len(freeReplicas) == 0 {
				r.Log.Info("No free replicas available for primary", "primaryNode", primaryNode.PodName)
				break
			}
			err := cluster.Toolbox.Replicates(freeReplicas[0], primaryNode)
			if err != nil {
				return err
			}
			freeReplicas = freeReplicas[1:]
			greenReplicaCount++
		}
	}

	// rebalance slots away from red primaries
	weights := make([]redis_cluster.NodeWeight, 0)
	for _, primaryNode := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primaryNode.IsRed(targetPodCount) {
			weights = append(weights, redis_cluster.NodeWeight{NodeID: primaryNode.ID, Weight: 0})
		}
	}
	if len(weights) > 0 {
		err := cluster.Toolbox.RebalanceWeighted(cluster, weights, false)
		if err != nil {
			return utils.WrapErr(err, "failed to rebalance")
		}
	}

	r.deactivatePods(ctx, cluster, int(targetPodCount))

	// and then nicely remove red primaries from the cluster (batch)
	for _, primaryNode := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primaryNode.IsRed(targetPodCount) {
			r.Log.Info("Removing red primary from cluster", "nodeName", primaryNode.PodName)
			success, failedIPs, forgetErr := cluster.Toolbox.ForgetNodeFromAll(cluster, primaryNode)
			if forgetErr != nil {
				return utils.WrapErr(forgetErr, "failed to forget red primary")
			}
			if len(failedIPs) > 0 {
				r.Log.Error(nil, "Some nodes failed to forget red primary",
					"nodeName", primaryNode.PodName,
					"success", success,
					"failedIPs", failedIPs)
			}
		}
	}

	// no need to return the updated cluster - after sharding down, reconciliation ends

	return nil
}

// countGreenPrimaries counts the primaries that will be retained after downscaling.
func countGreenPrimaries(cluster *redis_cluster.Cluster, targetPodCount int) int {
	count := 0
	for _, primaryNode := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primaryNode.IsGreen(targetPodCount) {
			count++
		}
	}
	return count
}

// rolesHaveBeenSwapped determines whether a primary has become a replica, and a replica has become a primary.
func rolesHaveBeenSwapped(cluster *redis_cluster.Cluster, primaryNode *redis_cluster.ClusterNode, replicaNode *redis_cluster.ClusterNode) bool {
	refreshedPrimaryNode, foundPrimary := cluster.LookupNodeByID(primaryNode.ID)
	refreshedReplicaNode, foundReplica := cluster.LookupNodeByID(replicaNode.ID)
	return foundPrimary && refreshedPrimaryNode.RoleIs(redis_cluster.ReplicaNode) && foundReplica && refreshedReplicaNode.RoleIs(redis_cluster.PrimaryNode)
}

// getReplicaPrimaryPair returns a replica/primary pair satisfying the constraints.
// Two-pass: non-draining replicas first, any replica as fallback.
func getReplicaPrimaryPair(cluster *redis_cluster.Cluster, targetPodCount int, replicaIsGreen bool, primaryIsGreen bool) (bool, *redis_cluster.ClusterNode, *redis_cluster.ClusterNode) {
	if found, r, p := findReplicaPrimaryPair(cluster, targetPodCount, replicaIsGreen, primaryIsGreen, true); found {
		return true, r, p
	}
	return findReplicaPrimaryPair(cluster, targetPodCount, replicaIsGreen, primaryIsGreen, false)
}

func findReplicaPrimaryPair(cluster *redis_cluster.Cluster, targetPodCount int, replicaIsGreen bool, primaryIsGreen bool, requireNonDraining bool) (bool, *redis_cluster.ClusterNode, *redis_cluster.ClusterNode) {
	for _, replicaNode := range cluster.Nodes[redis_cluster.ReplicaNode] {
		if requireNonDraining && replicaNode.IsDrainCandidate() {
			continue
		}
		if rGreen := replicaNode.PodIndex < targetPodCount; rGreen != replicaIsGreen {
			continue
		}
		primaryNode, found := cluster.LookupNodeByID(replicaNode.PrimaryID)
		if !found {
			continue // should never happen?
		}
		if pGreen := primaryNode.PodIndex < targetPodCount; pGreen != primaryIsGreen {
			continue
		}
		return true, replicaNode, primaryNode
	}
	return false, nil, nil
}

// retryRefreshClusterUntil refreshes the cluster and tests it until the test passes or the timeout is reached.
func (r *RedisClusterReconciler) retryRefreshClusterUntil(ctx context.Context, cluster *redis_cluster.Cluster, test func(c *redis_cluster.Cluster) bool, wait time.Duration, timeout time.Duration) (*redis_cluster.Cluster, bool) {
	start := time.Now()
	for {
		c, err := r.refreshCluster(ctx, cluster)
		if err != nil {
			r.Log.Error(err, "Failed to refresh the cluster")
			return cluster, false
		}
		if test(c) {
			return c, true
		}
		if time.Since(start) > timeout {
			return c, false
		}
		r.Log.Info("Condition not met, will retry")
		time.Sleep(wait)
	}
}

// refreshCluster reloads the cluster.
func (r *RedisClusterReconciler) refreshCluster(ctx context.Context, cluster *redis_cluster.Cluster) (*redis_cluster.Cluster, error) {

	r.Log.Info("Refresh cluster")

	// Get the RedisCluster resource, abort if not found
	redisCluster := &cachev1.RedisCluster{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: cluster.RedisCluster.Name}, redisCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			err = fmt.Errorf("RedisCluster resource not found")
			r.Log.Error(err, "Failed to refresh cluster")
			return nil, utils.WrapErr(err, "failed to refresh cluster")
		}
		// failed to get
		r.Log.Error(err, "Failed to get the RedisCluster resource")
		_ = r.Metrics.CountBadSituation("could_not_get_cluster")
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}

	// ensure the StatefulSet is still ready
	statefulSetIsReady := redisCluster.Status.StatefulSetReady
	if !statefulSetIsReady {
		err = fmt.Errorf("statefulSet is not ready")
		r.Log.Error(err, "failed to refresh cluster", "statefulSetReady", statefulSetIsReady)
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}

	// ensure reconciliation is still enabled
	shouldReconcile := r.shouldReconcile(ctx, redisCluster, false)
	if !shouldReconcile {
		err = fmt.Errorf("reconciliation is disabled")
		r.Log.Error(err, "Failed to refresh cluster")
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}

	// Get the StatefulSet resource
	statefulSet, err := r.GetStatefulSet(ctx, redisCluster)
	if err != nil {
		err = utils.WrapErr(err, "failed to get the StatefulSet")
		r.Log.Error(err, "Failed to refresh cluster")
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}

	// Verify that chart versions have not changed
	clusterChartVersion, statefulSetChartVersion, err := getChartVersions(redisCluster, statefulSet, r.Log)
	if err != nil {
		err = utils.WrapErr(err, "failed to get the chart versions")
		r.Log.Error(err, "Failed to refresh cluster")
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}
	if clusterChartVersion != redisCluster.Labels["chart_version"] || statefulSetChartVersion != statefulSet.Labels["chart_version"] {
		err = fmt.Errorf("chart versions have changed")
		r.Log.Error(err, "Failed to refresh cluster")
		return nil, utils.WrapErr(err, "failed to refresh cluster")
	}

	// Get the cluster
	cluster, err = r.GetCluster(ctx, redisCluster, statefulSet)
	if err != nil {
		err = utils.WrapErr(err, "failed to get the cluster's nodes")
		r.Log.Error(err, "Failed to refresh the cluster")
		return nil, utils.WrapErr(err, "failed to refresh the cluster")
	}
	r.Log.Info("Refreshed cluster", "nodes", cluster.AllNodes)

	return cluster, nil
}

// readyClusterPods sets the ready label on pods depending on the status of the corresponding node.
// It returns the names of pods whose labels were updated successfully.
func (r *RedisClusterReconciler) readyClusterPods(ctx context.Context, cluster *redis_cluster.Cluster) (map[string]struct{}, error) {
	updatedPods := make(map[string]struct{})
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to ready-label pods")
		return nil, utils.WrapErr(err, "failed to ready-label pods")
	}
	for _, pod := range podList.Items {
		node, found := cluster.LookupNodeByIP(pod.Status.PodIP)
		nodeIsReady := found && (node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode))
		value, found := pod.Labels[constants.RedisClusterReadyPodLabel]
		hasReadyLabel := found && value == "true"
		hasActiveLabel := constants.IsRoleActive(pod.Labels)
		labelsCorrect := hasReadyLabel == nodeIsReady && hasActiveLabel == nodeIsReady
		if labelsCorrect {
			continue
		}

		if nodeIsReady {
			constants.ActivateRedisCluster(pod.Labels)
			if !constants.IsRoleActive(pod.Labels) {
				constants.ActivateRole(pod.Labels)
			}
		} else {
			constants.DeactivateRedisCluster(pod.Labels)
			constants.DeactivateRole(pod.Labels)
		}

		if err := r.Update(ctx, &pod); err != nil {
			r.Log.Error(err, "Failed to ready-label pod, skip", "podName", pod.Name)
		} else {
			updatedPods[pod.Name] = struct{}{}
			r.Log.Info("Ready-labeled a pod", "podName", pod.Name, "ready", nodeIsReady)
		}
	}
	return updatedPods, nil
}

// deactivatePods removes role=active from pods with ordinal >= minOrdinal,
// taking them out of the service before they are removed from the cluster.
func (r *RedisClusterReconciler) deactivatePods(ctx context.Context, cluster *redis_cluster.Cluster, minOrdinal int) {
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to list pods for deactivation")
		return
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		parts := strings.Split(pod.Name, "-")
		if len(parts) == 0 {
			continue
		}
		ordinal, parseErr := strconv.Atoi(parts[len(parts)-1])
		if parseErr != nil {
			continue
		}
		if ordinal >= minOrdinal && constants.IsRoleActive(pod.Labels) {
			constants.DeactivateRole(pod.Labels)
			if err := r.Update(ctx, pod); err != nil {
				r.Log.Error(err, "Failed to deactivate pod", "pod", pod.Name)
			} else {
				r.Log.Info("Deactivated pod (removed role=active)", "pod", pod.Name)
			}
		}
	}
}

// readyClusterPods sets the ready label on all pods.
func (r *RedisClusterReconciler) readyClusterAllPods(ctx context.Context, cluster *redis_cluster.Cluster) error {
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to ready-label pods")
		return utils.WrapErr(err, "failed to ready-label pods")
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
			constants.ActivateRedisCluster(pod.Labels)
			constants.ActivateRole(pod.Labels)
			if err := r.Update(ctx, &pod); err != nil {
				r.Log.Error(err, "Failed to ready-label pod, skip", "podName", pod.Name)
			} else {
				r.Log.Info("Ready-labeled a pod", "podName", pod.Name, "ready", "true")
			}
		}
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Scaling (up/down)
// ────────────────────────────────────────────────────────────────────────────

// handleScaling handles scale up/down when pod count doesn't match spec.
// This is separate from blue-green deployment (which handles same-size rollouts).
// After updating the StatefulSet, maintain.go handles CLUSTER MEET, addPrimaries, and rebalance.
func (r *RedisClusterReconciler) handleScaling(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	targetPodCount int32,
	currentPodCount int32,
) (ctrl.Result, health.ClusterHealth, error) {
	log := r.Log.WithName("scaling")

	// Don't scale during BG deployment
	bgPhase := cluster.RedisCluster.Status.BlueGreen.Phase
	if bgPhase != "" && bgPhase != cachev1.BlueGreenPhaseNone {
		log.Info("Skipping scaling during blue-green deployment", "bgPhase", bgPhase)
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
	}

	// Persist the target topology before mutating the StatefulSet. If the status
	// update fails, the next reconcile must not mistake the completed scale for a
	// same-size spec change and start an unrelated blue/green rollout.
	if err := r.stampScalingSpecHash(ctx, cluster); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to stamp spec hash before scaling")
	}

	if targetPodCount > currentPodCount {
		log.Info("Scaling up", "from", currentPodCount, "to", targetPodCount)

		sts := cluster.StatefulSet
		if sts.Spec.Template.Labels[constants.BlueGreenRoleLabel] != "" {
			log.Info("Removing role label from STS template so new pods don't join service before cluster formation",
				"was", sts.Spec.Template.Labels[constants.BlueGreenRoleLabel])
			constants.DeactivateRole(sts.Spec.Template.Labels)
		}
		constants.DeactivateRedisCluster(sts.Spec.Template.Labels)
	} else {
		log.Info("Scaling down", "from", currentPodCount, "to", targetPodCount)
		if err := r.downShard(ctx, cluster); err != nil {
			return ctrl.Result{}, health.Warn, utils.WrapErr(err, "failed to downShard")
		}
	}

	if err := r.ScaleStatefulSet(ctx, cluster.StatefulSet, targetPodCount); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to scale StatefulSet")
	}
	return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
}

// updateAnnotation sets a single annotation on a pod and persists it via Update.
func updateAnnotation(ctx context.Context, c client.Client, pod *corev1.Pod, key string, val string) error {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[key] = val
	return c.Update(ctx, pod)
}

// patchAnnotation sets one annotation via MergePatch (avoids conflicts with concurrent updates).
func patchAnnotation(ctx context.Context, c client.Client, pod *corev1.Pod, key string, val string) error {
	patch, _ := json.Marshal(map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{key: val},
		},
	})
	return c.Patch(ctx, pod, client.RawPatch(types.MergePatchType, patch))
}
