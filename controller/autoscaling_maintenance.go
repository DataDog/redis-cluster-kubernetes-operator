package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
)

var (
	errWaitingForRedisEndpointReady  = errors.New("waiting for Redis client endpoint to become ready")
	errWaitingForAutoscalingCapacity = errors.New("waiting for safe autoscaling repair capacity")
)

// autoscalingReplicaMigrationGuard prevents Redis from moving replicas while
// the operator is assembling an automatically added primary group. The
// current cluster is supplied to every operation because maintenance refreshes
// and replaces its cluster snapshot between topology changes.
type autoscalingReplicaMigrationGuard struct {
	reconciler          *RedisClusterReconciler
	log                 logr.Logger
	disabled            bool
	setReplicaMigration func([]*redis_cluster.ClusterNode, bool) error
}

func newAutoscalingReplicaMigrationGuard(
	reconciler *RedisClusterReconciler,
	log logr.Logger,
) *autoscalingReplicaMigrationGuard {
	return &autoscalingReplicaMigrationGuard{
		reconciler: reconciler,
		log:        log,
	}
}

func (g *autoscalingReplicaMigrationGuard) disable(ctx context.Context, cluster *redis_cluster.Cluster) error {
	if g.disabled {
		return nil
	}
	if !cluster.RedisCluster.Status.Autoscaling.ReplicaMigrationDisabled {
		if err := g.reconciler.updateRedisClusterStatusWithRetry(ctx, cluster.RedisCluster, func(status *cachev1.RedisClusterStatus) {
			status.Autoscaling.ReplicaMigrationDisabled = true
		}); err != nil {
			return utils.WrapErr(err, "failed to persist replica migration guard before scale-up")
		}
	}
	members := replicaMigrationGuardNodes(cluster)
	if err := g.setReplicaMigrationForCluster(cluster, members, false); err != nil {
		return utils.WrapErr(err, "failed to disable autonomous Redis replica migration during scale-up")
	}
	g.disabled = true
	g.log.Info("Disabled autonomous Redis replica migration during operator-managed scale-up",
		"memberCount", len(members))
	_ = g.reconciler.Metrics.CountAutoscalingDecision("guard", "replica_migration_disabled")
	return nil
}

func (g *autoscalingReplicaMigrationGuard) restore(ctx context.Context, cluster *redis_cluster.Cluster) error {
	markerSet := cluster.RedisCluster.Status.Autoscaling.ReplicaMigrationDisabled
	if !g.disabled && !markerSet {
		g.disabled = false
		return nil
	}
	members := replicaMigrationGuardNodes(cluster)
	if err := g.setReplicaMigrationForCluster(cluster, members, true); err != nil {
		return utils.WrapErr(err, "failed to restore autonomous Redis replica migration after scale-up")
	}
	if markerSet {
		if err := g.reconciler.updateRedisClusterStatusWithRetry(ctx, cluster.RedisCluster, func(status *cachev1.RedisClusterStatus) {
			status.Autoscaling.ReplicaMigrationDisabled = false
		}); err != nil {
			return utils.WrapErr(err, "failed to persist restored replica migration guard")
		}
	}
	g.disabled = false
	g.log.Info("Restored autonomous Redis replica migration after operator-managed scale-up",
		"memberCount", len(members))
	_ = g.reconciler.Metrics.CountAutoscalingDecision("guard", "replica_migration_restored")
	return nil
}

func (g *autoscalingReplicaMigrationGuard) setReplicaMigrationForCluster(
	cluster *redis_cluster.Cluster,
	members []*redis_cluster.ClusterNode,
	enabled bool,
) error {
	if g.setReplicaMigration != nil {
		return g.setReplicaMigration(members, enabled)
	}
	return cluster.Toolbox.SetReplicaMigration(members, enabled)
}

func replicaMigrationGuardNodes(cluster *redis_cluster.Cluster) []*redis_cluster.ClusterNode {
	nodes := make([]*redis_cluster.ClusterNode, 0, len(cluster.AllNodes))
	for _, role := range []redis_cluster.NodeRole{
		redis_cluster.PrimaryNode,
		redis_cluster.ReplicaNode,
		redis_cluster.OutsiderNode,
	} {
		nodes = append(nodes, cluster.Nodes[role]...)
	}
	return nodes
}

// prepareAutoscalingScaleUpMaintenance runs the autoscaling-specific gates
// that must precede the generic primary and replica maintenance sequence.
func (r *RedisClusterReconciler) prepareAutoscalingScaleUpMaintenance(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	log logr.Logger,
) (*redis_cluster.Cluster, bool, error) {
	waitingPods, err := r.prepareAutoscalingScaleUpMemberEndpoints(ctx, cluster)
	if err != nil {
		return cluster, false, utils.WrapErr(err, "failed to prepare autoscaling member endpoints")
	}
	if len(waitingPods) > 0 {
		log.Info("Waiting for autoscaling member client endpoints before continuing topology changes",
			"pods", waitingPods,
			"requeueAfter", constants.QuickRequeue.String())
		return cluster, false, fmt.Errorf("%w: %s", errWaitingForRedisEndpointReady, strings.Join(waitingPods, ", "))
	}

	return r.repairAutoscalingNLAReplicaRedundancy(ctx, cluster, log)
}

func (r *RedisClusterReconciler) ensureEmptyPrimaryEndpointsReadyForRebalance(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	log logr.Logger,
) error {
	waitingPods, err := r.prepareEmptyPrimariesForRebalance(ctx, cluster)
	if err != nil {
		return utils.WrapErr(err, "failed to prepare empty primaries for rebalance")
	}
	if len(waitingPods) == 0 {
		return nil
	}

	log.Info("Waiting for empty primary client endpoints before rebalance",
		"pods", waitingPods,
		"requeueAfter", constants.QuickRequeue.String())
	return fmt.Errorf("%w: %s", errWaitingForRedisEndpointReady, strings.Join(waitingPods, ", "))
}

// prepareEmptyPrimariesForRebalance publishes healthy Redis nodes through the
// headless Service, then verifies that every empty primary is Kubernetes-ready
// and its exact Redis-announced hostname is stable in DNS before assigning slots.
func (r *RedisClusterReconciler) prepareEmptyPrimariesForRebalance(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
) ([]string, error) {
	newlyLabeled, err := r.readyClusterPods(ctx, cluster)
	if err != nil {
		return nil, err
	}

	emptyPrimaries := make([]*redis_cluster.ClusterNode, 0)
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if len(primary.SlotsRanges) == 0 {
			emptyPrimaries = append(emptyPrimaries, primary)
		}
	}
	return r.waitForRedisNodeEndpoints(ctx, cluster, emptyPrimaries, newlyLabeled, "slot assignment")
}

// prepareAutoscalingScaleUpMemberEndpoints gates every Redis member created by
// the current one-primary autoscaling step. The previous topology size is
// derivable because automatic growth advances EffectivePrimaries exactly one at
// a time. Outsiders are intentionally excluded until they join Redis; once a
// node is a primary or replica it must have a stable advertised endpoint before
// the operator performs another topology mutation.
func (r *RedisClusterReconciler) prepareAutoscalingScaleUpMemberEndpoints(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
) ([]string, error) {
	previousPodCount, scaling := autoscalingPreviousPodCount(cluster.RedisCluster)
	if !scaling {
		return nil, nil
	}

	newlyLabeled, err := r.readyClusterPods(ctx, cluster)
	if err != nil {
		return nil, err
	}

	newMembers := make([]*redis_cluster.ClusterNode, 0)
	for _, role := range []redis_cluster.NodeRole{redis_cluster.PrimaryNode, redis_cluster.ReplicaNode} {
		for _, node := range cluster.Nodes[role] {
			// A drain-marked replica can be reset and reattached at a lower
			// ordinal while the new group is formed. Gate it too so a changed
			// node ID or temporarily removed DNS record cannot bypass the
			// scale-up barrier.
			if int32(node.PodIndex) >= previousPodCount || node.IsDrainCandidate() {
				newMembers = append(newMembers, node)
			}
		}
	}
	return r.waitForRedisNodeEndpoints(ctx, cluster, newMembers, newlyLabeled, "continuing autoscaling")
}

func autoscalingPreviousPodCount(redisCluster *cachev1.RedisCluster) (int32, bool) {
	if redisCluster == nil || !autoscalingIsScaling(&redisCluster.Status.Autoscaling) {
		return 0, false
	}
	targetPrimaries := redisCluster.EffectivePrimaries()
	groupSize := int32(1) + redisCluster.Spec.Replicas
	if targetPrimaries <= 1 || groupSize <= 0 {
		return 0, false
	}
	return (targetPrimaries - 1) * groupSize, true
}

func (r *RedisClusterReconciler) waitForRedisNodeEndpoints(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	nodes []*redis_cluster.ClusterNode,
	newlyLabeled map[string]struct{},
	purpose string,
) ([]string, error) {
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to list pods after ready-labeling")
	}

	podsByName := make(map[string]*corev1.Pod, len(podList.Items))
	podsByIP := make(map[string]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		podsByName[pod.Name] = pod
		if pod.Status.PodIP != "" {
			podsByIP[pod.Status.PodIP] = pod
		}
	}

	waitingPods := make([]string, 0)
	for _, node := range nodes {
		pod := podsByName[node.PodName]
		if pod == nil && node.IP != "" {
			pod = podsByIP[node.IP]
		}

		podName := node.PodName
		if podName == "" {
			podName = node.IP
		}
		if podName == "" {
			podName = node.ID
		}

		_, labelsJustUpdated := newlyLabeled[podName]
		hasReadyLabel := false
		isReady := false
		phase := corev1.PodPhase("")
		terminating := false
		if pod != nil {
			podName = pod.Name
			_, labelsJustUpdated = newlyLabeled[pod.Name]
			hasReadyLabel = pod.Labels[constants.RedisClusterReadyPodLabel] == "true"
			isReady = podHasReadyCondition(pod)
			phase = pod.Status.Phase
			terminating = pod.DeletionTimestamp != nil
		}

		if pod == nil || labelsJustUpdated || !hasReadyLabel || terminating || phase != corev1.PodRunning || !isReady {
			r.resetPrimaryEndpointReadiness(pod)
			waitingPods = append(waitingPods, podName)
			r.Log.Info("Redis member is not ready for client endpoint verification",
				"podName", podName,
				"nodeRole", node.Role,
				"purpose", purpose,
				"podFound", pod != nil,
				"labelsJustUpdated", labelsJustUpdated,
				"hasReadyLabel", hasReadyLabel,
				"terminating", terminating,
				"phase", phase,
				"podReady", isReady)
			continue
		}

		endpointReady, endpointStatus := r.primaryEndpointReady(ctx, cluster, node, pod)
		if !endpointReady {
			waitingPods = append(waitingPods, podName)
			if endpointStatus.Reason != primaryEndpointReasonStabilizing {
				_ = r.Metrics.CountBadSituation(
					primaryEndpointNotReadyBadSituation,
					primaryEndpointNotReadyMetricReason+string(endpointStatus.Reason),
				)
			}
			r.Log.Info("Redis member client endpoint is not ready",
				"podName", podName,
				"nodeRole", node.Role,
				"purpose", purpose,
				"hostname", endpointStatus.Hostname,
				"announcedHostname", endpointStatus.AnnouncedHostname,
				"expectedIPs", endpointStatus.ExpectedIPs,
				"resolvedIPs", endpointStatus.ResolvedIPs,
				"expectedNodeID", endpointStatus.ExpectedNodeID,
				"observedNodeID", endpointStatus.ObservedNodeID,
				"successes", endpointStatus.Successes,
				"requiredSuccesses", primaryEndpointRequiredSuccesses,
				"retryAfter", endpointStatus.RetryAfter.String(),
				"reason", endpointStatus.Reason,
				"probeError", endpointStatus.ProbeErr)
			continue
		}

		if endpointStatus.Reason == primaryEndpointReasonReady {
			r.Log.Info("Redis member client endpoint is ready",
				"podName", podName,
				"nodeRole", node.Role,
				"purpose", purpose,
				"hostname", endpointStatus.Hostname,
				"podIPs", endpointStatus.ExpectedIPs,
				"nodeID", endpointStatus.ExpectedNodeID,
				"successes", endpointStatus.Successes)
		}
	}

	return waitingPods, nil
}

// repairAutoscalingNLAReplicaRedundancy uses one pod from the newly expanded
// autoscaling group to give an endangered primary a healthy, non-draining
// replica. It normally runs before promotion, but it also repairs a partially
// formed group after an interrupted scale-up. Capacity is calculated from the
// live primary and replica deficits so the post-promotion recovery path does
// not reserve a primary pod that already exists.
func (r *RedisClusterReconciler) repairAutoscalingNLAReplicaRedundancy(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	log logr.Logger,
) (*redis_cluster.Cluster, bool, error) {
	if optionalProviderIsNil(r.LifecycleAdapter) {
		return cluster, false, nil
	}
	previousPodCount, scaling := autoscalingPreviousPodCount(cluster.RedisCluster)
	if !scaling {
		return cluster, false, nil
	}

	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		return cluster, false, utils.WrapErr(err, "failed to inspect pods for autoscaling NLA redundancy repair")
	}
	podsByName := make(map[string]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		podsByName[pod.Name] = pod
	}

	targetPrimary := r.primaryNeedingNLAReplicaRepair(cluster, podsByName)
	if targetPrimary == nil {
		return cluster, false, nil
	}

	newOutsiders := nonDrainingOutsidersAtOrAboveOrdinal(cluster, previousPodCount)
	remainingAfterRepair, requiredAfterRepair, enoughCapacity := cleanOutsiderCapacityAfterNLARepair(cluster, len(newOutsiders))
	if !enoughCapacity {
		_ = r.Metrics.CountBadSituation(
			"autoscaling_nla_repair_capacity",
			"available_clean_outsiders:"+fmt.Sprint(len(newOutsiders)),
			"required_after_repair:"+fmt.Sprint(requiredAfterRepair),
		)
		return cluster, false, fmt.Errorf(
			"%w: primary %s needs a non-draining replica, but using one of %d clean new outsiders would leave %d while %d are still required for missing primaries and replicas",
			errWaitingForAutoscalingCapacity,
			targetPrimary.ID,
			len(newOutsiders),
			remainingAfterRepair,
			requiredAfterRepair,
		)
	}

	outsider := newOutsiders[0]
	log.Info("Repairing NLA replica redundancy during autoscaling",
		"targetPrimaryID", targetPrimary.ID,
		"targetPrimaryPodName", targetPrimary.PodName,
		"newReplicaID", outsider.ID,
		"newReplicaIP", outsider.IP,
		"newReplicaPodName", outsider.PodName,
		"previousPodCount", previousPodCount)
	if err := cluster.Toolbox.AddReplica(targetPrimary, outsider, cluster.RedisPort, targetPrimary.ID); err != nil {
		return cluster, false, utils.WrapErr(err, "failed to attach autoscaling pod for NLA replica redundancy repair")
	}

	var attached bool
	cluster, attached = r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
		node, found := c.LookupNodeByID(outsider.ID)
		return found &&
			node.Info.ClusterStateIsOK &&
			node.RoleIs(redis_cluster.ReplicaNode) &&
			node.PrimaryID == targetPrimary.ID
	}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)
	if !attached {
		return cluster, false, fmt.Errorf(
			"autoscaling pod %s did not become a healthy replica of primary %s before timeout",
			outsider.PodName,
			targetPrimary.ID,
		)
	}

	_ = r.Metrics.CountAutoscalingDecision("repair", "nla_replica_redundancy")
	return cluster, true, nil
}

func cleanOutsiderCapacityAfterNLARepair(cluster *redis_cluster.Cluster, availableCleanOutsiders int) (remaining, required int, enough bool) {
	remaining = availableCleanOutsiders - 1
	required = cleanOutsidersRequiredAfterNLARepair(cluster)
	return remaining, required, remaining >= required
}

// cleanOutsidersRequiredAfterNLARepair returns the clean pods that must remain
// after one outsider becomes the endangered primary's replica. A missing
// primary consumes one outsider, and every replica still absent after the
// repair consumes another. Existing primaries (including an already promoted
// empty primary) are not reserved a second time.
func cleanOutsidersRequiredAfterNLARepair(cluster *redis_cluster.Cluster) int {
	expectedPrimaries := int(cluster.RedisCluster.EffectivePrimaries())
	missingPrimaries := expectedPrimaries - len(cluster.Nodes[redis_cluster.PrimaryNode])
	if missingPrimaries < 0 {
		missingPrimaries = 0
	}

	expectedReplicas := expectedPrimaries * int(cluster.RedisCluster.Spec.Replicas)
	replicasAfterRepair := len(cluster.Nodes[redis_cluster.ReplicaNode]) + 1
	missingReplicas := expectedReplicas - replicasAfterRepair
	if missingReplicas < 0 {
		missingReplicas = 0
	}
	return missingPrimaries + missingReplicas
}

func (r *RedisClusterReconciler) primaryNeedingNLAReplicaRepair(
	cluster *redis_cluster.Cluster,
	podsByName map[string]*corev1.Pod,
) *redis_cluster.ClusterNode {
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primary.ID == "" || len(primary.SlotsRanges) == 0 {
			continue
		}
		if r.healthyNonDrainingReplicaCount(cluster, primary.ID, podsByName) > 0 {
			continue
		}
		for _, replica := range cluster.Nodes[redis_cluster.ReplicaNode] {
			if replica.PrimaryID != primary.ID || !replica.IsDrainCandidate() {
				continue
			}
			pod := podsByName[replica.PodName]
			if pod == nil {
				continue
			}
			if r.acknowledgedNLACandidateBlocker(cluster, replica, pod, podsByName, false) == "" {
				return primary
			}
		}
	}
	return nil
}

func nonDrainingOutsidersAtOrAboveOrdinal(cluster *redis_cluster.Cluster, minimumOrdinal int32) []*redis_cluster.ClusterNode {
	outsiders := make([]*redis_cluster.ClusterNode, 0)
	for _, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
		if outsider.IsDrainCandidate() || outsider.ID == "" || outsider.IP == "" || int32(outsider.PodIndex) < minimumOrdinal {
			continue
		}
		outsiders = append(outsiders, outsider)
	}
	return outsiders
}

func nonDrainingPrimaryOutsider(cluster *redis_cluster.Cluster) *redis_cluster.ClusterNode {
	if previousPodCount, scaling := autoscalingPreviousPodCount(cluster.RedisCluster); scaling {
		for _, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
			if !outsider.IsDrainCandidate() && outsider.ID != "" && outsider.IP != "" && int32(outsider.PodIndex) >= previousPodCount {
				return outsider
			}
		}
	}
	for _, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
		if !outsider.IsDrainCandidate() && outsider.ID != "" && outsider.IP != "" {
			return outsider
		}
	}
	return nil
}

func preferredReplicaOutsider(cluster *redis_cluster.Cluster) *redis_cluster.ClusterNode {
	for _, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
		if !outsider.IsDrainCandidate() && outsider.ID != "" && outsider.IP != "" {
			return outsider
		}
	}
	for _, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
		if outsider.ID != "" && outsider.IP != "" {
			return outsider
		}
	}
	return nil
}
