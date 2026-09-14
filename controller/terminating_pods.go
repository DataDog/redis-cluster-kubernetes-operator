package controller

import (
	"context"
	"fmt"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	v1 "k8s.io/api/core/v1"
)

// handleTerminatingPods fails over terminating primaries and FORGETs terminating
// replicas. Pods owned by an optional lifecycle adapter are deferred to it.
func (r *RedisClusterReconciler) handleTerminatingPods(ctx context.Context, cluster *redis_cluster.Cluster) error {
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		return fmt.Errorf("handleTerminatingPods: failed to list pods: %w", err)
	}

	var primaryCount, replicaCount, skippedNLACount, otherCount int
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp == nil {
			continue
		}
		node, found := cluster.LookupNodeByIP(pod.Status.PodIP)
		if !found {
			continue
		}

		// Adapter-managed pods are handled by the adapter; skip to avoid
		// conflicting acknowledgement and failover actions.
		if !optionalProviderIsNil(r.LifecycleAdapter) {
			state, err := r.LifecycleAdapter.PodState(pod)
			if err != nil {
				return fmt.Errorf("handleTerminatingPods: lifecycle adapter could not inspect pod %s: %w", pod.Name, err)
			}
			if state.Managed {
				skippedNLACount++
				continue
			}
		}

		switch {
		case node.RoleIs(redis_cluster.PrimaryNode):
			primaryCount++
			r.Log.Info("Terminating primary, ensuring failover",
				"pod_name", pod.Name, "nodeID", node.ID)
			if err := r.ensureNotPrimary(ctx, cluster, podList, node, pod.Name); err != nil {
				r.Log.Error(err, "Failed to failover primary",
					"pod_name", pod.Name, "nodeID", node.ID)
				_ = r.Metrics.CountBadSituation("terminating_primary_failover_failed", "pod_name:"+pod.Name)
			} else {
				// Failover succeeded — node is now a replica. FORGET it so other
				// nodes stop gossiping about it before the pod terminates.
				r.Log.Info("Failover succeeded, forgetting demoted node",
					"pod_name", pod.Name, "nodeID", node.ID)
				forgetNode(ctx, cluster, node, r.Log, r.Metrics)
			}

		case node.RoleIs(redis_cluster.ReplicaNode):
			replicaCount++
			r.Log.Info("Terminating replica, proactive FORGET",
				"pod_name", pod.Name, "nodeID", node.ID)
			forgetNode(ctx, cluster, node, r.Log, r.Metrics)

		default:
			otherCount++
		}
	}

	r.Log.V(1).Info("handleTerminatingPods complete",
		"primaries", primaryCount,
		"replicas", replicaCount,
		"skipped_nla", skippedNLACount,
		"other", otherCount)

	return nil
}

// findNonDrainingReplica returns the first replica of `primary` whose pod is
// not draining/terminating. Reads from the live podList (cluster snapshots lag).
func findNonDrainingReplica(cluster *redis_cluster.Cluster, podList *v1.PodList, primary *redis_cluster.ClusterNode) *redis_cluster.ClusterNode {
	for _, replica := range cluster.GetReplicasOf(primary) {
		if !replica.IsDrainCandidate() && !podIsDrainingOrTerminating(podList, replica.IP) {
			return replica
		}
	}
	return nil
}

// podIsDrainingOrTerminating returns true if the pod is draining, terminating, or missing.
func podIsDrainingOrTerminating(podList *v1.PodList, podIP string) bool {
	pod := findPodByIP(podList, podIP)
	if pod == nil {
		return true
	}
	return redis_cluster.PodHasDrainCandidateCondition(pod) || pod.DeletionTimestamp != nil
}

// ensureNotPrimary fails `node` over to a non-draining replica, falling back
// to any replica to avoid leaving a terminating primary without a successor.
func (r *RedisClusterReconciler) ensureNotPrimary(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	podList *v1.PodList,
	node *redis_cluster.ClusterNode,
	podName string,
) error {
	if !node.RoleIs(redis_cluster.PrimaryNode) {
		return nil
	}

	replica := findNonDrainingReplica(cluster, podList, node)
	if replica == nil {
		replica = cluster.GetReplicaOf(node)
		if replica != nil {
			r.Log.Info("ensureNotPrimary: only draining replicas available — promoting anyway to avoid data loss",
				"pod_name", podName, "nodeID", node.ID, "replicaNodeID", replica.ID)
			_ = r.Metrics.CountBadSituation("ensure_not_primary_promoting_draining_replica", "pod_name:"+podName)
		}
	}
	if replica == nil {
		_ = r.Metrics.CountBadSituation("ensure_not_primary_no_replica", "pod_name:"+podName)
		return fmt.Errorf("ensureNotPrimary: no replica available to failover primary %s (pod %s)", node.ID, podName)
	}

	return r.performFailover(ctx, cluster, replica, node)
}
