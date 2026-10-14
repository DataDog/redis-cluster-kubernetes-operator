// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// handleTerminatingPods fails over terminating primaries and FORGETs terminating
// replicas. Pods owned by an optional lifecycle adapter are deferred to it.
func (r *RedisClusterReconciler) handleTerminatingPods(ctx context.Context, cluster *redis_cluster.Cluster) error {
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		return fmt.Errorf("handleTerminatingPods: failed to list pods: %w", err)
	}
	return r.handleTerminatingPodsFromList(ctx, cluster, podList)
}

func (r *RedisClusterReconciler) handleTerminatingPodsFromList(ctx context.Context, cluster *redis_cluster.Cluster, podList *v1.PodList) error {
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
			forgetDemotedNode, err := r.ensureNotPrimary(ctx, cluster, podList, node, pod)
			if err != nil {
				r.Log.Error(err, "Failed to failover primary",
					"pod_name", pod.Name, "nodeID", node.ID)
				_ = r.Metrics.CountBadSituation("terminating_primary_failover_failed", "pod_name:"+pod.Name)
			} else if forgetDemotedNode {
				// Failover succeeded — node is now a replica. FORGET it so other
				// nodes stop gossiping about it before the pod terminates.
				r.Log.Info("Failover succeeded, forgetting demoted node",
					"pod_name", pod.Name, "nodeID", node.ID)
				forgetNode(ctx, cluster, node, r.Log, r.Metrics)
			} else {
				r.Log.Info("Primary Pod was replaced; preserving node membership",
					"pod_name", pod.Name, "nodeID", node.ID)
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

// handleDrainCandidatePrimary corrects a Redis failover that promoted a pod
// marked as a drain candidate. Redis does not know about Kubernetes pod
// conditions, so it can win the election race before handleTerminatingPods
// selects a healthy sibling.
func (r *RedisClusterReconciler) handleDrainCandidatePrimary(ctx context.Context, cluster *redis_cluster.Cluster, podList *v1.PodList) (bool, error) {
	primary, replica := findDrainCandidatePrimaryFailover(cluster, podList)
	if primary == nil {
		return false, nil
	}

	r.Log.Info("Correcting drain candidate promoted by Redis failover",
		"primaryPod", primary.PodName,
		"primaryNodeID", primary.ID,
		"replicaPod", replica.PodName,
		"replicaNodeID", replica.ID)
	if err := r.performFailover(ctx, cluster, replica, primary); err != nil {
		_ = r.Metrics.CountBadSituation("drain_candidate_primary_failover_failed", "pod_name:"+primary.PodName)
		return false, fmt.Errorf("failed to demote drain candidate primary %s: %w", primary.ID, err)
	}

	return true, nil
}

// findDrainCandidatePrimaryFailover returns one live, drain-marked primary and
// a healthy sibling that can replace it. Terminating primaries remain owned by
// handleTerminatingPods, and a stale condition in the cluster snapshot is
// confirmed against the live Pod before any Redis operation is attempted.
func findDrainCandidatePrimaryFailover(cluster *redis_cluster.Cluster, podList *v1.PodList) (*redis_cluster.ClusterNode, *redis_cluster.ClusterNode) {
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primary.IsTerminating || !primary.HasDrainCandidateCond {
			continue
		}
		pod := findPodByIP(podList, primary.IP)
		if pod == nil || pod.DeletionTimestamp != nil || !redis_cluster.PodHasDrainCandidateCondition(pod) {
			continue
		}
		if replica := findNonDrainingReplica(cluster, podList, primary); replica != nil {
			return primary, replica
		}
	}
	return nil, nil
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
// The boolean reports whether the demoted node still belongs to the terminating
// Pod and should be forgotten. A replacement Pod with the same Redis identity
// must remain in the cluster.
func (r *RedisClusterReconciler) ensureNotPrimary(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	podList *v1.PodList,
	node *redis_cluster.ClusterNode,
	pod *v1.Pod,
) (bool, error) {
	if !node.RoleIs(redis_cluster.PrimaryNode) {
		return false, nil
	}

	replica := findNonDrainingReplica(cluster, podList, node)
	if replica == nil {
		replica = cluster.GetReplicaOf(node)
		if replica != nil {
			r.Log.Info("ensureNotPrimary: only draining replicas available — promoting anyway to avoid data loss",
				"pod_name", pod.Name, "nodeID", node.ID, "replicaNodeID", replica.ID)
			_ = r.Metrics.CountBadSituation("ensure_not_primary_promoting_draining_replica", "pod_name:"+pod.Name)
		}
	}
	if replica == nil {
		_ = r.Metrics.CountBadSituation("ensure_not_primary_no_replica", "pod_name:"+pod.Name)
		return false, fmt.Errorf("ensureNotPrimary: no replica available to failover primary %s (pod %s)", node.ID, pod.Name)
	}

	err := r.performFailoverWithAbort(ctx, cluster, replica, node, func(ctx context.Context, replicationInfo string) (bool, error) {
		return r.replicaFollowsReplacementPrimary(ctx, pod, replicationInfo)
	})
	if errors.Is(err, errFailoverSuperseded) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_, replaced, err := r.replacementPod(ctx, pod)
	if err != nil {
		return false, err
	}
	return !replaced, nil
}

// replicaFollowsReplacementPrimary reports whether Kubernetes replaced the
// terminating Pod and the replica is fully synchronized to that replacement.
// Both checks are required: a new Pod alone does not prove Redis recovered.
func (r *RedisClusterReconciler) replicaFollowsReplacementPrimary(
	ctx context.Context,
	terminatingPod *v1.Pod,
	replicationInfo string,
) (bool, error) {
	masterHost, synchronized := synchronizedReplicaMaster(replicationInfo)
	if !synchronized || terminatingPod == nil || terminatingPod.UID == "" {
		return false, nil
	}

	current, replaced, err := r.replacementPod(ctx, terminatingPod)
	if err != nil {
		return false, err
	}

	return replaced &&
		current.DeletionTimestamp == nil &&
		current.Status.PodIP != "" &&
		current.Status.PodIP == masterHost, nil
}

func (r *RedisClusterReconciler) replacementPod(ctx context.Context, original *v1.Pod) (*v1.Pod, bool, error) {
	current := &v1.Pod{}
	err := r.Get(ctx, client.ObjectKeyFromObject(original), current)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get current pod %s/%s: %w", original.Namespace, original.Name, err)
	}
	return current, original.UID != "" && current.UID != original.UID, nil
}
