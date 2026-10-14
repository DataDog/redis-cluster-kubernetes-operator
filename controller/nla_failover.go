package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	v1 "k8s.io/api/core/v1"
)

// Local aliases for NLA constants. Canonical values in package constants so
// redis_cluster can reuse them when populating ClusterNode drain fields.
const (
	nlaAnnotation            = constants.NLAAnnotation
	nlaProcessing            = constants.NLAProcessing
	nlaFailed                = constants.NLAFailed
	nlaDone                  = constants.NLADone
	nlaConditionType         = constants.NLAConditionType
	nlaConditionReason       = constants.NLAConditionReason
	nlaForgottenAnnotation   = constants.NLAForgottenAnnotation
	nlaTestTriggerAnnotation = constants.NLATestTriggerAnnotation
)

// LifecycleAdapter integrates an optional infrastructure lifecycle protocol.
// The portable reconciler leaves it nil and continues to handle Kubernetes pod
// termination and drain-candidate safety without external acknowledgements.
type LifecycleAdapter interface {
	PodState(pod *v1.Pod) (LifecyclePodState, error)
	Reconcile(ctx context.Context, reconciler *RedisClusterReconciler, cluster *redis_cluster.Cluster) (bool, error)
}

// LifecyclePodState is the provider-neutral view used by core safety logic.
// Protocol-specific annotation and condition values remain inside the adapter.
type LifecyclePodState struct {
	Managed            bool
	BlocksScaling      bool
	ScalingBlockReason string
	Acknowledged       bool
	Retriggered        bool
}

// DatadogNLAAdapter enables the legacy Datadog Node Lifecycle Automation
// recognition and acknowledgement protocol. It is opt-in and the reference
// binary does not install it.
type DatadogNLAAdapter struct{}

func (DatadogNLAAdapter) PodState(pod *v1.Pod) (LifecyclePodState, error) {
	annotation := pod.Annotations[nlaAnnotation]
	phase := annotation
	if phase == "" {
		phase = "pending"
	}
	acknowledged := isNLADone(annotation)
	retriggered := acknowledged && isNLARetriggeredOrTestReset(pod, annotation)
	managed := hasNLATrigger(pod) || annotation == nlaProcessing || annotation == nlaFailed
	scalingBlockReason := fmt.Sprintf("active NLA state %s", phase)
	if annotation == nlaProcessing || annotation == nlaFailed {
		scalingBlockReason = fmt.Sprintf("NLA state %s", phase)
	}
	return LifecyclePodState{
		Managed:            managed,
		BlocksScaling:      annotation == nlaProcessing || annotation == nlaFailed,
		ScalingBlockReason: scalingBlockReason,
		Acknowledged:       acknowledged,
		Retriggered:        retriggered,
	}, nil
}

func (DatadogNLAAdapter) Reconcile(
	ctx context.Context,
	reconciler *RedisClusterReconciler,
	cluster *redis_cluster.Cluster,
) (bool, error) {
	return reconciler.handleNLA(ctx, cluster)
}

func (r *RedisClusterReconciler) handleLifecycle(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	if optionalProviderIsNil(r.LifecycleAdapter) {
		return true, nil
	}
	return r.LifecycleAdapter.Reconcile(ctx, r, cluster)
}

func optionalProviderIsNil(provider any) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (r *RedisClusterReconciler) handleNLA(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	nlaStart := time.Now()
	r.Log.Info("Check for NLA")

	// refresh the pods list
	podList, err := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		return false, err
	}

	// Deferred FORGET for NLA-done pods that are now terminating.
	r.forgetTerminatingNLANodes(ctx, cluster, podList)

	nlaPending := 0
	nlaProcessingCount := 0
	nlaDoneCount := 0
	nlaFailedCount := 0

	var nlaPod *v1.Pod
	var interruptedNLAPod *v1.Pod
	for _, pod := range podList.Items {
		r.Log.Info("checking pod", "pod_name", pod.Name, "pod_ip", pod.Status.PodIP, "conditions", pod.Status.Conditions)

		if !hasNLATrigger(&pod) {
			continue
		}

		annotation := pod.Annotations[nlaAnnotation]

		if isNLADone(annotation) {
			if isNLARetriggeredOrTestReset(&pod, annotation) {
				r.Log.Info("NLA condition re-triggered after done, reprocessing",
					"pod_name", pod.Name,
					"annotation", annotation)
				nlaPending++
				if nlaPod == nil {
					nlaPod = &pod
				}
				_ = r.Metrics.Count(counters.NLARetriggered)
			} else {
				nlaDoneCount++
			}
			continue
		}

		switch annotation {
		case nlaProcessing:
			nlaProcessingCount++
			if interruptedNLAPod == nil {
				interruptedNLAPod = &pod
			}
		case nlaFailed:
			nlaFailedCount++
			if interruptedNLAPod == nil {
				interruptedNLAPod = &pod
			}
		default:
			nlaPending++
			if nlaPod == nil {
				nlaPod = &pod
			}
		}
	}
	// Prefer pending requests; otherwise resume interrupted or failed work.
	if nlaPod == nil {
		nlaPod = interruptedNLAPod
	}

	if nlaPending > 0 || nlaProcessingCount > 0 || nlaFailedCount > 0 {
		r.Log.Info("NLA pods status",
			"pending", nlaPending,
			"processing", nlaProcessingCount,
			"done", nlaDoneCount,
			"failed", nlaFailedCount,
			"totalPods", len(podList.Items))
	}

	_ = r.Metrics.Gauge(metrics.GaugeNLAPodsPending, float64(nlaPending))
	_ = r.Metrics.Gauge(metrics.GaugeNLAPodsFailed, float64(nlaFailedCount))

	if nlaPod == nil {
		r.Log.Info("No pods marked for NLA")
		return true, nil
	}

	r.Log.Info("Processing NLA pod",
		"pod_name", nlaPod.Name,
		"pod_ip", nlaPod.Status.PodIP,
		"k8s_node", nlaPod.Spec.NodeName)

	node, found := cluster.LookupNodeByIP(nlaPod.Status.PodIP)
	if !found {
		node = clusterNodeByPodName(cluster, nlaPod.Name)
		found = node != nil && node.ID != ""
	}
	if !found {
		r.Log.Info("NLA pod has no confirmed Redis identity; leaving the request pending",
			"pod_name", nlaPod.Name,
			"pod_ip", nlaPod.Status.PodIP)
		return false, nil
	}

	// Skip if all replicas are draining (avoids ping-pong); selection itself is in ensureNotPrimary.
	if node.RoleIs(redis_cluster.PrimaryNode) {
		replicas := cluster.GetReplicasOf(node)
		if len(replicas) > 0 && findNonDrainingReplica(cluster, podList, node) == nil {
			r.Log.Info("Skipping NLA: all replicas are also drain candidates — will retry next reconcile",
				"primaryPod", nlaPod.Name,
				"replicaCount", len(replicas))
			_ = r.Metrics.CountBadSituation("nla_skipped_replica_draining", "pod_name:"+nlaPod.Name)
			return false, nil
		}
	}

	if err = updateAnnotation(ctx, r.Client, nlaPod, nlaAnnotation, nlaProcessing); err != nil {
		r.Log.Error(err, "Failed to update annotations")
		return false, err
	}

	r.Log.Info("Preparing Redis node for NLA",
		"pod_name", nlaPod.Name,
		"nodeID", node.ID,
		"nodeRole", node.Role,
		"isPrimary", node.RoleIs(redis_cluster.PrimaryNode))

	// FAILOVER only — defer FORGET to forgetTerminatingNLANodes when the pod actually terminates.
	preparedForTermination := !node.RoleIs(redis_cluster.PrimaryNode)
	if node.RoleIs(redis_cluster.PrimaryNode) {
		preparedForTermination, err = r.ensureNotPrimary(ctx, cluster, podList, node, nlaPod)
	} else {
		r.Log.Info("NLA: node stays in cluster until pod termination",
			"pod_name", nlaPod.Name, "nodeID", node.ID, "nodeRole", node.Role)
	}

	// Urgent path: pod already terminating — FORGET now and persist the marker.
	if err == nil && nlaPod.DeletionTimestamp != nil && preparedForTermination {
		r.Log.Info("NLA: pod already terminating, forgetting immediately",
			"pod_name", nlaPod.Name, "nodeID", node.ID)
		forgetNode(ctx, cluster, node, r.Log, r.Metrics)
		if patchErr := patchAnnotation(ctx, r.Client, nlaPod, nlaForgottenAnnotation, "true"); patchErr != nil {
			r.Log.Error(patchErr, "NLA: failed to persist forgotten annotation",
				"pod_name", nlaPod.Name)
		}
	} else if err == nil && nlaPod.DeletionTimestamp != nil {
		r.Log.Info("NLA: primary Pod was replaced; preserving node membership",
			"pod_name", nlaPod.Name, "nodeID", node.ID)
	}

	nlaDuration := time.Since(nlaStart).Milliseconds()

	if err != nil {
		processingErr := err
		r.Log.Error(err, "NLA processing failed",
			"pod_name", nlaPod.Name,
			"nodeID", node.ID,
			"nodeRole", node.Role,
			"duration_ms", nlaDuration)
		_ = r.Metrics.Count(counters.NLAFailed)
		_ = r.Metrics.Timing(metrics.TimingNLADuration, nlaDuration, "status:failed")
		if annotationErr := updateAnnotation(ctx, r.Client, nlaPod, nlaAnnotation, nlaFailed); annotationErr != nil {
			r.Log.Error(annotationErr, "Failed to update annotations to failed")
			return false, errors.Join(processingErr, annotationErr)
		}
		return false, processingErr
	}

	r.Log.Info("NLA processing completed successfully",
		"pod_name", nlaPod.Name,
		"nodeID", node.ID,
		"nodeRole", node.Role,
		"duration_ms", nlaDuration)

	_ = r.Metrics.Count(counters.NLAProcessed)
	_ = r.Metrics.Timing(metrics.TimingNLADuration, nlaDuration, "status:success")

	// "done:<unix-ts>" so we can detect later re-triggers.
	doneWithTimestamp := fmt.Sprintf("%s:%d", nlaDone, time.Now().Unix())
	return false, updateAnnotation(ctx, r.Client, nlaPod, nlaAnnotation, doneWithTimestamp)
}

// forgetNLANode issues CLUSTER FORGET and sets the forgotten annotation.
// Refuses primaries — forgetting one drops its hash slots. This is the
// single source of truth for the "no FORGET on a primary" invariant.
func (r *RedisClusterReconciler) forgetNLANode(ctx context.Context, cluster *redis_cluster.Cluster, pod *v1.Pod, node *redis_cluster.ClusterNode) {
	if node.RoleIs(redis_cluster.PrimaryNode) {
		r.Log.Error(nil, "REFUSING to forget NLA node: still a primary",
			"pod_name", pod.Name, "nodeID", node.ID)
		_ = r.Metrics.CountBadSituation("forget_terminating_primary", "pod_name:"+pod.Name)
		return
	}

	r.Log.Info("Forgetting NLA node",
		"pod_name", pod.Name,
		"pod_ip", pod.Status.PodIP,
		"nodeID", node.ID,
		"nodeRole", node.Role)
	forgetNode(ctx, cluster, node, r.Log, r.Metrics)

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[nlaForgottenAnnotation] = "true"
}

// forgetTerminatingNLANodes FORGETs NLA-done pods that are now terminating.
// Only currently-replica nodes are processed; primaries are skipped (their role
// may have changed since "done") and left to zombie cleanup or forgetNLANode's
// last-line guard.
func (r *RedisClusterReconciler) forgetTerminatingNLANodes(ctx context.Context, cluster *redis_cluster.Cluster, podList *v1.PodList) {
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.DeletionTimestamp == nil {
			continue
		}
		r.Log.Info("forgetTerminatingNLANodes: pod is terminating",
			"pod_name", pod.Name,
			"deletion_ts", pod.DeletionTimestamp.Time,
			"nla_annotation", pod.Annotations[nlaAnnotation],
			"forgotten", pod.Annotations[nlaForgottenAnnotation])

		if !isNLADone(pod.Annotations[nlaAnnotation]) {
			r.Log.Info("forgetTerminatingNLANodes: skipping — not NLA done", "pod_name", pod.Name)
			continue
		}
		if pod.Annotations[nlaForgottenAnnotation] == "true" {
			r.Log.Info("forgetTerminatingNLANodes: skipping — already forgotten", "pod_name", pod.Name)
			continue
		}
		node, found := cluster.LookupNodeByIP(pod.Status.PodIP)
		if !found {
			r.Log.Info("forgetTerminatingNLANodes: skipping — node not found by IP",
				"pod_name", pod.Name, "pod_ip", pod.Status.PodIP)
			continue
		}
		if !node.RoleIs(redis_cluster.ReplicaNode) {
			r.Log.Info("forgetTerminatingNLANodes: skipping — node is not a replica",
				"pod_name", pod.Name, "node_role", node.Role)
			continue
		}
		r.Log.Info("forgetTerminatingNLANodes: FORGETting node during grace period",
			"pod_name", pod.Name, "nodeID", node.ID, "nodeRole", node.Role)
		r.forgetNLANode(ctx, cluster, pod, node)

		if err := patchAnnotation(ctx, r.Client, pod, nlaForgottenAnnotation, "true"); err != nil {
			r.Log.Error(err, "forgetTerminatingNLANodes: failed to persist forgotten annotation",
				"pod_name", pod.Name)
		}
	}
}

// hasNLATrigger: NodeDrainCandidate condition (draino) or test-nla annotation.
func hasNLATrigger(pod *v1.Pod) bool {
	return redis_cluster.PodHasDrainCandidateCondition(pod) || pod.Annotations[nlaTestTriggerAnnotation] == "true"
}

// isNLARetriggeredOrTestReset: real NLA compares condition timestamp vs done
// timestamp; test annotation alone is treated as already-processed.
func isNLARetriggeredOrTestReset(pod *v1.Pod, annotation string) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == nlaConditionType && cond.Status == v1.ConditionTrue && cond.Reason == nlaConditionReason {
			return isNLARetriggered(annotation, cond.LastTransitionTime.Time)
		}
	}
	return false
}

// isNLADone accepts the legacy bare value and the current timestamped value.
// Invalid timestamped values are treated as pending so they cannot bypass
// failover or autoscaling safety checks.
func isNLADone(annotation string) bool {
	if annotation == nlaDone {
		return true
	}
	parts := strings.SplitN(annotation, ":", 2)
	if len(parts) != 2 || parts[0] != nlaDone {
		return false
	}
	_, err := strconv.ParseInt(parts[1], 10, 64)
	return err == nil
}

func findPodByIP(podList *v1.PodList, podIP string) *v1.Pod {
	for i := range podList.Items {
		if podList.Items[i].Status.PodIP == podIP {
			return &podList.Items[i]
		}
	}
	return nil
}

// isPodDrainCandidate: pod has NodeDrainCandidate condition or test-nla annotation.
func isPodDrainCandidate(podList *v1.PodList, podIP string) bool {
	pod := findPodByIP(podList, podIP)
	if pod == nil {
		return false
	}
	return redis_cluster.PodHasDrainCandidateCondition(pod)
}

// isPodTerminating: pod has DeletionTimestamp set.
func isPodTerminating(podList *v1.PodList, podIP string) bool {
	pod := findPodByIP(podList, podIP)
	return pod != nil && pod.DeletionTimestamp != nil
}

// isNLARetriggered: condition lastTransitionTime is after the "done:<unix-ts>"
// annotation timestamp. Legacy "done" without timestamp returns false.
func isNLARetriggered(annotation string, conditionTime time.Time) bool {
	if annotation == nlaDone {
		return false
	}
	parts := strings.SplitN(annotation, ":", 2)
	if len(parts) != 2 {
		return false
	}
	doneTimestamp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	return conditionTime.After(time.Unix(doneTimestamp, 0))
}
