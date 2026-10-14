// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build blackbox_e2e

package blackbox

import (
	"context"
	"fmt"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (h *harness) rolePods(s scenario) (primary, replica corev1.Pod, nodes []redisNode) {
	h.t.Helper()
	pods := h.pods(s)
	if len(pods) == 0 {
		h.t.Fatal("no Redis pods")
	}
	raw, err := h.redisInPod(h.ctx, pods[0], "CLUSTER", "NODES")
	if err != nil {
		h.t.Fatal(err)
	}
	nodes = parseClusterNodes(raw)
	podByIP := map[string]corev1.Pod{}
	for _, pod := range pods {
		podByIP[pod.Status.PodIP] = pod
	}
	for _, node := range nodes {
		pod, ok := podByIP[node.Address]
		if !ok || node.Failed {
			continue
		}
		if node.Primary && primary.Name == "" {
			primary = pod
		}
		if !node.Primary && node.MasterID != "-" && replica.Name == "" {
			replica = pod
		}
	}
	if primary.Name == "" || replica.Name == "" {
		h.t.Fatalf("could not map a primary and replica to pods; nodes=%+v", nodes)
	}
	return primary, replica, nodes
}

func (h *harness) deleteAndWaitRecreated(s scenario, pod corev1.Pod) {
	h.t.Helper()
	uid := pod.UID
	if err := h.client.Delete(h.ctx, &pod); err != nil {
		h.t.Fatalf("delete pod %s: %v", pod.Name, err)
	}
	h.eventually("pod "+pod.Name+" recreated", readyTimeout, func(ctx context.Context) error {
		current := &corev1.Pod{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: pod.Name}, current); err != nil {
			return err
		}
		if current.UID == uid || !podReady(*current) {
			return fmt.Errorf("UID=%s ready=%t", current.UID, podReady(*current))
		}
		return nil
	})
	h.waitReady(s, h.cluster(s).Spec.Primaries, h.cluster(s).Spec.Replicas)
}

func TestCleanroom_PrimaryAndReplicaRecovery(t *testing.T) {
	h := newHarness(t)
	s := h.reference()
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	primary, replica, _ := h.rolePods(s)
	h.deleteAndWaitRecreated(s, replica)
	h.verifyData(s)
	primary, _, _ = h.rolePods(s)
	h.deleteAndWaitRecreated(s, primary)
	h.verifyData(s)
}

func TestCleanroom_TerminatingPrimarySafety(t *testing.T) {
	h := newHarness(t)
	s := h.reference()
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	primary, _, nodes := h.rolePods(s)
	primaryID := nodeIDForIP(nodes, primary.Status.PodIP)
	primary.Finalizers = append(primary.Finalizers, "cleanroom.redis-operator/hold-termination")
	if err := h.client.Update(h.ctx, &primary); err != nil {
		t.Fatal(err)
	}
	if err := h.client.Delete(h.ctx, &primary); err != nil {
		t.Fatal(err)
	}
	h.eventually("terminating primary relinquishes slots", topologyTimeout, func(ctx context.Context) error {
		pods := h.pods(s)
		for _, pod := range pods {
			if !podReady(pod) || pod.Name == primary.Name {
				continue
			}
			raw, err := h.redisInPod(ctx, pod, "CLUSTER", "NODES")
			if err != nil {
				return err
			}
			for _, node := range parseClusterNodes(raw) {
				if node.ID == primaryID && node.Primary && len(node.Slots) > 0 {
					return fmt.Errorf("terminating node %s still owns slots", primaryID)
				}
			}
			return nil
		}
		return fmt.Errorf("no healthy Redis view")
	})
	current := &corev1.Pod{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(&primary), current); err == nil {
		current.Finalizers = nil
		if err := h.client.Update(h.ctx, current); err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
	}
	h.waitReady(s, 3, 1)
	h.verifyData(s)
}

func TestCleanroom_DrainCandidateSafety(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("drain-candidate")
	h.createHeadlessService(s)
	rc := h.clusterFromReference(s)
	rc.Spec.Replicas = 2
	replicas := int32(9)
	rc.Spec.StatefulsetTemplate.Spec.Replicas = &replicas
	if err := h.client.Create(h.ctx, rc); err != nil {
		t.Fatalf("create drain-candidate RedisCluster: %v", err)
	}
	h.waitReady(s, 3, 2)
	h.seedAndVerify(s)

	pods := h.pods(s)
	raw, err := h.redisInPod(h.ctx, pods[0], "CLUSTER", "NODES")
	if err != nil {
		t.Fatal(err)
	}
	nodes := parseClusterNodes(raw)
	podsByIP := make(map[string]corev1.Pod, len(pods))
	for _, pod := range pods {
		podsByIP[pod.Status.PodIP] = pod
	}
	var primary, candidate corev1.Pod
	var primaryID, candidateID, siblingID string
	for _, node := range nodes {
		if !node.Primary || node.Failed {
			continue
		}
		var replicasForPrimary []redisNode
		for _, possibleReplica := range nodes {
			if !possibleReplica.Failed && !possibleReplica.Primary && possibleReplica.MasterID == node.ID {
				replicasForPrimary = append(replicasForPrimary, possibleReplica)
			}
		}
		if len(replicasForPrimary) != 2 {
			continue
		}
		primaryPod, primaryOK := podsByIP[node.Address]
		candidatePod, candidateOK := podsByIP[replicasForPrimary[0].Address]
		siblingPod, siblingOK := podsByIP[replicasForPrimary[1].Address]
		if !primaryOK || !candidateOK || !siblingOK || !podReady(candidatePod) || !podReady(siblingPod) {
			continue
		}
		primary, candidate = primaryPod, candidatePod
		primaryID, candidateID, siblingID = node.ID, replicasForPrimary[0].ID, replicasForPrimary[1].ID
		break
	}
	if primary.Name == "" {
		t.Fatalf("could not find a primary with two healthy replica siblings; nodes=%+v", nodes)
	}

	primaryUID := primary.UID
	candidate.Status.Conditions = append(candidate.Status.Conditions, corev1.PodCondition{
		Type: "NodeLifecycle", Status: corev1.ConditionTrue, Reason: "NodeDrainCandidate",
		Message: "portable clean-room drain-safety qualification", LastTransitionTime: metav1.Now(),
	})
	if err := h.client.Status().Update(h.ctx, &candidate); err != nil {
		t.Fatalf("mark drain candidate: %v", err)
	}
	const holdFinalizer = "cleanroom.redis-operator/hold-termination"
	primary.Finalizers = append(primary.Finalizers, holdFinalizer)
	if err := h.client.Update(h.ctx, &primary); err != nil {
		t.Fatalf("hold primary termination: %v", err)
	}
	releasePrimary := func(ctx context.Context) error {
		current := &corev1.Pod{}
		if err := h.client.Get(ctx, client.ObjectKeyFromObject(&primary), current); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return err
		}
		finalizers := current.Finalizers[:0]
		found := false
		for _, finalizer := range current.Finalizers {
			if finalizer == holdFinalizer {
				found = true
				continue
			}
			finalizers = append(finalizers, finalizer)
		}
		if !found {
			return nil
		}
		current.Finalizers = finalizers
		return h.client.Update(ctx, current)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := releasePrimary(ctx); err != nil {
			t.Errorf("release held primary during cleanup: %v", err)
		}
	})
	if err := h.client.Delete(h.ctx, &primary); err != nil {
		t.Fatal(err)
	}
	var lastTopologyError error
	topologyError := func(err error) error {
		lastTopologyError = err
		return err
	}
	h.eventually("healthy sibling promoted instead of drain candidate", topologyTimeout, func(ctx context.Context) error {
		raw, err := h.redisInPod(ctx, candidate, "CLUSTER", "NODES")
		if err != nil {
			if lastTopologyError != nil {
				return fmt.Errorf("last topology mismatch: %v; latest cluster query failed: %w", lastTopologyError, err)
			}
			return err
		}
		currentNodes := parseClusterNodes(raw)
		var currentCandidate, currentSibling *redisNode
		for i := range currentNodes {
			switch currentNodes[i].ID {
			case candidateID:
				currentCandidate = &currentNodes[i]
			case siblingID:
				currentSibling = &currentNodes[i]
			}
		}
		if currentCandidate == nil || currentSibling == nil {
			return topologyError(fmt.Errorf("candidate or sibling is missing from cluster view"))
		}
		if currentCandidate.Primary || currentCandidate.MasterID != siblingID {
			return topologyError(fmt.Errorf("drain candidate %s role primary=%t master=%s, want replica of %s", candidateID, currentCandidate.Primary, currentCandidate.MasterID, siblingID))
		}
		if !currentSibling.Primary || currentSibling.Failed {
			return topologyError(fmt.Errorf("healthy sibling %s was not promoted", siblingID))
		}
		primaries, replicas := topologyCounts(currentNodes)
		if primaries != 3 || replicas != 5 {
			return topologyError(fmt.Errorf("topology has %d primaries/%d replicas while former primary is held, want 3/5", primaries, replicas))
		}
		coverage, err := slotCoverage(currentNodes)
		if err != nil {
			return topologyError(err)
		}
		if coverage != totalRedisSlots {
			return topologyError(fmt.Errorf("slot coverage=%d, want %d", coverage, totalRedisSlots))
		}
		return nil
	})
	if err := releasePrimary(h.ctx); err != nil {
		t.Fatalf("release primary termination: %v", err)
	}
	h.eventually("former primary recreated after safe failover", readyTimeout, func(ctx context.Context) error {
		var recreatedPrimary corev1.Pod
		if err := h.client.Get(ctx, client.ObjectKeyFromObject(&primary), &recreatedPrimary); err != nil {
			return err
		}
		if recreatedPrimary.UID == primaryUID || !podReady(recreatedPrimary) {
			return fmt.Errorf("deleted primary %s has not been recreated and readied", primaryID)
		}
		return nil
	})
	h.waitReady(s, 3, 2)
	h.verifyData(s)
}

func TestCleanroom_OperatorRestartResume(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("operator-restart")
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	h.patchCluster(s, func(rc *cachev1.RedisCluster) {
		rc.Spec.Primaries = 4
		replicas := int32(8)
		rc.Spec.StatefulsetTemplate.Spec.Replicas = &replicas
	})
	var deployments appsv1.DeploymentList
	if err := h.client.List(h.ctx, &deployments, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: operatorComponent}); err != nil || len(deployments.Items) != 1 {
		t.Fatalf("find operator Deployment: count=%d err=%v", len(deployments.Items), err)
	}
	var operatorPods corev1.PodList
	if err := h.client.List(h.ctx, &operatorPods, client.InNamespace(s.Namespace), client.MatchingLabels(deployments.Items[0].Spec.Selector.MatchLabels)); err != nil || len(operatorPods.Items) == 0 {
		t.Fatalf("find operator pod: count=%d err=%v", len(operatorPods.Items), err)
	}
	if err := h.client.Delete(h.ctx, &operatorPods.Items[0]); err != nil {
		t.Fatal(err)
	}
	h.waitReady(s, 4, 1)
	h.verifyData(s)
}

func nodeIDForIP(nodes []redisNode, ip string) string {
	for _, node := range nodes {
		if node.Address == ip {
			return node.ID
		}
	}
	return ""
}
