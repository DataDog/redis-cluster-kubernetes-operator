// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const terminationHold = 45 * time.Second

// TestE2E_TerminatingPrimary verifies that the portable controller promotes a
// healthy replica and forgets the old primary while Kubernetes terminates it.
func TestE2E_TerminatingPrimary(t *testing.T) {
	s := newTerminationCluster(t)
	victim := pickConsensusPrimary(t, s)
	oldUID := s.PodUID(t, victim.podName)
	require.NotEmpty(t, oldUID)

	beginPodTermination(t, s, victim.podName)
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	s.EventuallyErrorT(t, "terminating primary demoted or forgotten", failoverTimeout, func() error {
		return requireNodeNotPrimaryInEveryView(s, victim.nodeID)
	})

	finishPodTermination(t, s, victim.podName)
	s.WaitPodRecreated(t, victim.podName, oldUID, failoverTimeout)
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
	s.RequireTopology(t, clusterName, redisbuilder.DefaultPrimaries, redisbuilder.DefaultReplicas, failoverTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}

// TestE2E_TerminatingReplica verifies that the portable controller forgets a
// terminating replica before Kubernetes replaces it.
func TestE2E_TerminatingReplica(t *testing.T) {
	s := newTerminationCluster(t)
	victim := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	victimID := s.RedisNodeIDForPod(t, victim)
	oldUID := s.PodUID(t, victim)
	require.NotEmpty(t, oldUID)

	beginPodTermination(t, s, victim)
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	s.EventuallyErrorT(t, "terminating replica forgotten", failoverTimeout, func() error {
		return requireNodeAbsentInEveryView(s, victimID)
	})

	finishPodTermination(t, s, victim)
	s.WaitPodRecreated(t, victim, oldUID, failoverTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}

type terminatingVictim struct {
	podName string
	nodeID  string
}

func newTerminationCluster(t *testing.T) *rckind.Suite {
	t.Helper()
	s := rckind.NewSuite(t)
	rc := rckind.NewRedisCluster(clusterName, s.Namespace)
	grace := int64(60)
	rc.Spec.StatefulsetTemplate.Spec.Template.Spec.TerminationGracePeriodSeconds = &grace
	redis := &rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0]
	redis.Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{
		Exec: &corev1.ExecAction{Command: []string{"sleep", fmt.Sprintf("%.0f", terminationHold.Seconds())}},
	}}
	s.CreateCluster(t, rc, clusterReadyTimeout)
	return s
}

// beginPodTermination leaves the Redis process alive in its fixed preStop
// delay, giving the controller a deterministic window to observe the standard
// Kubernetes deletion timestamp and coordinate topology safety.
func beginPodTermination(t *testing.T, s *rckind.Suite, podName string) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: s.Namespace}}
	require.NoError(t, s.Client.Delete(s.Ctx, pod, &client.DeleteOptions{GracePeriodSeconds: ptr.To(int64(60))}))
	s.EventuallyT(t, "pod enters Terminating", topologySettleTimeout, func() bool {
		current := &corev1.Pod{}
		return s.Client.Get(s.Ctx, types.NamespacedName{Namespace: s.Namespace, Name: podName}, current) == nil &&
			current.DeletionTimestamp != nil
	})
}

// finishPodTermination shortens the already-safe pod's grace period so the
// scenario does not spend the full preStop delay waiting for replacement.
func finishPodTermination(t *testing.T, s *rckind.Suite, podName string) {
	t.Helper()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: s.Namespace}}
	err := s.Client.Delete(s.Ctx, pod, &client.DeleteOptions{GracePeriodSeconds: ptr.To(int64(0))})
	require.NoError(t, client.IgnoreNotFound(err))
}

// pickConsensusPrimary returns a primary reported by every live Redis node.
func pickConsensusPrimary(t *testing.T, s *rckind.Suite) terminatingVictim {
	t.Helper()
	var victim terminatingVictim
	s.EventuallyErrorT(t, "find primary agreed by every Redis view", topologySettleTimeout, func() error {
		views, err := s.TryClusterViews(clusterName)
		if err != nil {
			return err
		}
		if len(views) == 0 {
			return fmt.Errorf("Redis cluster returned no node views")
		}
		ready := make(map[string]bool, len(views))
		podByIP := make(map[string]string, len(views))
		for _, view := range views {
			ready[view.ViewerIP] = true
			podByIP[view.ViewerIP] = view.ViewerPod
		}
		for _, candidate := range views[0].Nodes {
			if !candidate.IsMaster || candidate.Failed || !ready[candidate.IP] || podByIP[candidate.IP] == "" {
				continue
			}
			agreed := true
			for _, view := range views[1:] {
				if node, ok := rckind.FindNodeByID(view.Nodes, candidate.ID); !ok || node.Failed || !node.IsMaster {
					agreed = false
					break
				}
			}
			if agreed {
				victim = terminatingVictim{podName: podByIP[candidate.IP], nodeID: candidate.ID}
				return nil
			}
		}
		return fmt.Errorf("no live primary has converged across %d Redis views", len(views))
	})
	return victim
}

func requireNodeNotPrimaryInEveryView(s *rckind.Suite, nodeID string) error {
	views, err := s.TryClusterViews(clusterName)
	if err != nil {
		return err
	}
	for _, view := range views {
		if node, ok := rckind.FindNodeByID(view.Nodes, nodeID); ok && !node.Failed && node.IsMaster {
			return fmt.Errorf("node %s remains primary in Redis view %s", nodeID, view.ViewerIP)
		}
	}
	return nil
}

func requireNodeAbsentInEveryView(s *rckind.Suite, nodeID string) error {
	views, err := s.TryClusterViews(clusterName)
	if err != nil {
		return err
	}
	for _, view := range views {
		if _, ok := rckind.FindNodeByID(view.Nodes, nodeID); ok {
			return fmt.Errorf("node %s remains in Redis view %s", nodeID, view.ViewerIP)
		}
	}
	return nil
}
