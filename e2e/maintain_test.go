// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
)

// Maintenance tests assert recovered topology rather than operator Events.
// Quorum-loss coverage still needs a dedicated status condition.

// TestE2E_Maintain_EmptyPrimaryToReplica resets a replica into an empty primary
// (CLUSTER RESET SOFT) and asserts maintain converts it back, preserving slot
// coverage and replica topology.
func TestE2E_Maintain_EmptyPrimaryToReplica(t *testing.T) {
	s := newCluster(t, redisbuilder.WithReplicas(1))

	victim := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	s.RedisCommandAtIP(t, s.PodIP(t, victim), "CLUSTER", "RESET", "SOFT")
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)

	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}

// TestE2E_Maintain_RebalanceBeforeAddReplicas grows primaries + replicas at once
// and asserts maintain rebalances slots without a blue-green, keeping coverage
// and replica topology intact.
func TestE2E_Maintain_RebalanceBeforeAddReplicas(t *testing.T) {
	requireSlow(t, "slow: 0→1 grow + add-primary + rebalance convergence (~1-2 min)")
	s := newCluster(t, redisbuilder.WithReplicas(0))

	s.PatchTopology(t, clusterName, scaleTargetPrimaries, 1)

	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireTopology(t, clusterName, scaleTargetPrimaries, 1, scaleConvergeTimeout)
	s.RequireIdle(t, clusterName)
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}

// TestE2E_Maintain_RecreatedPodKeepsNodeID force-deletes a replica and asserts
// no peer ever forgets its node ID: the pod comes back with the same nodes.conf,
// so its old ID is not a zombie even while the new pod is not Running yet.
func TestE2E_Maintain_RecreatedPodKeepsNodeID(t *testing.T) {
	s := newCluster(t, redisbuilder.WithReplicas(1))

	victim := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	nodeID := s.RedisNodeIDForPod(t, victim)
	oldUID := s.PodUID(t, victim)
	require.NotEmpty(t, oldUID)

	s.ForceDeleteRedisPod(t, victim)
	var forgottenBy []string
	s.EventuallyErrorT(t, "peers list the recreated pod's node ID at its new IP", failoverTimeout, func() error {
		views, err := s.TryClusterViews(clusterName)
		if err != nil {
			return err
		}
		newIP := ""
		if uid := s.PodUID(t, victim); uid != "" && uid != oldUID {
			newIP = s.PodIP(t, victim)
		}
		for _, view := range views {
			if view.ViewerPod == victim {
				continue
			}
			node, ok := rckind.FindNodeByID(view.Nodes, nodeID)
			if !ok {
				forgottenBy = append(forgottenBy, view.ViewerPod)
				return nil
			}
			if newIP == "" || node.IP != newIP {
				return fmt.Errorf("%s lists node %s at %q, want new pod IP %q", view.ViewerPod, nodeID, node.IP, newIP)
			}
		}
		return nil
	})
	require.Empty(t, forgottenBy, "peers forgot node %s of recreated pod %s", nodeID, victim)
	require.Equal(t, nodeID, s.RedisNodeIDForPod(t, victim), "recreated pod did not keep its node ID")

	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}

// TestE2E_Maintain_ZombieCleanupAndPersist gives a replica a new node ID
// (CLUSTER RESET HARD rewrites nodes.conf) and force-deletes it, so peers keep
// its old ID at an address no pod owns. It asserts maintain forgets that zombie,
// the reset node rejoins, and the zombie stays forgotten.
func TestE2E_Maintain_ZombieCleanupAndPersist(t *testing.T) {
	s := newCluster(t, redisbuilder.WithReplicas(1))

	victim := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	oldNodeID := s.RedisNodeIDForPod(t, victim)
	oldUID := s.PodUID(t, victim)
	require.NotEmpty(t, oldUID)

	require.Equal(t, "OK", strings.TrimSpace(s.RedisCommandAtIP(t, s.PodIP(t, victim), "CLUSTER", "RESET", "HARD")))
	s.ForceDeleteRedisPod(t, victim)
	s.WaitPodRecreated(t, victim, oldUID, failoverTimeout)
	require.NotEqual(t, oldNodeID, s.RedisNodeIDForPod(t, victim), "reset pod kept its old node ID")
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)

	s.EventuallyErrorT(t, "old Redis node ID forgotten", failoverTimeout, func() error {
		return requireNodeAbsentInPeerViews(s, oldNodeID, "")
	})
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	require.NoError(t, requireNodeAbsentInPeerViews(s, oldNodeID, ""), "forgotten zombie came back")
}
