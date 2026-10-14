// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"strings"
	"testing"

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

// TestE2E_Maintain_ZombieCleanupAndPersist force-deletes a replica, leaving its
// node ID in gossip, and asserts maintain forgets the zombie node ID and the
// cluster re-converges.
func TestE2E_Maintain_ZombieCleanupAndPersist(t *testing.T) {
	s := newCluster(t, redisbuilder.WithReplicas(1))

	victim := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	oldNodeID := s.RedisNodeIDForPod(t, victim)

	s.ForceDeleteRedisPod(t, victim)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)

	s.EventuallyT(t, "old Redis node ID forgotten", failoverTimeout, func() bool {
		out, err := s.TryClusterNodes()
		return err == nil && !strings.Contains(out, oldNodeID)
	})
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
}
