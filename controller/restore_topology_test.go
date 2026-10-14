// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsClusterAlreadyFormedDuringPendingRestore(t *testing.T) {
	cluster := restoredTopologyTestCluster(restoredPrimary(0))
	cluster.RedisCluster = &cachev1.RedisCluster{}
	reconciler := &RedisClusterReconciler{}

	assert.False(t, reconciler.isClusterAlreadyFormed(cluster))

	cluster.RedisCluster.Status.BackupRestored = true
	assert.False(t, reconciler.isClusterAlreadyFormed(cluster))

	cluster.RedisCluster.Status.RestoreTopologyPending = true
	assert.True(t, reconciler.isClusterAlreadyFormed(cluster))
}

func TestRestorePathForClusterPreservesSelectedGeneration(t *testing.T) {
	cluster := &redis_cluster.Cluster{RedisCluster: &cachev1.RedisCluster{}}
	cluster.RedisCluster.Status.RestorePath = "cache/previous"
	assert.Equal(t, "cache/previous", restorePathForCluster(cluster))

	cluster.RedisCluster.Spec.RestoreFrom = "manual/backup"
	assert.Equal(t, "manual/backup", restorePathForCluster(cluster))
}

func TestReconcileRestoredSlotOwnershipRequiresPendingRestore(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0, redis_cluster.SlotsRange{First: 0, Last: 16383}),
	)
	cluster.RedisCluster = &cachev1.RedisCluster{}
	cluster.RedisCluster.Status.BackupRestored = true

	updated, err := (&RedisClusterReconciler{}).reconcileRestoredSlotOwnership(context.Background(), cluster, logr.Discard())

	require.NoError(t, err)
	assert.False(t, updated)
	assert.False(t, cluster.RedisCluster.Status.RestoreTopologyPending)
}

func TestReconcileRestoredSlotOwnershipClearsPendingAfterConvergence(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0, redis_cluster.SlotsRange{First: 0, Last: 16383}),
	)
	cluster.RedisCluster = &cachev1.RedisCluster{}
	cluster.RedisCluster.Spec.Primaries = 1
	cluster.RedisCluster.Status.BackupRestored = true
	cluster.RedisCluster.Status.RestoreTopologyPending = true

	updated, err := (&RedisClusterReconciler{}).reconcileRestoredSlotOwnership(context.Background(), cluster, logr.Discard())

	require.NoError(t, err)
	assert.False(t, updated)
	assert.False(t, cluster.RedisCluster.Status.RestoreTopologyPending)
}

func TestPlanRestoredSlotOwnershipUsesDataBearingPrimaries(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0),
		restoredPrimary(1, redis_cluster.SlotsRange{First: 100, Last: 100}),
		restoredPrimary(2, redis_cluster.SlotsRange{First: 7000, Last: 7000}),
		restoredPrimary(3),
		restoredPrimary(4),
		restoredPrimary(5, redis_cluster.SlotsRange{First: 12000, Last: 12000}),
	)
	coverage := map[int]string{
		0: "0-5460",
		2: "5461-10922",
		4: "10923-16383",
	}

	plans, err := planRestoredSlotOwnership(cluster, coverage)

	require.NoError(t, err)
	require.Len(t, plans, 3)
	assert.Equal(t, 0, plans[0].sourcePodIndex)
	assert.Equal(t, 1, plans[0].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{
		{First: 0, Last: 99},
		{First: 101, Last: 5460},
	}, plans[0].missing)
	assert.Equal(t, 2, plans[1].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{
		{First: 5461, Last: 6999},
		{First: 7001, Last: 10922},
	}, plans[1].missing)
	assert.Equal(t, 5, plans[2].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{
		{First: 10923, Last: 11999},
		{First: 12001, Last: 16383},
	}, plans[2].missing)

	candidates, err := restoredReplicaCandidates(cluster, plans)
	require.NoError(t, err)
	assert.Equal(t, []int{0, 3, 4}, restoredPodIndices(candidates))
}

func TestPlanRestoredSlotOwnershipFallsBackToSourcePrimaries(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0),
		restoredPrimary(1),
		restoredPrimary(2),
		restoredPrimary(3),
		restoredPrimary(4),
		restoredPrimary(5),
	)
	coverage := map[int]string{
		0: "0-5460",
		2: "5461-10922",
		4: "10923-16383",
	}

	plans, err := planRestoredSlotOwnership(cluster, coverage)

	require.NoError(t, err)
	require.Len(t, plans, 3)
	assert.Equal(t, 0, plans[0].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{{First: 0, Last: 5460}}, plans[0].missing)
	assert.Equal(t, 2, plans[1].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{{First: 5461, Last: 10922}}, plans[1].missing)
	assert.Equal(t, 4, plans[2].target.PodIndex)
	assert.Equal(t, []redis_cluster.SlotsRange{{First: 10923, Last: 16383}}, plans[2].missing)
}

func TestPlanRestoredSlotOwnershipRejectsSplitShard(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0, redis_cluster.SlotsRange{First: 1, Last: 1}),
		restoredPrimary(1, redis_cluster.SlotsRange{First: 2, Last: 2}),
	)

	_, err := planRestoredSlotOwnership(cluster, map[int]string{0: "0-16383"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "slots on 2 primaries")
}

func TestPlanRestoredSlotOwnershipRejectsOnePrimaryForTwoShards(t *testing.T) {
	cluster := restoredTopologyTestCluster(
		restoredPrimary(0,
			redis_cluster.SlotsRange{First: 1, Last: 1},
			redis_cluster.SlotsRange{First: 6000, Last: 6000}),
		restoredPrimary(1),
	)

	_, err := planRestoredSlotOwnership(cluster, map[int]string{
		0: "0-5460",
		1: "5461-16383",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "matches manifest shards")
}

func TestRestoredReplicaCandidatesRejectsNonTargetWithSlots(t *testing.T) {
	target := restoredPrimary(0, redis_cluster.SlotsRange{First: 0, Last: 100})
	nonTarget := restoredPrimary(1, redis_cluster.SlotsRange{First: 101, Last: 200})
	cluster := restoredTopologyTestCluster(target, nonTarget)

	_, err := restoredReplicaCandidates(cluster, []restoredShardPlan{{target: target}})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "still owns slots")
}

func restoredPrimary(podIndex int, slots ...redis_cluster.SlotsRange) *redis_cluster.ClusterNode {
	return &redis_cluster.ClusterNode{
		PodName:     "redis-" + string(rune('0'+podIndex)),
		PodIndex:    podIndex,
		Role:        redis_cluster.PrimaryNode,
		SlotsRanges: slots,
	}
}

func restoredPodIndices(nodes []*redis_cluster.ClusterNode) []int {
	indices := make([]int, len(nodes))
	for i, node := range nodes {
		indices[i] = node.PodIndex
	}
	return indices
}

func restoredTopologyTestCluster(nodes ...*redis_cluster.ClusterNode) *redis_cluster.Cluster {
	return &redis_cluster.Cluster{
		AllNodes: nodes,
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: nodes,
		},
	}
}
