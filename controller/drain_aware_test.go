// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── orderReplicasNonDrainingFirst ──

func TestOrderReplicasNonDrainingFirst_Empty(t *testing.T) {
	got := orderReplicasNonDrainingFirst(nil)
	assert.Empty(t, got)
}

func TestOrderReplicasNonDrainingFirst_AllHealthy(t *testing.T) {
	a := &redis_cluster.ClusterNode{ID: "a"}
	b := &redis_cluster.ClusterNode{ID: "b"}
	got := orderReplicasNonDrainingFirst([]*redis_cluster.ClusterNode{a, b})
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].ID)
	assert.Equal(t, "b", got[1].ID, "stable order preserved when no drains")
}

func TestOrderReplicasNonDrainingFirst_AllDraining(t *testing.T) {
	a := &redis_cluster.ClusterNode{ID: "a", IsTerminating: true}
	b := &redis_cluster.ClusterNode{ID: "b", HasDrainCandidateCond: true}
	got := orderReplicasNonDrainingFirst([]*redis_cluster.ClusterNode{a, b})
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].ID, "stable order preserved when all draining")
	assert.Equal(t, "b", got[1].ID)
}

func TestOrderReplicasNonDrainingFirst_Mixed(t *testing.T) {
	draining := &redis_cluster.ClusterNode{ID: "draining", IsTerminating: true}
	healthy1 := &redis_cluster.ClusterNode{ID: "healthy1"}
	healthy2 := &redis_cluster.ClusterNode{ID: "healthy2"}

	// Draining is first in input but should land last in output.
	got := orderReplicasNonDrainingFirst([]*redis_cluster.ClusterNode{draining, healthy1, healthy2})
	require.Len(t, got, 3)
	assert.Equal(t, "healthy1", got[0].ID)
	assert.Equal(t, "healthy2", got[1].ID)
	assert.Equal(t, "draining", got[2].ID)
}

func TestOrderReplicasNonDrainingFirst_DoesNotMutateInput(t *testing.T) {
	a := &redis_cluster.ClusterNode{ID: "a", IsTerminating: true}
	b := &redis_cluster.ClusterNode{ID: "b"}
	input := []*redis_cluster.ClusterNode{a, b}
	_ = orderReplicasNonDrainingFirst(input)
	// Input slice unchanged.
	assert.Equal(t, "a", input[0].ID)
	assert.Equal(t, "b", input[1].ID)
}

// ── getReplicaPrimaryPair drain-aware preference ──

func TestGetReplicaPrimaryPair_PrefersNonDrainingReplica(t *testing.T) {
	// First replica drain-marked, second healthy — pass 1 must return the healthy one.
	primary := &redis_cluster.ClusterNode{ID: "p1", Role: redis_cluster.PrimaryNode, PodIndex: 0}
	drainingReplica := &redis_cluster.ClusterNode{ID: "r-drain", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", PodIndex: 1, IsTerminating: true}
	healthyReplica := &redis_cluster.ClusterNode{ID: "r-healthy", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", PodIndex: 2}

	cluster := &redis_cluster.Cluster{
		AllNodes: []*redis_cluster.ClusterNode{primary, drainingReplica, healthyReplica},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {primary},
			redis_cluster.ReplicaNode: {drainingReplica, healthyReplica},
		},
	}

	found, replica, returnedPrimary := getReplicaPrimaryPair(cluster, 3 /*targetPodCount*/, true /*replicaIsGreen*/, true /*primaryIsGreen*/)
	require.True(t, found)
	assert.Equal(t, "r-healthy", replica.ID, "drain-marked replica must not be returned when a healthy one is available")
	assert.Equal(t, "p1", returnedPrimary.ID)
}

func TestGetReplicaPrimaryPair_FallsBackToDrainingWhenAllDraining(t *testing.T) {
	// Only candidate is drain-marked — pass 2 must fall back and return it.
	primary := &redis_cluster.ClusterNode{ID: "p1", Role: redis_cluster.PrimaryNode, PodIndex: 0}
	drainingReplica := &redis_cluster.ClusterNode{ID: "r-drain", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", PodIndex: 1, IsTerminating: true}

	cluster := &redis_cluster.Cluster{
		AllNodes: []*redis_cluster.ClusterNode{primary, drainingReplica},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {primary},
			redis_cluster.ReplicaNode: {drainingReplica},
		},
	}

	found, replica, _ := getReplicaPrimaryPair(cluster, 3, true, true)
	require.True(t, found, "must fall back to draining replica rather than returning not-found")
	assert.Equal(t, "r-drain", replica.ID)
}

func TestGetReplicaPrimaryPair_NoMatchingPair(t *testing.T) {
	// All replicas are RED but caller asks for GREEN — no pair.
	primary := &redis_cluster.ClusterNode{ID: "p1", Role: redis_cluster.PrimaryNode, PodIndex: 0}
	redReplica := &redis_cluster.ClusterNode{ID: "r1", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", PodIndex: 5}

	cluster := &redis_cluster.Cluster{
		AllNodes: []*redis_cluster.ClusterNode{primary, redReplica},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {primary},
			redis_cluster.ReplicaNode: {redReplica},
		},
	}

	found, _, _ := getReplicaPrimaryPair(cluster, 3, true /*replicaIsGreen*/, true)
	assert.False(t, found)
}
