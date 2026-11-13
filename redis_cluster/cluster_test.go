// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeRedisNodesKeepsUnavailableMemberFromHealthyPeerView(t *testing.T) {
	const unavailableIP = "10.0.0.1"
	const healthyIP = "10.0.0.2"
	stub := toolbox.New().
		OnCommand("unavailable", func(_, _ string, command []string) bool {
			return strings.Contains(strings.Join(command, " "), unavailableIP)
		}, toolbox.Failure(1, "connection refused")).
		OnCommand("cluster-info", func(_, _ string, command []string) bool {
			joined := strings.Join(command, " ")
			return strings.Contains(joined, healthyIP) && strings.Contains(joined, "CLUSTER INFO")
		}, toolbox.Stdout("cluster_state:fail\ncluster_known_nodes:2\ncluster_size:1\n")).
		OnCommand("cluster-nodes", func(_, _ string, command []string) bool {
			joined := strings.Join(command, " ")
			return strings.Contains(joined, healthyIP) && strings.Contains(joined, "CLUSTER NODES")
		}, toolbox.Stdout(
			testReplicaID+" "+healthyIP+":6379@16379,cache-1 slave,myself "+testPrimaryID+" 0 0 2 connected\n"+
				testPrimaryID+" "+unavailableIP+":6379@16379,cache-0 master,fail - 0 0 1 disconnected 0-16383\n",
		))
	tb := newToolboxWithStub(stub)
	nodes := []*ClusterNode{
		{PodName: "cache-0", PodIndex: 0, IP: unavailableIP, Views: make(map[int]*NodeView)},
		{PodName: "cache-1", PodIndex: 1, IP: healthyIP, Views: make(map[int]*NodeView)},
	}

	merged, err := mergeRedisNodes(tb, nodes)
	require.NoError(t, err)
	require.Len(t, merged, 2)
	assert.Equal(t, testPrimaryID, merged[0].ID)
	assert.Contains(t, merged[0].Flags, "master")
	assert.Contains(t, merged[0].Flags, "fail")
	assert.Equal(t, FailingNode, merged[0].determineRole())
	assert.Equal(t, testReplicaID, merged[1].ID)
	assert.Equal(t, ReplicaNode, merged[1].determineRole())
}

func TestMergeRedisNodesFailsWhenNoProcessReturnsClusterView(t *testing.T) {
	stub := toolbox.New().OnCommand("unavailable", func(_, _ string, _ []string) bool { return true }, toolbox.Failure(1, "connection refused"))
	tb := newToolboxWithStub(stub)
	nodes := []*ClusterNode{{PodName: "cache-0", PodIndex: 0, IP: "10.0.0.1", Views: make(map[int]*NodeView)}}

	_, err := mergeRedisNodes(tb, nodes)
	require.ErrorContains(t, err, "no Redis process returned a usable CLUSTER NODES view")
}

// A pod recreated with a new IP keeps its node ID. Until gossip catches up, a
// lower-index peer still lists it at the old IP. That entry must be folded into
// the live pod, not reported as a zombie whose ID would then be forgotten.
func TestMergeRedisNodesDoesNotTreatLiveNodeAtStaleAddressAsZombie(t *testing.T) {
	const primaryIP, replicaIP, staleReplicaIP = "10.0.0.1", "10.0.0.3", "10.0.0.9"
	on := func(ip, sub string) func(_, _ string, command []string) bool {
		return func(_, _ string, command []string) bool {
			joined := strings.Join(command, " ")
			return strings.Contains(joined, ip) && strings.Contains(joined, sub)
		}
	}
	info := "cluster_state:ok\ncluster_known_nodes:2\ncluster_size:1\n"
	stub := toolbox.New().
		OnCommand("info-0", on(primaryIP, "CLUSTER INFO"), toolbox.Stdout(info)).
		OnCommand("info-1", on(replicaIP, "CLUSTER INFO"), toolbox.Stdout(info)).
		OnCommand("nodes-0", on(primaryIP, "CLUSTER NODES"), toolbox.Stdout(
			testPrimaryID+" "+primaryIP+":6379@16379 myself,master - 0 0 1 connected 0-16383\n"+
				testReplicaID+" "+staleReplicaIP+":6379@16379 slave "+testPrimaryID+" 0 0 1 connected\n")).
		OnCommand("nodes-1", on(replicaIP, "CLUSTER NODES"), toolbox.Stdout(
			testReplicaID+" "+replicaIP+":6379@16379 myself,slave "+testPrimaryID+" 0 0 1 connected\n"+
				testPrimaryID+" "+primaryIP+":6379@16379 master - 0 0 1 connected 0-16383\n"))
	tb := newToolboxWithStub(stub)
	nodes := []*ClusterNode{
		{PodName: "cache-0", PodIndex: 0, IP: primaryIP, Views: make(map[int]*NodeView)},
		{PodName: "cache-1", PodIndex: 1, IP: replicaIP, Views: make(map[int]*NodeView)},
	}

	merged, err := mergeRedisNodes(tb, nodes)
	require.NoError(t, err)
	require.Len(t, merged, 2, "stale peer entry must not survive as a separate node")
	groups := groupNodes(merged)
	assert.Empty(t, groups[ZombieNode])
	assert.Equal(t, testReplicaID, merged[1].ID)
	assert.Equal(t, ReplicaNode, merged[1].Role)
	require.Contains(t, merged[1].Views, 0, "peer view of the live node is kept")
	assert.Equal(t, testPrimaryID, merged[1].Views[0].Master)
}

func TestMergeRedisNodesStillReportsRealZombie(t *testing.T) {
	const primaryIP = "10.0.0.1"
	const goneID = "cccccccccccccccccccccccccccccccccccccccc"
	stub := toolbox.New().
		OnCommand("info", func(_, _ string, command []string) bool {
			return strings.Contains(strings.Join(command, " "), "CLUSTER INFO")
		}, toolbox.Stdout("cluster_state:ok\ncluster_known_nodes:2\ncluster_size:1\n")).
		OnCommand("nodes", func(_, _ string, command []string) bool {
			return strings.Contains(strings.Join(command, " "), "CLUSTER NODES")
		}, toolbox.Stdout(
			testPrimaryID+" "+primaryIP+":6379@16379 myself,master - 0 0 1 connected 0-16383\n"+
				goneID+" 10.0.0.42:6379@16379 slave,fail "+testPrimaryID+" 0 0 1 disconnected\n"))
	tb := newToolboxWithStub(stub)
	nodes := []*ClusterNode{{PodName: "cache-0", PodIndex: 0, IP: primaryIP, Views: make(map[int]*NodeView)}}

	merged, err := mergeRedisNodes(tb, nodes)
	require.NoError(t, err)
	groups := groupNodes(merged)
	require.Len(t, groups[ZombieNode], 1)
	assert.Equal(t, goneID, groups[ZombieNode][0].ID)
}
