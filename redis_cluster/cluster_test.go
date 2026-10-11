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
