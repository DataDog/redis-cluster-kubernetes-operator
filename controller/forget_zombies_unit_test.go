// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
)

// A recreated pod whose Redis is still starting has no node ID yet, while peers
// still list its (live) ID at the old IP, which surfaces as a zombie. The operator
// must not FORGET it. The Cluster has no Toolbox, so any FORGET attempt would panic.
func TestForgetZombiesDefersWhilePodHasNoNodeID(t *testing.T) {
	primary := &redis_cluster.ClusterNode{PodName: "cache-0", PodIndex: 0, IP: "10.0.0.1", ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	starting := &redis_cluster.ClusterNode{PodName: "cache-1", PodIndex: 1, IP: "10.0.0.3"}
	staleView := &redis_cluster.ClusterNode{PodIndex: -1, IP: "10.0.0.9", ID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	cluster := &redis_cluster.Cluster{
		AllNodes: []*redis_cluster.ClusterNode{primary, starting, staleView},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {primary},
			redis_cluster.OtherNode:   {starting},
			redis_cluster.ZombieNode:  {staleView},
		},
	}
	r := &RedisClusterReconciler{ClusterClient: redis_cluster.ClusterClient{Metrics: metrics.CreateNoop()}}

	got, err := r.forgetZombies(context.Background(), cluster, logr.Discard())
	require.NoError(t, err)
	require.Same(t, cluster, got)
}

func TestPodNodesWithoutID(t *testing.T) {
	cluster := &redis_cluster.Cluster{AllNodes: []*redis_cluster.ClusterNode{
		{PodName: "cache-0", PodIndex: 0, ID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{PodName: "cache-1", PodIndex: 1},
		{PodIndex: -1, ID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	}}
	require.Equal(t, []string{"cache-1"}, podNodesWithoutID(cluster))

	cluster.AllNodes[1].ID = "cccccccccccccccccccccccccccccccccccccccc"
	require.Empty(t, podNodesWithoutID(cluster))
}
