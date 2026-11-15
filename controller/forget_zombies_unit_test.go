// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/DataDog/redis-cluster-kubernetes-operator/timeouts"
)

const (
	zombieTestPrimaryID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	zombieTestStaleID   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// newZombieTestCluster builds a cluster with a healthy primary, a pod (cache-1)
// whose Redis has not reported a node ID yet, and a zombie entry that peers still
// list at cache-1's previous IP. podAge is how long ago cache-1 was created.
func newZombieTestCluster(stub *toolbox.Stub, podAge time.Duration) *redis_cluster.Cluster {
	now := time.Now()
	primary := &redis_cluster.ClusterNode{
		PodName: "cache-0", PodIndex: 0, PodCreation: now.Add(-24 * time.Hour).Unix(),
		IP: "10.0.0.1", ID: zombieTestPrimaryID, Role: redis_cluster.PrimaryNode,
	}
	starting := &redis_cluster.ClusterNode{
		PodName: "cache-1", PodIndex: 1, PodCreation: now.Add(-podAge).Unix(),
		IP: "10.0.0.3", Role: redis_cluster.OtherNode,
	}
	staleView := &redis_cluster.ClusterNode{
		PodIndex: -1, IP: "10.0.0.9", ID: zombieTestStaleID, Role: redis_cluster.ZombieNode,
	}
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary, starting, staleView})
	cluster.RedisPort = 6379
	cluster.Timeouts = timeouts.NewConfig(3)
	cluster.RedisCluster = newTestRedisCluster()
	cluster.Toolbox = redis_cluster.NewToolboxForTesting(context.Background(), stub, 6379)
	return cluster
}

// forgetCalls returns the CLUSTER FORGET commands the stub received.
func forgetCalls(stub *toolbox.Stub) []string {
	var calls []string
	for _, c := range stub.Calls() {
		cmd := strings.Join(c.Command, " ")
		if strings.Contains(strings.ToUpper(cmd), "CLUSTER FORGET") {
			calls = append(calls, cmd)
		}
	}
	return calls
}

// A recreated pod whose Redis is still starting has no node ID yet, while peers
// still list its (live) ID at the old IP, which surfaces as a zombie. The operator
// must not FORGET it, and must say which pod is blocking cleanup.
func TestForgetZombiesDefersWhileNewPodHasNoNodeID(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("FORGET", toolbox.Stdout("OK"))
	cluster := newZombieTestCluster(stub, 10*time.Second)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	got, err := r.forgetZombies(context.Background(), cluster, logr.Discard())
	require.NoError(t, err)
	require.Same(t, cluster, got)
	assert.Empty(t, forgetCalls(stub), "no CLUSTER FORGET may be sent while a new pod has no node ID")

	deferred, ok := rec.findBadSituation("zombie_forget_deferred_pod_without_id")
	require.True(t, ok)
	assert.Equal(t, []string{"pod_name:cache-1"}, deferred.tags)
}

// A pod that stays Running but never reports a node ID (crash loop, broken
// nodes.conf) must not block zombie cleanup forever: past the startup grace
// period the zombie is forgotten.
func TestForgetZombiesProceedsWhenPodWithoutIDIsPastGrace(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("FORGET", toolbox.Stdout("OK"))
	cluster := newZombieTestCluster(stub, time.Hour)
	require.Greater(t, time.Hour, cluster.Timeouts.OperationTimeout)
	rec := &recordingMetrics{}
	// The fake client has no RedisCluster object, so the refresh after FORGET
	// fails; this test only checks that FORGET was sent.
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	_, err := r.forgetZombies(context.Background(), cluster, logr.Discard())
	require.ErrorContains(t, err, "failed to refresh cluster")

	calls := forgetCalls(stub)
	require.Len(t, calls, 1, "the zombie must be forgotten by the one healthy primary")
	assert.Contains(t, calls[0], "-h 10.0.0.1")
	assert.Contains(t, calls[0], zombieTestStaleID)

	_, deferred := rec.findBadSituation("zombie_forget_deferred_pod_without_id")
	assert.False(t, deferred)
	stuck, ok := rec.findBadSituation("zombie_forget_pod_without_id_past_grace")
	require.True(t, ok)
	assert.Equal(t, []string{"pod_name:cache-1"}, stuck.tags)
}

func TestPodNodesWithoutID(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	grace := 5 * time.Minute
	cluster := &redis_cluster.Cluster{AllNodes: []*redis_cluster.ClusterNode{
		{PodName: "cache-0", PodIndex: 0, PodCreation: now.Add(-time.Minute).Unix(), ID: zombieTestPrimaryID},
		{PodName: "cache-1", PodIndex: 1, PodCreation: now.Add(-time.Minute).Unix()},
		{PodName: "cache-2", PodIndex: 2, PodCreation: now.Add(-time.Hour).Unix()},
		{PodName: "cache-3", PodIndex: 3},
		{PodIndex: -1, ID: zombieTestStaleID},
		nil,
	}}

	starting, stuck := podNodesWithoutID(cluster, now, grace)
	assert.Equal(t, []string{"cache-1"}, podNames(starting))
	assert.Equal(t, []string{"cache-2", "cache-3"}, podNames(stuck), "old pods and pods with an unknown creation time are not deferred")

	for _, n := range cluster.AllNodes[1:4] {
		n.ID = "cccccccccccccccccccccccccccccccccccccccc"
	}
	starting, stuck = podNodesWithoutID(cluster, now, grace)
	assert.Empty(t, starting)
	assert.Empty(t, stuck)
}
