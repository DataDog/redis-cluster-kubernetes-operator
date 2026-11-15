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
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/DataDog/redis-cluster-kubernetes-operator/timeouts"
)

const (
	zombieTestPrimaryID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	zombieTestStaleID   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	zombieTestReplicaID = "cccccccccccccccccccccccccccccccccccccccc"
)

// newZombieTestCluster builds a cluster with a healthy primary, a pod (cache-1)
// whose Redis has not reported a node ID yet, and a zombie entry that peers still
// list at cache-1's previous IP.
func newZombieTestCluster(stub *toolbox.Stub) *redis_cluster.Cluster {
	primary := &redis_cluster.ClusterNode{
		PodName: "cache-0", PodIndex: 0, IP: "10.0.0.1", ID: zombieTestPrimaryID, Role: redis_cluster.PrimaryNode,
	}
	starting := &redis_cluster.ClusterNode{
		PodName: "cache-1", PodIndex: 1, IP: "10.0.0.3", Role: redis_cluster.OtherNode,
	}
	return newZombieTestClusterWith(stub, primary, starting)
}

// newZombieTestClusterWith builds a two-pod cluster from the given pod-backed
// nodes plus a zombie entry at 10.0.0.9.
func newZombieTestClusterWith(stub *toolbox.Stub, podNodes ...*redis_cluster.ClusterNode) *redis_cluster.Cluster {
	staleView := &redis_cluster.ClusterNode{
		PodIndex: -1, IP: "10.0.0.9", ID: zombieTestStaleID, Role: redis_cluster.ZombieNode,
	}
	cluster := newClusterWithNodes(append(podNodes, staleView))
	cluster.StatefulSet = &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "cache"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(2))},
	}
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
	cluster := newZombieTestCluster(stub)
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

// A force-deleted pod is listed only once it is Running with an IP. Until then
// its ID, still listed by peers at the old IP, has no pod and looks like a
// zombie, but the recreated pod loads the same nodes.conf and comes back with it.
func TestForgetZombiesDefersWhileRecreatedPodIsNotRunning(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("FORGET", toolbox.Stdout("OK"))
	cluster := newZombieTestClusterWith(stub, &redis_cluster.ClusterNode{
		PodName: "cache-0", PodIndex: 0, IP: "10.0.0.1", ID: zombieTestPrimaryID, Role: redis_cluster.PrimaryNode,
	})
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	_, err := r.forgetZombies(context.Background(), cluster, logr.Discard())
	assert.Empty(t, forgetCalls(stub), "no CLUSTER FORGET may be sent while an expected pod is not Running yet")
	require.NoError(t, err)

	deferred, ok := rec.findBadSituation("zombie_forget_deferred_pod_without_id")
	require.True(t, ok)
	assert.Equal(t, []string{"pod_name:cache-1"}, deferred.tags)
}

// A pod that never reports a node ID (crash loop, broken nodes.conf, stuck
// Pending) must not block zombie cleanup forever: once the zombie has been seen
// for the grace period, it is forgotten.
func TestForgetZombiesProceedsWhenZombieOutlivesGrace(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("FORGET", toolbox.Stdout("OK"))
	cluster := newZombieTestCluster(stub)
	require.Greater(t, time.Hour, cluster.Timeouts.OperationTimeout)
	rec := &recordingMetrics{}
	// The fake client has no RedisCluster object, so the refresh after FORGET
	// fails; this test only checks that FORGET was sent.
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)
	r.zombieTracker().Observe(zombieTrackerKey(cluster), []string{zombieTestStaleID}, time.Now().Add(-time.Hour))

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

// When every expected pod reports its own node ID, no live pod can own the
// zombie, so it is forgotten on first sight.
func TestForgetZombiesForgetsAtOnceWhenEveryPodReportsID(t *testing.T) {
	stub := toolbox.New().OnRedisCommand("FORGET", toolbox.Stdout("OK"))
	cluster := newZombieTestClusterWith(stub,
		&redis_cluster.ClusterNode{PodName: "cache-0", PodIndex: 0, IP: "10.0.0.1", ID: zombieTestPrimaryID, Role: redis_cluster.PrimaryNode},
		&redis_cluster.ClusterNode{PodName: "cache-1", PodIndex: 1, IP: "10.0.0.3", ID: zombieTestReplicaID, Role: redis_cluster.ReplicaNode},
	)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	_, err := r.forgetZombies(context.Background(), cluster, logr.Discard())
	require.ErrorContains(t, err, "failed to refresh cluster")
	assert.Len(t, forgetCalls(stub), 2, "both healthy nodes must forget the zombie")

	_, deferred := rec.findBadSituation("zombie_forget_deferred_pod_without_id")
	assert.False(t, deferred)
}

func TestPodsWithoutNodeID(t *testing.T) {
	cluster := &redis_cluster.Cluster{
		StatefulSet: &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{Name: "cache"},
			Spec:       appsv1.StatefulSetSpec{Replicas: ptr.To(int32(4))},
		},
		AllNodes: []*redis_cluster.ClusterNode{
			{PodName: "cache-0", PodIndex: 0, ID: zombieTestPrimaryID},
			{PodName: "cache-2", PodIndex: 2},
			{PodName: "cache-5", PodIndex: 5},
			{PodIndex: -1, ID: zombieTestStaleID},
			nil,
		},
	}
	assert.Equal(t, []string{"cache-1", "cache-2", "cache-3", "cache-5"}, podsWithoutNodeID(cluster),
		"missing or not-Running expected pods and listed pods without an ID, in index order")

	cluster.StatefulSet = nil
	assert.Equal(t, []string{"cache-2", "cache-5"}, podsWithoutNodeID(cluster), "without a StatefulSet only listed pods count")

	for _, n := range cluster.AllNodes[1:3] {
		n.ID = zombieTestReplicaID
	}
	assert.Empty(t, podsWithoutNodeID(cluster))
}

func TestZombieFirstSeenTracker(t *testing.T) {
	var tracker zombieFirstSeenTracker
	t0 := time.Unix(1_800_000_000, 0)

	seen := tracker.Observe("ns/a", []string{"z1"}, t0)
	assert.Equal(t, map[string]time.Time{"z1": t0}, seen)

	seen = tracker.Observe("ns/a", []string{"z1", "z2"}, t0.Add(time.Minute))
	assert.Equal(t, map[string]time.Time{"z1": t0, "z2": t0.Add(time.Minute)}, seen, "first sight is kept")
	seen["z1"] = time.Time{}
	assert.Equal(t, t0, tracker.Observe("ns/a", []string{"z1"}, t0.Add(2*time.Minute))["z1"], "callers get a copy")

	assert.Equal(t, t0.Add(3*time.Minute), tracker.Observe("ns/b", []string{"z1"}, t0.Add(3*time.Minute))["z1"], "clusters are tracked apart")

	tracker.Observe("ns/a", []string{"z2"}, t0.Add(4*time.Minute))
	assert.Equal(t, t0.Add(5*time.Minute), tracker.Observe("ns/a", []string{"z1"}, t0.Add(5*time.Minute))["z1"],
		"an ID that was not seen once starts over")

	assert.Empty(t, tracker.Observe("ns/a", nil, t0))
	assert.Equal(t, t0.Add(6*time.Minute), tracker.Observe("ns/a", []string{"z2"}, t0.Add(6*time.Minute))["z2"], "no zombies clears the cluster")
}
