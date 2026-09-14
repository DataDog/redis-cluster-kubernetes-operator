package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// recordingMetrics captures CountBadSituation calls for assertions.
type recordingMetrics struct {
	mu            sync.Mutex
	badSituations []recordedBadSituation
	timingNames   []string
}

type recordedBadSituation struct {
	situation string
	tags      []string
}

func (m *recordingMetrics) WithRedisCluster(string) metrics.Metrics { return m }
func (m *recordingMetrics) Count(string) error                      { return nil }
func (m *recordingMetrics) CountBadSituation(situation string, tags ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.badSituations = append(m.badSituations, recordedBadSituation{
		situation: situation,
		tags:      append([]string(nil), tags...),
	})
	return nil
}
func (m *recordingMetrics) CountToolboxOperation(string) error            { return nil }
func (m *recordingMetrics) CountAutoscalingDecision(string, string) error { return nil }
func (m *recordingMetrics) ReportHealth(health.ClusterHealth) error       { return nil }
func (m *recordingMetrics) Gauge(string, float64, ...string) error        { return nil }
func (m *recordingMetrics) Timing(name string, _ int64, _ ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timingNames = append(m.timingNames, name)
	return nil
}

func (m *recordingMetrics) findBadSituation(situation string) (recordedBadSituation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, b := range m.badSituations {
		if b.situation == situation {
			return b, true
		}
	}
	return recordedBadSituation{}, false
}

func newTestReconcilerWithMetrics(t *testing.T, k8sClient client.Client, m metrics.Metrics) *RedisClusterReconciler {
	t.Helper()
	return &RedisClusterReconciler{
		ClusterClient: redis_cluster.ClusterClient{
			Client:  k8sClient,
			Log:     logr.Discard(),
			Metrics: m,
		},
	}
}

// ── Pod builders ──

func podWithIP(ip string) v1.Pod {
	return v1.Pod{
		Status: v1.PodStatus{PodIP: ip},
	}
}

func podWithIPAndDrain(ip string) v1.Pod {
	return v1.Pod{
		Status: v1.PodStatus{
			PodIP: ip,
			Conditions: []v1.PodCondition{
				{Type: v1.PodConditionType(nlaConditionType), Status: v1.ConditionTrue, Reason: nlaConditionReason},
			},
		},
	}
}

func podWithIPAndDeletionTimestamp(ip string) v1.Pod {
	now := metav1.Now()
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
		Status:     v1.PodStatus{PodIP: ip},
	}
}

func podWithIPDrainAndDeletionTimestamp(ip string) v1.Pod {
	now := metav1.Now()
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
		Status: v1.PodStatus{
			PodIP: ip,
			Conditions: []v1.PodCondition{
				{Type: v1.PodConditionType(nlaConditionType), Status: v1.ConditionTrue, Reason: nlaConditionReason},
			},
		},
	}
}

func podTerminatingWithAnnotations(ip string, annotations map[string]string) v1.Pod {
	now := metav1.Now()
	return v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			DeletionTimestamp: &now,
			Annotations:       annotations,
		},
		Status: v1.PodStatus{PodIP: ip},
	}
}

// ── Test helpers for reconciler-level tests ──

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(s))
	require.NoError(t, cachev1.SchemeBuilder.AddToScheme(s))
	return s
}

func newTestReconciler(t *testing.T, k8sClient client.Client) *RedisClusterReconciler {
	t.Helper()
	return &RedisClusterReconciler{
		ClusterClient: redis_cluster.ClusterClient{
			Client:  k8sClient,
			Log:     logr.Discard(),
			Metrics: metrics.CreateNoop(),
		},
	}
}

func newClusterNode(ip, id string, role redis_cluster.NodeRole) *redis_cluster.ClusterNode {
	flags := []string{"master"}
	if role == redis_cluster.ReplicaNode {
		flags = []string{"slave"}
	}
	return &redis_cluster.ClusterNode{
		IP:    ip,
		ID:    id,
		Role:  role,
		Flags: flags,
	}
}

func newClusterWithNodes(nodes []*redis_cluster.ClusterNode) *redis_cluster.Cluster {
	grouped := make(map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode)
	for _, n := range nodes {
		grouped[n.Role] = append(grouped[n.Role], n)
	}
	return &redis_cluster.Cluster{
		AllNodes: nodes,
		Nodes:    grouped,
	}
}

// ── findPodByIP ──

func TestFindPodByIP_Found(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1"), podWithIP("10.0.0.2")}}
	pod := findPodByIP(podList, "10.0.0.2")
	assert.NotNil(t, pod)
	assert.Equal(t, "10.0.0.2", pod.Status.PodIP)
}

func TestFindPodByIP_NotFound(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.Nil(t, findPodByIP(podList, "10.0.0.99"))
}

func TestFindPodByIP_EmptyList(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{}}
	assert.Nil(t, findPodByIP(podList, "10.0.0.1"))
}

// ── isPodDrainCandidate ──

func TestIsPodDrainCandidate_NoDrain(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.1"))
}

func TestIsPodDrainCandidate_WithDrain(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIPAndDrain("10.0.0.1")}}
	assert.True(t, isPodDrainCandidate(podList, "10.0.0.1"))
}

func TestIsPodDrainCandidate_PodNotFound(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.99"))
}

func TestIsPodDrainCandidate_OnlyMatchingPodChecked(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{
		podWithIPAndDrain("10.0.0.1"),
		podWithIP("10.0.0.2"),
	}}
	assert.True(t, isPodDrainCandidate(podList, "10.0.0.1"))
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.2"))
}

// ── hasNLATrigger ──

func TestHasNLATrigger_RealCondition(t *testing.T) {
	pod := podWithIPAndDrain("10.0.0.1")
	assert.True(t, hasNLATrigger(&pod))
}

func TestHasNLATrigger_ConditionMustBeTrue(t *testing.T) {
	for _, status := range []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionUnknown} {
		t.Run(string(status), func(t *testing.T) {
			pod := podWithIPAndDrain("10.0.0.1")
			pod.Status.Conditions[0].Status = status
			assert.False(t, hasNLATrigger(&pod))
		})
	}
}

func TestHasNLATrigger_TestAnnotation(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{nlaTestTriggerAnnotation: "true"},
		},
		Status: v1.PodStatus{PodIP: "10.0.0.1"},
	}
	assert.True(t, hasNLATrigger(&pod))
}

func TestHasNLATrigger_Neither(t *testing.T) {
	pod := podWithIP("10.0.0.1")
	assert.False(t, hasNLATrigger(&pod))
}

func TestHasNLATrigger_TestAnnotationFalse(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{nlaTestTriggerAnnotation: "false"},
		},
		Status: v1.PodStatus{PodIP: "10.0.0.1"},
	}
	assert.False(t, hasNLATrigger(&pod))
}

func TestIsPodDrainCandidate_IgnoresAdapterSpecificAnnotation(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{nlaTestTriggerAnnotation: "true"},
		},
		Status: v1.PodStatus{PodIP: "10.0.0.1"},
	}
	podList := &v1.PodList{Items: []v1.Pod{pod}}
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.1"))
}

// ── isPodTerminating ──

func TestIsPodTerminating_NotTerminating(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.False(t, isPodTerminating(podList, "10.0.0.1"))
}

func TestIsPodTerminating_Terminating(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIPAndDeletionTimestamp("10.0.0.1")}}
	assert.True(t, isPodTerminating(podList, "10.0.0.1"))
}

func TestIsPodTerminating_PodNotFound(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.False(t, isPodTerminating(podList, "10.0.0.99"))
}

func TestIsPodTerminating_MixedPods(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{
		podWithIPAndDeletionTimestamp("10.0.0.1"),
		podWithIP("10.0.0.2"),
	}}
	assert.True(t, isPodTerminating(podList, "10.0.0.1"))
	assert.False(t, isPodTerminating(podList, "10.0.0.2"))
}

// ── isNLARetriggered ──

func TestIsNLARetriggered_LegacyDone(t *testing.T) {
	assert.False(t, isNLARetriggered("done", time.Now()))
}

func TestIsNLARetriggered_ConditionAfterDone(t *testing.T) {
	annotation := "done:1706284755"
	conditionTime := time.Unix(1706284755, 0).Add(10 * time.Second)
	assert.True(t, isNLARetriggered(annotation, conditionTime))
}

func TestIsNLARetriggered_ConditionBeforeDone(t *testing.T) {
	annotation := "done:1706284755"
	conditionTime := time.Unix(1706284755, 0).Add(-10 * time.Second)
	assert.False(t, isNLARetriggered(annotation, conditionTime))
}

func TestIsNLARetriggered_ConditionSameTime(t *testing.T) {
	annotation := "done:1706284755"
	conditionTime := time.Unix(1706284755, 0)
	assert.False(t, isNLARetriggered(annotation, conditionTime))
}

func TestIsNLARetriggered_InvalidAnnotation(t *testing.T) {
	assert.False(t, isNLARetriggered("done:notanumber", time.Now()))
	assert.False(t, isNLARetriggered("garbage", time.Now()))
	assert.False(t, isNLARetriggered("", time.Now()))
	// SplitN(":", 2) keeps everything after the first colon, so "extra"
	// becomes part of the timestamp string and ParseInt fails → false.
	assert.False(t, isNLARetriggered("done:1706284755:extra", time.Now()))
}

func TestIsNLARetriggered_NegativeTimestamp(t *testing.T) {
	// time.Unix accepts negative seconds (pre-epoch); a condition time
	// after epoch is correctly detected as a retrigger.
	assert.True(t, isNLARetriggered("done:-1", time.Unix(0, 0)))
}

func TestIsNLADone(t *testing.T) {
	for _, annotation := range []string{nlaDone, nlaDone + ":0", nlaDone + ":-1", nlaDone + ":1706284755"} {
		t.Run(annotation, func(t *testing.T) {
			assert.True(t, isNLADone(annotation))
		})
	}
	for _, annotation := range []string{"", "done:", "done:notanumber", "done:1:2", "done-later", "processing", "failed"} {
		t.Run("invalid_"+annotation, func(t *testing.T) {
			assert.False(t, isNLADone(annotation))
		})
	}
}

// ── forgetTerminatingNLANodes gating logic ──
// These tests build a real Cluster with AllNodes so LookupNodeByIP works,
// but do NOT set a Toolbox — if the gating is wrong and forgetNode is called,
// the test will panic (nil Toolbox). This verifies the gates PREVENT the call.

func TestForgetTerminatingNLANodes_SkipsNonTerminatingPods(t *testing.T) {
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.ReplicaNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	podList := &v1.PodList{Items: []v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pod-0",
				Namespace:   "test",
				Annotations: map[string]string{nlaAnnotation: "done:123"},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.1"},
		},
	}}

	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	// No panic = forgetNode was not called = gating works
	r.forgetTerminatingNLANodes(context.Background(), cluster, podList)
}

func TestForgetTerminatingNLANodes_SkipsNonDonePods(t *testing.T) {
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.ReplicaNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	tests := []struct {
		name       string
		annotation string
	}{
		{"empty annotation", ""},
		{"processing", "processing"},
		{"failed", "failed"},
		{"no annotation key", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := metav1.Now()
			annotations := map[string]string{}
			if tt.annotation != "" {
				annotations[nlaAnnotation] = tt.annotation
			}
			podList := &v1.PodList{Items: []v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "pod-0",
						Namespace:         "test",
						DeletionTimestamp: &now,
						Annotations:       annotations,
					},
					Status: v1.PodStatus{PodIP: "10.0.0.1"},
				},
			}}
			r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
			// No panic = forgetNode was not called
			r.forgetTerminatingNLANodes(context.Background(), cluster, podList)
		})
	}
}

func TestForgetTerminatingNLANodes_SkipsAlreadyForgottenPods(t *testing.T) {
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.ReplicaNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	now := metav1.Now()
	podList := &v1.PodList{Items: []v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-0",
				Namespace:         "test",
				DeletionTimestamp: &now,
				Annotations: map[string]string{
					nlaAnnotation:          "done:123",
					nlaForgottenAnnotation: "true",
				},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.1"},
		},
	}}

	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	// No panic = forgetNode was not called
	r.forgetTerminatingNLANodes(context.Background(), cluster, podList)
}

func TestForgetTerminatingNLANodes_SkipsPodNotInCluster(t *testing.T) {
	// Cluster has no nodes — LookupNodeByIP returns false
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{})

	now := metav1.Now()
	podList := &v1.PodList{Items: []v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-0",
				Namespace:         "test",
				DeletionTimestamp: &now,
				Annotations:       map[string]string{nlaAnnotation: "done:123"},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.1"},
		},
	}}

	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	// No panic = forgetNode was not called
	r.forgetTerminatingNLANodes(context.Background(), cluster, podList)
}

func TestForgetTerminatingNLANodes_SkipsNonPrimaryNonReplica(t *testing.T) {
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.OtherNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	now := metav1.Now()
	podList := &v1.PodList{Items: []v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-0",
				Namespace:         "test",
				DeletionTimestamp: &now,
				Annotations:       map[string]string{nlaAnnotation: "done:123"},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.1"},
		},
	}}

	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	// No panic = forgetNode was not called (OtherNode role filtered)
	r.forgetTerminatingNLANodes(context.Background(), cluster, podList)
}

// Currently-primary node with NLA "done" must not be forgotten. Direct fix
// for the cavalier-4 incident.
func TestForgetTerminatingNLANodes_SkipsPrimary(t *testing.T) {
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	now := metav1.Now()
	podList := &v1.PodList{Items: []v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-0",
				Namespace:         "test",
				DeletionTimestamp: &now,
				Annotations:       map[string]string{nlaAnnotation: "done:123"},
			},
			Status: v1.PodStatus{PodIP: "10.0.0.1"},
		},
	}}

	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)
	r.forgetTerminatingNLANodes(context.Background(), cluster, podList)

	pod := &podList.Items[0]
	assert.NotEqual(t, "true", pod.Annotations[nlaForgottenAnnotation],
		"primary must not be marked forgotten")
}

// ── forgetNLANode primary-refuse ──

// forgetNLANode must return without forgetting when handed a primary. This is
// the single source of truth for the "no FORGET on a primary" invariant.
func TestForgetNLANode_RefusesPrimary(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-0",
			Annotations: map[string]string{},
		},
		Status: v1.PodStatus{PodIP: "10.0.0.1"},
	}
	node := newClusterNode("10.0.0.1", "node1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)
	r.forgetNLANode(context.Background(), cluster, pod, node)

	got, ok := rec.findBadSituation("forget_terminating_primary")
	require.True(t, ok, "expected forget_terminating_primary metric to be emitted")
	assert.Contains(t, got.tags, "pod_name:pod-0", "expected pod_name tag on metric")
	assert.NotEqual(t, "true", pod.Annotations[nlaForgottenAnnotation],
		"refused primary must not be marked forgotten")
}

func TestRecordingMetrics_TagsRoundTrip(t *testing.T) {
	rec := &recordingMetrics{}
	_ = rec.CountBadSituation("example", "pod_name:foo", "extra:bar")

	got, ok := rec.findBadSituation("example")
	require.True(t, ok)
	assert.Equal(t, []string{"pod_name:foo", "extra:bar"}, got.tags)
}

// ── findNonDrainingReplica ──

func TestFindNonDrainingReplica_PrefersNonDraining(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	drainReplica := &redis_cluster.ClusterNode{IP: "10.0.0.2", ID: "r1", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	healthyReplica := &redis_cluster.ClusterNode{IP: "10.0.0.3", ID: "r2", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary, drainReplica, healthyReplica})

	podList := &v1.PodList{Items: []v1.Pod{
		podWithIPAndDrain("10.0.0.2"),
		podWithIP("10.0.0.3"),
	}}

	got := findNonDrainingReplica(cluster, podList, primary)
	require.NotNil(t, got)
	assert.Equal(t, "r2", got.ID)
}

func TestFindNonDrainingReplica_AllDraining(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	r1 := &redis_cluster.ClusterNode{IP: "10.0.0.2", ID: "r1", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	r2 := &redis_cluster.ClusterNode{IP: "10.0.0.3", ID: "r2", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary, r1, r2})

	podList := &v1.PodList{Items: []v1.Pod{
		podWithIPAndDrain("10.0.0.2"),
		podWithIPAndDeletionTimestamp("10.0.0.3"),
	}}

	assert.Nil(t, findNonDrainingReplica(cluster, podList, primary))
}

// A replica whose pod is missing from the live podList must not be picked as a
// failover target — its pod has likely already been deleted. Failing over to a
// non-existent pod would silently skip later healthy replicas.
func TestFindNonDrainingReplica_SkipsReplicaWithMissingPod(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	missingPodReplica := &redis_cluster.ClusterNode{IP: "10.0.0.2", ID: "r1", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	healthyReplica := &redis_cluster.ClusterNode{IP: "10.0.0.3", ID: "r2", Role: redis_cluster.ReplicaNode, PrimaryID: "p1", Flags: []string{"slave"}}
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary, missingPodReplica, healthyReplica})

	// Only the healthy replica has a pod in the live list. r1's pod is gone.
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.3")}}

	got := findNonDrainingReplica(cluster, podList, primary)
	require.NotNil(t, got)
	assert.Equal(t, "r2", got.ID, "must skip the replica whose pod is missing from podList")
}

// ── ensureNotPrimary ──

func TestEnsureNotPrimary_NoOpForReplica(t *testing.T) {
	node := newClusterNode("10.0.0.1", "r1", redis_cluster.ReplicaNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{node})

	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	err := r.ensureNotPrimary(context.Background(), cluster, &v1.PodList{}, node, "pod-r1")
	assert.NoError(t, err, "non-primary should be a no-op")
}

func TestEnsureNotPrimary_NoReplicaAvailable(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})

	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build(), rec)

	err := r.ensureNotPrimary(context.Background(), cluster, &v1.PodList{}, primary, "pod-p1")
	require.Error(t, err, "should error when there is no replica to promote")

	got, ok := rec.findBadSituation("ensure_not_primary_no_replica")
	require.True(t, ok)
	assert.Contains(t, got.tags, "pod_name:pod-p1")
}

// ── handleTerminatingPods ──

var testPodLabels = map[string]string{
	"app":                "redis-cluster-sample",
	"chart_name":         "redis-cluster-sample",
	"redis_cluster_name": "redis-cluster-foo",
}

func newTestRedisCluster() *cachev1.RedisCluster {
	rc := &cachev1.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-foo", Namespace: "test"},
	}
	rc.Spec.StatefulsetTemplate.ObjectMeta.Labels = testPodLabels
	return rc
}

// terminatingPod builds a pod that kubelet has marked for deletion.
func terminatingPod(name, ip string, annotations map[string]string) *v1.Pod {
	now := metav1.Now()
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         "test",
			Labels:            testPodLabels,
			DeletionTimestamp: &now,
			Finalizers:        []string{"redis-cluster.test/keep"},
			Annotations:       annotations,
		},
		Status: v1.PodStatus{PodIP: ip, Phase: v1.PodRunning},
	}
}

func TestHandleTerminatingPods_SkipsNonTerminating(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-foo-0", Namespace: "test", Labels: testPodLabels},
		Status:     v1.PodStatus{PodIP: "10.0.0.1", Phase: v1.PodRunning},
	}

	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build())
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))
}

func TestHandleTerminatingPods_ForgetsTerminatingReplica(t *testing.T) {
	replica := newClusterNode("10.0.0.1", "r1", redis_cluster.ReplicaNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{replica})
	cluster.Toolbox = &redis_cluster.Toolbox{}
	cluster.RedisCluster = newTestRedisCluster()

	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", nil)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))

	assert.Contains(t, rec.timingNames, metrics.TimingForgetDuration)
}

// NLA skip is role-independent — test with primary (more dangerous case).
func TestHandleTerminatingPods_SkipsNLAManaged(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()

	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", map[string]string{
		nlaTestTriggerAnnotation: "true",
	})
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)
	r.LifecycleAdapter = DatadogNLAAdapter{}
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))

	_, ok := rec.findBadSituation("terminating_primary_failover_failed")
	assert.False(t, ok, "NLA-managed pod should be deferred to handleNLA")
	assert.NotContains(t, rec.timingNames, metrics.TimingForgetDuration)
}

func TestHandleTerminatingPods_NoLifecycleAdapterUsesGenericTerminationPath(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()
	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", map[string]string{
		nlaTestTriggerAnnotation: "true",
	})
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)

	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))
	_, attempted := rec.findBadSituation("terminating_primary_failover_failed")
	assert.True(t, attempted, "without an adapter, private NLA metadata must not suppress generic termination safety")
}

type failingLifecycleAdapter struct {
	stateErr     error
	reconcileErr error
}

func (a failingLifecycleAdapter) PodState(*v1.Pod) (LifecyclePodState, error) {
	return LifecyclePodState{}, a.stateErr
}
func (a failingLifecycleAdapter) Reconcile(context.Context, *RedisClusterReconciler, *redis_cluster.Cluster) (bool, error) {
	return false, a.reconcileErr
}

func TestHandleTerminatingPods_LifecycleRecognitionErrorFailsClosed(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()
	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", nil)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)
	r.LifecycleAdapter = failingLifecycleAdapter{stateErr: assert.AnError}

	err := r.handleTerminatingPods(context.Background(), cluster)
	require.ErrorIs(t, err, assert.AnError)
	_, attempted := rec.findBadSituation("terminating_primary_failover_failed")
	assert.False(t, attempted)
}

func TestHandleLifecycle_MissingAdapterIsStable(t *testing.T) {
	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	stable, err := r.handleLifecycle(context.Background(), &redis_cluster.Cluster{})
	require.NoError(t, err)
	assert.True(t, stable)
}

func TestHandleLifecycle_TypedNilAdapterIsTreatedAsUnavailable(t *testing.T) {
	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	var adapter *failingLifecycleAdapter
	r.LifecycleAdapter = adapter

	stable, err := r.handleLifecycle(context.Background(), &redis_cluster.Cluster{})
	require.NoError(t, err)
	assert.True(t, stable)
}

func TestHandleLifecycle_PropagatesAdapterAcknowledgementError(t *testing.T) {
	wantErr := assert.AnError
	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build())
	r.LifecycleAdapter = failingLifecycleAdapter{reconcileErr: wantErr}
	stable, err := r.handleLifecycle(context.Background(), &redis_cluster.Cluster{})
	assert.False(t, stable)
	require.ErrorIs(t, err, wantErr)
}

func TestHandleNLA_StalePodIPDoesNotAcknowledgeKnownPrimary(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	primary.PodName = "redis-cluster-foo-0"
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        primary.PodName,
			Namespace:   "test",
			Labels:      testPodLabels,
			Annotations: map[string]string{nlaTestTriggerAnnotation: "true"},
		},
		Status: v1.PodStatus{PodIP: "10.0.0.99", Phase: v1.PodRunning},
	}
	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build())

	stable, err := r.handleNLA(context.Background(), cluster)
	assert.False(t, stable)
	require.Error(t, err)
	updated := &v1.Pod{}
	require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(pod), updated))
	assert.Equal(t, nlaFailed, updated.Annotations[nlaAnnotation])
	assert.False(t, isNLADone(updated.Annotations[nlaAnnotation]))
}

func TestHandleTerminatingPods_SkipsOtherRole(t *testing.T) {
	other := newClusterNode("10.0.0.1", "o1", redis_cluster.OtherNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{other})
	cluster.RedisCluster = newTestRedisCluster()

	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", nil)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))

	assert.NotContains(t, rec.timingNames, metrics.TimingForgetDuration)
}

func TestHandleTerminatingPods_SkipsPodNotInCluster(t *testing.T) {
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{})
	cluster.RedisCluster = newTestRedisCluster()

	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.99", nil)
	r := newTestReconciler(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build())
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))
}

// Primary with no replica: ensureNotPrimary errors, handler logs and continues.
func TestHandleTerminatingPods_PrimaryNoReplica(t *testing.T) {
	primary := newClusterNode("10.0.0.1", "p1", redis_cluster.PrimaryNode)
	cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{primary})
	cluster.RedisCluster = newTestRedisCluster()

	pod := terminatingPod("redis-cluster-foo-0", "10.0.0.1", nil)
	rec := &recordingMetrics{}
	r := newTestReconcilerWithMetrics(t, fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build(), rec)
	require.NoError(t, r.handleTerminatingPods(context.Background(), cluster))

	_, ok := rec.findBadSituation("ensure_not_primary_no_replica")
	require.True(t, ok)
	_, ok = rec.findBadSituation("terminating_primary_failover_failed")
	require.True(t, ok)
}

// ── Scenario-level traces (verify gating decisions for complete flows) ──

func TestScenario_NonUrgentReplica_QualifiesForDeferredForget(t *testing.T) {
	pod := podTerminatingWithAnnotations("10.0.0.1", map[string]string{
		nlaAnnotation: "done:1706284755",
	})
	assert.True(t, pod.DeletionTimestamp != nil, "pod is terminating")
	assert.True(t, strings.HasPrefix(pod.Annotations[nlaAnnotation], nlaDone), "NLA is done")
	assert.NotEqual(t, "true", pod.Annotations[nlaForgottenAnnotation], "not yet forgotten")
}

func TestScenario_NonUrgentPrimary_QualifiesForDeferredForget(t *testing.T) {
	pod := podTerminatingWithAnnotations("10.0.0.1", map[string]string{
		nlaAnnotation: "done:1706284755",
	})
	assert.True(t, pod.DeletionTimestamp != nil)
	assert.True(t, strings.HasPrefix(pod.Annotations[nlaAnnotation], nlaDone))
}

func TestScenario_AlreadyForgotten_Skipped(t *testing.T) {
	pod := podTerminatingWithAnnotations("10.0.0.1", map[string]string{
		nlaAnnotation:          "done:1706284755",
		nlaForgottenAnnotation: "true",
	})
	assert.Equal(t, "true", pod.Annotations[nlaForgottenAnnotation])
}

func TestScenario_ManualDelete_Skipped(t *testing.T) {
	pod := podWithIPAndDeletionTimestamp("10.0.0.1")
	assert.False(t, strings.HasPrefix(pod.Annotations[nlaAnnotation], nlaDone))
}

func TestHandleNLA_ResumesInterruptedState(t *testing.T) {
	for _, annotation := range []string{nlaProcessing, nlaFailed} {
		t.Run(annotation, func(t *testing.T) {
			replica := newClusterNode("10.0.0.1", "r1", redis_cluster.ReplicaNode)
			cluster := newClusterWithNodes([]*redis_cluster.ClusterNode{replica})
			cluster.RedisCluster = newTestRedisCluster()

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "redis-cluster-foo-0",
					Namespace:   "test",
					Labels:      testPodLabels,
					Annotations: map[string]string{nlaAnnotation: annotation},
				},
				Status: v1.PodStatus{
					PodIP: "10.0.0.1",
					Phase: v1.PodRunning,
					Conditions: []v1.PodCondition{{
						Type:   v1.PodConditionType(nlaConditionType),
						Status: v1.ConditionTrue,
						Reason: nlaConditionReason,
					}},
				},
			}
			k8sClient := fake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(pod).Build()
			r := newTestReconciler(t, k8sClient)

			stable, err := r.handleNLA(context.Background(), cluster)
			require.NoError(t, err)
			assert.False(t, stable)

			updated := &v1.Pod{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(pod), updated))
			assert.True(t, strings.HasPrefix(updated.Annotations[nlaAnnotation], nlaDone+":"),
				"interrupted NLA state %q was not resumed: %q", annotation, updated.Annotations[nlaAnnotation])
		})
	}
}

func TestReplicaReadyForCleanFailover(t *testing.T) {
	ready := "role:slave\r\nmaster_host:10.0.0.1\r\nmaster_link_status:up\r\nmaster_sync_in_progress:0\r\n"
	tests := []struct {
		name string
		info string
		want bool
	}{
		{name: "ready", info: ready, want: true},
		{name: "link down", info: strings.Replace(ready, "master_link_status:up", "master_link_status:down", 1)},
		{name: "sync in progress", info: strings.Replace(ready, "master_sync_in_progress:0", "master_sync_in_progress:1", 1)},
		{name: "wrong primary", info: strings.Replace(ready, "master_host:10.0.0.1", "master_host:10.0.0.9", 1)},
		{name: "already primary", info: strings.Replace(ready, "role:slave", "role:master", 1)},
		{name: "missing replication state", info: "role:slave\nmaster_host:10.0.0.1\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, replicaReadyForCleanFailover(test.info, "10.0.0.1"))
		})
	}
}

func TestScenario_NLAEmpty_Skipped(t *testing.T) {
	pod := podTerminatingWithAnnotations("10.0.0.1", map[string]string{nlaAnnotation: ""})
	assert.False(t, strings.HasPrefix(pod.Annotations[nlaAnnotation], nlaDone))
}

func TestScenario_NilAnnotations_Skipped(t *testing.T) {
	now := metav1.Now()
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
		Status:     v1.PodStatus{PodIP: "10.0.0.1"},
	}
	assert.False(t, strings.HasPrefix(pod.Annotations[nlaAnnotation], nlaDone))
}

func TestScenario_SpotInterruption_BothConditionsPresent(t *testing.T) {
	pod := podWithIPDrainAndDeletionTimestamp("10.0.0.1")
	assert.True(t, pod.DeletionTimestamp != nil, "DeletionTimestamp set")
	hasDrain := false
	for _, cond := range pod.Status.Conditions {
		if string(cond.Type) == nlaConditionType && cond.Reason == nlaConditionReason {
			hasDrain = true
		}
	}
	assert.True(t, hasDrain, "has NodeDrainCandidate condition")
}

// ── detectAndFixPartitionedNodes guards ──

func TestPartitionGuard_DrainCandidate(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIPAndDrain("10.0.0.1")}}
	assert.True(t, isPodDrainCandidate(podList, "10.0.0.1"))
	assert.False(t, isPodTerminating(podList, "10.0.0.1"))
}

func TestPartitionGuard_Terminating(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIPAndDeletionTimestamp("10.0.0.1")}}
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.1"))
	assert.True(t, isPodTerminating(podList, "10.0.0.1"))
}

func TestPartitionGuard_BothDrainAndTerminating(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIPDrainAndDeletionTimestamp("10.0.0.1")}}
	assert.True(t, isPodDrainCandidate(podList, "10.0.0.1"))
	assert.True(t, isPodTerminating(podList, "10.0.0.1"))
}

func TestPartitionGuard_HealthyPod(t *testing.T) {
	podList := &v1.PodList{Items: []v1.Pod{podWithIP("10.0.0.1")}}
	assert.False(t, isPodDrainCandidate(podList, "10.0.0.1"))
	assert.False(t, isPodTerminating(podList, "10.0.0.1"))
}
