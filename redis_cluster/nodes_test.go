package redis_cluster

import (
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func clusterFor(nodes []*ClusterNode) *Cluster {
	return &Cluster{
		AllNodes: nodes,
		Nodes:    groupNodes(nodes),
	}
}

// clusterForExplicitRoles groups nodes by their pre-set Role field, bypassing
// determineRole(). Lets tests skip the Flags/KnownNodes/Views boilerplate.
func clusterForExplicitRoles(nodes []*ClusterNode) *Cluster {
	grouped := make(map[NodeRole][]*ClusterNode)
	for _, n := range nodes {
		grouped[n.Role] = append(grouped[n.Role], n)
	}
	return &Cluster{
		AllNodes: nodes,
		Nodes:    grouped,
	}
}

func TestGetUnassignedSlots0(t *testing.T) {
	assert := assert.New(t)

	result, err := clusterForExplicitRoles([]*ClusterNode{
		{Role: PrimaryNode, SlotsRanges: []SlotsRange{{0, 16383}}},
	}).GetUnassignedSlotsRanges()

	assert.Equal(nil, err)
	assert.Equal([]SlotsRange{}, result)
}

func TestGetUnassignedSlots1(t *testing.T) {
	assert := assert.New(t)

	result, err := clusterForExplicitRoles([]*ClusterNode{
		{Role: PrimaryNode, SlotsRanges: []SlotsRange{{0, 0}}},
	}).GetUnassignedSlotsRanges()

	assert.Equal(nil, err)
	assert.Equal([]SlotsRange{{First: 1, Last: 16383}}, result)
}

func TestGetUnassignedSlots2(t *testing.T) {
	assert := assert.New(t)

	result, err := clusterForExplicitRoles([]*ClusterNode{
		{Role: PrimaryNode, SlotsRanges: []SlotsRange{{0, 100}}},
	}).GetUnassignedSlotsRanges()

	assert.Equal(nil, err)
	assert.Equal([]SlotsRange{{First: 101, Last: 16383}}, result)
}

func TestGetUnassignedSlots3(t *testing.T) {
	assert := assert.New(t)

	result, err := clusterForExplicitRoles([]*ClusterNode{
		{Role: PrimaryNode, SlotsRanges: []SlotsRange{{0, 100}, {1000, 1100}}},
	}).GetUnassignedSlotsRanges()

	assert.Equal(nil, err)
	assert.Equal([]SlotsRange{
		{First: 101, Last: 999},
		{First: 1101, Last: 16383},
	}, result)
}

func TestParseNodeLineCapturesOpenSlots(t *testing.T) {
	node := &ClusterNode{
		PodName:  "redis-cluster-test-9",
		PodIndex: 9,
		IP:       "10.244.0.16",
		Views:    make(map[int]*NodeView),
	}
	nodes := []*ClusterNode{node}

	nodes = parseNodeLine(nodes, node,
		"81b2 10.244.0.16:6379@16379 myself,master - 0 0 10 connected 5461-6826 10923-11984 [11985-<-a755]",
		logr.Discard())

	require.Len(t, nodes, 1)
	assert.Equal(t, []SlotsRange{{First: 5461, Last: 6826}, {First: 10923, Last: 11984}}, node.SlotsRanges)
	require.Len(t, node.OpenSlots, 1)
	assert.Equal(t, OpenSlot{Slot: 11985, State: OpenSlotImporting, PeerID: "a755"}, node.OpenSlots[0])
}

func TestParseNodeLineCapturesAnnouncedHostname(t *testing.T) {
	node := &ClusterNode{
		PodName:  "redis-cluster-test-9",
		PodIndex: 9,
		IP:       "10.244.0.16",
		Views:    make(map[int]*NodeView),
	}
	nodes := []*ClusterNode{node}

	nodes = parseNodeLine(nodes, node,
		"81b2 10.244.0.16:6379@16379,redis-cluster-test-9.redis-cluster-test.default.svc myself,master - 0 0 10 connected",
		logr.Discard())

	require.Len(t, nodes, 1)
	assert.Equal(t,
		"redis-cluster-test-9.redis-cluster-test.default.svc",
		node.Hostname,
	)
}

func TestParseNodeLineCapturesIPv6Address(t *testing.T) {
	node := &ClusterNode{
		PodName:  "redis-cluster-test-0",
		PodIndex: 0,
		IP:       "2001:db8::10",
		Views:    make(map[int]*NodeView),
	}
	nodes := []*ClusterNode{node}

	nodes = parseNodeLine(nodes, node,
		"81b2 [2001:db8::10]:6379@16379,redis-0.redis.default.svc myself,master - 0 0 10 connected 0-16383",
		logr.Discard())

	require.Len(t, nodes, 1)
	assert.Equal(t, "81b2", node.ID)
	assert.Equal(t, "redis-0.redis.default.svc", node.Hostname)
	assert.Equal(t, []SlotsRange{{First: 0, Last: 16383}}, node.SlotsRanges)
}

func TestParseClusterNodeAddressAcceptsUnbracketedIPv6(t *testing.T) {
	host, hostname, ok := ParseClusterNodeAddress("2001:db8::10:6379@16379,redis-0.redis.default.svc")
	require.True(t, ok)
	assert.Equal(t, "2001:db8::10", host)
	assert.Equal(t, "redis-0.redis.default.svc", hostname)
}

func TestParseNodeLineCapturesOpenSlotsInPeerView(t *testing.T) {
	viewer := &ClusterNode{
		PodName:  "redis-cluster-test-0",
		PodIndex: 0,
		IP:       "10.244.0.27",
		Views:    make(map[int]*NodeView),
	}
	peer := &ClusterNode{
		PodName:  "redis-cluster-test-2",
		PodIndex: 2,
		IP:       "10.244.0.25",
		Views:    make(map[int]*NodeView),
	}
	nodes := []*ClusterNode{viewer, peer}

	nodes = parseNodeLine(nodes, viewer,
		"a755 10.244.0.25:6379@16379 master - 0 0 3 connected 11985-16383 [11985->-81b2]",
		logr.Discard())

	require.Len(t, nodes, 2)
	view, ok := peer.Views[viewer.PodIndex]
	require.True(t, ok)
	assert.Equal(t, []SlotsRange{{First: 11985, Last: 16383}}, view.SlotsRanges)
	require.Len(t, view.OpenSlots, 1)
	assert.Equal(t, OpenSlot{Slot: 11985, State: OpenSlotMigrating, PeerID: "81b2"}, view.OpenSlots[0])
}

func TestClusterOpenSlotsAggregatesSelfAndViews(t *testing.T) {
	c := clusterFor([]*ClusterNode{
		{
			OpenSlots: []OpenSlot{{Slot: 1, State: OpenSlotImporting, PeerID: "a"}},
			Views: map[int]*NodeView{
				0: {OpenSlots: []OpenSlot{{Slot: 2, State: OpenSlotMigrating, PeerID: "b"}}},
			},
		},
	})

	assert.Equal(t, []OpenSlot{
		{Slot: 1, State: OpenSlotImporting, PeerID: "a"},
		{Slot: 2, State: OpenSlotMigrating, PeerID: "b"},
	}, c.OpenSlots())
	assert.Equal(t, "1:importing:a,2:migrating:b", OpenSlotSummary(c.OpenSlots()))
}

func TestOpenSlotsCanExistWithFullSlotCoverage(t *testing.T) {
	c := clusterForExplicitRoles([]*ClusterNode{
		{
			Role:        PrimaryNode,
			SlotsRanges: []SlotsRange{{0, constants.TotalSlots - 1}},
			OpenSlots:   []OpenSlot{{Slot: 11985, State: OpenSlotImporting, PeerID: "a755"}},
		},
	})

	unassigned, err := c.GetUnassignedSlotsRanges()
	require.NoError(t, err)
	assert.Empty(t, unassigned)
	assert.Equal(t, []OpenSlot{{Slot: 11985, State: OpenSlotImporting, PeerID: "a755"}}, c.OpenSlots())
}

// ── ClusterNode.IsDrainCandidate ──

func TestClusterNode_IsDrainCandidate_NoMarkers(t *testing.T) {
	n := &ClusterNode{}
	assert.False(t, n.IsDrainCandidate())
}

func TestClusterNode_IsDrainCandidate_Terminating(t *testing.T) {
	n := &ClusterNode{IsTerminating: true}
	assert.True(t, n.IsDrainCandidate())
}

func TestClusterNode_IsDrainCandidate_DrainCondition(t *testing.T) {
	n := &ClusterNode{HasDrainCandidateCond: true}
	assert.True(t, n.IsDrainCandidate())
}

func TestClusterNode_IsDrainCandidate_AllMarkers(t *testing.T) {
	n := &ClusterNode{
		IsTerminating:         true,
		HasDrainCandidateCond: true,
	}
	assert.True(t, n.IsDrainCandidate())
}

// ── podsToClusterNodes drain-status enrichment ──

func newRunningPod(name, ip string, modifiers ...func(*corev1.Pod)) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Image: "redis:7.2"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
		},
	}
	for _, m := range modifiers {
		m(&pod)
	}
	return pod
}

func TestPodsToClusterNodes_PopulatesDrainFields_Plain(t *testing.T) {
	nodes, err := podsToClusterNodes([]corev1.Pod{
		newRunningPod("redis-cluster-foo-0", "10.0.0.1"),
	})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.False(t, nodes[0].IsTerminating)
	assert.False(t, nodes[0].HasDrainCandidateCond)
	assert.False(t, nodes[0].IsDrainCandidate())
}

func TestPodsToClusterNodes_PopulatesDrainFields_Terminating(t *testing.T) {
	now := metav1.Now()
	nodes, err := podsToClusterNodes([]corev1.Pod{
		newRunningPod("redis-cluster-foo-0", "10.0.0.1", func(p *corev1.Pod) {
			p.DeletionTimestamp = &now
		}),
	})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.True(t, nodes[0].IsTerminating)
	assert.True(t, nodes[0].IsDrainCandidate())
}

func TestPodsToClusterNodes_PopulatesDrainFields_DrainCondition(t *testing.T) {
	nodes, err := podsToClusterNodes([]corev1.Pod{
		newRunningPod("redis-cluster-foo-0", "10.0.0.1", func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{
				{Type: corev1.PodConditionType(constants.DrainCandidateConditionType), Status: corev1.ConditionTrue, Reason: constants.DrainCandidateConditionReason},
			}
		}),
	})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.True(t, nodes[0].HasDrainCandidateCond)
	assert.True(t, nodes[0].IsDrainCandidate())
}

func TestPodsToClusterNodes_IgnoresInactiveDrainCondition(t *testing.T) {
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown} {
		t.Run(string(status), func(t *testing.T) {
			nodes, err := podsToClusterNodes([]corev1.Pod{
				newRunningPod("redis-cluster-foo-0", "10.0.0.1", func(p *corev1.Pod) {
					p.Status.Conditions = []corev1.PodCondition{{
						Type:   corev1.PodConditionType(constants.DrainCandidateConditionType),
						Status: status,
						Reason: constants.DrainCandidateConditionReason,
					}}
				}),
			})
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			assert.False(t, nodes[0].HasDrainCandidateCond)
			assert.False(t, nodes[0].IsDrainCandidate())
		})
	}
}

func TestPodsToClusterNodes_IgnoresAdapterSpecificAnnotations(t *testing.T) {
	nodes, err := podsToClusterNodes([]corev1.Pod{
		newRunningPod("redis-cluster-foo-0", "10.0.0.1", func(p *corev1.Pod) {
			p.Annotations = map[string]string{constants.NLATestTriggerAnnotation: "true"}
		}),
	})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.False(t, nodes[0].IsDrainCandidate())
}

// ── GetNonDrainingReplicaOf ──

func TestGetNonDrainingReplicaOf_PrefersHealthy(t *testing.T) {
	primary := &ClusterNode{ID: "p1", Role: PrimaryNode}
	drainingReplica := &ClusterNode{ID: "r1", Role: ReplicaNode, PrimaryID: "p1", IsTerminating: true}
	healthyReplica := &ClusterNode{ID: "r2", Role: ReplicaNode, PrimaryID: "p1"}

	c := clusterForExplicitRoles([]*ClusterNode{primary, drainingReplica, healthyReplica})
	got := c.GetNonDrainingReplicaOf(primary)
	require.NotNil(t, got)
	assert.Equal(t, "r2", got.ID, "expected the non-draining replica r2")
}

func TestGetNonDrainingReplicaOf_AllDraining(t *testing.T) {
	primary := &ClusterNode{ID: "p1", Role: PrimaryNode}
	r1 := &ClusterNode{ID: "r1", Role: ReplicaNode, PrimaryID: "p1", IsTerminating: true}
	r2 := &ClusterNode{ID: "r2", Role: ReplicaNode, PrimaryID: "p1", HasDrainCandidateCond: true}

	c := clusterForExplicitRoles([]*ClusterNode{primary, r1, r2})
	assert.Nil(t, c.GetNonDrainingReplicaOf(primary))
}

func TestGetNonDrainingReplicaOf_NoReplicas(t *testing.T) {
	primary := &ClusterNode{ID: "p1", Role: PrimaryNode}
	c := clusterForExplicitRoles([]*ClusterNode{primary})
	assert.Nil(t, c.GetNonDrainingReplicaOf(primary))
}

func TestGetReplicaOf_StillReturnsAny(t *testing.T) {
	// GetReplicaOf stays drain-blind — only GetNonDrainingReplicaOf is drain-aware.
	primary := &ClusterNode{ID: "p1", Role: PrimaryNode}
	drainingReplica := &ClusterNode{ID: "r1", Role: ReplicaNode, PrimaryID: "p1", IsTerminating: true}

	c := clusterForExplicitRoles([]*ClusterNode{primary, drainingReplica})
	got := c.GetReplicaOf(primary)
	require.NotNil(t, got)
	assert.Equal(t, "r1", got.ID, "GetReplicaOf must remain drain-blind for fallback callers")
}
