package bluegreen

import (
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CLUSTER NODES fixtures modelled on the postmortem topology (3 shards, blue=alt→green=main).
// Format: <id> <ip:port@cport> <flags> <master|-> <ping> <pong> <epoch> <link> <slots...>
// Hand-built fixtures cover the blank-master and empty-IP cases from the incident.
const (
	// WaitingSync: green pods are replicas of blue primaries; blue owns all slots.
	nodesWaitingSync = `
blueA 10.0.1.1:6379@16379 master - 0 1 1 connected 0-5460
blueB 10.0.1.2:6379@16379 master - 0 2 2 connected 5461-10922
blueC 10.0.1.3:6379@16379 master - 0 3 3 connected 10923-16383
green0 10.0.2.1:6379@16379 slave blueA 0 4 1 connected
green1 10.0.2.2:6379@16379 slave blueB 0 5 2 connected
green2 10.0.2.3:6379@16379 slave blueC 0 6 3 connected
`
	// Promoted: green owns every shard, blue is gone.
	nodesPromoted = `
green0 10.0.2.1:6379@16379 master - 0 4 4 connected 0-5460
green1 10.0.2.2:6379@16379 master - 0 5 5 connected 5461-10922
green2 10.0.2.3:6379@16379 master - 0 6 6 connected 10923-16383
`
	// green-2 is blank, but blue-C still covers the shard.
	nodesDegradedCovered = `
green0 10.0.2.1:6379@16379 master - 0 4 4 connected 0-5460
green1 10.0.2.2:6379@16379 master - 0 5 5 connected 5461-10922
green2new 10.0.2.3:6379@16379 master - 0 6 7 connected
blueC 10.0.1.3:6379@16379 master - 0 3 3 connected 10923-16383
`
	// green-2 is blank and blue-C is gone: shard C has no owner.
	nodesUnsafe = `
green0 10.0.2.1:6379@16379 master - 0 4 4 connected 0-5460
green1 10.0.2.2:6379@16379 master - 0 5 5 connected 5461-10922
green2new 10.0.2.3:6379@16379 master - 0 6 7 connected
`
	// blue-C still lists shard C, but is disconnected and cannot cover it.
	nodesDisconnectedOwner = `
green0 10.0.2.1:6379@16379 master - 0 4 4 connected 0-5460
green1 10.0.2.2:6379@16379 master - 0 5 5 connected 5461-10922
green2new 10.0.2.3:6379@16379 master - 0 6 7 connected
blueC 10.0.1.3:6379@16379 master - 0 3 3 disconnected 10923-16383
`
	// myself line with empty IP (cluster-announce-ip unset) — must still be resolvable by id.
	nodesMyselfEmptyIP = `
green2new :6379@16379 myself,master - 0 0 7 connected
blueC 10.0.1.3:6379@16379 master - 0 3 3 connected 10923-16383
`
)

// threeShardMapping is the ShardMapping for the 3-shard fixtures above.
func threeShardMapping() []cachev1.ShardMappingEntry {
	return []cachev1.ShardMappingEntry{
		{GreenPodName: "green-0", GreenPodIP: "10.0.2.1", GreenNodeID: "green0", BluePrimaryIP: "10.0.1.1", BluePrimaryID: "blueA", SlotsRanges: "0-5460"},
		{GreenPodName: "green-1", GreenPodIP: "10.0.2.2", GreenNodeID: "green1", BluePrimaryIP: "10.0.1.2", BluePrimaryID: "blueB", SlotsRanges: "5461-10922"},
		{GreenPodName: "green-2", GreenPodIP: "10.0.2.3", GreenNodeID: "green2", BluePrimaryIP: "10.0.1.3", BluePrimaryID: "blueC", SlotsRanges: "10923-16383"},
	}
}

func TestAssessShards_WaitingSync_AllReadyCoveredByBlue(t *testing.T) {
	a := assessShards(nodesWaitingSync, threeShardMapping(), nil)
	assert.True(t, a.AllCovered, "blue owns every shard")
	assert.Empty(t, a.PendingRejoin)
	assert.Empty(t, a.UnsafeSlots)
	for _, s := range a.Shards {
		assert.Equal(t, ShardReady, s.State, "green %s is a synced replica", s.GreenPodName)
		assert.True(t, s.Covered)
	}
}

func TestAssessShards_Promoted_AllPromoted(t *testing.T) {
	a := assessShards(nodesPromoted, threeShardMapping(), nil)
	assert.True(t, a.AllCovered)
	assert.Empty(t, a.PendingRejoin)
	for _, s := range a.Shards {
		assert.Equal(t, ShardPromoted, s.State)
		assert.True(t, s.Covered)
	}
}

func TestAssessShards_BlankGreenButBlueOwns_DegradedAndDeferred(t *testing.T) {
	a := assessShards(nodesDegradedCovered, threeShardMapping(), nil)
	assert.True(t, a.AllCovered, "shard C still covered by blue")
	assert.Empty(t, a.UnsafeSlots)
	assert.Equal(t, []string{"green-2"}, a.PendingRejoin, "blank green-2 deferred to maintenance")

	states := map[string]ShardState{}
	for _, s := range a.Shards {
		states[s.GreenPodName] = s.State
	}
	assert.Equal(t, ShardPromoted, states["green-0"])
	assert.Equal(t, ShardPromoted, states["green-1"])
	assert.Equal(t, ShardDegraded, states["green-2"], "blank lone master is never promoted")
}

func TestAssessShards_BlankGreenAndBlueGone_Unsafe(t *testing.T) {
	a := assessShards(nodesUnsafe, threeShardMapping(), nil)
	assert.False(t, a.AllCovered, "nobody owns shard C")
	assert.Equal(t, []string{"10923-16383"}, a.UnsafeSlots)

	states := map[string]ShardState{}
	for _, s := range a.Shards {
		states[s.GreenPodName] = s.State
	}
	assert.Equal(t, ShardUnsafe, states["green-2"], "lost shard is UNSAFE, not merely degraded")
}

func TestClusterViewCoversMappings(t *testing.T) {
	mappings := threeShardMapping()
	replacementMappings := threeShardMapping()
	replacementMappings[2].GreenNodeID = "green2new"
	for _, tc := range []struct {
		name     string
		raw      string
		mappings []cachev1.ShardMappingEntry
		want     bool
	}{
		{name: "waiting sync", raw: nodesWaitingSync, mappings: mappings, want: true},
		{name: "promoted", raw: nodesPromoted, mappings: mappings, want: true},
		{name: "stale green identity", raw: nodesDegradedCovered, mappings: mappings, want: false},
		{name: "replacement identity covered by blue", raw: nodesDegradedCovered, mappings: replacementMappings, want: true},
		{name: "uncovered shard", raw: nodesUnsafe, mappings: replacementMappings, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			byID, ipToID := parseClusterNodes(tc.raw)
			assert.Equal(t, tc.want, clusterViewCoversMappings(byID, ipToID, tc.mappings))
		})
	}
}

func TestAssessShards_DisconnectedOwnerDoesNotCoverSlots(t *testing.T) {
	a := assessShards(nodesDisconnectedOwner, threeShardMapping(), nil)
	assert.False(t, a.AllCovered, "disconnected masters do not cover slots")
	assert.Equal(t, []string{"10923-16383"}, a.UnsafeSlots)

	states := map[string]ShardState{}
	for _, s := range a.Shards {
		states[s.GreenPodName] = s.State
	}
	assert.Equal(t, ShardUnsafe, states["green-2"])
}

func TestAssessShards_DataProbe_OwnsSlotsButEmpty_NotPromoted(t *testing.T) {
	// Owns slots, but an empty data probe keeps it degraded.
	dataPresent := func(ip string) bool { return ip != "10.0.2.1" }
	a := assessShards(nodesPromoted, threeShardMapping(), dataPresent)

	states := map[string]ShardState{}
	for _, s := range a.Shards {
		states[s.GreenPodName] = s.State
	}
	assert.Equal(t, ShardDegraded, states["green-0"], "owns slots but empty → degraded")
	assert.Equal(t, ShardPromoted, states["green-1"])
	assert.Contains(t, a.PendingRejoin, "green-0")
}

func TestIsHealthyPrimary(t *testing.T) {
	byID, ipToID := parseClusterNodes(nodesPromoted)

	tests := []struct {
		name          string
		nodeID, ip    string
		expectedSlots string
		want          bool
	}{
		{"master owns exact slots", "green0", "10.0.2.1", "0-5460", true},
		{"master owns superset", "green2", "10.0.2.3", "11000-12000", true},
		{"master owns partial only", "green0", "10.0.2.1", "0-9000", false},
		{"absent node", "ghost", "10.9.9.9", "0-5460", false},
		{"stale id does not resolve by ip", "stale-id", "10.0.2.2", "5461-10922", false},
		{"empty id does not resolve by ip", "", "10.0.2.2", "5461-10922", false},
		{"empty expected slots is never owned", "green0", "10.0.2.1", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isHealthyPrimary(byID, ipToID, tt.nodeID, tt.ip, tt.expectedSlots))
		})
	}
}

func TestIsHealthyPrimary_RejectsReusedIPWithConflictingID(t *testing.T) {
	const nodes = `
oldGreen 10.0.2.3:6379@16379 master - 0 5 5 connected 10923-16383
green2new 10.0.2.3:6379@16379 master - 0 6 6 connected
`
	byID, ipToID := parseClusterNodes(nodes)
	assert.False(t, isHealthyPrimary(byID, ipToID, "oldGreen", "10.0.2.3", "10923-16383"))
	assert.False(t, isHealthyPrimary(byID, ipToID, "green2new", "10.0.2.3", "10923-16383"))
}

func TestIsHealthyPrimary_ReplicaAndFail(t *testing.T) {
	byID, ipToID := parseClusterNodes(nodesWaitingSync)
	// green0 is a slave → not a healthy primary even though reachable.
	assert.False(t, isHealthyPrimary(byID, ipToID, "green0", "10.0.2.1", "0-5460"))
	// blueA is a healthy master owning 0-5460.
	assert.True(t, isHealthyPrimary(byID, ipToID, "blueA", "10.0.1.1", "0-5460"))
}

func TestIsHealthyPrimary_FailFlag(t *testing.T) {
	const nodes = `
blueA 10.0.1.1:6379@16379 master,fail - 0 1 1 disconnected 0-5460
`
	byID, ipToID := parseClusterNodes(nodes)
	assert.False(t, isHealthyPrimary(byID, ipToID, "blueA", "10.0.1.1", "0-5460"), "failing master is not healthy")
}

func TestIsHealthyPrimary_DisconnectedAndNoAddr(t *testing.T) {
	const nodes = `
blueA 10.0.1.1:6379@16379 master - 0 1 1 disconnected 0-5460
blueB 10.0.1.2:6379@16379 master,noaddr - 0 2 2 connected 5461-10922
`
	byID, ipToID := parseClusterNodes(nodes)
	assert.False(t, isHealthyPrimary(byID, ipToID, "blueA", "10.0.1.1", "0-5460"), "disconnected master is not healthy")
	assert.False(t, isHealthyPrimary(byID, ipToID, "blueB", "10.0.1.2", "5461-10922"), "noaddr master is not healthy")
	assert.Empty(t, byID["blueA"].slots)
	assert.Empty(t, byID["blueB"].slots)
}

func TestParseClusterNodes_MyselfEmptyIP_ResolvableByID(t *testing.T) {
	byID, ipToID := parseClusterNodes(nodesMyselfEmptyIP)
	// myself line has empty IP: not indexed by IP, but recorded by ID.
	assert.NotContains(t, ipToID, "")
	n, ok := byID["green2new"]
	assert.True(t, ok, "myself node recorded by id despite empty ip")
	assert.True(t, n.isMaster)
	assert.Empty(t, n.slots, "blank master owns no slots")
	// blue-C (a peer) is indexed normally.
	assert.Equal(t, "blueC", ipToID["10.0.1.3"])
}

func TestParseClusterNodesFrom_BackfillsOnlyMyselfIP(t *testing.T) {
	byID, ipToID := parseClusterNodesFrom(nodesMyselfEmptyIP, "10.0.2.3")
	assert.Equal(t, "10.0.2.3", byID["green2new"].ip)
	assert.Equal(t, "green2new", ipToID["10.0.2.3"])

	mapping := []cachev1.ShardMappingEntry{{
		GreenNodeID: "green2new",
		GreenPodIP:  "10.0.2.3",
		SlotsRanges: "10923-16383",
	}}
	assert.True(t, clusterViewCoversMappings(byID, ipToID, mapping))
}

func TestParseClusterNodesSupportsIPv6(t *testing.T) {
	byID, ipToID := parseClusterNodes("node-a [2001:db8::10]:6379@16379 master - 0 0 1 connected 0-16383")

	require.Equal(t, "2001:db8::10", byID["node-a"].ip)
	require.Equal(t, "node-a", ipToID["2001:db8::10"])
	require.Equal(t, []redis_cluster.SlotsRange{{First: 0, Last: 16383}}, byID["node-a"].slots)
}

func TestCoversAll(t *testing.T) {
	r := parseSlotRangeString
	tests := []struct {
		name       string
		have, want string
		expect     bool
	}{
		{"exact", "0-5460", "0-5460", true},
		{"superset", "0-16383", "5461-10922", true},
		{"union of adjacent covers want", "0-5460,5461-10922", "0-10922", true},
		{"partial not covered", "0-5460", "0-9000", false},
		{"single slot covered", "0-100", "50", true},
		{"empty want", "0-5460", "", false},
		{"nothing owned", "", "0-5460", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expect, coversAll(r(tt.have), r(tt.want)))
		})
	}
}

func TestMergeRanges_MaxIntDoesNotOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	ranges := []redis_cluster.SlotsRange{
		{First: maxInt, Last: maxInt},
		{First: maxInt, Last: maxInt},
	}

	assert.Equal(t, []redis_cluster.SlotsRange{{First: maxInt, Last: maxInt}}, mergeRanges(ranges))
}

func TestDecisionTable(t *testing.T) {
	shards := func(states ...ShardState) []ShardAssessment {
		out := make([]ShardAssessment, len(states))
		for i, s := range states {
			out[i] = ShardAssessment{GreenPodName: "green-" + string(rune('0'+i)), State: s, Covered: s != ShardUnsafe}
		}
		return out
	}
	tests := []struct {
		name      string
		a         ClusterAssessment
		hasBackup bool
		want      bgAction
	}{
		{"all green -> finish forward", ClusterAssessment{AllCovered: true, Shards: shards(ShardPromoted, ShardPromoted, ShardPromoted)}, false, ActFinishForward},
		{"all blue -> roll back", ClusterAssessment{AllCovered: true, Shards: shards(ShardOnBlue, ShardReady, ShardOnBlue)}, false, ActRollBack},
		{"split -> restore", ClusterAssessment{AllCovered: true, Shards: shards(ShardPromoted, ShardOnBlue, ShardPromoted)}, false, ActRestore},
		{"uncovered + backup -> restore", ClusterAssessment{AllCovered: false, Shards: shards(ShardPromoted, ShardUnsafe), UnsafeSlots: []string{"10923-16383"}}, true, ActRestore},
		{"uncovered + no backup -> salvage", ClusterAssessment{AllCovered: false, Shards: shards(ShardPromoted, ShardUnsafe), UnsafeSlots: []string{"10923-16383"}}, false, ActSalvage},
		{"no shards -> wait", ClusterAssessment{AllCovered: true, Shards: nil}, false, ActWait},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decisionTable(tt.a, tt.hasBackup)
			assert.Equal(t, tt.want, got.Action, got.Reason)
		})
	}
}

// Never finish forward unless every shard is green-owned.
func TestDecisionTable_NeverFinishForwardWhenNotAllGreen(t *testing.T) {
	partial := ClusterAssessment{AllCovered: true, Shards: []ShardAssessment{
		{GreenPodName: "green-0", State: ShardPromoted, Covered: true},
		{GreenPodName: "green-1", State: ShardOnBlue, Covered: true}, // blue owns this one
	}}
	assert.NotEqual(t, ActFinishForward, decisionTable(partial, false).Action,
		"must not cut over while blue still owns a shard")
}

func TestParseSlotRangeString(t *testing.T) {
	assert.Equal(t, 1, len(parseSlotRangeString("0-5460")))
	assert.Equal(t, 2, len(parseSlotRangeString("0-100,200-300")))
	single := parseSlotRangeString("5000")
	assert.Equal(t, 1, len(single))
	assert.Equal(t, 5000, single[0].First)
	assert.Equal(t, 5000, single[0].Last)
	assert.Nil(t, parseSlotRangeString(""))
	assert.Empty(t, parseSlotRangeString("garbage"))
	// malformed reversed range is skipped, valid one kept.
	assert.Equal(t, 1, len(parseSlotRangeString("300-200,0-100")))
}

func TestClusterNodesProbeIPsPrefersCurrentPods(t *testing.T) {
	rc := &cachev1.RedisCluster{Spec: cachev1.RedisClusterSpec{
		StatefulsetTemplate: appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "cache"}},
	}}
	cluster := &redis_cluster.Cluster{
		RedisCluster: rc,
		AllNodes: []*redis_cluster.ClusterNode{
			{PodName: GreenSTSName(rc) + "-0", IP: "10.0.0.2"},
			{PodName: "unrelated-0", IP: "10.0.0.3"},
			{PodName: BlueSTSName(rc) + "-0", IP: "10.0.0.1"},
		},
	}
	mappings := []cachev1.ShardMappingEntry{
		{GreenPodIP: "10.0.0.9"},
		{GreenPodIP: "10.0.0.2"},
		{GreenPodIP: "10.0.0.9"},
	}

	assert.Equal(t, []string{"10.0.0.9", "10.0.0.2"}, mappedGreenPodIPs(mappings))
	assert.Equal(t, []string{"10.0.0.2", "10.0.0.9"},
		clusterNodesProbeIPs(cluster, mappings))
}
