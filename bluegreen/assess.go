// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
)

// assess.go classifies slot ownership from CLUSTER NODES so phases gate on data safety.

// ShardState is a green pod's role in the migration, judged from live topology.
type ShardState string

const (
	// ShardPromoted: green is a non-failing master that owns its expected slots.
	ShardPromoted ShardState = "PROMOTED"
	// ShardReady: the green pod is a healthy replica, ready to be promoted when it is its turn.
	ShardReady ShardState = "READY"
	// ShardOnBlue: the green pod is not yet promoted and the slots are still owned by blue.
	ShardOnBlue ShardState = "ON_BLUE"
	// ShardDegraded: green is blank, unreachable, or owns the wrong slots.
	ShardDegraded ShardState = "DEGRADED"
	// ShardUnsafe: no live node owns this shard's slots.
	ShardUnsafe ShardState = "UNSAFE"
)

// ShardAssessment is the per-shard result of assessShards.
type ShardAssessment struct {
	// GreenPodName identifies the shard by its stable green pod name.
	GreenPodName string
	// State is the green pod's role in the migration.
	State ShardState
	// Covered reports whether any live master owns this shard's slots.
	Covered bool
}

// ClusterAssessment is the whole-cluster result of assessShards.
type ClusterAssessment struct {
	// AllCovered is true when every shard's slots have a live owner.
	AllCovered bool
	// Shards is the per-shard assessment, in ShardMapping order.
	Shards []ShardAssessment
	// PendingRejoin lists DEGRADED green pods for maintenance.
	PendingRejoin []string
	// UnsafeSlots lists the expected slot-range strings that no live node owns.
	UnsafeSlots []string
}

// parsedNode is one node's role and slots as reported by a single CLUSTER NODES line.
type parsedNode struct {
	id        string
	ip        string // "" for the "myself" line when cluster-announce-ip is unset
	isMaster  bool
	isFail    bool // fail or fail? (PFAIL) — not a trustworthy owner
	isNoAddr  bool
	connected bool
	primaryID string
	slots     []redis_cluster.SlotsRange
}

// parseClusterNodes records nodes by ID and indexes non-empty IPs.
func parseClusterNodes(clusterNodes string) (byID map[string]parsedNode, ipToID map[string]string) {
	return parseClusterNodesFrom(clusterNodes, "")
}

// parseClusterNodesFrom supplies the queried IP for an empty myself address.
func parseClusterNodesFrom(clusterNodes, viewerIP string) (byID map[string]parsedNode, ipToID map[string]string) {
	byID = make(map[string]parsedNode)
	ipToID = make(map[string]string)

	for _, line := range strings.Split(clusterNodes, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		id := fields[0]
		ip, _, addressOK := redis_cluster.ParseClusterNodeAddress(fields[1])
		if !addressOK {
			continue
		}
		flags := fields[2]
		if ip == "" && viewerIP != "" && hasNodeFlag(flags, "myself") {
			ip = viewerIP
		}
		linkState := ""
		if len(fields) >= 8 {
			linkState = fields[7]
		}

		n := parsedNode{
			id:        id,
			ip:        ip,
			isMaster:  hasNodeFlag(flags, "master"),
			isFail:    hasNodeFlag(flags, "fail") || hasNodeFlag(flags, "fail?"),
			isNoAddr:  hasNodeFlag(flags, "noaddr"),
			connected: linkState == "connected",
		}
		if len(fields) >= 4 && fields[3] != "-" {
			n.primaryID = fields[3]
		}
		// Slot tokens follow the link-state field (index 7). Skip open-slot markers "[...]".
		if n.canOwnSlots() && len(fields) >= 9 {
			n.slots = parseSlotTokens(fields[8:])
		}

		byID[id] = n
		if ip != "" {
			ipToID[ip] = id
		}
	}
	return byID, ipToID
}

func hasNodeFlag(flags, flag string) bool {
	for _, f := range strings.Split(flags, ",") {
		if f == flag {
			return true
		}
	}
	return false
}

func (n parsedNode) canOwnSlots() bool {
	return n.isMaster && !n.isFail && !n.isNoAddr && n.connected
}

// parseSlotTokens parses CLUSTER NODES slot tokens ("0-5460", "12345") into ranges,
// ignoring transient open-slot markers ("[12345-<-nodeid]", "[12345->-nodeid]").
func parseSlotTokens(tokens []string) []redis_cluster.SlotsRange {
	ranges := make([]redis_cluster.SlotsRange, 0, len(tokens))
	for _, tok := range tokens {
		if strings.HasPrefix(tok, "[") {
			continue
		}
		if r, ok := parseOneRange(tok); ok {
			ranges = append(ranges, r)
		}
	}
	return ranges
}

// parseSlotRangeString parses a ShardMappingEntry.SlotsRanges string (e.g. "0-5460" or
// "0-5460,10923-16383") into ranges.
func parseSlotRangeString(s string) []redis_cluster.SlotsRange {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	ranges := make([]redis_cluster.SlotsRange, 0)
	for _, tok := range strings.Split(s, ",") {
		if r, ok := parseOneRange(strings.TrimSpace(tok)); ok {
			ranges = append(ranges, r)
		}
	}
	return ranges
}

// parseOneRange parses a single "first-last" or single-slot token.
func parseOneRange(tok string) (redis_cluster.SlotsRange, bool) {
	if tok == "" {
		return redis_cluster.SlotsRange{}, false
	}
	if !strings.Contains(tok, "-") {
		n, err := strconv.Atoi(tok)
		if err != nil {
			return redis_cluster.SlotsRange{}, false
		}
		return redis_cluster.SlotsRange{First: n, Last: n}, true
	}
	parts := strings.SplitN(tok, "-", 2)
	first, err1 := strconv.Atoi(parts[0])
	last, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || last < first {
		return redis_cluster.SlotsRange{}, false
	}
	return redis_cluster.SlotsRange{First: first, Last: last}, true
}

// coversAll reports whether the union of have covers every slot in want.
// An empty want is trivially not covered (we never treat "no expected slots" as owned).
func coversAll(have []redis_cluster.SlotsRange, want []redis_cluster.SlotsRange) bool {
	if len(want) == 0 {
		return false
	}
	merged := mergeRanges(have)
	for _, w := range want {
		if !rangeCovered(merged, w) {
			return false
		}
	}
	return true
}

// mergeRanges returns a sorted, coalesced copy of ranges (insertion sort — the slice is tiny).
func mergeRanges(ranges []redis_cluster.SlotsRange) []redis_cluster.SlotsRange {
	if len(ranges) == 0 {
		return nil
	}
	sorted := make([]redis_cluster.SlotsRange, len(ranges))
	copy(sorted, ranges)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].First < sorted[j-1].First; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	merged := []redis_cluster.SlotsRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		if r.First <= last.Last || (last.Last < r.First && last.Last == r.First-1) {
			if r.Last > last.Last {
				last.Last = r.Last
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// rangeCovered reports whether a single want range lies wholly within one merged range.
func rangeCovered(merged []redis_cluster.SlotsRange, want redis_cluster.SlotsRange) bool {
	for _, m := range merged {
		if want.First >= m.First && want.Last <= m.Last {
			return true
		}
	}
	return false
}

// resolveNode requires ID and IP to agree when both are known.
func resolveNode(byID map[string]parsedNode, ipToID map[string]string, nodeID, ip string) (parsedNode, bool) {
	if nodeID == "" {
		return parsedNode{}, false
	}
	n, ok := byID[nodeID]
	if !ok {
		return parsedNode{}, false
	}
	if ip == "" {
		return n, true
	}
	if n.ip != ip || ipToID[ip] != nodeID {
		return parsedNode{}, false
	}
	return n, true
}

// isHealthyPrimary requires a non-failing master that owns every expected slot.
func isHealthyPrimary(byID map[string]parsedNode, ipToID map[string]string, nodeID, ip, expectedSlots string) bool {
	n, ok := resolveNode(byID, ipToID, nodeID, ip)
	if !ok {
		return false
	}
	if !n.canOwnSlots() {
		return false
	}
	return coversAll(n.slots, parseSlotRangeString(expectedSlots))
}

// slotsOwner returns the id of a non-failing master that owns all of want, or "" if none does.
func slotsOwner(byID map[string]parsedNode, want []redis_cluster.SlotsRange) string {
	for id, n := range byID {
		if n.canOwnSlots() && coversAll(n.slots, want) {
			return id
		}
	}
	return ""
}

// isPromoted uses slot ownership when a live view is available.
func isPromoted(byID map[string]parsedNode, ipToID map[string]string, viewErr error, m cachev1.ShardMappingEntry, replInfo string) bool {
	if viewErr == nil {
		return isHealthyPrimary(byID, ipToID, m.GreenNodeID, m.GreenPodIP, m.SlotsRanges)
	}
	return strings.Contains(replInfo, constants.RedisRoleMaster)
}

// bgAction is the watchdog's data-safety decision.
type bgAction int

const (
	// ActWait: data is safe but the phase hasn't converged yet — no destructive action.
	ActWait bgAction = iota
	// ActFinishForward: green owns every shard with data — cut over, FORGET blue.
	ActFinishForward
	// ActRollBack: blue still owns everything, green isn't ready — drop green, keep blue.
	ActRollBack
	// ActRestore: some shards are only-on-one-side or have a recoverable gap — restore from backup.
	ActRestore
	// ActSalvage: no live owner and no backup; keep surviving data and page.
	ActSalvage
)

func (a bgAction) String() string {
	switch a {
	case ActFinishForward:
		return "FinishForward"
	case ActRollBack:
		return "RollBack"
	case ActRestore:
		return "Restore"
	case ActSalvage:
		return "Salvage"
	default:
		return "Wait"
	}
}

// bgDecision is the decision-table result.
type bgDecision struct {
	Action      bgAction
	Reason      string
	UnsafeSlots []string
}

// decisionTable chooses the watchdog action from coverage and green ownership.
func decisionTable(a ClusterAssessment, hasBackup bool) bgDecision {
	total := len(a.Shards)
	if total == 0 {
		return bgDecision{Action: ActWait, Reason: "no shards to assess"}
	}

	greenOwns := 0
	for _, s := range a.Shards {
		if s.State == ShardPromoted {
			greenOwns++
		}
	}

	if a.AllCovered {
		switch {
		case greenOwns == total:
			return bgDecision{Action: ActFinishForward, Reason: "green owns all shards with data"}
		case greenOwns == 0:
			return bgDecision{Action: ActRollBack, Reason: "blue still owns all shards; green not ready"}
		default:
			return bgDecision{Action: ActRestore, Reason: fmt.Sprintf("split: green owns %d/%d, restore the rest onto one survivor", greenOwns, total)}
		}
	}

	// Some slot has no live owner.
	if hasBackup {
		return bgDecision{Action: ActRestore, Reason: "uncovered slots, backup exists — restore", UnsafeSlots: a.UnsafeSlots}
	}
	return bgDecision{Action: ActSalvage, Reason: "uncovered slots, no backup — salvage what has data and PAGE", UnsafeSlots: a.UnsafeSlots}
}

// fetchClusterNodes prefers a mapping-complete view, then the richest non-isolated view.
func (h *Handler) fetchClusterNodes(
	ctx context.Context,
	bgStatus *cachev1.BlueGreenStatus,
	toolbox *redis_cluster.Toolbox,
) (string, string, error) {
	viewCtx, cancelView := context.WithTimeout(ctx, blueGreenClusterViewTimeout)
	defer cancelView()

	best := ""
	bestViewerIP := ""
	bestNodes := 0
	responders := 0
	for _, ip := range mappedGreenPodIPs(bgStatus.ShardMapping) {
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		_, stdout, _, err := toolbox.ClusterNodes(probeCtx, ip)
		cancelProbe()
		if err != nil || stdout == "" {
			continue
		}
		responders++
		byID, ipToID := parseClusterNodesFrom(stdout, ip)
		if clusterViewCoversMappings(byID, ipToID, bgStatus.ShardMapping) {
			return stdout, ip, nil
		}
		if len(byID) > bestNodes {
			best, bestViewerIP, bestNodes = stdout, ip, len(byID)
		}
	}
	if responders == 0 {
		return "", "", fmt.Errorf("no green node responded to CLUSTER NODES")
	}
	if bestNodes < len(bgStatus.ShardMapping) {
		return "", "", fmt.Errorf("no green node has a complete cluster view (best sees %d nodes, want >= %d shards) — refusing to assess from an isolated view",
			bestNodes, len(bgStatus.ShardMapping))
	}
	return best, bestViewerIP, nil
}

func clusterViewCoversMappings(
	byID map[string]parsedNode,
	ipToID map[string]string,
	mappings []cachev1.ShardMappingEntry,
) bool {
	if len(mappings) == 0 || len(byID) < len(mappings) {
		return false
	}
	for _, mapping := range mappings {
		if _, ok := resolveNode(byID, ipToID, mapping.GreenNodeID, mapping.GreenPodIP); !ok {
			return false
		}
		if slotsOwner(byID, parseSlotRangeString(mapping.SlotsRanges)) == "" {
			return false
		}
	}
	return true
}

// clusterNodesView fetches and parses the best available CLUSTER NODES view.
func (h *Handler) clusterNodesView(ctx context.Context, bgStatus *cachev1.BlueGreenStatus, toolbox *redis_cluster.Toolbox) (map[string]parsedNode, map[string]string, error) {
	stdout, viewerIP, err := h.fetchClusterNodes(ctx, bgStatus, toolbox)
	if err != nil {
		return nil, nil, err
	}
	byID, ipToID := parseClusterNodesFrom(stdout, viewerIP)
	return byID, ipToID, nil
}

// assessLiveShards fetches live cluster state and classifies every shard (§3.4).
func (h *Handler) assessLiveShards(ctx context.Context, bgStatus *cachev1.BlueGreenStatus, toolbox *redis_cluster.Toolbox) (ClusterAssessment, error) {
	stdout, viewerIP, err := h.fetchClusterNodes(ctx, bgStatus, toolbox)
	if err != nil {
		return ClusterAssessment{}, err
	}
	return assessShardsFrom(stdout, viewerIP, bgStatus.ShardMapping, nil), nil
}

// assessShards classifies each shard from one CLUSTER NODES snapshot.
// dataPresent optionally rejects green pods that own slots but are empty.
func assessShards(
	clusterNodes string,
	mapping []cachev1.ShardMappingEntry,
	dataPresent func(ip string) bool,
) ClusterAssessment {
	return assessShardsFrom(clusterNodes, "", mapping, dataPresent)
}

func assessShardsFrom(
	clusterNodes string,
	viewerIP string,
	mapping []cachev1.ShardMappingEntry,
	dataPresent func(ip string) bool,
) ClusterAssessment {
	byID, ipToID := parseClusterNodesFrom(clusterNodes, viewerIP)

	result := ClusterAssessment{
		AllCovered: true,
		Shards:     make([]ShardAssessment, 0, len(mapping)),
	}

	for _, m := range mapping {
		want := parseSlotRangeString(m.SlotsRanges)
		ownerID := slotsOwner(byID, want)
		covered := ownerID != ""

		sa := ShardAssessment{GreenPodName: m.GreenPodName, Covered: covered}

		greenNode, greenFound := resolveNode(byID, ipToID, m.GreenNodeID, m.GreenPodIP)
		greenOwns := covered && (ownerID == greenNode.id) && greenFound

		switch {
		case greenOwns && (dataPresent == nil || dataPresent(m.GreenPodIP)):
			sa.State = ShardPromoted
		case greenOwns:
			// Owns slots, but the data probe says empty.
			sa.State = ShardDegraded
		case !greenFound:
			// Green is absent from the cluster view and needs rejoin.
			sa.State = ShardDegraded
		case greenNode.isFail:
			sa.State = ShardDegraded
		case greenNode.isMaster:
			// A master without expected slots is blank/lone.
			sa.State = ShardDegraded
		case !greenNode.isMaster && greenNode.primaryID != "":
			// Healthy replica, ready for its promotion turn.
			sa.State = ShardReady
		case covered:
			// Blue or another node still covers the slots.
			sa.State = ShardOnBlue
		default:
			sa.State = ShardDegraded
		}

		if !covered {
			sa.State = ShardUnsafe
			result.AllCovered = false
			result.UnsafeSlots = append(result.UnsafeSlots, m.SlotsRanges)
		}
		if sa.State == ShardDegraded {
			result.PendingRejoin = append(result.PendingRejoin, m.GreenPodName)
		}

		result.Shards = append(result.Shards, sa)
	}

	return result
}
