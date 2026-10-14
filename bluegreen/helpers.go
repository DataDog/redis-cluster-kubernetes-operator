// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	blueGreenClusterViewTimeout = 30 * time.Second
	blueGreenRedisProbeTimeout  = 5 * time.Second
)

// GetPodIndex extracts the numeric index from a pod name (e.g., "redis-cluster-richa-12" -> 12).
// Returns -1 if no numeric suffix found.
func GetPodIndex(podName string) int {
	parts := strings.Split(podName, "-")
	if len(parts) == 0 {
		return -1
	}
	idx, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return -1
	}
	return idx
}

// ParseInfoField extracts an integer value from INFO output by field name.
func ParseInfoField(info string, field string) int64 {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if strings.HasPrefix(line, field+":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				val, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
				if err == nil {
					return val
				}
			}
		}
	}
	return -1
}

// ParseClusterNodesOutput parses CLUSTER NODES output and returns:
// - ipToNodeID: map of IP address to node ID
// - slotRangeToPrimaryIP: map of slot range (e.g. "0-3276") to primary IP
func ParseClusterNodesOutput(clusterNodesStr string) (ipToNodeID map[string]string, slotRangeToPrimaryIP map[string]string) {
	ipToNodeID = make(map[string]string)
	slotRangeToPrimaryIP = make(map[string]string)

	for _, line := range strings.Split(clusterNodesStr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		nodeID := fields[0]
		ip := strings.Split(strings.Split(fields[1], "@")[0], ":")[0]

		// A node's "myself" entry reports an empty IP when cluster-announce-ip
		// is unset. Mapping ip="" would let the querying node be misclassified
		// (e.g. FORGET'd as if it were blue), so skip it.
		if ip == "" {
			continue
		}

		ipToNodeID[ip] = nodeID

		if len(fields) >= 3 {
			flags := fields[2]
			if strings.Contains(flags, "master") && !strings.Contains(flags, "fail") && len(fields) >= 9 {
				for _, slotPart := range fields[8:] {
					if strings.Contains(slotPart, "-") && !strings.HasPrefix(slotPart, "[") {
						slotRangeToPrimaryIP[slotPart] = ip
					}
				}
			}
		}
	}
	return
}

// setPhase transitions to a new phase, stamping PhaseEntryTime so the watchdog measures from real
// entry. Does not touch LastProgressTime (intra-phase counters own that).
func setPhase(bgStatus *cachev1.BlueGreenStatus, phase cachev1.BlueGreenPhase, msg string) {
	if bgStatus.Phase != phase || phase != cachev1.BlueGreenPhasePromoting {
		bgStatus.ConsistencyGateAttempts = 0
	}
	bgStatus.Phase = phase
	bgStatus.PhaseEntryTime = time.Now().Unix()
	if msg != "" {
		bgStatus.Message = msg
	}
}

// FormatSlotsRanges formats a slice of SlotsRange into a human-readable string.
func FormatSlotsRanges(ranges []redis_cluster.SlotsRange) string {
	parts := make([]string, 0, len(ranges))
	for _, r := range ranges {
		if r.First == r.Last {
			parts = append(parts, strconv.Itoa(r.First))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", r.First, r.Last))
		}
	}
	return strings.Join(parts, ",")
}

// ────────────────────────────────────────────────────────────────────────────
// Handler helper methods
// ────────────────────────────────────────────────────────────────────────────

// getHealthyGreenIP finds a responsive green node from ShardMapping.
func (h *Handler) getHealthyGreenIP(shardMapping []cachev1.ShardMappingEntry, toolbox *redis_cluster.Toolbox) (string, string, error) {
	for _, m := range shardMapping {
		if m.GreenPodIP == "" {
			continue
		}
		if toolbox.PingIP(m.GreenPodIP) {
			return m.GreenPodIP, m.GreenPodName, nil
		}
	}
	return "", "", fmt.Errorf("no healthy green node found in ShardMapping (%d entries)", len(shardMapping))
}

// refreshShardMappingIPs refreshes green pod IPs from Kubernetes.
func (h *Handler) refreshShardMappingIPs(ctx context.Context, bgStatus *cachev1.BlueGreenStatus, rc *cachev1.RedisCluster) {
	greenPods, _ := h.getGreenPods(ctx, rc)
	podIPs := make(map[string]string)
	for _, pod := range greenPods {
		podIPs[pod.Name] = pod.Status.PodIP
	}
	for i := range bgStatus.ShardMapping {
		mapping := &bgStatus.ShardMapping[i]
		if freshIP, ok := podIPs[mapping.GreenPodName]; ok && freshIP != "" {
			if mapping.GreenPodIP != "" && mapping.GreenPodIP != freshIP {
				mapping.GreenNodeID = ""
			}
			mapping.GreenPodIP = freshIP
		}
	}
}

// refreshShardMappingFromCluster refreshes stale IPs and node IDs in shardMapping
// by querying CLUSTER NODES. Call this at the start of phases that use shardMapping
// to handle cases where pods were deleted/recreated or failovers occurred.
func (h *Handler) refreshShardMappingFromCluster(ctx context.Context, bgStatus *cachev1.BlueGreenStatus, cluster *redis_cluster.Cluster) error {
	toolbox := cluster.Toolbox
	viewCtx, cancelView := context.WithTimeout(ctx, blueGreenClusterViewTimeout)
	defer cancelView()

	h.refreshShardMappingIPs(ctx, bgStatus, cluster.RedisCluster)

	if len(bgStatus.ShardMapping) == 0 {
		return nil
	}

	for i := range bgStatus.ShardMapping {
		mapping := &bgStatus.ShardMapping[i]
		oldID := mapping.GreenNodeID
		mapping.GreenNodeID = ""
		if mapping.GreenPodIP == "" {
			continue
		}
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		freshID, err := toolbox.GetNodeIDByIPContext(probeCtx, mapping.GreenPodIP)
		cancelProbe()
		if err != nil {
			h.log.Info("Could not refresh green node ID", "pod", mapping.GreenPodName, "error", err)
			continue
		}
		mapping.GreenNodeID = freshID
		if oldID != "" && oldID != freshID {
			h.log.Info("Refreshed stale green node ID", "pod", mapping.GreenPodName, "old", oldID, "new", freshID)
		}
	}

	candidateIPs := clusterNodesProbeIPs(cluster, bgStatus.ShardMapping)

	var clusterNodesStr string
	bestNodes := 0
	for _, ip := range candidateIPs {
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		_, stdout, _, err := toolbox.ClusterNodes(probeCtx, ip)
		cancelProbe()
		if err != nil || stdout == "" {
			continue
		}
		nodes, ipToID := parseClusterNodesFrom(stdout, ip)
		if len(nodes) > bestNodes {
			clusterNodesStr = stdout
			bestNodes = len(nodes)
		}
		if clusterViewCoversMappings(nodes, ipToID, bgStatus.ShardMapping) {
			clusterNodesStr = stdout
			break
		}
	}
	if clusterNodesStr == "" {
		return fmt.Errorf("could not get CLUSTER NODES from any cluster pod")
	}

	ipToNodeID, slotRangeToPrimaryIP := ParseClusterNodesOutput(clusterNodesStr)

	for i := range bgStatus.ShardMapping {
		m := &bgStatus.ShardMapping[i]

		if m.SlotsRanges != "" {
			if freshIP, ok := slotRangeToPrimaryIP[m.SlotsRanges]; ok && freshIP != "" && freshIP != m.BluePrimaryIP {
				if freshIP == m.GreenPodIP {
					h.log.Info("Slot owner is the green pod itself (green auto-promoted after blue crash), skipping BluePrimaryIP update",
						"slots", m.SlotsRanges, "greenPod", m.GreenPodName)
				} else {
					h.log.Info("Refreshed stale BluePrimaryIP (failover detected)", "slots", m.SlotsRanges, "old", m.BluePrimaryIP, "new", freshIP)
					m.BluePrimaryIP = freshIP
					if freshID, ok := ipToNodeID[freshIP]; ok {
						m.BluePrimaryID = freshID
					}
				}
			}
		}
	}

	return nil
}

func mappedGreenPodIPs(mappings []cachev1.ShardMappingEntry) []string {
	result := make([]string, 0, len(mappings))
	for _, mapping := range mappings {
		result = append(result, mapping.GreenPodIP)
	}
	return uniqueNonEmptyIPs(result)
}

func clusterNodesProbeIPs(cluster *redis_cluster.Cluster, mappings []cachev1.ShardMappingEntry) []string {
	greenPrefix := GreenSTSName(cluster.RedisCluster) + "-"
	bluePrefix := BlueSTSName(cluster.RedisCluster) + "-"
	result := make([]string, 0, len(cluster.AllNodes)+len(mappings))
	for _, node := range cluster.AllNodes {
		if constants.MatchesSTSPrefix(node.PodName, greenPrefix, bluePrefix) {
			result = append(result, node.IP)
		}
	}
	result = append(result, mappedGreenPodIPs(mappings)...)
	return uniqueNonEmptyIPs(result)
}

func uniqueNonEmptyIPs(ips []string) []string {
	result := make([]string, 0, len(ips))
	seen := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		if ip == "" {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		result = append(result, ip)
	}
	return result
}

// bgClusterConsistencyCheck verifies cluster_state == ok and no FAIL slots
// on all green nodes using shard mapping IPs.
func (h *Handler) bgClusterConsistencyCheck(
	ctx context.Context,
	bgStatus *cachev1.BlueGreenStatus,
	toolbox *redis_cluster.Toolbox,
) (ok bool, reason string) {
	viewCtx, cancelView := context.WithTimeout(ctx, blueGreenClusterViewTimeout)
	defer cancelView()

	unreachableCount := 0
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenPodIP == "" {
			unreachableCount++
			continue
		}
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		rc, infoStr, _, err := toolbox.ClusterInfo(probeCtx, mapping.GreenPodIP)
		cancelProbe()
		if err != nil || rc != 0 {
			h.log.Info("Consistency check: unreachable node, skipping", "pod", mapping.GreenPodName)
			unreachableCount++
			continue
		}
		var clusterStateOK bool
		var slotsFail int
		for _, line := range strings.Split(infoStr, "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "cluster_state:") {
				clusterStateOK = strings.TrimPrefix(line, "cluster_state:") == "ok"
			}
			if strings.HasPrefix(line, "cluster_slots_fail:") {
				fmt.Sscanf(strings.TrimPrefix(line, "cluster_slots_fail:"), "%d", &slotsFail)
			}
		}
		if !clusterStateOK {
			reason = fmt.Sprintf("green pod %s reports cluster_state != ok", mapping.GreenPodName)
			h.log.Info("Consistency check: cluster_state not ok", "pod", mapping.GreenPodName)
			_ = h.metrics.CountBadSituation("bluegreen_partition_detected")
			return false, reason
		}
		if slotsFail > 0 {
			reason = fmt.Sprintf("green pod %s reports %d slots in FAIL state", mapping.GreenPodName, slotsFail)
			h.log.Info("Consistency check: slots in FAIL", "pod", mapping.GreenPodName, "slotsFail", slotsFail)
			_ = h.metrics.CountBadSituation("bluegreen_partition_detected")
			return false, reason
		}
	}
	if unreachableCount > 0 {
		return false, fmt.Sprintf("%d/%d green pods unreachable for CLUSTER INFO",
			unreachableCount, len(bgStatus.ShardMapping))
	}
	return true, ""
}

// bgResetIfGreenSTSGone checks if the green StatefulSet is missing or scaled to 0.
// If so, it forgets orphaned green nodes and resets BG to None.
func (h *Handler) bgResetIfGreenSTSGone(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen
	gName := GreenSTSName(cluster.RedisCluster)
	bName := BlueSTSName(cluster.RedisCluster)

	if gName == "" {
		return false, nil
	}

	blueSTS, _ := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, bName)
	if blueSTS == nil {
		h.log.Info("Blue STS gone — BG cycle already completed, resetting to None",
			"blueSTS", bName, "greenSTS", gName, "surviving", gName)
		_ = h.resetBGToNone(ctx, cluster, gName, "BG reset (blue STS deleted)")
		return true, nil
	}

	var reason string
	greenSTS, _ := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, gName)
	if greenSTS == nil {
		reason = "deleted"
	} else if greenSTS.Spec.Replicas != nil && *greenSTS.Spec.Replicas == 0 {
		reason = "scaled to 0"
	} else {
		return false, nil
	}

	h.log.Info("Green StatefulSet gone — resetting BG to None",
		"greenSTS", gName, "reason", reason, "surviving", bName)

	forgotCount := 0
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenNodeID != "" {
			greenNode := &redis_cluster.ClusterNode{
				ID:      mapping.GreenNodeID,
				IP:      mapping.GreenPodIP,
				PodName: mapping.GreenPodName,
			}
			h.log.Info("Forgetting orphaned green node", "nodeID", mapping.GreenNodeID, "pod", mapping.GreenPodName)
			h.deps.ForgetNode(ctx, cluster, greenNode)
			forgotCount++
		}
	}

	if err := h.resetBGToNone(ctx, cluster, bName,
		fmt.Sprintf("BG reset (green STS %s, forgot %d nodes)", reason, forgotCount)); err != nil {
		return true, err
	}
	return true, nil
}

// resetBGToNone resets BG status to None and updates activeClusterName
// to the surviving STS so the next BG alternates correctly.
// It calls cleanupAllBGArtifacts to strip all BG state (pod labels, service
// selectors, annotations) in one shot — callers should NOT duplicate cleanup.
func (h *Handler) resetBGToNone(ctx context.Context, cluster *redis_cluster.Cluster, survivingSTSName, message string) error {
	bName := BlueSTSName(cluster.RedisCluster)
	gName := GreenSTSName(cluster.RedisCluster)
	svcNames := []string{bName, gName}

	// Re-enable replica migration (BG disabled it at Start). Single exit point for
	// all terminal paths, so HA self-healing is always restored.
	allIPs := make([]string, 0, len(cluster.AllNodes))
	for _, n := range cluster.AllNodes {
		if n.IP != "" {
			allIPs = append(allIPs, n.IP)
		}
	}
	if len(allIPs) > 0 {
		h.setReplicaMigration(ctx, cluster.Toolbox, allIPs, true)
	}

	h.cleanupAllBGArtifacts(ctx, cluster, svcNames...)

	for _, name := range []string{bName, gName} {
		if name != "" && name != survivingSTSName {
			h.log.Info("Deleting non-surviving STS", "name", name, "surviving", survivingSTSName)
			if err := h.deps.DeleteStatefulSet(ctx, cluster.RedisCluster.Namespace, name); err != nil {
				h.log.Error(err, "Failed to delete non-surviving STS", "name", name)
			}
		}
	}

	cluster.RedisCluster.Status.ActiveClusterName = survivingSTSName
	cluster.RedisCluster.Status.BlueGreen = cachev1.BlueGreenStatus{
		Phase:       cachev1.BlueGreenPhaseNone,
		Message:     message,
		CompletedAt: time.Now().Unix(),
	}
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Failed to persist status during BG reset")
		return err
	}
	h.log.Info("BG status reset to None", "activeClusterName", survivingSTSName, "message", message)
	if survivingSTSName != "" && cluster.RedisCluster.Spec.StatefulsetTemplate.Name != survivingSTSName {
		h.log.Info("Updating spec name to surviving STS",
			"from", cluster.RedisCluster.Spec.StatefulsetTemplate.Name, "to", survivingSTSName)
		cluster.RedisCluster.Spec.StatefulsetTemplate.Name = survivingSTSName
		if err := h.deps.UpdateSpec(ctx, cluster.RedisCluster); err != nil {
			h.log.Error(err, "Failed to persist spec/annotations during BG reset")
		}
	}
	return nil
}

// checkForceReset handles the "bluegreen.force-reset" annotation.
// Resets BG status to None from any phase. Returns (result, true) if reset was triggered.
// In post-promotion phases (Promoting, SwitchingDNS, ScalingDownBlue, TearingDown),
// green owns the data, so it becomes the surviving STS.
func (h *Handler) checkForceReset(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, bool) {
	annotations := cluster.RedisCluster.GetAnnotations()
	if annotations == nil || annotations[constants.BlueGreenForceReset] != "true" {
		return ctrl.Result{}, false
	}

	phase := cluster.RedisCluster.Status.BlueGreen.Phase
	h.log.Info("MANUAL FORCE RESET triggered via annotation", "phase", phase)

	survivingSTS := BlueSTSName(cluster.RedisCluster)
	switch phase {
	case cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseScalingDownBlue,
		cachev1.BlueGreenPhaseFormingCluster,
		cachev1.BlueGreenPhaseTearingDown:
		survivingSTS = GreenSTSName(cluster.RedisCluster)
	case cachev1.BlueGreenPhaseFailed:
		h.log.Info("Force reset from Failed state — defaulting to blue STS. "+
			"If failure occurred after promotion, manually verify which STS owns slots",
			"blue", BlueSTSName(cluster.RedisCluster),
			"green", GreenSTSName(cluster.RedisCluster))
	}

	h.log.Info("Force reset: determined surviving STS", "surviving", survivingSTS, "phase", phase)

	_ = h.resetBGToNone(ctx, cluster, survivingSTS, "Reset via force-reset annotation")
	return ctrl.Result{Requeue: true}, true
}

// checkTopologyConvergence counts views that report every promoted node as a master.
func (h *Handler) checkTopologyConvergence(
	ctx context.Context,
	toolbox *redis_cluster.Toolbox,
	promotedGreenNodeIDs map[string]bool,
	allIPs []string,
) (converged int, total int) {
	for _, ip := range allIPs {
		probeCtx, cancelProbe := context.WithTimeout(ctx, blueGreenRedisProbeTimeout)
		_, stdout, _, execErr := toolbox.ClusterNodes(probeCtx, ip)
		cancelProbe()
		if execErr != nil {
			total++
			continue
		}
		total++
		if allNodesSeenAsMaster(stdout, promotedGreenNodeIDs) {
			converged++
		}
	}
	return converged, total
}

// allNodesSeenAsMaster parses CLUSTER NODES output and checks that every
// node ID in wantMaster appears with the "master" flag (without "fail").
func allNodesSeenAsMaster(clusterNodes string, wantMaster map[string]bool) bool {
	found := make(map[string]bool)
	for _, line := range strings.Split(clusterNodes, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 3 {
			continue
		}
		nodeID := fields[0]
		if !wantMaster[nodeID] {
			continue
		}
		flags := fields[2]
		if strings.Contains(flags, "master") && !strings.Contains(flags, "fail") {
			found[nodeID] = true
		}
	}
	return len(found) == len(wantMaster)
}

// forgetConverged reports whether no forgotten blue ID still appears on any green
// primary. Unreachable nodes count as not-converged (listed as "<ip>:unreachable").
func (h *Handler) forgetConverged(
	ctx context.Context,
	toolbox *redis_cluster.Toolbox,
	greenPrimaryIPs []string,
	forgottenIDs []string,
) (bool, []string) {
	idSet := make(map[string]bool, len(forgottenIDs))
	for _, id := range forgottenIDs {
		idSet[id] = true
	}

	var stillPresentOn []string
	for _, ip := range greenPrimaryIPs {
		if ip == "" {
			continue
		}
		rc, stdout, _, err := toolbox.ClusterNodes(ctx, ip)
		if err != nil || rc != 0 {
			stillPresentOn = append(stillPresentOn, fmt.Sprintf("%s:unreachable(rc=%d)", ip, rc))
			continue
		}
		for _, line := range strings.Split(stdout, "\n") {
			fields := strings.Fields(strings.TrimSpace(line))
			if len(fields) < 1 {
				continue
			}
			if idSet[fields[0]] {
				stillPresentOn = append(stillPresentOn, ip)
				break
			}
		}
	}
	return len(stillPresentOn) == 0, stillPresentOn
}

// isSlaveOfGreenPod reports whether the pod is a slave of a green pod that owns
// expectedSlots. Mismatched slots return false so the caller still promotes the shard.
// Returns (isSlave, masterHost, masterSlots).
func isSlaveOfGreenPod(replicationInfo string, bgStatus *cachev1.BlueGreenStatus, expectedSlots string) (bool, string, string) {
	var masterHost string
	for _, line := range strings.Split(replicationInfo, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "master_host:") {
			masterHost = strings.TrimPrefix(line, "master_host:")
			break
		}
	}
	if masterHost == "" {
		return false, "", ""
	}
	for _, m := range bgStatus.ShardMapping {
		if m.GreenPodIP == masterHost {
			return m.SlotsRanges == expectedSlots, masterHost, m.SlotsRanges
		}
	}
	return false, masterHost, ""
}

// setReplicaMigration sets cluster-allow-replica-migration on the given nodes.
// Disabled during BG to stop gossip races from moving replicas to wrong primaries.
func (h *Handler) setReplicaMigration(ctx context.Context, toolbox *redis_cluster.Toolbox, nodeIPs []string, enabled bool) {
	val := "yes"
	if !enabled {
		val = "no"
	}
	set, failed := 0, 0
	for _, ip := range nodeIPs {
		if ip == "" {
			continue
		}
		if rc, _, stderr, err := toolbox.ConfigureReplicaMigration(ctx, ip, enabled); err != nil || rc != 0 {
			h.log.Info("CONFIG SET cluster-allow-replica-migration failed (non-fatal)", "ip", ip, "value", val, "rc", rc, "stderr", stderr, "err", err)
			failed++
		} else {
			set++
		}
	}
	h.log.Info("Set cluster-allow-replica-migration", "value", val, "set", set, "failed", failed)
}

// countGreenOnlySlots returns cluster_slots_assigned from the first responsive green
// node (green-only coverage post-FORGET), or -1 if none respond.
func (h *Handler) countGreenOnlySlots(ctx context.Context, toolbox *redis_cluster.Toolbox, bgStatus *cachev1.BlueGreenStatus) int64 {
	for _, m := range bgStatus.ShardMapping {
		if m.GreenPodIP == "" {
			continue
		}
		rc, info, _, err := toolbox.ClusterInfo(ctx, m.GreenPodIP)
		if err != nil || rc != 0 {
			continue
		}
		if assigned := ParseInfoField(info, "cluster_slots_assigned"); assigned >= 0 {
			return assigned
		}
	}
	return -1
}

// topologyConverged checks every mapped green primary before SwitchingDNS.
// Other green replicas join during FormingCluster.
func (h *Handler) topologyConverged(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	promotedIDs map[string]bool,
	bgStatus *cachev1.BlueGreenStatus,
) bool {
	start := time.Now()
	greenIPs := mappedGreenPodIPs(bgStatus.ShardMapping)
	converged, total := h.checkTopologyConvergence(ctx, cluster.Toolbox, promotedIDs, greenIPs)
	elapsed := time.Since(start).Milliseconds()
	if converged < total {
		h.log.Info("Topology not converged, waiting",
			"converged", converged, "total", total)
		return false
	}
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, elapsed, "phase:convergence")
	return true
}

// broadcastClusterMeet sends CLUSTER MEET <greenIP> <port> from every node
// in the cluster to each newly promoted green primary. This forces immediate
// gossip handshake (~5-10ms) instead of waiting for random propagation (~200-500ms),
// shrinking the window where a client singleflight could cache stale slot data.
func (h *Handler) broadcastClusterMeet(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	bgStatus *cachev1.BlueGreenStatus,
	batch []cachev1.ShardMappingEntry,
) {
	allIPs := clusterNodesProbeIPs(cluster, bgStatus.ShardMapping)
	port := cluster.RedisPort
	for _, m := range batch {
		meetCount := 0
		for _, ip := range allIPs {
			if ip == m.GreenPodIP {
				continue
			}
			probeCtx, cancelProbe := context.WithTimeout(ctx, blueGreenRedisProbeTimeout)
			_, _, _, err := cluster.Toolbox.MeetAddress(probeCtx, ip, m.GreenPodIP, port)
			cancelProbe()
			if err == nil {
				meetCount++
			}
		}
		h.log.Info("CLUSTER MEET broadcast complete",
			"greenPod", m.GreenPodName, "greenIP", m.GreenPodIP,
			"nodesNotified", meetCount, "totalNodes", len(allIPs)-1)
	}
}

// cleanupAllBGArtifacts is the single entry point for removing all blue/green
// state from the cluster after a BG cycle completes or is reset. It:
//   - Pod labels: sets role=active on ALL Redis pods
//   - Service selectors: ensures role=active on headless service selectors
//   - CR annotations: clears force-reset and force-cutover
func (h *Handler) cleanupAllBGArtifacts(ctx context.Context, cluster *redis_cluster.Cluster, svcNames ...string) {
	ns := cluster.RedisCluster.Namespace

	allPods, err := h.deps.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		h.log.Error(err, "cleanupAllBGArtifacts: failed to list pods (non-fatal)")
	} else {
		cleaned := 0
		for i := range allPods.Items {
			pod := &allPods.Items[i]
			changed := false
			if pod.Labels == nil {
				pod.Labels = make(map[string]string)
			}
			if !constants.IsRoleActive(pod.Labels) {
				constants.ActivateRole(pod.Labels)
				changed = true
			}
			if pod.Labels[constants.RedisClusterReadyPodLabel] != "true" {
				constants.ActivateRedisCluster(pod.Labels)
				changed = true
			}
			if changed {
				if err := h.deps.Update(ctx, pod); err != nil {
					h.log.Error(err, "cleanupAllBGArtifacts: failed to clean pod labels (non-fatal)", "pod", pod.Name)
				} else {
					cleaned++
				}
			}
		}
		if cleaned > 0 {
			h.log.Info("Cleaned BG labels from pods", "cleaned", cleaned, "total", len(allPods.Items))
		}
	}

	h.stripBGStateFromSTSTemplates(ctx, cluster, ns)

	h.cleanupServiceSelectors(ctx, ns, svcNames...)

	for attempt := 0; attempt < constants.BlueGreenAnnotationCleanupMaxAttempts; attempt++ {
		fresh := cluster.RedisCluster.DeepCopy()
		if err := h.deps.Get(ctx, types.NamespacedName{Name: fresh.Name, Namespace: fresh.Namespace}, fresh); err != nil {
			h.log.Error(err, "cleanupAllBGArtifacts: failed to re-fetch CR for annotation cleanup")
			break
		}
		annotations := fresh.GetAnnotations()
		if annotations == nil {
			break
		}
		changed := false
		for _, key := range []string{constants.BlueGreenForceReset, constants.BlueGreenForceCutover} {
			if annotations[key] == "true" {
				delete(annotations, key)
				changed = true
			}
		}
		if !changed {
			break
		}
		fresh.SetAnnotations(annotations)
		if err := h.deps.UpdateSpec(ctx, fresh); err != nil {
			h.log.Error(err, "cleanupAllBGArtifacts: annotation cleanup failed, retrying", "attempt", attempt+1)
			continue
		}
		h.log.Info("Cleared BG trigger annotations from CR")
		break
	}
}

// stripBGStateFromSTSTemplates patches the surviving STS pod template to ensure
// role=active is set. Without this, restarted pods would be recreated WITHOUT
// the role label (CreateGreenStatefulSet deletes it from the template) and be
// invisible to the service selector.
func (h *Handler) stripBGStateFromSTSTemplates(ctx context.Context, cluster *redis_cluster.Cluster, ns string) {
	for _, name := range []string{BlueSTSName(cluster.RedisCluster), GreenSTSName(cluster.RedisCluster)} {
		if name == "" {
			continue
		}
		sts, err := h.deps.GetStatefulSetByName(ctx, ns, name)
		if err != nil || sts == nil {
			continue
		}
		if sts.Spec.Template.Labels == nil {
			sts.Spec.Template.Labels = make(map[string]string)
		}
		if constants.IsRoleActive(sts.Spec.Template.Labels) {
			continue
		}
		constants.ActivateRole(sts.Spec.Template.Labels)
		if err := h.deps.Update(ctx, sts); err != nil {
			h.log.Error(err, "Failed to patch STS template with role=active (non-fatal)", "sts", name)
		} else {
			h.log.Info("Patched STS template: set role=active", "sts", name)
		}
	}
}

// cleanupServiceSelectors ensures headless service selectors have role=active
// so all surviving pods match after BG completes.
func (h *Handler) cleanupServiceSelectors(ctx context.Context, namespace string, svcNames ...string) {
	for _, name := range svcNames {
		if name == "" {
			continue
		}
		svc := &corev1.Service{}
		if err := h.deps.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, svc); err != nil {
			continue
		}
		newSelector := make(map[string]string)
		for k, v := range svc.Spec.Selector {
			newSelector[k] = v
		}
		newSelector[constants.BlueGreenRoleLabel] = constants.BlueGreenRoleActive
		if err := h.deps.UpdateServiceSelector(ctx, namespace, name, newSelector); err != nil {
			h.log.Error(err, "Failed to finalize service selector (non-fatal)", "service", name)
		} else {
			h.log.Info("Finalized service selector: set role=active", "service", name)
		}
	}
}
