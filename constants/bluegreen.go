// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package constants

import (
	"strings"
	"time"
)

const (
	// Labels & annotations
	BlueGreenForceCutover = "bluegreen.force-cutover"
	BlueGreenForceReset   = "bluegreen.force-reset"

	// Common pod/service labels
	LabelApp              = "app"
	LabelRedisClusterName = "redis_cluster_name"

	// Service selector label — pods with role=active are included in service endpoints.
	BlueGreenRoleLabel  = "redis-cluster-role"
	BlueGreenRoleActive = "active"
	// BlueGreenStatefulSetLabel gives peer-discovery Services a stable,
	// rollout-specific selector without exposing non-serving pods to clients.
	BlueGreenStatefulSetLabel = "redis-cluster-statefulset"

	// STS suffix for the alternate StatefulSet used during blue/green deployments.
	// Names toggle between "base" and "base-alt" across BG cycles.
	// BlueGreenSTSSuffix is appended/stripped to toggle between blue and green STS names.
	// Note: names ending in "-alt" would collide. Consider "-bgalt" for new deployments.
	BlueGreenSTSSuffix = "-alt"

	// Polling intervals
	BlueGreenPodReadyDelay       = 5 * time.Second
	BlueGreenSyncCheckDelay      = 10 * time.Second
	BlueGreenFailoverDelay       = 10 * time.Second
	BlueGreenFailoverPollDelay   = 200 * time.Millisecond
	BlueGreenFailoverPollRetries = 10

	// Phase timeouts
	BlueGreenOverallTimeout   = 45 * time.Minute
	BlueGreenSyncStallTimeout = 15 * time.Minute

	// Failover escalation thresholds
	//
	//   attempts 0–1  →  soft failover
	//   attempts 2–3  →  CLUSTER FAILOVER FORCE
	//   attempts 4–6  →  CLUSTER FAILOVER TAKEOVER
	//   attempts 7+   →  capped, manual intervention required
	BlueGreenForceAttemptThreshold    = 2
	BlueGreenTakeoverAttemptThreshold = 4
	BlueGreenMaxResumeForwardAttempts = 7

	// BlueGreenMaxConsecutivePhaseErrors is the maximum number of consecutive
	// reconcile errors in a single BG phase before aborting. Prevents blind
	// retrying for the full timeout (e.g. 45 min of "exit code 1").
	BlueGreenMaxConsecutivePhaseErrors int32 = 10

	// Consistency gate — skip cluster-consistency check after this many
	// resume-forward attempts to avoid blocking on a partitioned cluster.
	BlueGreenConsistencyGateAttempts = 3

	// Inter-shard failover delay — poll interval for CLUSTER SLOTS convergence.
	// After each failover, we poll every node's CLUSTER SLOTS until 100% agree
	// the promoted green nodeID owns the slot. This is adaptive: gossip usually
	// propagates in 200-500ms, so per-shard cost is ~0.5-2s instead of a fixed delay.
	BlueGreenInterShardFailoverDelay = 2 * time.Second

	// Forget convergence requeue interval — how long ScalingDownBlue waits
	// between reconciles while verifying every green primary has dropped the
	// forgotten blue node IDs from CLUSTER NODES before scaling blue to 0.
	BlueGreenInterForgetDelay = 2 * time.Second

	// Forget convergence timeout — upper bound on waiting for FORGET to clear
	// from all green primaries (measured from FORGET completion / DrainStartedAt).
	// Past this, proceed with a warning rather than wedging the deployment.
	BlueGreenForgetConvergenceTimeout = 30 * time.Second

	// Gossip settle delay — after all replica MEETs in FormingCluster, wait
	// for gossip to fully propagate to all nodes. This prevents clients from
	// querying a fresh replica that hasn't learned the full topology yet,
	// which causes ErrNoSlot and topology flapping.
	BlueGreenGossipSettleDelay = 5 * time.Second

	// CLIENT PAUSE duration for blue pods before FORGET. Must be longer than
	// FORGET + convergence + scale-to-0 + pod termination (~30s). If it
	// expires before blue is killed, blue resumes responding with stale
	// CLUSTER SLOTS → rueidis sees conflict → slot wipe. 60s gives margin.
	BlueGreenClientPauseTimeout = 60 * time.Second

	// Promote stall timeout — if no new shards are promoted within this window,
	// escalate failover mode (soft → FORCE → TAKEOVER) automatically.
	BlueGreenPromoteStallTimeout = 5 * time.Minute

	// Watchdog no-progress bounds for phases without existing stall timeouts.
	BlueGreenFormClusterStallTimeout = 5 * time.Minute
	BlueGreenScaleDownStallTimeout   = 5 * time.Minute
	BlueGreenSwitchDNSStallTimeout   = 2 * time.Minute
	BlueGreenTeardownStallTimeout    = 5 * time.Minute
	BlueGreenDefaultStallTimeout     = 5 * time.Minute

	// Max percentage of keyspace (shards) to failover in a single batch.
	// Higher = faster Promoting phase, but more slots briefly unavailable.
	// 15% means ~15% of shards failover simultaneously per batch.
	BlueGreenMaxBatchKeyspacePercent = 15

	// BlueGreenAnnotationCleanupMaxAttempts is the max retries for clearing
	// force-reset/force-cutover annotations after BG reset.
	BlueGreenAnnotationCleanupMaxAttempts = 3

	// BlueGreenSmartBatchThreshold is the minimum number of pending replicas
	// before smart batching (by replica index) is used in FormingCluster.
	// Below this threshold, all replicas are processed in one pass.
	BlueGreenSmartBatchThreshold = 20

	// BlueGreenClientRefreshDelay is the wait time for clients to refresh
	// CLUSTER SLOTS after topology changes (DNS switch, FORGET).
	BlueGreenClientRefreshDelay = 15 * time.Second

	// Grace period after BG completes. During this window, the maintain loop
	// skips zombie/outsider restructuring to let the cluster topology stabilize
	// and prevent rapid-fire CLUSTER FORGET + addPrimary/addReplica churn.
	BlueGreenPostCompletionGracePeriod = 60 * time.Second
)

// AlternateSTSName returns the unused STS name from [base, base-alt].
// If serving is "base-alt", returns "base". Otherwise returns "base-alt".
func AlternateSTSName(servingName string) string {
	if strings.HasSuffix(servingName, BlueGreenSTSSuffix) {
		return strings.TrimSuffix(servingName, BlueGreenSTSSuffix)
	}
	return servingName + BlueGreenSTSSuffix
}

// MatchesSTSPrefix returns true if podName belongs to the STS with wantPrefix
// and NOT to the STS with otherPrefix. Since one STS name may be a prefix of
// the other (e.g. "base-" vs "base-alt-"), the longer prefix wins: a pod matching
// both is assigned to the longer one.
func MatchesSTSPrefix(podName, wantPrefix, otherPrefix string) bool {
	if !strings.HasPrefix(podName, wantPrefix) {
		return false
	}
	if len(wantPrefix) >= len(otherPrefix) {
		return true
	}
	return !strings.HasPrefix(podName, otherPrefix)
}

// ActivateRole sets the BG role label to active.
func ActivateRole(labels map[string]string) {
	labels[BlueGreenRoleLabel] = BlueGreenRoleActive
}

// DeactivateRole removes the BG role label.
func DeactivateRole(labels map[string]string) {
	delete(labels, BlueGreenRoleLabel)
}

// IsRoleActive returns true if the BG role label is active.
func IsRoleActive(labels map[string]string) bool {
	return labels[BlueGreenRoleLabel] == BlueGreenRoleActive
}

// ActivateRedisCluster sets the redis-cluster-ready label to true.
func ActivateRedisCluster(labels map[string]string) {
	labels[RedisClusterReadyPodLabel] = "true"
}

// DeactivateRedisCluster removes the redis-cluster-ready label.
func DeactivateRedisCluster(labels map[string]string) {
	delete(labels, RedisClusterReadyPodLabel)
}
