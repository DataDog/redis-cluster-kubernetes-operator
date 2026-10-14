// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"context"
	"fmt"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
)

// blueGreenPhaseProgressBound returns the allowed no-progress duration for a phase.
func blueGreenPhaseProgressBound(phase cachev1.BlueGreenPhase) time.Duration {
	switch phase {
	case cachev1.BlueGreenPhaseWaitingSync:
		return constants.BlueGreenSyncStallTimeout
	case cachev1.BlueGreenPhasePromoting:
		return constants.BlueGreenPromoteStallTimeout
	case cachev1.BlueGreenPhaseSwitchingDNS:
		return constants.BlueGreenSwitchDNSStallTimeout
	case cachev1.BlueGreenPhaseFormingCluster:
		return constants.BlueGreenFormClusterStallTimeout
	case cachev1.BlueGreenPhaseScalingDownBlue:
		return constants.BlueGreenScaleDownStallTimeout
	case cachev1.BlueGreenPhaseTearingDown:
		return constants.BlueGreenTeardownStallTimeout
	default:
		return constants.BlueGreenDefaultStallTimeout
	}
}

// checkProgressWatchdog handles stalled BG phases.
// A true handled return means the caller must skip phase dispatch.
func (h *Handler) checkProgressWatchdog(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, bool, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen
	phase := bgStatus.Phase

	// Only data-bearing phases need the watchdog.
	if len(bgStatus.ShardMapping) == 0 ||
		phase == cachev1.BlueGreenPhaseFailed ||
		phase == cachev1.BlueGreenPhaseNone {
		return ctrl.Result{}, health.Warn, false, nil
	}

	// Measure from the latest phase entry or intra-phase progress.
	ref := bgStatus.LastProgressTime
	if bgStatus.PhaseEntryTime > ref {
		ref = bgStatus.PhaseEntryTime
	}
	if ref == 0 {
		// Stamp a missing timestamp and measure from the next reconcile.
		bgStatus.PhaseEntryTime = time.Now().Unix()
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{}, health.Warn, false, nil
	}

	stall := time.Since(time.Unix(ref, 0))
	// Emit the series used by stalled-phase monitors.
	_ = h.metrics.Gauge(metrics.GaugeBlueGreenProgressStalled, stall.Seconds(), fmt.Sprintf("phase:%s", phase))

	if stall <= blueGreenPhaseProgressBound(phase) {
		return ctrl.Result{}, health.Warn, false, nil
	}

	// Past the bound, decide from live slot ownership.
	assessment, err := h.assessLiveShards(ctx, bgStatus, cluster.Toolbox)
	if err != nil {
		h.log.Info("Watchdog: phase stalled and cluster view unavailable — holding + paging",
			"phase", phase, "stall", stall.Truncate(time.Second), "err", err)
		_ = h.metrics.CountBadSituation("bluegreen_watchdog_stall_no_view")
		bgStatus.Message = fmt.Sprintf("Watchdog: %s stalled %s — live shard view unavailable: %v (paged, holding for operator)",
			phase, stall.Truncate(time.Second), err)
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, true, nil
	}

	// Choose the stalled-phase action from slot ownership.
	dec := decisionTable(assessment, cluster.RedisCluster.Spec.BackupEnabled)
	h.log.Info("Watchdog: phase stalled — decision table",
		"phase", phase, "stall", stall.Truncate(time.Second), "decision", dec.Action.String(),
		"reason", dec.Reason, "pendingRejoin", assessment.PendingRejoin, "unsafeSlots", dec.UnsafeSlots)

	if dec.Action == ActFinishForward {
		// Green owns all data; ScalingDownBlue still gates blue deletion.
		_ = h.metrics.CountBadSituation("bluegreen_watchdog_finish_forward", fmt.Sprintf("phase:%s", phase))
		setPhase(bgStatus, cachev1.BlueGreenPhaseScalingDownBlue,
			"Watchdog: green owns all shards, finishing forward (blue delete gated by coverage floor)")
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, true, nil
	}

	// Risky or incomplete view: page and hold for an operator.
	_ = h.metrics.CountBadSituation("bluegreen_watchdog_hold_"+strings.ToLower(dec.Action.String()), fmt.Sprintf("phase:%s", phase))
	bgStatus.Message = fmt.Sprintf("Watchdog: %s stalled %s — %s (paged, holding for operator)",
		phase, stall.Truncate(time.Second), dec.Reason)
	_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
	return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, true, nil
}
