// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package metrics

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/prometheus/client_golang/prometheus"
)

const prometheusNamespace = "redis_cluster_operator"

var (
	counterValues = stringSet(
		counters.AppStarted, counters.Reconcile, counters.ClusterCreated, counters.ClusterMeet, counters.ClusterRestored,
		counters.ClusterBackedUp, counters.StatefulSetCreated, counters.StatefulSetUpdated, counters.ZombieDetected,
		counters.ZombiePersisted, counters.NLAProcessed, counters.NLAFailed, counters.NLARetriggered,
		counters.BlueGreenStarted, counters.BlueGreenCompleted, counters.BlueGreenFailed,
		counters.BlueGreenFailoverAttempted, counters.BlueGreenFailoverSucceeded,
	)
	toolboxValues = stringSet(
		"set_replica_migration", "reset_node", "add_replica", "add_primary", "meet", "meet_cluster",
		"create_cluster", "rebalance", "rebalance_weighted", "fix_cluster", "failover", "replicate",
		"add_slot", "add_slots", "replicate_ip", "create_backup", "restore_backup",
	)
	badSituationValues = stringSet(
		"autoscaling_metrics_degraded", "autoscaling_wpa_drift", "backup_failed", "backup_incomplete_not_promoted",
		"autoscaling_nla_repair_capacity", "primary_endpoint_not_ready",
		"backup_manifest_write_failed", "backup_probe_failed_blocking_empty_assign", "backup_promote_failed",
		"bluegreen_activate_blocked_not_owner", "bluegreen_blue_pods_gone", "bluegreen_consistency_gate_bypassed",
		"bluegreen_error", "bluegreen_force_cutover_triggered", "bluegreen_forget_convergence_timeout",
		"bluegreen_forget_not_converged", "bluegreen_orphaned_pods_deleted", "bluegreen_partial_blue_failure",
		"bluegreen_partition_detected", "bluegreen_pending_rejoin", "bluegreen_pods_quota_blocked",
		"bluegreen_pre_delete_slot_gap", "bluegreen_promote_stall_escalation", "bluegreen_replica_migration_detected",
		"bluegreen_resume_forward_after_failure", "bluegreen_switchdns_view_unavailable", "bluegreen_watchdog_finish_forward",
		"bluegreen_watchdog_hold", "bluegreen_watchdog_stall_no_view", "cluster_partitioned", "could_not_get_cluster",
		"empty_master_detected", "ensure_not_primary_no_replica", "ensure_not_primary_promoting_draining_replica",
		"failover_promoting_draining_replica", "forget_partial_failure", "forget_terminating_primary", "could_not_meet_peers",
		"hard_reset_skipped_data_present", "hard_reset_skipped_dbsize_unknown", "missing_slots_auto_recovery",
		"missing_slots_no_primary", "nla_skipped_replica_draining", "orphaned_nodes_detected", "other_nodes_found",
		"panic_recovered", "restore_duplicate_primary_flushed", "restore_fallback_to_previous",
		"restore_slot_loss_recovery", "restore_slot_loss_recovery_failed", "restore_slot_ownership_ambiguous",
		"restore_slot_ownership_from_manifest", "slot_loss_no_backup_blocking_empty_assign", "slot_reown_from_manifest",
		"split_topology_detected", "statefulset_not_ready", "take_node_out_promoting_draining_replica",
		"terminating_primary_failover_failed", "unassigned_slots_no_affected_node", "zombie_persisted_after_forget",
	)
	autoscalingResults = stringSet("guard", "repair", "blocked", "drift", "degraded", "accepted")
	autoscalingReasons = stringSet(
		"replica_migration_disabled", "replica_migration_restored", "nla_replica_redundancy", "maximum_bound",
		"dry_run", "scaling_in_progress", "completed", "cooldown", "safety_gate", "recommendation",
		"resourcedrift", "dryrundrift", "ownerreferenceerror", "datadogmetricerror", "wpaerror", "wpa_missing",
		"stale_generation", "generation_unobserved", "non_positive_recommendation", "invalid_group_size",
		"metric_status_unavailable",
	)
	gaugeNames = stringSet(
		GaugeZombieNodes, GaugeClusterKnownNodes, GaugeClusterPrimaries, GaugeClusterReplicas, GaugeClusterOtherNodes,
		GaugeClusterExpectedPrimaries, GaugeClusterExpectedReplicas, GaugeNLAPodsPending, GaugeNLAPodsFailed,
		GaugeAutoscalingEffectivePrimaries, GaugeAutoscalingCombinedRecommendation, GaugeAutoscalingCPURecommendation,
		GaugeAutoscalingMemoryRecommendation, GaugeBlueGreenPhase, GaugeBlueGreenShardsSynced,
		GaugeBlueGreenForgetResult, GaugeBlueGreenProgressStalled, GaugeBackupNodeCount, GaugeBackupFailedNodeCount,
	)
	timingNames = stringSet(
		TimingForgetDuration, TimingFailoverDuration, TimingNLADuration, TimingBlueGreenDuration,
		TimingBlueGreenPhaseDuration, TimingBackupDuration, TimingRestoreDuration, TimingReconcileStep,
	)
	statusDetails = stringSet("status:timeout", "status:success", "status:failed")
	resultDetails = stringSet("result:success", "result:failed")
	phaseDetails  = stringSet(
		"phase:convergence", "phase:replicating", "phase:waiting_sync", "phase:promoting", "phase:switch_dns",
		"phase:scale_down_blue", "phase:forming_cluster", "phase:CreatingGreen", "phase:WaitingPods",
		"phase:Replicating", "phase:WaitingSync", "phase:Promoting", "phase:ScalingDownBlue",
		"phase:FormingCluster", "phase:SwitchingDNS", "phase:TearingDown",
	)
	stageDetails = stringSet("stage:failover_confirmed", "stage:promote")
	stepDetails  = stringSet(StepTagHandleTerminating, StepTagNLA, StepTagMaintainCluster, StepTagBackup)
	gaugeDetails = map[string]map[string]bool{
		GaugeBlueGreenForgetResult:    resultDetails,
		GaugeBlueGreenProgressStalled: phaseDetails,
		GaugeBlueGreenShardsSynced:    stageDetails,
	}
	timingDetails = map[string]map[string]bool{
		TimingFailoverDuration:       statusDetails,
		TimingNLADuration:            statusDetails,
		TimingBlueGreenDuration:      resultDetails,
		TimingBlueGreenPhaseDuration: phaseDetails,
		TimingReconcileStep:          stepDetails,
	}
	durationBuckets = prometheus.ExponentialBuckets(0.1, 2, 18)
)

// prometheusMetrics implements Metrics using a fixed collector set. It never
// places cluster names, namespaces, pod names, or arbitrary tag values in labels.
type prometheusMetrics struct {
	events        *prometheus.CounterVec
	badSituations *prometheus.CounterVec
	toolbox       *prometheus.CounterVec
	autoscaling   *prometheus.CounterVec
	health        prometheus.Gauge
	gauges        map[string]*prometheus.GaugeVec
	durations     map[string]*prometheus.HistogramVec
}

// CreatePrometheus registers a self-contained collector set with registerer.
// Callers should inject their registry; no collector is registered globally by
// this constructor unless the caller explicitly supplies the global registry.
func CreatePrometheus(registerer prometheus.Registerer) (Metrics, error) {
	m, _, err := CreatePrometheusWithCleanup(registerer)
	return m, err
}

// CreatePrometheusWithCleanup registers a self-contained collector set and
// returns an idempotent function that unregisters it. The caller owns the
// returned cleanup function and should call it when the reporter is no longer
// used.
func CreatePrometheusWithCleanup(registerer prometheus.Registerer) (Metrics, func(), error) {
	if registerer == nil {
		return nil, nil, errors.New("metrics registerer is required")
	}
	m := &prometheusMetrics{
		events:        prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: prometheusNamespace, Name: "events_total", Help: "Operator lifecycle events."}, []string{"event"}),
		badSituations: prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: prometheusNamespace, Name: "bad_situations_total", Help: "Detected situations requiring operator attention."}, []string{"situation"}),
		toolbox:       prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: prometheusNamespace, Name: "toolbox_operations_total", Help: "Typed Redis toolbox operations."}, []string{"operation"}),
		autoscaling:   prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: prometheusNamespace, Name: "autoscaling_decisions_total", Help: "Autoscaling decisions."}, []string{"result", "reason"}),
		health:        prometheus.NewGauge(prometheus.GaugeOpts{Namespace: prometheusNamespace, Name: "health", Help: "Latest observed cluster health (1=OK, 2=Warn, 3=Error, 4=Pending)."}),
		gauges:        make(map[string]*prometheus.GaugeVec, len(gaugeNames)),
		durations:     make(map[string]*prometheus.HistogramVec, len(timingNames)),
	}
	for name := range gaugeNames {
		m.gauges[name] = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: prometheusNamespace, Name: name, Help: "Latest observed operator gauge " + name + "."}, []string{"detail"})
	}
	for name := range timingNames {
		prometheusName := strings.TrimSuffix(name, "_ms") + "_seconds"
		m.durations[name] = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: prometheusNamespace, Name: prometheusName, Help: "Operator duration " + name + " in seconds.", Buckets: durationBuckets}, []string{"detail"})
	}
	registered := make([]prometheus.Collector, 0, len(m.collectors()))
	for _, collector := range m.collectors() {
		if err := registerer.Register(collector); err != nil {
			for _, prior := range registered {
				registerer.Unregister(prior)
			}
			return nil, nil, fmt.Errorf("register Prometheus metrics: %w", err)
		}
		registered = append(registered, collector)
	}
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			for _, collector := range registered {
				registerer.Unregister(collector)
			}
		})
	}
	return m, cleanup, nil
}

func (m *prometheusMetrics) collectors() []prometheus.Collector {
	collectors := []prometheus.Collector{m.events, m.badSituations, m.toolbox, m.autoscaling, m.health}
	for _, name := range sortedKeys(m.gauges) {
		collectors = append(collectors, m.gauges[name])
	}
	for _, name := range sortedKeys(m.durations) {
		collectors = append(collectors, m.durations[name])
	}
	return collectors
}

// WithRedisCluster preserves the interface without turning arbitrary CR names
// into labels. Counters and histograms aggregate across clusters; health and
// gauges intentionally expose the latest observation.
func (m *prometheusMetrics) WithRedisCluster(_ string) Metrics { return m }

func (m *prometheusMetrics) Count(counter string) error {
	if !counterValues[counter] {
		return fmt.Errorf("unsupported counter %q", counter)
	}
	m.events.WithLabelValues(counter).Inc()
	return nil
}

func (m *prometheusMetrics) CountBadSituation(situation string, _ ...string) error {
	if strings.HasPrefix(situation, "bluegreen_watchdog_hold_") {
		situation = "bluegreen_watchdog_hold"
	}
	m.badSituations.WithLabelValues(bounded(situation, badSituationValues)).Inc()
	return nil
}

func (m *prometheusMetrics) CountToolboxOperation(operation string) error {
	m.toolbox.WithLabelValues(bounded(operation, toolboxValues)).Inc()
	return nil
}

func (m *prometheusMetrics) CountAutoscalingDecision(result string, reason string) error {
	m.autoscaling.WithLabelValues(bounded(result, autoscalingResults), bounded(reason, autoscalingReasons)).Inc()
	return nil
}

func (m *prometheusMetrics) ReportHealth(h health.ClusterHealth) error {
	if h == health.Unknown {
		return nil
	}
	if h < health.OK || h > health.Pending {
		return fmt.Errorf("unsupported cluster health %d", h)
	}
	m.health.Set(float64(h))
	return nil
}

func (m *prometheusMetrics) Gauge(name string, value float64, tags ...string) error {
	if !gaugeNames[name] {
		return fmt.Errorf("unsupported gauge %q", name)
	}
	m.gauges[name].WithLabelValues(boundedDetail(tags, gaugeDetails[name])).Set(value)
	return nil
}

func (m *prometheusMetrics) Timing(name string, milliseconds int64, tags ...string) error {
	if !timingNames[name] {
		return fmt.Errorf("unsupported timing %q", name)
	}
	if milliseconds < 0 {
		return fmt.Errorf("timing %q must not be negative", name)
	}
	m.durations[name].WithLabelValues(boundedDetail(tags, timingDetails[name])).Observe(float64(milliseconds) / 1000)
	return nil
}

func stringSet(values ...string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func bounded(value string, allowed map[string]bool) string {
	if allowed[value] {
		return value
	}
	return "other"
}

func boundedDetail(tags []string, allowed map[string]bool) string {
	for _, tag := range tags {
		if allowed[tag] {
			return tag
		}
	}
	if len(tags) != 0 {
		return "other"
	}
	return "none"
}

func sortedKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
