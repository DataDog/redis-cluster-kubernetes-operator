// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package metrics

import "github.com/DataDog/redis-cluster-kubernetes-operator/health"

const (
	GaugeZombieNodes                       = "zombie_nodes"
	GaugeClusterKnownNodes                 = "cluster_known_nodes"
	GaugeClusterPrimaries                  = "cluster_primaries"
	GaugeClusterReplicas                   = "cluster_replicas"
	GaugeClusterOtherNodes                 = "cluster_other_nodes"
	GaugeClusterExpectedPrimaries          = "cluster_expected_primaries"
	GaugeClusterExpectedReplicas           = "cluster_expected_replicas"
	GaugeNLAPodsPending                    = "nla_pods_pending"
	GaugeNLAPodsFailed                     = "nla_pods_failed"
	GaugeAutoscalingEffectivePrimaries     = "autoscaling_effective_primaries"
	GaugeAutoscalingCombinedRecommendation = "autoscaling_combined_recommendation"
	GaugeAutoscalingCPURecommendation      = "autoscaling_cpu_recommendation"
	GaugeAutoscalingMemoryRecommendation   = "autoscaling_memory_recommendation"
)

const (
	TimingForgetDuration         = "forget_duration_ms"
	TimingFailoverDuration       = "failover_duration_ms"
	TimingNLADuration            = "nla_duration_ms"
	TimingBlueGreenDuration      = "bluegreen_duration_ms"
	TimingBlueGreenPhaseDuration = "bluegreen_phase_duration_ms"
	TimingBackupDuration         = "backup_duration_ms"
	TimingRestoreDuration        = "restore_duration_ms"
	// TimingReconcileStep is tagged with a "step:" tag per reconcile phase.
	TimingReconcileStep = "reconcile_step_duration_ms"
)

const (
	StepTagHandleTerminating = "step:handle_terminating"
	StepTagNLA               = "step:nla"
	StepTagMaintainCluster   = "step:maintain_cluster"
	StepTagBackup            = "step:backup"
)

const (
	GaugeBlueGreenPhase        = "bluegreen_phase"
	GaugeBlueGreenShardsSynced = "bluegreen_shards_synced"
	GaugeBlueGreenForgetResult = "bluegreen_forget_result"
	// GaugeBlueGreenProgressStalled tracks seconds since the current BG phase last progressed.
	GaugeBlueGreenProgressStalled = "bluegreen_progress_stalled_seconds"
	GaugeBackupNodeCount          = "backup_node_count"
	GaugeBackupFailedNodeCount    = "backup_failed_node_count"
)

type Metrics interface {
	WithRedisCluster(name string) Metrics
	Count(counter string) error
	CountBadSituation(situation string, tags ...string) error
	CountToolboxOperation(operation string) error
	CountAutoscalingDecision(result string, reason string) error
	ReportHealth(h health.ClusterHealth) error
	Gauge(name string, value float64, tags ...string) error
	Timing(name string, milliseconds int64, tags ...string) error
}
