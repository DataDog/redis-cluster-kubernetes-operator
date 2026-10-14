// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package metrics

import (
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPrometheusRegistersWithIndependentRegistries(t *testing.T) {
	for range 2 {
		registry := prometheus.NewRegistry()
		m, err := CreatePrometheus(registry)
		require.NoError(t, err)
		require.NotNil(t, m)
		families, err := registry.Gather()
		require.NoError(t, err)
		require.Contains(t, metricFamiliesByName(families), "redis_cluster_operator_health")
	}
}

func TestPrometheusCleanupAllowsRegistrationWithSameRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	for range 2 {
		m, cleanup, err := CreatePrometheusWithCleanup(registry)
		require.NoError(t, err)
		require.NotNil(t, m)

		families, err := registry.Gather()
		require.NoError(t, err)
		require.Contains(t, metricFamiliesByName(families), "redis_cluster_operator_health")

		cleanup()
		cleanup()
		families, err = registry.Gather()
		require.NoError(t, err)
		require.NotContains(t, metricFamiliesByName(families), "redis_cluster_operator_health")
	}
}

func TestPrometheusDetailsAreMetricSpecific(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)

	require.NoError(t, m.Gauge(GaugeBlueGreenForgetResult, 1, "status:success"))
	require.NoError(t, m.Timing(TimingFailoverDuration, 1, "phase:promoting"))

	families, err := registry.Gather()
	require.NoError(t, err)
	got := metricFamiliesByName(families)
	require.Equal(t, "other", labelValue(got["redis_cluster_operator_bluegreen_forget_result"].Metric[0], "detail"))
	require.Equal(t, "other", labelValue(got["redis_cluster_operator_failover_duration_seconds"].Metric[0], "detail"))
}

func metricFamiliesByName(families []*dto.MetricFamily) map[string]*dto.MetricFamily {
	result := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func TestPrometheusRegistrationFailureIsReturnedAndRolledBack(t *testing.T) {
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: prometheusNamespace,
		Name:      "health",
		Help:      "conflicting collector",
	})))

	m, err := CreatePrometheus(registry)
	require.Nil(t, m)
	require.ErrorContains(t, err, "register Prometheus metrics")

	families, gatherErr := registry.Gather()
	require.NoError(t, gatherErr)
	require.NotContains(t, metricFamiliesByName(families), "redis_cluster_operator_events_total")
}

func labelValue(metric *dto.Metric, name string) string {
	for _, label := range metric.Label {
		if label.GetName() == name {
			return label.GetValue()
		}
	}
	return ""
}

func TestPrometheusExposesObservations(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)

	require.NoError(t, m.Count("reconcile"))
	require.NoError(t, m.CountBadSituation("cluster_partitioned", "pod_name:arbitrary-pod"))
	require.NoError(t, m.CountToolboxOperation("rebalance"))
	require.NoError(t, m.CountAutoscalingDecision("blocked", "cooldown"))
	require.NoError(t, m.ReportHealth(health.Warn))
	require.NoError(t, m.Gauge(GaugeClusterPrimaries, 3))
	require.NoError(t, m.Timing(TimingFailoverDuration, 2500, "status:success"))

	families, err := registry.Gather()
	require.NoError(t, err)
	got := metricFamiliesByName(families)
	require.Equal(t, float64(1), got["redis_cluster_operator_events_total"].Metric[0].Counter.GetValue())
	require.Equal(t, "reconcile", labelValue(got["redis_cluster_operator_events_total"].Metric[0], "event"))
	require.Equal(t, float64(2), got["redis_cluster_operator_health"].Metric[0].Gauge.GetValue())
	require.Equal(t, float64(3), got["redis_cluster_operator_cluster_primaries"].Metric[0].Gauge.GetValue())
	require.Equal(t, uint64(1), got["redis_cluster_operator_failover_duration_seconds"].Metric[0].Histogram.GetSampleCount())
	require.Equal(t, float64(2.5), got["redis_cluster_operator_failover_duration_seconds"].Metric[0].Histogram.GetSampleSum())
}

func TestPrometheusDurationBucketsCoverLongOperations(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)
	require.NoError(t, m.Timing(TimingBackupDuration, 30_000))

	families, err := registry.Gather()
	require.NoError(t, err)
	histogram := metricFamiliesByName(families)["redis_cluster_operator_backup_duration_seconds"].Metric[0].Histogram
	require.Equal(t, uint64(1), histogram.GetSampleCount())
	require.Greater(t, histogram.Bucket[len(histogram.Bucket)-1].GetUpperBound(), float64(10))
	require.Equal(t, uint64(1), bucketCountAtOrAbove(histogram, 30))
}

func bucketCountAtOrAbove(histogram *dto.Histogram, value float64) uint64 {
	for _, bucket := range histogram.Bucket {
		if bucket.GetUpperBound() >= value {
			return bucket.GetCumulativeCount()
		}
	}
	return 0
}

func TestPrometheusUsesLatestGaugeAndHealthObservationAcrossClusters(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)

	require.NoError(t, m.WithRedisCluster("first").ReportHealth(health.Error))
	require.NoError(t, m.WithRedisCluster("second").ReportHealth(health.OK))
	require.NoError(t, m.WithRedisCluster("first").Gauge(GaugeClusterPrimaries, 3))
	require.NoError(t, m.WithRedisCluster("second").Gauge(GaugeClusterPrimaries, 6))

	families, err := registry.Gather()
	require.NoError(t, err)
	got := metricFamiliesByName(families)
	require.Equal(t, float64(health.OK), got["redis_cluster_operator_health"].Metric[0].Gauge.GetValue())
	require.Equal(t, float64(6), got["redis_cluster_operator_cluster_primaries"].Metric[0].Gauge.GetValue())
}

func TestPrometheusPreservesKnownFiniteCallerValues(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)

	require.NoError(t, m.CountBadSituation("primary_endpoint_not_ready"))
	require.NoError(t, m.CountBadSituation("autoscaling_nla_repair_capacity"))
	for _, reason := range []string{
		"resourcedrift", "dryrundrift", "ownerreferenceerror", "datadogmetricerror", "wpaerror", "wpa_missing",
		"stale_generation", "generation_unobserved", "non_positive_recommendation", "invalid_group_size",
		"metric_status_unavailable",
	} {
		require.NoError(t, m.CountAutoscalingDecision("degraded", reason))
	}
	require.NoError(t, m.Gauge(GaugeBlueGreenProgressStalled, 1, "phase:WaitingSync"))

	families, err := registry.Gather()
	require.NoError(t, err)
	got := metricFamiliesByName(families)
	require.Len(t, got["redis_cluster_operator_bad_situations_total"].Metric, 2)
	require.Len(t, got["redis_cluster_operator_autoscaling_decisions_total"].Metric, 11)
	require.Equal(t, "phase:WaitingSync", labelValue(got["redis_cluster_operator_bluegreen_progress_stalled_seconds"].Metric[0], "detail"))
}

func TestPrometheusBoundsAllCallerControlledLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)

	for _, value := range []string{"customer-a", "customer-b", "customer-c"} {
		clusterMetrics := m.WithRedisCluster(value)
		require.NoError(t, clusterMetrics.CountBadSituation(value, "pod_name:"+value))
		require.NoError(t, clusterMetrics.CountToolboxOperation(value))
		require.NoError(t, clusterMetrics.CountAutoscalingDecision(value, value))
		require.NoError(t, clusterMetrics.Gauge(GaugeBlueGreenPhase, 1, "phase:"+value))
		require.NoError(t, clusterMetrics.Timing(TimingBlueGreenPhaseDuration, 1, "phase:"+value))
	}

	families, err := registry.Gather()
	require.NoError(t, err)
	got := metricFamiliesByName(families)
	for _, name := range []string{
		"redis_cluster_operator_bad_situations_total",
		"redis_cluster_operator_toolbox_operations_total",
		"redis_cluster_operator_autoscaling_decisions_total",
		"redis_cluster_operator_bluegreen_phase",
		"redis_cluster_operator_bluegreen_phase_duration_seconds",
	} {
		require.Len(t, got[name].Metric, 1, name)
	}
	require.Equal(t, "other", labelValue(got["redis_cluster_operator_bad_situations_total"].Metric[0], "situation"))
	require.Equal(t, "other", labelValue(got["redis_cluster_operator_toolbox_operations_total"].Metric[0], "operation"))
	for _, family := range families {
		for _, metric := range family.Metric {
			require.Empty(t, labelValue(metric, "cluster"))
			require.Empty(t, labelValue(metric, "namespace"))
		}
	}
}

func TestPrometheusRejectsUnknownMetricNames(t *testing.T) {
	m, err := CreatePrometheus(prometheus.NewRegistry())
	require.NoError(t, err)
	require.Error(t, m.Count("attacker_controlled_counter"))
	require.Error(t, m.Gauge("attacker_controlled_gauge", 1))
	require.Error(t, m.Timing("attacker_controlled_timing", 1))
}

func TestHealthUnknownDoesNotOverwriteLastKnownValue(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := CreatePrometheus(registry)
	require.NoError(t, err)
	require.NoError(t, m.ReportHealth(health.OK))
	require.NoError(t, m.ReportHealth(health.Unknown))

	families, err := registry.Gather()
	require.NoError(t, err)
	require.Equal(t, float64(1), metricFamiliesByName(families)["redis_cluster_operator_health"].Metric[0].Gauge.GetValue())
}

func TestNoopRetainsInterfaceSemantics(t *testing.T) {
	m := CreateNoop().WithRedisCluster("anything")
	require.NoError(t, m.Count("anything"))
	require.NoError(t, m.CountBadSituation("anything", "anything:anything"))
	require.NoError(t, m.CountToolboxOperation("anything"))
	require.NoError(t, m.CountAutoscalingDecision("anything", "anything"))
	require.NoError(t, m.ReportHealth(health.Error))
	require.NoError(t, m.Gauge("anything", 1, "anything:anything"))
	require.NoError(t, m.Timing("anything", 1, "anything:anything"))
}
