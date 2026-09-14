package metrics

import "github.com/DataDog/redis-cluster-kubernetes-operator/health"

type NoopMetrics struct{}

func (metrics *NoopMetrics) WithRedisCluster(name string) Metrics { return metrics }

func (metrics *NoopMetrics) Count(counter string) error { return nil }

func (metrics *NoopMetrics) CountBadSituation(situation string, tags ...string) error { return nil }

func (metrics *NoopMetrics) CountToolboxOperation(operation string) error { return nil }

func (metrics *NoopMetrics) CountAutoscalingDecision(result string, reason string) error { return nil }

func (metrics *NoopMetrics) ReportHealth(h health.ClusterHealth) error { return nil }

func (metrics *NoopMetrics) Gauge(name string, value float64, tags ...string) error { return nil }

func (metrics *NoopMetrics) Timing(name string, milliseconds int64, tags ...string) error { return nil }

func CreateNoop() Metrics {
	return &NoopMetrics{}
}
