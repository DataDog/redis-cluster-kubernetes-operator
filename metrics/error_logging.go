package metrics

import (
	"sync"

	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/go-logr/logr"
)

// WithErrorLogging logs the first delivery failure from each Metrics method.
// RedisCluster-scoped reporters share the same suppression state.
func WithErrorLogging(reporter Metrics, log logr.Logger) Metrics {
	return &errorLoggingMetrics{
		reporter: reporter,
		log:      log,
		state: &metricsErrorLogState{
			loggedOperations: make(map[string]struct{}),
		},
	}
}

type errorLoggingMetrics struct {
	reporter Metrics
	log      logr.Logger
	state    *metricsErrorLogState
}

type metricsErrorLogState struct {
	mu               sync.Mutex
	loggedOperations map[string]struct{}
}

func (m *errorLoggingMetrics) WithRedisCluster(name string) Metrics {
	return &errorLoggingMetrics{
		reporter: m.reporter.WithRedisCluster(name),
		log:      m.log,
		state:    m.state,
	}
}

func (m *errorLoggingMetrics) Count(counter string) error {
	return m.logFirstError("Count", m.reporter.Count(counter))
}

func (m *errorLoggingMetrics) CountBadSituation(situation string, tags ...string) error {
	return m.logFirstError("CountBadSituation", m.reporter.CountBadSituation(situation, tags...))
}

func (m *errorLoggingMetrics) CountToolboxOperation(operation string) error {
	return m.logFirstError("CountToolboxOperation", m.reporter.CountToolboxOperation(operation))
}

func (m *errorLoggingMetrics) CountAutoscalingDecision(result string, reason string) error {
	return m.logFirstError("CountAutoscalingDecision", m.reporter.CountAutoscalingDecision(result, reason))
}

func (m *errorLoggingMetrics) ReportHealth(clusterHealth health.ClusterHealth) error {
	return m.logFirstError("ReportHealth", m.reporter.ReportHealth(clusterHealth))
}

func (m *errorLoggingMetrics) Gauge(name string, value float64, tags ...string) error {
	return m.logFirstError("Gauge", m.reporter.Gauge(name, value, tags...))
}

func (m *errorLoggingMetrics) Timing(name string, milliseconds int64, tags ...string) error {
	return m.logFirstError("Timing", m.reporter.Timing(name, milliseconds, tags...))
}

func (m *errorLoggingMetrics) logFirstError(operation string, err error) error {
	if err == nil {
		return nil
	}

	m.state.mu.Lock()
	_, alreadyLogged := m.state.loggedOperations[operation]
	if !alreadyLogged {
		m.state.loggedOperations[operation] = struct{}{}
	}
	m.state.mu.Unlock()

	if !alreadyLogged {
		m.log.Error(err, "Metrics delivery failed", "operation", operation)
	}
	return err
}
