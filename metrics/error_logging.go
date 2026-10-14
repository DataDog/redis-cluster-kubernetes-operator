package metrics

import (
	"errors"
	"reflect"
	"sync"

	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/go-logr/logr"
)

// WithErrorLogging logs the first delivery failure from each Metrics method.
// RedisCluster-scoped reporters share the same suppression state.
func WithErrorLogging(reporter Metrics, log logr.Logger) Metrics {
	if isNilMetrics(reporter) {
		reporter = &failedMetrics{err: errNilMetricsReporter}
	}
	return &errorLoggingMetrics{
		reporter: reporter,
		log:      log,
		state: &metricsErrorLogState{
			loggedOperations: make(map[string]struct{}),
		},
	}
}

var (
	errNilMetricsReporter        = errors.New("metrics reporter is nil")
	errNilClusterMetricsReporter = errors.New("metrics reporter returned nil from WithRedisCluster")
)

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
	reporter := m.reporter.WithRedisCluster(name)
	if isNilMetrics(reporter) {
		reporter = &failedMetrics{err: errNilClusterMetricsReporter}
	}
	return &errorLoggingMetrics{
		reporter: reporter,
		log:      m.log,
		state:    m.state,
	}
}

func isNilMetrics(reporter Metrics) bool {
	if reporter == nil {
		return true
	}
	value := reflect.ValueOf(reporter)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type failedMetrics struct {
	err error
}

func (m *failedMetrics) WithRedisCluster(string) Metrics               { return m }
func (m *failedMetrics) Count(string) error                            { return m.err }
func (m *failedMetrics) CountBadSituation(string, ...string) error     { return m.err }
func (m *failedMetrics) CountToolboxOperation(string) error            { return m.err }
func (m *failedMetrics) CountAutoscalingDecision(string, string) error { return m.err }
func (m *failedMetrics) ReportHealth(health.ClusterHealth) error       { return m.err }
func (m *failedMetrics) Gauge(string, float64, ...string) error        { return m.err }
func (m *failedMetrics) Timing(string, int64, ...string) error         { return m.err }

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
