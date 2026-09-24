package metrics

import (
	"errors"
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
)

func TestWithErrorLoggingLogsFirstFailurePerOperationAcrossChildren(t *testing.T) {
	deliveryErr := errors.New("reporter unavailable")
	childReporter := &failingMetrics{err: deliveryErr}
	reporter := &failingMetrics{err: deliveryErr, child: childReporter}
	var logs strings.Builder
	wrapped := WithErrorLogging(reporter, funcr.New(func(prefix, args string) {
		logs.WriteString(prefix)
		logs.WriteString(args)
	}, funcr.Options{}))

	require.ErrorIs(t, wrapped.Count("first"), deliveryErr)
	require.ErrorIs(t, wrapped.Count("second"), deliveryErr)

	child := wrapped.WithRedisCluster("cache")
	require.NotSame(t, childReporter, child)
	require.ErrorIs(t, child.Count("third"), deliveryErr)
	require.ErrorIs(t, child.Gauge("nodes", 3), deliveryErr)
	require.ErrorIs(t, child.Gauge("nodes", 4), deliveryErr)

	output := logs.String()
	require.Equal(t, 2, strings.Count(output, "Metrics delivery failed"), output)
	require.Equal(t, 1, strings.Count(output, `"operation"="Count"`), output)
	require.Equal(t, 1, strings.Count(output, `"operation"="Gauge"`), output)
	require.Equal(t, 1, reporter.withRedisClusterCalls)
	require.Equal(t, 2, reporter.countCalls)
	require.Equal(t, 1, childReporter.countCalls)
	require.Equal(t, 2, childReporter.gaugeCalls)
}

type failingMetrics struct {
	err                   error
	child                 Metrics
	withRedisClusterCalls int
	countCalls            int
	gaugeCalls            int
}

func (m *failingMetrics) WithRedisCluster(string) Metrics {
	m.withRedisClusterCalls++
	return m.child
}

func (m *failingMetrics) Count(string) error {
	m.countCalls++
	return m.err
}

func (m *failingMetrics) CountBadSituation(string, ...string) error { return m.err }

func (m *failingMetrics) CountToolboxOperation(string) error { return m.err }

func (m *failingMetrics) CountAutoscalingDecision(string, string) error { return m.err }

func (m *failingMetrics) ReportHealth(health.ClusterHealth) error { return m.err }

func (m *failingMetrics) Gauge(string, float64, ...string) error {
	m.gaugeCalls++
	return m.err
}

func (m *failingMetrics) Timing(string, int64, ...string) error { return m.err }
