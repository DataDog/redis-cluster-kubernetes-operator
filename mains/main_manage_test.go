package mains

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/controller"
	operatormetrics "github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type providerMetrics struct{ operatormetrics.NoopMetrics }

func TestResolveMetricsUsesInjectedReporterWhenEndpointIsDisabled(t *testing.T) {
	want := &providerMetrics{}
	got, cleanup, err := resolveMetrics("0", want)
	require.NoError(t, err)
	require.Same(t, want, got)
	require.Nil(t, cleanup)
}

func TestResolveMetricsDefaultsToNoopWhenEndpointIsDisabled(t *testing.T) {
	got, cleanup, err := resolveMetrics("0", nil)
	require.NoError(t, err)
	require.IsType(t, &operatormetrics.NoopMetrics{}, got)
	require.Nil(t, cleanup)
}

func TestResolveMetricsRejectsTypedNilReporter(t *testing.T) {
	var typedNil *providerMetrics
	_, _, err := resolveMetrics("0", typedNil)
	require.ErrorContains(t, err, "reporter is nil")
}

func TestResolveMetricsCleansUpDefaultPrometheusReporter(t *testing.T) {
	for range 2 {
		got, cleanup, err := resolveMetrics(":8080", nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.NotNil(t, cleanup)
		cleanup()
	}
}

func TestManageWithOptionsCleansUpDefaultPrometheusOnStartupError(t *testing.T) {
	t.Setenv("REDIS_AUTH_MODE", "none")
	t.Setenv("WATCH_NAMESPACE", "default")
	var factory *controller.BackupBackendRegistry
	options := ManageOptions{
		MetricsAddr:          ":8080",
		BackupBackendFactory: factory,
	}

	for range 2 {
		err := ManageWithOptions(runtime.NewScheme(), logr.Discard(), options)
		require.ErrorContains(t, err, "backup backend factory is nil")
	}
}

func TestResolveBackupBackendFactoryRejectsTypedNil(t *testing.T) {
	var factory *controller.BackupBackendRegistry
	_, err := resolveBackupBackendFactory(factory)
	require.ErrorContains(t, err, "factory is nil")
}

type recordingHealthManager struct {
	healthChecks map[string]healthz.Checker
	readyChecks  map[string]healthz.Checker
	runnable     manager.Runnable
}

func (m *recordingHealthManager) AddHealthzCheck(name string, check healthz.Checker) error {
	if m.healthChecks == nil {
		m.healthChecks = make(map[string]healthz.Checker)
	}
	m.healthChecks[name] = check
	return nil
}

func (m *recordingHealthManager) AddReadyzCheck(name string, check healthz.Checker) error {
	if m.readyChecks == nil {
		m.readyChecks = make(map[string]healthz.Checker)
	}
	m.readyChecks[name] = check
	return nil
}

func (m *recordingHealthManager) Add(runnable manager.Runnable) error {
	m.runnable = runnable
	return nil
}

func TestCredentialProviderFromMode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    string
		wantErr bool
	}{
		{name: "default no authentication"},
		{name: "explicit no authentication", mode: "none"},
		{name: "mounted Secret", mode: "secret"},
		{name: "unknown provider", mode: "vault", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := credentialProviderFromMode(tc.mode); (err != nil) != tc.wantErr {
				t.Fatalf("credentialProviderFromMode(%q) error = %v, wantErr %t", tc.mode, err, tc.wantErr)
			}
		})
	}
}

func TestConfigureHealthChecksReadinessTracksManagerStartup(t *testing.T) {
	mgr := &recordingHealthManager{}
	if err := configureHealthChecks(mgr); err != nil {
		t.Fatalf("configureHealthChecks() error = %v", err)
	}

	if err := mgr.healthChecks["healthz"](nil); err != nil {
		t.Fatalf("liveness check error = %v", err)
	}
	if err := mgr.readyChecks["readyz"](nil); err == nil {
		t.Fatal("readiness succeeded before the manager started its non-leader runnables")
	}
	if mgr.runnable == nil {
		t.Fatal("manager-start readiness runnable was not registered")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() {
		done <- mgr.runnable.Start(ctx)
	}()
	require.Eventually(t, func() bool {
		return mgr.readyChecks["readyz"](nil) == nil
	}, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("readiness runnable error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness runnable did not stop")
	}
}

func TestConfigureHealthChecksPropagatesRegistrationFailure(t *testing.T) {
	wantErr := errors.New("registration failed")
	mgr := &failingHealthManager{err: wantErr}
	if err := configureHealthChecks(mgr); !errors.Is(err, wantErr) {
		t.Fatalf("configureHealthChecks() error = %v, want %v", err, wantErr)
	}
}

type failingHealthManager struct{ err error }

func (m *failingHealthManager) AddHealthzCheck(string, healthz.Checker) error { return m.err }
func (m *failingHealthManager) AddReadyzCheck(string, healthz.Checker) error  { return m.err }
func (m *failingHealthManager) Add(manager.Runnable) error                    { return m.err }
