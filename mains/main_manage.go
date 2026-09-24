package mains

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"sync/atomic"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/controller"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// ManageOptions configures the controller manager and its integration providers.
type ManageOptions struct {
	// Context controls manager shutdown. Nil uses context.Background; embedding
	// applications should normally provide their own cancellation context.
	Context              context.Context
	MetricsAddr          string
	HealthProbeAddr      string
	EnableLeaderElection bool
	Metrics              metrics.Metrics
	BackupBackendFactory controller.BackupBackendFactory
}

func Manage(scheme *runtime.Scheme, metricsAddr, healthProbeAddr string, enableLeaderElection bool, log logr.Logger) {
	err := ManageWithOptions(scheme, log, ManageOptions{
		Context:              ctrl.SetupSignalHandler(),
		MetricsAddr:          metricsAddr,
		HealthProbeAddr:      healthProbeAddr,
		EnableLeaderElection: enableLeaderElection,
	})
	if err != nil {
		log.Error(err, "Manager exited with an error")
		os.Exit(1)
	}
}

// ManageWithOptions runs the controller with optional integration providers.
// Nil integrations preserve the reference binary defaults: Prometheus metrics
// (or no-op when its endpoint is disabled) and AWS S3 backup storage.
func ManageWithOptions(scheme *runtime.Scheme, log logr.Logger, options ManageOptions) error {
	log.Info("Manager starting", "leader_election", options.EnableLeaderElection)
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	credentialProvider, err := credentialProviderFromMode(os.Getenv("REDIS_AUTH_MODE"))
	if err != nil {
		return fmt.Errorf("invalid Redis authentication configuration: %w", err)
	}

	watchNamespace, found := os.LookupEnv("WATCH_NAMESPACE")
	if !found {
		return errors.New("missing environment variable WATCH_NAMESPACE")
	}

	log.Info("Watching namespace", "namespace", watchNamespace)
	operatorMetrics, cleanupMetrics, err := resolveMetrics(options.MetricsAddr, options.Metrics)
	if err != nil {
		return err
	}
	if cleanupMetrics != nil {
		defer cleanupMetrics()
	}
	operatorMetrics = metrics.WithErrorLogging(operatorMetrics, log.WithName("metrics"))
	_ = operatorMetrics.Count(counters.AppStarted)
	backupBackendFactory, err := resolveBackupBackendFactory(options.BackupBackendFactory)
	if err != nil {
		return err
	}

	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("get Kubernetes configuration: %w", err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: options.MetricsAddr,
		},
		HealthProbeBindAddress: options.HealthProbeAddr,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: constants.ManagerPort,
		}),
		LeaderElection:   options.EnableLeaderElection,
		LeaderElectionID: constants.LeaderElectionID,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				watchNamespace: {},
			},
			SyncPeriod: &constants.ReconcileFreq,
		},
		LeaseDuration: &constants.LeaseDuration,
		RenewDeadline: &constants.RenewDeadline,
	})

	if err != nil {
		return fmt.Errorf("create controller manager: %w", err)
	}
	if err := configureHealthChecks(mgr); err != nil {
		return err
	}

	cc := redis_cluster.ClusterClient{
		Client:             mgr.GetClient(),
		ClusterScheme:      mgr.GetScheme(),
		RestConfig:         cfg,
		Namespace:          watchNamespace,
		Log:                ctrl.Log.WithName("controller"),
		Metrics:            operatorMetrics,
		CredentialProvider: credentialProvider,
	}

	if err = (&controller.RedisClustersReconciler{
		ClusterClient:        cc,
		BackupBackendFactory: backupBackendFactory,
	}).SetupWithManager(mgr); err != nil {
		return fmt.Errorf("create Redis cluster controller: %w", err)
	}
	log.Info("Start the manager")
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("run controller manager: %w", err)
	}
	log.Info("Manager has exited")
	return nil
}

func resolveMetrics(metricsAddr string, reporter metrics.Metrics) (metrics.Metrics, func(), error) {
	if reporter != nil {
		if isNilInterface(reporter) {
			return nil, nil, errors.New("metrics reporter is nil")
		}
		return reporter, nil, nil
	}
	if metricsAddr == "0" {
		return metrics.CreateNoop(), nil, nil
	}
	return metrics.CreatePrometheusWithCleanup(ctrlmetrics.Registry)
}

func resolveBackupBackendFactory(factory controller.BackupBackendFactory) (controller.BackupBackendFactory, error) {
	if factory == nil {
		return controller.NewBackupBackendRegistry(), nil
	}
	if isNilInterface(factory) {
		return nil, errors.New("backup backend factory is nil")
	}
	return factory, nil
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

type healthCheckManager interface {
	AddHealthzCheck(string, healthz.Checker) error
	AddReadyzCheck(string, healthz.Checker) error
	Add(manager.Runnable) error
}

// configureHealthChecks exposes process liveness and manager readiness.
// Controller-runtime starts non-leader runnables only after its cache has
// synchronized, so the readiness state deliberately does not depend on the
// health of any RedisCluster resource.
func configureHealthChecks(mgr healthCheckManager) error {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("register liveness check: %w", err)
	}

	var managerReady atomic.Bool
	if err := mgr.AddReadyzCheck("readyz", func(_ *http.Request) error {
		if !managerReady.Load() {
			return errors.New("manager cache has not started")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("register readiness check: %w", err)
	}

	if err := mgr.Add(&managerReadinessRunnable{ready: &managerReady}); err != nil {
		return fmt.Errorf("register manager readiness runnable: %w", err)
	}
	return nil
}

type managerReadinessRunnable struct {
	ready *atomic.Bool
}

func (r *managerReadinessRunnable) Start(ctx context.Context) error {
	r.ready.Store(true)
	defer r.ready.Store(false)
	<-ctx.Done()
	return nil
}

// NeedLeaderElection returns false so controller-runtime starts this runnable
// in its post-cache-sync "Others" group even when leader election is enabled.
func (*managerReadinessRunnable) NeedLeaderElection() bool { return false }

func credentialProviderFromMode(mode string) (redis_cluster.CredentialProvider, error) {
	switch mode {
	case "", "none":
		return redis_cluster.NoAuthentication{}, nil
	case "secret":
		return redis_cluster.MountedSecretAuthentication{}, nil
	default:
		return nil, errors.New("REDIS_AUTH_MODE must be one of: none, secret")
	}
}
