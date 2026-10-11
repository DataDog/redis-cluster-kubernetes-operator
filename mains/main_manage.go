package mains

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
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

func Manage(scheme *runtime.Scheme, metricsAddr, healthProbeAddr string, enableLeaderElection bool, log logr.Logger) {
	log.Info("Manager starting", "leader_election", enableLeaderElection)
	credentialProvider, err := credentialProviderFromMode(os.Getenv("REDIS_AUTH_MODE"))
	if err != nil {
		log.Error(err, "Invalid Redis authentication configuration")
		os.Exit(1)
	}

	watchNamespace, found := os.LookupEnv("WATCH_NAMESPACE")
	if !found {
		log.Error(errors.New("missing environment variable WATCH_NAMESPACE"), "Failed to determine namespace to watch")
		os.Exit(1)
	}

	log.Info("Watching namespace", "namespace", watchNamespace)
	operatorMetrics := metrics.CreateNoop()
	if metricsAddr != "0" {
		operatorMetrics, err = metrics.CreatePrometheus(ctrlmetrics.Registry)
		if err != nil {
			log.Error(err, "Failed to register Prometheus metrics")
			os.Exit(1)
		}
		_ = operatorMetrics.Count(counters.AppStarted)
	}

	cfg := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress: healthProbeAddr,
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: constants.ManagerPort,
		}),
		LeaderElection:   enableLeaderElection,
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
		log.Error(err, "Failed to start the manager")
		os.Exit(1)
	}
	if err := configureHealthChecks(mgr); err != nil {
		log.Error(err, "Failed to register health checks")
		os.Exit(1)
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
		ClusterClient: cc,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "Failed to create the controller")
		os.Exit(1)
	}
	log.Info("Start the manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "Failed to start the manager")
		os.Exit(1)
	}
	log.Info("Manager has exited")
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
