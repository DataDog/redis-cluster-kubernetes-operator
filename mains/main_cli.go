package mains

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/DataDog/redis-cluster-kubernetes-operator/cli"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

func Cli(scheme *runtime.Scheme, metricsAddr string, log logr.Logger) {
	clusterNamespace, found := os.LookupEnv("WATCH_NAMESPACE")
	if !found {
		log.Error(fmt.Errorf("missing environment variable WATCH_NAMESPACE"), "error")
		os.Exit(1)
	}

	clusterName, found := os.LookupEnv("REDIS_OPERATOR_DEV_CLUSTER")
	if !found {
		log.Error(fmt.Errorf("missing environment variable REDIS_OPERATOR_DEV_CLUSTER"), "error")
		os.Exit(1)
	}

	// see: https://nakamasato.medium.com/kubernetes-operator-series-1-controller-runtime-aa50d1d93c5c

	log.Info("Start", "namespace", clusterNamespace, "name", clusterName)

	cfg := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		WebhookServer: webhook.NewServer(webhook.Options{
			Port: constants.ManagerPort,
		}),
		// we do NOT want this manager to participate in the leader election
		// -> there MAY be conflicting actions if you are not careful!
		LeaderElection:   false, //cfg.enableLeaderElection,
		LeaderElectionID: constants.LeaderElectionID,
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				clusterNamespace: {},
			},
			SyncPeriod: &constants.ReconcileFreq,
		},
		LeaseDuration: &constants.LeaseDuration,
		RenewDeadline: &constants.RenewDeadline,
	})
	if err != nil {
		log.Error(err, "error: failed to create a manager")
		os.Exit(1)
	}

	cc := redis_cluster.ClusterClient{
		Client:             mgr.GetClient(),
		ClusterScheme:      mgr.GetScheme(),
		RestConfig:         cfg,
		Namespace:          clusterNamespace,
		Log:                log,
		Metrics:            metrics.CreateNoop(),
		CredentialProvider: redis_cluster.NoAuthentication{},
	}

	mc := NewManagerContext()

	_ = mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		err = cli.Run(ctx, cc, clusterNamespace, clusterName)
		mc.Cancel()
		return err
	}))

	log.Info("Start the manager")
	if err := mgr.Start(mc.Context); err != nil {
		log.Error(err, "error: failed to start the manager")
		os.Exit(1)
	}
	log.Info("Manager has exited")
}

// ManagerContext implements a cancellable context.
// see ctrl.SetupSignalHandler()
type ManagerContext struct {
	onlyOneSignalHandler chan struct{}
	channel              chan os.Signal
	Context              context.Context
}

func NewManagerContext() ManagerContext {
	c := ManagerContext{
		onlyOneSignalHandler: make(chan struct{}),
		channel:              make(chan os.Signal, 2),
	}
	close(c.onlyOneSignalHandler) // panics when called twice

	ctx, cancel := context.WithCancel(context.Background())

	shutdownSignals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Notify(c.channel, shutdownSignals...)
	go func() {
		<-c.channel
		cancel()
		<-c.channel
		os.Exit(1) // second signal. Exit directly.
	}()

	c.Context = ctx
	return c
}

func (mc ManagerContext) Cancel() {
	mc.channel <- os.Interrupt
}
