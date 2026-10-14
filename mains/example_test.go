package mains_test

import (
	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/controller"
	"github.com/DataDog/redis-cluster-kubernetes-operator/mains"
	operatormetrics "github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// ExampleManageWithOptions_explicitDefaults shows how an embedding binary can
// select the same Prometheus, cloud storage, and primary endpoint integrations
// used by the reference binary. Leaving Metrics, BackupBackendFactory, and
// PrimaryEndpointResolver nil has the same effect; explicit construction is
// useful when the application wants to add providers to the registry before
// starting the manager.
func ExampleManageWithOptions_explicitDefaults() {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cachev1.AddToScheme(scheme))

	reporter, cleanup, err := operatormetrics.CreatePrometheusWithCleanup(ctrlmetrics.Registry)
	if err != nil {
		panic(err)
	}
	defer cleanup()

	options := mains.ManageOptions{
		Context:                 ctrl.SetupSignalHandler(),
		MetricsAddr:             ":8080",
		HealthProbeAddr:         ":8081",
		Metrics:                 reporter,
		BackupBackendFactory:    controller.NewBackupBackendRegistry(),
		PrimaryEndpointResolver: controller.KubernetesServiceDNSResolver{},
	}
	if err := mains.ManageWithOptions(scheme, logr.Discard(), options); err != nil {
		panic(err)
	}
}
