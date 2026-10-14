// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package envtest

import (
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
)

// NewClusterClient builds a ClusterClient wired to the envtest Suite's real
// k8s client + rest config, with logging discarded and a no-op metrics impl.
// Sufficient for tests that exercise reconciler helpers (GetOrCreateStatefulSet,
// shouldReconcile, handleScaling, ...) against the in-memory API server.
//
// Tests that need a RedisClusterReconciler embed this directly:
//
//	cc := envtest.NewClusterClient(t, s, "redis-ns")
//	r := &controller.RedisClusterReconciler{ClusterClient: cc}
func NewClusterClient(t *testing.T, s *Suite, namespace string) redis_cluster.ClusterClient {
	t.Helper()
	return redis_cluster.ClusterClient{
		Client:             s.Client,
		ClusterScheme:      s.Scheme,
		RestConfig:         s.Cfg,
		Namespace:          namespace,
		Log:                logr.Discard(),
		Metrics:            &metrics.NoopMetrics{},
		CredentialProvider: redis_cluster.NoAuthentication{},
	}
}
