package controller

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// notFoundCache lifecycle: NotFound populates → hit within TTL suppresses
// Get → expires after TTL → re-Get. Asserts both timestamp and Get count.
func TestSynctest_NotFoundCache_NoTightLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		scheme := runtime.NewScheme()
		require.NoError(t, clientgoscheme.AddToScheme(scheme))
		require.NoError(t, cachev1.SchemeBuilder.AddToScheme(scheme))

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		counting := &countingClient{Client: fakeClient}
		r := &RedisClustersReconciler{
			ClusterClient: redis_cluster.ClusterClient{
				Client:        counting,
				ClusterScheme: scheme,
				Namespace:     "default",
				Log:           logr.Discard(),
				Metrics:       &metrics.NoopMetrics{},
			},
		}
		req := ctrl.Request{NamespacedName: types.NamespacedName{
			Namespace: "default",
			Name:      "redis-cluster-missing",
		}}

		// cache miss → Get + populate
		_, err := r.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 1, counting.getCalls)
		ts1 := loadNotFoundCacheTimestamp(t, r, req)

		// within TTL → no Get, timestamp unchanged
		time.Sleep(time.Second)
		_, err = r.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 1, counting.getCalls, "cache hit must skip Get")
		ts2 := loadNotFoundCacheTimestamp(t, r, req)
		require.True(t, ts1.Equal(ts2))

		// past TTL → Get fires, timestamp refreshed
		time.Sleep(constants.NotFoundCacheTTL + time.Second)
		_, err = r.Reconcile(context.Background(), req)
		require.NoError(t, err)
		require.Equal(t, 2, counting.getCalls)
		ts3 := loadNotFoundCacheTimestamp(t, r, req)
		require.True(t, ts3.After(ts2))
	})
}

func TestReconcileAcceptsRedisClusterNameWithoutLegacyPrefix(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.SchemeBuilder.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	counting := &countingClient{Client: fakeClient}
	sharedHealth := &healthPreservingMetrics{lastHealth: health.Error}
	r := &RedisClustersReconciler{
		ClusterClient: redis_cluster.ClusterClient{
			Client:        counting,
			ClusterScheme: scheme,
			Namespace:     "default",
			Log:           logr.Discard(),
			Metrics:       sharedHealth,
		},
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: "default",
		Name:      "cache",
	}}

	_, err := r.Reconcile(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 1, counting.getCalls, "every RedisCluster name must reach reconciliation")
	_, cached := r.notFoundCache.Load(req.NamespacedName.String())
	require.True(t, cached, "a non-prefixed missing CR must follow the normal NotFound path")
	require.Equal(t, health.Error, sharedHealth.lastHealth,
		"a missing CR must not overwrite the shared operator health")
}

type healthPreservingMetrics struct {
	metrics.NoopMetrics
	lastHealth health.ClusterHealth
}

func (m *healthPreservingMetrics) WithRedisCluster(string) metrics.Metrics { return m }

func (m *healthPreservingMetrics) ReportHealth(h health.ClusterHealth) error {
	if h != health.Unknown {
		m.lastHealth = h
	}
	return nil
}

// countingClient counts Get calls — the cache short-circuit is upstream of Get.
type countingClient struct {
	client.Client
	getCalls int
}

func (c *countingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.getCalls++
	return c.Client.Get(ctx, key, obj, opts...)
}

func loadNotFoundCacheTimestamp(t *testing.T, r *RedisClustersReconciler, req ctrl.Request) time.Time {
	t.Helper()
	v, ok := r.notFoundCache.Load(req.NamespacedName.String())
	require.True(t, ok, "notFoundCache must contain an entry for %s", req.NamespacedName)
	return v.(time.Time)
}
