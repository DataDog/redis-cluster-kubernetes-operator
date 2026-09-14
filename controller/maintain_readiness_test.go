package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testAuxiliaryReadyCondition corev1.PodConditionType = "example.com/network-ready"

type fakePrimaryEndpointProber struct {
	result    primaryEndpointProbeResult
	err       error
	calls     int
	hostnames []string
}

func (p *fakePrimaryEndpointProber) Probe(
	_ context.Context,
	_ *redis_cluster.Cluster,
	hostname string,
) (primaryEndpointProbeResult, error) {
	p.calls++
	p.hostnames = append(p.hostnames, hostname)
	return p.result, p.err
}

type emptyPrimaryReadinessFixture struct {
	reconciler *RedisClusterReconciler
	cluster    *redis_cluster.Cluster
	pod        *corev1.Pod
	primary    *redis_cluster.ClusterNode
	prober     *fakePrimaryEndpointProber
	now        time.Time
}

func (f *emptyPrimaryReadinessFixture) prepare(ctx context.Context) ([]string, error) {
	return f.reconciler.prepareEmptyPrimariesForRebalance(ctx, f.cluster)
}

func (f *emptyPrimaryReadinessFixture) advance(d time.Duration) {
	f.now = f.now.Add(d)
}

func TestPrepareEmptyPrimariesForRebalance_RequiresStableExactEndpoint(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})

	waitingPods, err := fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods,
		"a newly ready-labeled primary must wait before probing DNS")
	require.Zero(t, fixture.prober.calls)

	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	require.Equal(t, 1, fixture.prober.calls)

	// A watched Pod update can reconcile immediately. It may probe again, but it
	// must not advance the success streak before the minimum interval.
	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	require.Equal(t, 2, fixture.prober.calls)

	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Empty(t, waitingPods)
	require.Equal(t, primaryEndpointRequiredSuccesses+1, fixture.prober.calls)
	require.Equal(t, []string{
		"redis-cluster-test-6.redis-cluster-test.default.svc",
		"redis-cluster-test-6.redis-cluster-test.default.svc",
		"redis-cluster-test-6.redis-cluster-test.default.svc",
		"redis-cluster-test-6.redis-cluster-test.default.svc",
	}, fixture.prober.hostnames)
}

type fixedPrimaryEndpointResolver struct {
	hostname string
}

func (r fixedPrimaryEndpointResolver) ResolveHostname(
	_ *redis_cluster.Cluster,
	_ *redis_cluster.ClusterNode,
	_ *corev1.Pod,
) (string, bool) {
	return r.hostname, true
}

func TestPrepareEmptyPrimariesForRebalance_UsesInjectedPrivateHostnameResolver(t *testing.T) {
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	const privateHostname = "redis-cluster-test-6.redis-cluster-test.default.private.example"
	fixture.primary.Hostname = privateHostname
	fixture.reconciler.PrimaryEndpointResolver = fixedPrimaryEndpointResolver{hostname: privateHostname}

	_, err := fixture.prepare(context.Background())
	require.NoError(t, err)
	_, err = fixture.prepare(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{privateHostname}, fixture.prober.hostnames)
}

func TestPrepareAutoscalingScaleUpMemberEndpoints_RequiresStableExactEndpoint(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	fixture.cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries = 4
	fixture.cluster.RedisCluster.Spec.Replicas = 1
	setAutoscalingCondition(
		&fixture.cluster.RedisCluster.Status.Autoscaling,
		fixture.cluster.RedisCluster.Generation,
		cachev1.AutoscalingConditionScaling,
		metav1.ConditionTrue,
		"RecommendationAccepted",
		"scaling",
	)

	waitingPods, err := fixture.reconciler.prepareAutoscalingScaleUpMemberEndpoints(ctx, fixture.cluster)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods,
		"a newly admitted scale-up member must wait after its discovery label changes")
	require.Zero(t, fixture.prober.calls)

	waitingPods, err = fixture.reconciler.prepareAutoscalingScaleUpMemberEndpoints(ctx, fixture.cluster)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.reconciler.prepareAutoscalingScaleUpMemberEndpoints(ctx, fixture.cluster)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.reconciler.prepareAutoscalingScaleUpMemberEndpoints(ctx, fixture.cluster)
	require.NoError(t, err)
	require.Empty(t, waitingPods)
	require.Equal(t, primaryEndpointRequiredSuccesses, fixture.prober.calls)
}

func TestPrepareAutoscalingScaleUpMemberEndpoints_GatesReattachedDrainCandidateAtOldOrdinal(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	fixture.cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries = 4
	fixture.cluster.RedisCluster.Spec.Replicas = 1
	setAutoscalingCondition(
		&fixture.cluster.RedisCluster.Status.Autoscaling,
		fixture.cluster.RedisCluster.Generation,
		cachev1.AutoscalingConditionScaling,
		metav1.ConditionTrue,
		"RecommendationAccepted",
		"scaling",
	)
	fixture.primary.PodIndex = 1
	fixture.primary.HasDrainCandidateCond = true

	waitingPods, err := fixture.reconciler.prepareAutoscalingScaleUpMemberEndpoints(ctx, fixture.cluster)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods,
		"a reset and reattached drain candidate must be re-gated even below the new-group ordinal")
}

func TestPrepareEmptyPrimariesForRebalance_WaitsForAggregatePodReadyBeforeDNS(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		{Type: testAuxiliaryReadyCondition, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	})

	waitingPods, err := fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	// An unrelated auxiliary condition is present, but aggregate
	// PodReady is still false, so the DNS probe must not run.
	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	require.Zero(t, fixture.prober.calls)

	updatedPod := &corev1.Pod{}
	require.NoError(t, fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.pod), updatedPod))
	for i := range updatedPod.Status.Conditions {
		if updatedPod.Status.Conditions[i].Type == corev1.PodReady {
			updatedPod.Status.Conditions[i].Status = corev1.ConditionTrue
		}
	}
	require.NoError(t, fixture.reconciler.Status().Update(ctx, updatedPod))

	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods,
		"PodReady starts, but does not bypass, the DNS stability gate")
	require.Equal(t, 1, fixture.prober.calls)
}

func TestPrepareEmptyPrimariesForRebalance_EndpointFailuresResetStreak(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})

	_, err := fixture.prepare(ctx) // ready-label
	require.NoError(t, err)
	_, err = fixture.prepare(ctx) // success 1
	require.NoError(t, err)
	fixture.advance(primaryEndpointSuccessInterval)
	_, err = fixture.prepare(ctx) // success 2
	require.NoError(t, err)

	fixture.prober.err = errors.New("no such host")
	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err := fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	fixture.prober.err = nil
	fixture.advance(primaryEndpointSuccessInterval)
	_, err = fixture.prepare(ctx) // restarted at success 1
	require.NoError(t, err)
	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.prepare(ctx) // success 2
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	fixture.advance(primaryEndpointSuccessInterval)
	waitingPods, err = fixture.prepare(ctx) // success 3
	require.NoError(t, err)
	require.Empty(t, waitingPods)
}

func TestPrepareEmptyPrimariesForRebalance_RejectsWrongEndpoint(t *testing.T) {
	tests := []struct {
		name             string
		configureFixture func(*emptyPrimaryReadinessFixture)
	}{
		{
			name: "wrong resolved IP",
			configureFixture: func(f *emptyPrimaryReadinessFixture) {
				f.prober.result.ResolvedIPs = []string{"10.0.0.99"}
			},
		},
		{
			name: "wrong Redis node ID",
			configureFixture: func(f *emptyPrimaryReadinessFixture) {
				f.prober.result.NodeID = "different-node-id"
			},
		},
		{
			name: "wrong announced hostname",
			configureFixture: func(f *emptyPrimaryReadinessFixture) {
				f.primary.Hostname = "wrong.redis.hostname"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			})
			test.configureFixture(fixture)

			_, err := fixture.prepare(context.Background()) // ready-label
			require.NoError(t, err)
			fixture.advance(primaryEndpointSuccessInterval)
			waitingPods, err := fixture.prepare(context.Background())
			require.NoError(t, err)
			require.Equal(t, []string{fixture.pod.Name}, waitingPods)
		})
	}
}

func TestPrepareEmptyPrimariesForRebalance_DefaultsToServiceDNSWithoutEnv(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	storedPod := &corev1.Pod{}
	require.NoError(t, fixture.reconciler.Get(ctx, client.ObjectKeyFromObject(fixture.pod), storedPod))
	storedPod.Spec.Containers[0].Env = nil
	require.NoError(t, fixture.reconciler.Update(ctx, storedPod))

	waitingPods, err := fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)

	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	require.Equal(t, 1, fixture.prober.calls)
	require.Equal(t, []string{"redis-cluster-test-6.redis-cluster-test.default.svc"}, fixture.prober.hostnames)
}

type ipOnlyPrimaryEndpointResolver struct{}

func (ipOnlyPrimaryEndpointResolver) ResolveHostname(*redis_cluster.Cluster, *redis_cluster.ClusterNode, *corev1.Pod) (string, bool) {
	return "", false
}

func TestPrepareEmptyPrimariesForRebalance_InjectedResolverCanSelectIPEndpoint(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	fixture.primary.Hostname = ""
	fixture.reconciler.PrimaryEndpointResolver = ipOnlyPrimaryEndpointResolver{}

	waitingPods, err := fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{fixture.pod.Name}, waitingPods)
	waitingPods, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Empty(t, waitingPods)
	require.Zero(t, fixture.prober.calls)
}

func TestPrepareEmptyPrimariesForRebalance_TypedNilResolverUsesServiceDNS(t *testing.T) {
	ctx := context.Background()
	fixture := newEmptyPrimaryReadinessFixture(t, []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	})
	var resolver *fixedPrimaryEndpointResolver
	fixture.reconciler.PrimaryEndpointResolver = resolver

	_, err := fixture.prepare(ctx)
	require.NoError(t, err)
	_, err = fixture.prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"redis-cluster-test-6.redis-cluster-test.default.svc"}, fixture.prober.hostnames)
}

func newEmptyPrimaryReadinessFixture(
	t *testing.T,
	conditions []corev1.PodCondition,
) *emptyPrimaryReadinessFixture {
	t.Helper()

	scheme := newAutoscalingScheme(t)
	redisCluster := validAutoscalingCluster()
	hostname := "redis-cluster-test-6.redis-cluster-test.default.svc"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cluster-test-6",
			Namespace: redisCluster.Namespace,
			Labels:    copyStringMap(redisCluster.Spec.StatefulsetTemplate.Labels),
			UID:       types.UID("new-empty-primary"),
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name: constants.RedisContainerName,
				Env: []corev1.EnvVar{{
					Name:  redisServiceNameEnv,
					Value: redisCluster.Name,
				}},
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			PodIP:      "10.0.0.27",
			PodIPs:     []corev1.PodIP{{IP: "10.0.0.27"}},
			Conditions: conditions,
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod).
		WithStatusSubresource(pod).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	primary := &redis_cluster.ClusterNode{
		ID:       "new-primary-id",
		IP:       pod.Status.PodIP,
		Hostname: hostname,
		PodName:  pod.Name,
		PodIndex: 6,
		Role:     redis_cluster.PrimaryNode,
	}
	cluster := &redis_cluster.Cluster{
		RedisCluster: redisCluster,
		StatefulSet:  &redisCluster.Spec.StatefulsetTemplate,
		AllNodes:     []*redis_cluster.ClusterNode{primary},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {primary},
		},
	}

	now := time.Unix(1_000, 0)
	prober := &fakePrimaryEndpointProber{
		result: primaryEndpointProbeResult{
			ResolvedIPs: []string{pod.Status.PodIP},
			NodeID:      primary.ID,
		},
	}
	fixture := &emptyPrimaryReadinessFixture{
		reconciler: reconciler,
		cluster:    cluster,
		pod:        pod,
		primary:    primary,
		prober:     prober,
		now:        now,
	}
	reconciler.primaryEndpointProber = prober
	reconciler.primaryEndpointNow = func() time.Time { return fixture.now }
	return fixture
}
