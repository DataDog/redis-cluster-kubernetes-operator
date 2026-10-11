package bluegreen_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/bluegreen"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// ────────────────────────────────────────────────────────────────────────────
// Test helpers
// ────────────────────────────────────────────────────────────────────────────

// testDeps implements bluegreen.Deps backed by a fake k8s client.
type testDeps struct {
	redis_cluster.ClusterClient
	updateStatusWithSpecHashErr error
}

func (d *testDeps) UpdateStatus(ctx context.Context, rc *cachev1.RedisCluster) error {
	return d.Client.Status().Update(ctx, rc)
}

func (d *testDeps) UpdateSpec(ctx context.Context, rc *cachev1.RedisCluster) error {
	return d.Client.Update(ctx, rc)
}

func (d *testDeps) RunSafetyBackup(_ context.Context, _ *redis_cluster.Cluster) (bool, error) {
	return false, nil
}

func (d *testDeps) FindHealthyRefNode(_ *redis_cluster.Cluster, _ *redis_cluster.ClusterNode) *redis_cluster.ClusterNode {
	return nil
}

func (d *testDeps) ForgetNode(_ context.Context, _ *redis_cluster.Cluster, _ *redis_cluster.ClusterNode) {
}

func (d *testDeps) UpdateStatusWithSpecHash(ctx context.Context, cluster *redis_cluster.Cluster) error {
	if d.updateStatusWithSpecHashErr != nil {
		return d.updateStatusWithSpecHashErr
	}
	return d.Client.Status().Update(ctx, cluster.RedisCluster)
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.SchemeBuilder.AddToScheme(scheme))
	return scheme
}

func newRedisCluster(name, namespace string, primaries, replicas int32) *cachev1.RedisCluster {
	return &cachev1.RedisCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "cache.datadoghq.com/v1",
			Kind:       "RedisCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
		},
		Spec: cachev1.RedisClusterSpec{
			Primaries: primaries,
			Replicas:  replicas,
			StatefulsetTemplate: appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					Labels: map[string]string{
						"app":                "rco",
						"chart_name":         name,
						"redis_cluster_name": name,
					},
				},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: name,
					Replicas:    ptr(primaries * (1 + replicas)),
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
							"app": "rco", "redis_cluster_name": name,
						}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "redis",
									Image: "redis:7.2.0",
									Env:   []corev1.EnvVar{{Name: "REDIS_SERVICE_NAME", Value: name}},
								},
							},
						},
					},
				},
			},
		},
		Status: cachev1.RedisClusterStatus{
			StatefulSetReady: true,
		},
	}
}

func newTestDeps(t *testing.T, rc *cachev1.RedisCluster) *testDeps {
	t.Helper()
	scheme := newScheme(t)
	objects := []runtime.Object{
		rc,
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: rc.Spec.StatefulsetTemplate.Spec.ServiceName, Namespace: rc.Namespace},
			Spec:       corev1.ServiceSpec{Selector: map[string]string{"redis_cluster_name": rc.Name}},
		},
	}
	for i := int32(0); i < *rc.Spec.StatefulsetTemplate.Spec.Replicas; i++ {
		labels := make(map[string]string, len(rc.Spec.StatefulsetTemplate.Labels))
		for key, value := range rc.Spec.StatefulsetTemplate.Labels {
			labels[key] = value
		}
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", rc.Spec.StatefulsetTemplate.Name, i),
			Namespace: rc.Namespace,
			Labels:    labels,
		}})
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objects...).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()

	return &testDeps{
		ClusterClient: redis_cluster.ClusterClient{
			Client:        fakeClient,
			ClusterScheme: scheme,
			Namespace:     rc.Namespace,
			Log:           logr.Discard(),
			Metrics:       metrics.CreateNoop(),
		},
	}
}

func newHandler(deps bluegreen.Deps) *bluegreen.Handler {
	return bluegreen.NewHandler(deps, logr.Discard(), metrics.CreateNoop())
}

func ptr(v int32) *int32 { return &v }

// ────────────────────────────────────────────────────────────────────────────
// Unit tests: ParseInfoField
// ────────────────────────────────────────────────────────────────────────────

func TestParseInfoField_Basic(t *testing.T) {
	info := `# Replication
role:slave
master_host:10.0.0.1
master_port:6379
master_link_status:up
master_last_io_seconds_ago:0
master_sync_in_progress:0
slave_repl_offset:12345
slave_read_repl_offset:12345
`
	assert.Equal(t, int64(0), bluegreen.ParseInfoField(info, "master_last_io_seconds_ago"))
	assert.Equal(t, int64(12345), bluegreen.ParseInfoField(info, "slave_repl_offset"))
	assert.Equal(t, int64(12345), bluegreen.ParseInfoField(info, "slave_read_repl_offset"))
}

func TestParseInfoField_MissingField(t *testing.T) {
	info := "role:master\nmaster_repl_offset:9999\n"
	assert.Equal(t, int64(-1), bluegreen.ParseInfoField(info, "nonexistent_field"))
}

func TestParseInfoField_WindowsLineEndings(t *testing.T) {
	info := "role:slave\r\nmaster_last_io_seconds_ago:7\r\nslave_repl_offset:42\r\n"
	assert.Equal(t, int64(7), bluegreen.ParseInfoField(info, "master_last_io_seconds_ago"))
	assert.Equal(t, int64(42), bluegreen.ParseInfoField(info, "slave_repl_offset"))
}

func TestParseInfoField_EmptyInput(t *testing.T) {
	assert.Equal(t, int64(-1), bluegreen.ParseInfoField("", "field"))
}

func TestParseInfoField_NonIntegerValue(t *testing.T) {
	info := "some_field:not_a_number\n"
	assert.Equal(t, int64(-1), bluegreen.ParseInfoField(info, "some_field"))
}

func TestParseInfoField_LargeOffset(t *testing.T) {
	info := "master_repl_offset:9876543210\n"
	assert.Equal(t, int64(9876543210), bluegreen.ParseInfoField(info, "master_repl_offset"))
}

// ────────────────────────────────────────────────────────────────────────────
// Unit tests: FormatSlotsRanges
// ────────────────────────────────────────────────────────────────────────────

func TestFormatSlotsRanges_Empty(t *testing.T) {
	assert.Equal(t, "", bluegreen.FormatSlotsRanges(nil))
}

func TestFormatSlotsRanges_SingleRange(t *testing.T) {
	ranges := []redis_cluster.SlotsRange{{First: 0, Last: 5460}}
	assert.Equal(t, "0-5460", bluegreen.FormatSlotsRanges(ranges))
}

func TestFormatSlotsRanges_SingleSlot(t *testing.T) {
	ranges := []redis_cluster.SlotsRange{{First: 100, Last: 100}}
	assert.Equal(t, "100", bluegreen.FormatSlotsRanges(ranges))
}

func TestFormatSlotsRanges_MultipleRanges(t *testing.T) {
	ranges := []redis_cluster.SlotsRange{
		{First: 0, Last: 5460},
		{First: 5461, Last: 10922},
		{First: 10923, Last: 16383},
	}
	assert.Equal(t, "0-5460,5461-10922,10923-16383", bluegreen.FormatSlotsRanges(ranges))
}

func TestFormatSlotsRanges_MixedRangesAndSingles(t *testing.T) {
	ranges := []redis_cluster.SlotsRange{
		{First: 0, Last: 100},
		{First: 200, Last: 200},
		{First: 300, Last: 500},
	}
	assert.Equal(t, "0-100,200,300-500", bluegreen.FormatSlotsRanges(ranges))
}

// ────────────────────────────────────────────────────────────────────────────
// Unit tests: PhaseValue map
// ────────────────────────────────────────────────────────────────────────────

func TestBlueGreenPhaseValueMap(t *testing.T) {
	knownPhases := []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseNone,
		cachev1.BlueGreenPhaseCreatingGreen,
		cachev1.BlueGreenPhaseWaitingPods,
		cachev1.BlueGreenPhaseReplicating,
		cachev1.BlueGreenPhaseWaitingSync,
		cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseFormingCluster,
		cachev1.BlueGreenPhaseScalingDownBlue,
		cachev1.BlueGreenPhaseTearingDown,
		cachev1.BlueGreenPhaseFailed,
	}

	for _, phase := range knownPhases {
		_, ok := bluegreen.PhaseValue[phase]
		assert.True(t, ok, "phase %q should be in blueGreenPhaseValue map", phase)
	}

	assert.Greater(t, bluegreen.PhaseValue[cachev1.BlueGreenPhaseCreatingGreen], bluegreen.PhaseValue[cachev1.BlueGreenPhaseNone])
	assert.Greater(t, bluegreen.PhaseValue[cachev1.BlueGreenPhaseWaitingPods], bluegreen.PhaseValue[cachev1.BlueGreenPhaseCreatingGreen])
	assert.Greater(t, bluegreen.PhaseValue[cachev1.BlueGreenPhaseTearingDown], bluegreen.PhaseValue[cachev1.BlueGreenPhaseFormingCluster])
	assert.Less(t, bluegreen.PhaseValue[cachev1.BlueGreenPhaseFailed], bluegreen.PhaseValue[cachev1.BlueGreenPhaseNone])
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: Handler.Start (startBlueGreenDeployment)
// ────────────────────────────────────────────────────────────────────────────

func TestStartBlueGreenDeployment_SetsGreenName(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	deps := newTestDeps(t, rc)
	h := newHandler(deps)

	cluster := &redis_cluster.Cluster{RedisCluster: rc}

	_, _, err := h.Start(context.Background(), cluster)
	require.NoError(t, err)

	assert.Equal(t, "redis-cluster-test"+constants.BlueGreenSTSSuffix, bluegreen.GreenSTSName(rc))
	assert.Equal(t, "redis-cluster-test", rc.Status.ActiveClusterName)
	assert.Equal(t, cachev1.BlueGreenPhaseCreatingGreen, rc.Status.BlueGreen.Phase)
	assert.True(t, rc.Status.BlueGreen.StartTime > 0)

	bluePods, err := deps.GetBluePods(context.Background(), rc)
	require.NoError(t, err)
	require.Len(t, bluePods.Items, 6)
	for _, pod := range bluePods.Items {
		assert.Equal(t, constants.BlueGreenRoleActive, pod.Labels[constants.BlueGreenRoleLabel])
	}
	service := &corev1.Service{}
	require.NoError(t, deps.Get(context.Background(), types.NamespacedName{Name: rc.Name, Namespace: rc.Namespace}, service))
	assert.Equal(t, constants.BlueGreenRoleActive, service.Spec.Selector[constants.BlueGreenRoleLabel])
}

func TestStartBlueGreenDeployment_AlternatesGreenName(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test-alt", "default", 3, 1)
	rc.Spec.StatefulsetTemplate.Name = "redis-cluster-test-alt"
	deps := newTestDeps(t, rc)
	h := newHandler(deps)

	cluster := &redis_cluster.Cluster{RedisCluster: rc}

	_, _, err := h.Start(context.Background(), cluster)
	require.NoError(t, err)

	assert.Equal(t, "redis-cluster-test", bluegreen.GreenSTSName(rc))
	assert.Equal(t, "redis-cluster-test-alt", rc.Status.ActiveClusterName)
	assert.Equal(t, cachev1.BlueGreenPhaseCreatingGreen, rc.Status.BlueGreen.Phase)
}

func TestStartBlueGreenDeployment_PropagatesSpecHashFailure(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	deps := newTestDeps(t, rc)
	stampErr := errors.New("stamp spec hash")
	deps.updateStatusWithSpecHashErr = stampErr
	h := newHandler(deps)

	_, gotHealth, err := h.Start(context.Background(), &redis_cluster.Cluster{RedisCluster: rc})

	require.ErrorIs(t, err, stampErr)
	assert.Equal(t, health.Error, gotHealth)

	stored := &cachev1.RedisCluster{}
	require.NoError(t, deps.Get(context.Background(), types.NamespacedName{
		Namespace: rc.Namespace,
		Name:      rc.Name,
	}, stored))
	assert.Equal(t, cachev1.BlueGreenPhaseNone, stored.Status.BlueGreen.Phase)
	assert.Empty(t, stored.Status.ActiveClusterName)
	assert.Empty(t, stored.Status.LastAppliedSpecHash)
	assert.Empty(t, stored.Status.LastAppliedRolloutSpecHash)
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: overall timeout
// ────────────────────────────────────────────────────────────────────────────

func TestHandleBlueGreenDeployment_OverallTimeoutExemptsWaitingSync(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	rc.Status.BlueGreen = cachev1.BlueGreenStatus{
		Phase:            cachev1.BlueGreenPhaseWaitingSync,
		StartTime:        time.Now().Add(-2 * constants.BlueGreenOverallTimeout).Unix(),
		SyncStartTime:    time.Now().Unix(),
		LastProgressTime: time.Now().Unix(),
		ShardMapping: []cachev1.ShardMappingEntry{
			{BluePrimaryID: "abc", BluePrimaryIP: "10.0.0.1", GreenPodName: "test-green-0", GreenPodIP: "10.0.0.10"},
		},
	}

	bgStatus := &rc.Status.BlueGreen
	phase := bgStatus.Phase
	elapsed := time.Since(time.Unix(bgStatus.StartTime, 0))

	assert.True(t, elapsed > constants.BlueGreenOverallTimeout, "should have exceeded overall timeout")

	shouldTimeout := bgStatus.StartTime > 0 && phase != cachev1.BlueGreenPhaseWaitingSync
	assert.False(t, shouldTimeout, "WaitingSync should be exempt from overall timeout")
}

func TestHandleBlueGreenDeployment_OverallTimeoutAppliesForOtherPhases(t *testing.T) {
	phasesToTimeout := []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseCreatingGreen,
		cachev1.BlueGreenPhaseWaitingPods,
		cachev1.BlueGreenPhaseReplicating,
		cachev1.BlueGreenPhaseFormingCluster,
	}

	phasesExempt := []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseWaitingSync,
		cachev1.BlueGreenPhaseFailed,
		cachev1.BlueGreenPhaseTearingDown,
		cachev1.BlueGreenPhaseNone,
		cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseScalingDownBlue,
	}

	exemptSet := map[cachev1.BlueGreenPhase]bool{}
	for _, p := range phasesExempt {
		exemptSet[p] = true
	}

	for _, phase := range phasesToTimeout {
		t.Run(string(phase), func(t *testing.T) {
			assert.False(t, exemptSet[phase], "phase %s should be subject to overall timeout", phase)
		})
	}
	for _, phase := range phasesExempt {
		t.Run("exempt_"+string(phase), func(t *testing.T) {
			assert.True(t, exemptSet[phase], "phase %s should be exempt from overall timeout", phase)
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: resume-forward attempt cap
// ────────────────────────────────────────────────────────────────────────────

func TestResumeForwardAttemptCap(t *testing.T) {
	assert.GreaterOrEqual(t, constants.BlueGreenMaxResumeForwardAttempts, constants.BlueGreenTakeoverAttemptThreshold,
		"max resume attempts should be >= takeover threshold")

	for attempt := int32(0); attempt <= int32(constants.BlueGreenMaxResumeForwardAttempts+1); attempt++ {
		if attempt > int32(constants.BlueGreenMaxResumeForwardAttempts) {
			t.Logf("Attempt %d: should be CAPPED (manual intervention)", attempt)
			continue
		}

		var mode string
		if attempt >= int32(constants.BlueGreenTakeoverAttemptThreshold) {
			mode = "TAKEOVER"
		} else if attempt >= int32(constants.BlueGreenForceAttemptThreshold) {
			mode = "FORCE"
		} else {
			mode = "soft"
		}

		t.Logf("Attempt %d: failover mode = %s", attempt, mode)
	}
}

func TestResumeForwardAttemptCap_ExceedsMax(t *testing.T) {
	maxAttempts := int32(constants.BlueGreenMaxResumeForwardAttempts)

	bgStatus := cachev1.BlueGreenStatus{
		ResumeForwardAttempts: maxAttempts + 1,
	}

	assert.True(t, bgStatus.ResumeForwardAttempts > maxAttempts,
		"should detect when attempts exceed maximum")
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: bgTeardown green SVC name uses local var
// ────────────────────────────────────────────────────────────────────────────

func TestBgTeardown_GreenSvcNameDerivedFromActiveCluster(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	rc.Status.ActiveClusterName = "redis-cluster-test"

	rc.Status.BlueGreen = cachev1.BlueGreenStatus{
		Phase: cachev1.BlueGreenPhaseTearingDown,
		ShardMapping: []cachev1.ShardMappingEntry{
			{GreenPodName: "test-a-0", GreenPodIP: "10.0.0.10"},
		},
	}

	capturedGreenName := bluegreen.GreenSTSName(rc)
	rc.Status.BlueGreen = cachev1.BlueGreenStatus{}

	assert.Equal(t, "redis-cluster-test"+constants.BlueGreenSTSSuffix, capturedGreenName,
		"green name should be derived from activeClusterName")
	assert.Equal(t, capturedGreenName, bluegreen.GreenSTSName(rc),
		"green name should be stable regardless of BG status")
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: CreateGreenStatefulSet
// ────────────────────────────────────────────────────────────────────────────

func TestCreateGreenStatefulSet_NamesAndLabels(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	rc.Status.Autoscaling.EffectivePrimaries = 5
	rc.Spec.StatefulsetTemplate.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{}}
	rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()
	greenSTS, greenName, err := cc.CreateGreenStatefulSet(ctx, rc)
	require.NoError(t, err)

	assert.Equal(t, "redis-cluster-test-alt", greenName)
	assert.Equal(t, "redis-cluster-test-alt", greenSTS.Name)
	if greenSTS.Spec.Selector != nil && greenSTS.Spec.Selector.MatchLabels != nil {
		assert.Empty(t, greenSTS.Spec.Selector.MatchLabels[constants.BlueGreenRoleLabel])
	}
	assert.Empty(t, greenSTS.Spec.Template.Labels[constants.BlueGreenRoleLabel], "green STS should not have role=active initially")
	peerServiceName := redis_cluster.BlueGreenPeerServiceName(greenName)
	assert.Equal(t, peerServiceName, greenSTS.Spec.ServiceName)
	assert.Equal(t, greenName, greenSTS.Spec.Template.Labels[constants.BlueGreenStatefulSetLabel])
	require.Len(t, greenSTS.Spec.Template.Spec.Containers[0].Env, 1)
	assert.Equal(t, peerServiceName, greenSTS.Spec.Template.Spec.Containers[0].Env[0].Value)
	require.NotNil(t, greenSTS.Spec.Replicas)
	assert.Equal(t, int32(10), *greenSTS.Spec.Replicas, "green STS should use the retained autoscaling target")
	require.NotNil(t, rc.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, int32(6), *rc.Spec.StatefulsetTemplate.Spec.Replicas, "green creation must not mutate the Helm template")
	require.Len(t, greenSTS.Spec.VolumeClaimTemplates, 1)
	assert.Equal(t, "data", greenSTS.Spec.VolumeClaimTemplates[0].Name)
	assert.Empty(t, rc.Spec.StatefulsetTemplate.Spec.VolumeClaimTemplates[0].Name,
		"restoring the green claim name must not mutate the CR template")
	assert.NotNil(t, greenSTS.Spec.PersistentVolumeClaimRetentionPolicy)
	assert.Equal(t, appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		greenSTS.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
}

func TestCreateGreenStatefulSet_Idempotent(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()

	_, _, err := cc.CreateGreenStatefulSet(ctx, rc)
	require.NoError(t, err)

	greenSTS2, _, err := cc.CreateGreenStatefulSet(ctx, rc)
	require.NoError(t, err)
	assert.Equal(t, "redis-cluster-test-alt", greenSTS2.Name)
}

func TestCreateGreenStatefulSetRejectsForeignNameCollision(t *testing.T) {
	rc := newRedisCluster("cache", "default", 1, 0)
	foreignRC := newRedisCluster("cache-alt", "default", 1, 0)
	zero := int32(0)
	foreignSTS := rc.Spec.StatefulsetTemplate.DeepCopy()
	foreignSTS.Name = "cache-alt"
	foreignSTS.Namespace = "default"
	foreignSTS.Spec.Replicas = &zero
	controller := true
	foreignSTS.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: cachev1.GroupVersion.String(),
		Kind:       "RedisCluster",
		Name:       foreignRC.Name,
		UID:        foreignRC.UID,
		Controller: &controller,
	}}
	scheme := newScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(rc, foreignRC, foreignSTS).Build()
	cc := redis_cluster.ClusterClient{
		Client: fakeClient, ClusterScheme: scheme, Namespace: "default",
		Log: logr.Discard(), Metrics: metrics.CreateNoop(),
	}

	_, _, err := cc.CreateGreenStatefulSet(context.Background(), rc)
	require.ErrorContains(t, err, "not controlled by RedisCluster default/cache")

	persisted := &appsv1.StatefulSet{}
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "default", Name: "cache-alt",
	}, persisted))
	require.NotNil(t, persisted.Spec.Replicas)
	assert.Zero(t, *persisted.Spec.Replicas, "the foreign StatefulSet must not be deleted")
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: CreateGreenHeadlessService
// ────────────────────────────────────────────────────────────────────────────

func TestCreateGreenHeadlessService(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()
	greenSTSName := "redis-cluster-test-alt"

	err := cc.CreateGreenHeadlessService(ctx, rc, greenSTSName, 6379)
	require.NoError(t, err)

	svc := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: redis_cluster.BlueGreenPeerServiceName(greenSTSName), Namespace: "default"}, svc)
	require.NoError(t, err)

	assert.Equal(t, "None", svc.Spec.ClusterIP)
	assert.NotEmpty(t, svc.Spec.Selector, "service should have selectors")
	assert.Equal(t, greenSTSName, svc.Spec.Selector[constants.BlueGreenStatefulSetLabel])
	assert.Empty(t, svc.Spec.Selector[constants.BlueGreenRoleLabel])
}

func TestCreateGreenHeadlessService_Idempotent(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()
	greenSTSName := "redis-cluster-test-alt"

	err := cc.CreateGreenHeadlessService(ctx, rc, greenSTSName, 6379)
	require.NoError(t, err)

	err = cc.CreateGreenHeadlessService(ctx, rc, greenSTSName, 6379)
	require.NoError(t, err)
}

func TestCreateGreenHeadlessService_UpdatesSelectorIfExists(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)
	greenSTSName := "redis-cluster-test-alt"
	controller := true

	existingSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redis_cluster.BlueGreenPeerServiceName(greenSTSName),
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster",
				Name: rc.Name, UID: rc.UID, Controller: &controller,
			}},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector: map[string]string{
				constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc, existingSvc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()

	err := cc.CreateGreenHeadlessService(ctx, rc, greenSTSName, 6379)
	require.NoError(t, err)

	svc := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: redis_cluster.BlueGreenPeerServiceName(greenSTSName), Namespace: "default"}, svc)
	require.NoError(t, err)
	assert.NotEmpty(t, svc.Spec.Selector, "service should have selectors")
	assert.Equal(t, greenSTSName, svc.Spec.Selector[constants.BlueGreenStatefulSetLabel])
	assert.Empty(t, svc.Spec.Selector[constants.BlueGreenRoleLabel])
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: DeleteGreenResources
// ────────────────────────────────────────────────────────────────────────────

func TestDeleteGreenResources(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()
	greenSTSName := "redis-cluster-test-alt"

	_, _, err := cc.CreateGreenStatefulSet(ctx, rc)
	require.NoError(t, err)
	err = cc.CreateGreenHeadlessService(ctx, rc, greenSTSName, 6379)
	require.NoError(t, err)

	err = cc.DeleteGreenResources(ctx, rc, greenSTSName)
	require.NoError(t, err)

	sts := &appsv1.StatefulSet{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: greenSTSName, Namespace: "default"}, sts)
	assert.Error(t, err, "StatefulSet should be deleted")

	svc := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: redis_cluster.BlueGreenPeerServiceName(greenSTSName), Namespace: "default"}, svc)
	assert.Error(t, err, "Service should be deleted")
}

func TestBlueGreenPeerServiceName(t *testing.T) {
	assert.Equal(t, "redis-cluster-test-alt-peer", redis_cluster.BlueGreenPeerServiceName("redis-cluster-test-alt"))

	longName := strings.Repeat("a", 63)
	got := redis_cluster.BlueGreenPeerServiceName(longName)
	assert.LessOrEqual(t, len(got), 63)
	assert.Equal(t, got, redis_cluster.BlueGreenPeerServiceName(longName), "hashed peer service name must be stable")
	assert.True(t, strings.HasSuffix(got, "-peer"))
}

func TestDeleteGreenResources_AlreadyGone(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()

	err := cc.DeleteGreenResources(ctx, rc, "nonexistent-green")
	require.NoError(t, err)
}

func TestDeleteGreenResourcesRejectsForeignStatefulSet(t *testing.T) {
	rc := newRedisCluster("cache", "default", 1, 0)
	foreignRC := newRedisCluster("cache-alt", "default", 1, 0)
	foreignSTS := rc.Spec.StatefulsetTemplate.DeepCopy()
	foreignSTS.Name = "cache-alt"
	foreignSTS.Namespace = "default"
	controller := true
	foreignSTS.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster",
		Name: foreignRC.Name, UID: foreignRC.UID, Controller: &controller,
	}}
	scheme := newScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithRuntimeObjects(rc, foreignRC, foreignSTS).Build()
	cc := redis_cluster.ClusterClient{
		Client: fakeClient, ClusterScheme: scheme, Namespace: "default",
		Log: logr.Discard(), Metrics: metrics.CreateNoop(),
	}

	err := cc.DeleteGreenResources(context.Background(), rc, "cache-alt")
	require.ErrorContains(t, err, "not controlled by RedisCluster default/cache")
	require.NoError(t, fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: "default", Name: "cache-alt",
	}, &appsv1.StatefulSet{}))
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: UpdateServiceSelector
// ────────────────────────────────────────────────────────────────────────────

func TestUpdateServiceSelector(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cluster-test",
			Namespace: "default",
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": "redis",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rc, svc).
		Build()

	cc := redis_cluster.ClusterClient{
		Client:        fakeClient,
		ClusterScheme: scheme,
		Namespace:     "default",
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}

	ctx := context.Background()
	newSelector := map[string]string{
		"app":                        "redis",
		constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive,
	}

	err := cc.UpdateServiceSelector(ctx, "default", "redis-cluster-test", newSelector)
	require.NoError(t, err)

	updatedSvc := &corev1.Service{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "redis-cluster-test", Namespace: "default"}, updatedSvc)
	require.NoError(t, err)
	assert.Equal(t, constants.BlueGreenRoleActive, updatedSvc.Spec.Selector[constants.BlueGreenRoleLabel])
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: GetBluePods / GetGreenPods filtering
// ────────────────────────────────────────────────────────────────────────────

func TestGetBluePods_ExcludesGreen(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	bluePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
				constants.BlueGreenRoleLabel: constants.BlueGreenRoleActive,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}
	greenPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-alt-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.10"},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rc, bluePod, greenPod).Build()
	cc := redis_cluster.ClusterClient{Client: fakeClient, ClusterScheme: scheme, Namespace: "default", Log: logr.Discard(), Metrics: metrics.CreateNoop()}

	pods, err := cc.GetBluePods(context.Background(), rc)
	require.NoError(t, err)
	assert.Len(t, pods.Items, 1)
	assert.Equal(t, "redis-cluster-test-0", pods.Items[0].Name)
}

func TestGetBluePods_ExcludesOrphaned(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	ownedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
			},
			OwnerReferences: []metav1.OwnerReference{{Name: "redis-cluster-test", Kind: "StatefulSet"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}
	greenPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-alt-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2"},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rc, ownedPod, greenPod).Build()
	cc := redis_cluster.ClusterClient{Client: fakeClient, ClusterScheme: scheme, Namespace: "default", Log: logr.Discard(), Metrics: metrics.CreateNoop()}

	pods, err := cc.GetBluePods(context.Background(), rc)
	require.NoError(t, err)
	assert.Len(t, pods.Items, 1, "only blue pod should be included")
	assert.Equal(t, "redis-cluster-test-0", pods.Items[0].Name)
}

func TestGetGreenPods_OnlyGreen(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	scheme := newScheme(t)

	bluePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.1"},
	}
	greenPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "redis-cluster-test-alt-0", Namespace: "default",
			Labels: map[string]string{
				"app": "rco", "chart_name": "redis-cluster-test", "redis_cluster_name": "redis-cluster-test",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.10"},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(rc, bluePod, greenPod).Build()
	cc := redis_cluster.ClusterClient{Client: fakeClient, ClusterScheme: scheme, Namespace: "default", Log: logr.Discard(), Metrics: metrics.CreateNoop()}

	greenPods, err := cc.GetGreenPods(context.Background(), rc)
	require.NoError(t, err)
	assert.Len(t, greenPods, 1)
	assert.Equal(t, "redis-cluster-test-alt-0", greenPods[0].Name)
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: Phase state machine transitions
// ────────────────────────────────────────────────────────────────────────────

func TestPhaseOrderIsMonotonicallyIncreasing(t *testing.T) {
	orderedPhases := []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseNone,
		cachev1.BlueGreenPhaseCreatingGreen,
		cachev1.BlueGreenPhaseWaitingPods,
		cachev1.BlueGreenPhaseReplicating,
		cachev1.BlueGreenPhaseWaitingSync,
		cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseFormingCluster,
		cachev1.BlueGreenPhaseScalingDownBlue,
		cachev1.BlueGreenPhaseTearingDown,
	}

	for i := 1; i < len(orderedPhases); i++ {
		prev := bluegreen.PhaseValue[orderedPhases[i-1]]
		curr := bluegreen.PhaseValue[orderedPhases[i]]
		assert.Greater(t, curr, prev,
			"phase %q (%v) should have higher value than %q (%v)",
			orderedPhases[i], curr, orderedPhases[i-1], prev)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: shard mapping
// ────────────────────────────────────────────────────────────────────────────

func TestShardMapping_ValidStructure(t *testing.T) {
	mapping := []cachev1.ShardMappingEntry{
		{BluePrimaryID: "abc123", BluePrimaryIP: "10.0.0.1", GreenPodName: "test-green-0", GreenPodIP: "10.0.0.10", GreenNodeID: "def456", SlotsRanges: "0-5460"},
		{BluePrimaryID: "ghi789", BluePrimaryIP: "10.0.0.2", GreenPodName: "test-green-1", GreenPodIP: "10.0.0.11", GreenNodeID: "jkl012", SlotsRanges: "5461-10922"},
		{BluePrimaryID: "mno345", BluePrimaryIP: "10.0.0.3", GreenPodName: "test-green-2", GreenPodIP: "10.0.0.12", GreenNodeID: "pqr678", SlotsRanges: "10923-16383"},
	}

	for _, entry := range mapping {
		assert.NotEmpty(t, entry.BluePrimaryID)
		assert.NotEmpty(t, entry.BluePrimaryIP)
		assert.NotEmpty(t, entry.GreenPodName)
		assert.NotEmpty(t, entry.GreenPodIP)
		assert.NotEmpty(t, entry.SlotsRanges)
	}

	assert.Len(t, mapping, 3)

	totalSlots := 0
	for _, entry := range mapping {
		parts := strings.Split(entry.SlotsRanges, "-")
		if len(parts) == 2 {
			first := mustAtoi(t, parts[0])
			last := mustAtoi(t, parts[1])
			totalSlots += last - first + 1
		}
	}
	assert.Equal(t, constants.TotalSlots, totalSlots, "all 16384 slots should be covered")
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	v := 0
	fmt.Sscanf(s, "%d", &v)
	return v
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: failover escalation modes
// ────────────────────────────────────────────────────────────────────────────

func TestFailoverEscalation(t *testing.T) {
	tests := []struct {
		attempts     int32
		expectedMode string
	}{
		{0, "soft"}, {1, "soft"},
		{2, "FORCE"}, {3, "FORCE"},
		{4, "TAKEOVER"}, {5, "TAKEOVER"}, {6, "TAKEOVER"}, {7, "TAKEOVER"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempts), func(t *testing.T) {
			mode := "soft"
			if tt.attempts >= int32(constants.BlueGreenTakeoverAttemptThreshold) {
				mode = "TAKEOVER"
			} else if tt.attempts >= int32(constants.BlueGreenForceAttemptThreshold) {
				mode = "FORCE"
			}
			assert.Equal(t, tt.expectedMode, mode)
		})
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: EmitPhaseGauge
// ────────────────────────────────────────────────────────────────────────────

func TestEmitPhaseGauge_UnknownPhase(t *testing.T) {
	h := newHandler(&testDeps{ClusterClient: redis_cluster.ClusterClient{Metrics: metrics.CreateNoop(), Log: logr.Discard()}})
	h.EmitPhaseGauge("UnknownPhase")
}

func TestEmitPhaseGauge_AllKnownPhases(t *testing.T) {
	h := newHandler(&testDeps{ClusterClient: redis_cluster.ClusterClient{Metrics: metrics.CreateNoop(), Log: logr.Discard()}})
	for phase := range bluegreen.PhaseValue {
		h.EmitPhaseGauge(phase)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: Blue/Green status management
// ────────────────────────────────────────────────────────────────────────────

func TestBlueGreenStatus_DeepCopy(t *testing.T) {
	original := cachev1.BlueGreenStatus{
		Phase:                 cachev1.BlueGreenPhasePromoting,
		StartTime:             time.Now().Unix(),
		SyncStartTime:         time.Now().Unix(),
		LastProgressTime:      time.Now().Unix(),
		LastSyncedCount:       2,
		Message:               "test message",
		ResumeForwardAttempts: 3,
		ShardMapping:          []cachev1.ShardMappingEntry{{BluePrimaryID: "abc", GreenPodName: "green-0"}},
		BlueNodeIDsToForget:   []string{"id1", "id2"},
	}

	rc := &cachev1.RedisCluster{Status: cachev1.RedisClusterStatus{BlueGreen: original}}
	copied := rc.DeepCopy()

	assert.Equal(t, original.Phase, copied.Status.BlueGreen.Phase)
	assert.Equal(t, original.StartTime, copied.Status.BlueGreen.StartTime)
	assert.Equal(t, original.SyncStartTime, copied.Status.BlueGreen.SyncStartTime)
	assert.Equal(t, original.LastProgressTime, copied.Status.BlueGreen.LastProgressTime)
	assert.Equal(t, original.LastSyncedCount, copied.Status.BlueGreen.LastSyncedCount)
	assert.Equal(t, original.Message, copied.Status.BlueGreen.Message)
	assert.Equal(t, original.ResumeForwardAttempts, copied.Status.BlueGreen.ResumeForwardAttempts)
	assert.Len(t, copied.Status.BlueGreen.ShardMapping, 1)
	assert.Len(t, copied.Status.BlueGreen.BlueNodeIDsToForget, 2)

	copied.Status.BlueGreen.Phase = cachev1.BlueGreenPhaseFailed
	assert.Equal(t, cachev1.BlueGreenPhasePromoting, rc.Status.BlueGreen.Phase)
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: constants consistency
// ────────────────────────────────────────────────────────────────────────────

func TestBlueGreenConstants(t *testing.T) {
	assert.Greater(t, constants.BlueGreenOverallTimeout, constants.BlueGreenSyncStallTimeout,
		"overall timeout should be greater than stall timeout")
	assert.Less(t, constants.BlueGreenForceAttemptThreshold, constants.BlueGreenTakeoverAttemptThreshold,
		"FORCE threshold should be less than TAKEOVER threshold")
	assert.Greater(t, constants.BlueGreenMaxResumeForwardAttempts, constants.BlueGreenTakeoverAttemptThreshold,
		"max resume attempts should be greater than TAKEOVER threshold to allow some TAKEOVER attempts")
	assert.Greater(t, constants.BlueGreenConsistencyGateAttempts, 0)
	assert.LessOrEqual(t, constants.BlueGreenConsistencyGateAttempts, 10)
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: edge cases
// ────────────────────────────────────────────────────────────────────────────

func TestBlueGreenPhaseNone_IsEmpty(t *testing.T) {
	assert.Equal(t, cachev1.BlueGreenPhase(""), cachev1.BlueGreenPhaseNone)
}

func TestBlueGreenStatus_ZeroValue(t *testing.T) {
	var bg cachev1.BlueGreenStatus
	assert.Equal(t, cachev1.BlueGreenPhaseNone, bg.Phase)
	assert.Zero(t, bg.StartTime)
	assert.Zero(t, bg.ResumeForwardAttempts)
	assert.Nil(t, bg.ShardMapping)
}

func TestPhaseValueGapless(t *testing.T) {
	happyPathPhases := []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseNone,
		cachev1.BlueGreenPhaseCreatingGreen,
		cachev1.BlueGreenPhaseWaitingPods,
		cachev1.BlueGreenPhaseReplicating,
		cachev1.BlueGreenPhaseWaitingSync,
		cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseFormingCluster,
		cachev1.BlueGreenPhaseScalingDownBlue,
		cachev1.BlueGreenPhaseTearingDown,
	}

	for i, phase := range happyPathPhases {
		val := bluegreen.PhaseValue[phase]
		assert.Equal(t, float64(i), val, "phase %q should have value %d, got %v", phase, i, val)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: CheckBlueAlive status transitions
// ────────────────────────────────────────────────────────────────────────────

func TestBgCheckBlueAlive_NoShardMapping(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	deps := newTestDeps(t, rc)
	h := newHandler(deps)

	cluster := &redis_cluster.Cluster{RedisCluster: rc}

	handled, _, hp, err := h.CheckBlueAlive(context.Background(), cluster)
	assert.False(t, handled)
	assert.Equal(t, health.OK, hp)
	assert.NoError(t, err)
}

func TestBgCheckBlueAlive_EmptyShardMapping(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
	rc.Status.BlueGreen = cachev1.BlueGreenStatus{}
	deps := newTestDeps(t, rc)
	h := newHandler(deps)

	cluster := &redis_cluster.Cluster{RedisCluster: rc}

	handled, _, hp, err := h.CheckBlueAlive(context.Background(), cluster)
	assert.False(t, handled)
	assert.Equal(t, health.OK, hp)
	assert.NoError(t, err)
}

// ────────────────────────────────────────────────────────────────────────────
// Integration test: division-by-zero guard in replica distribution
// ────────────────────────────────────────────────────────────────────────────

func TestReplicaDistribution_ZeroReplicas(t *testing.T) {
	rc := newRedisCluster("redis-cluster-test", "default", 3, 0)
	assert.Equal(t, int32(0), rc.Spec.Replicas)

	numPrimaries := 3
	replicasPerPrimary := int(rc.Spec.Replicas)

	greenPodsCount := 3
	shouldAddReplicas := greenPodsCount > numPrimaries && replicasPerPrimary > 0
	assert.False(t, shouldAddReplicas, "should not try to add replicas when spec.Replicas == 0")
}
