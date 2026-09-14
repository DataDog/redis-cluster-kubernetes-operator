package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func autoscalingInt32(value int32) *int32 { return &value }

func newAutoscalingScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))
	return scheme
}

func newAutoscalingReconciler(scheme *runtime.Scheme, k8sClient client.Client) *RedisClusterReconciler {
	return &RedisClusterReconciler{ClusterClient: redis_cluster.ClusterClient{
		Client:        k8sClient,
		ClusterScheme: scheme,
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}}
}

func validAutoscalingCluster() *cachev1.RedisCluster {
	labels := map[string]string{
		"app":                "rco",
		"chart_name":         "redis-cluster-test",
		"redis_cluster_name": "redis-cluster-test",
	}
	replicas := int32(6)
	return &cachev1.RedisCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "redis-cluster-test",
			Namespace:  "default",
			UID:        types.UID("redis-cluster-uid"),
			Generation: 7,
		},
		Spec: cachev1.RedisClusterSpec{
			Primaries: 3,
			Replicas:  1,
			Autoscaling: &cachev1.RedisClusterAutoscalingSpec{
				Mode:                   cachev1.AutoscalingModeScaleUp,
				MinPrimaries:           autoscalingInt32(3),
				MaxPrimaries:           autoscalingInt32(8),
				UpscaleCooldownSeconds: autoscalingInt32(600),
				CPU: &cachev1.RedisClusterAutoscalingMetricPolicy{
					HighWatermark:       resource.MustParse("800m"),
					UpscaleDelaySeconds: autoscalingInt32(30),
				},
				Memory: &cachev1.RedisClusterAutoscalingMetricPolicy{
					HighWatermark:       resource.MustParse("750m"),
					UpscaleDelaySeconds: autoscalingInt32(45),
				},
			},
			StatefulsetTemplate: appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-test", Namespace: "default", Labels: copyStringMap(labels)},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: copyStringMap(labels)}},
				},
			},
		},
		Status: cachev1.RedisClusterStatus{
			StatefulSetReady: true,
			Autoscaling:      cachev1.RedisClusterAutoscalingStatus{EffectivePrimaries: 3},
		},
	}
}

func copyStringMap(input map[string]string) map[string]string {
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func TestValidateAutoscalingPolicy(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*cachev1.RedisCluster)
		wantError string
		wantMode  cachev1.AutoscalingMode
	}{
		{
			name: "omitted policy is disabled",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling = nil
			},
			wantMode: cachev1.AutoscalingModeDisabled,
		},
		{
			name: "omitted mode is disabled",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling = &cachev1.RedisClusterAutoscalingSpec{}
			},
			wantMode: cachev1.AutoscalingModeDisabled,
		},
		{
			name: "unknown mode",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.Mode = "GrowFast"
			},
			wantError: "unsupported autoscaling mode",
		},
		{
			name: "missing minimum",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.MinPrimaries = nil
			},
			wantError: "minPrimaries is required",
		},
		{
			name: "cross-field bounds",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.MinPrimaries = autoscalingInt32(9)
			},
			wantError: "minPrimaries cannot exceed maxPrimaries",
		},
		{
			name: "missing cooldown",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.UpscaleCooldownSeconds = nil
			},
			wantError: "upscaleCooldownSeconds is required",
		},
		{
			name: "missing metrics",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.CPU = nil
				cluster.Spec.Autoscaling.Memory = nil
			},
			wantError: "at least one of cpu or memory",
		},
		{
			name: "zero watermark",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.CPU.HighWatermark = resource.Quantity{}
			},
			wantError: "cpu.highWatermark must be greater than zero",
		},
		{
			name: "missing metric delay",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.Memory.UpscaleDelaySeconds = nil
			},
			wantError: "memory.upscaleDelaySeconds is required",
		},
		{
			name: "recommend is valid",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.Mode = cachev1.AutoscalingModeRecommend
			},
			wantMode: cachev1.AutoscalingModeRecommend,
		},
		{
			name: "phase two mode is accepted",
			mutate: func(cluster *cachev1.RedisCluster) {
				cluster.Spec.Autoscaling.Mode = cachev1.AutoscalingModeScaleUpAndDown
			},
			wantMode: cachev1.AutoscalingModeScaleUpAndDown,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := validAutoscalingCluster()
			test.mutate(cluster)
			policy, err := validateAutoscalingPolicy(cluster)
			if test.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantMode, policy.mode)
		})
	}
}

func TestPrepareAutoscalingDoesNotExposePolicyValidationErrorInStatus(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	cluster.Spec.Autoscaling.MinPrimaries = nil
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	_, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)

	for _, conditionType := range []string{
		cachev1.AutoscalingConditionReady,
		cachev1.AutoscalingConditionScalingBlocked,
	} {
		condition := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, conditionType)
		require.NotNil(t, condition)
		assert.Equal(t, invalidAutoscalingPolicyStatusMessage, condition.Message)
		assert.NotContains(t, condition.Message, "minPrimaries")
	}
}

func TestCombineRecommendationsUsesHealthyMaximumAndClamps(t *testing.T) {
	cpu := int32(5)
	memory := int32(7)
	require.Equal(t, int32(7), *combineRecommendations(&cpu, &memory, 3, 10))
	require.Equal(t, int32(5), *combineRecommendations(&cpu, nil, 3, 10), "one degraded metric must not discard the healthy recommendation")
	cpu = 1
	require.Equal(t, int32(3), *combineRecommendations(&cpu, nil, 3, 10))
	memory = 20
	require.Equal(t, int32(10), *combineRecommendations(nil, &memory, 3, 10))
	assert.Nil(t, combineRecommendations(nil, nil, 3, 10))
}

func TestInitializeRetainedTargetUsesAlignedActiveTopology(t *testing.T) {
	scheme := newAutoscalingScheme(t)
	tests := []struct {
		name            string
		statefulSetPods *int32
		minimum         int32
		want            int32
	}{
		{name: "larger aligned topology", statefulSetPods: autoscalingInt32(10), minimum: 3, want: 5},
		{name: "unaligned topology ignored", statefulSetPods: autoscalingInt32(11), minimum: 3, want: 3},
		{name: "configured minimum does not resize bootstrap", minimum: 4, want: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := validAutoscalingCluster()
			cluster.Status.Autoscaling = cachev1.RedisClusterAutoscalingStatus{}
			cluster.Spec.Autoscaling.MinPrimaries = autoscalingInt32(test.minimum)
			objects := []client.Object{}
			if test.statefulSetPods != nil {
				objects = append(objects, &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace},
					Spec:       appsv1.StatefulSetSpec{Replicas: test.statefulSetPods},
				})
			}
			k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			reconciler := newAutoscalingReconciler(scheme, k8sClient)
			status := cachev1.RedisClusterAutoscalingStatus{}
			initialized, err := reconciler.initializeRetainedTarget(context.Background(), cluster, &status)
			require.NoError(t, err)
			assert.True(t, initialized)
			assert.Equal(t, test.want, status.EffectivePrimaries)
		})
	}
}

func TestPrepareAutoscalingRetainsHelmFloorAndAcknowledgesReset(t *testing.T) {
	tests := []struct {
		name          string
		specPrimaries int32
		retained      int32
		minimum       int32
		reset         bool
		want          int32
	}{
		{name: "Helm decrease is retained", specPrimaries: 2, retained: 6, minimum: 3, want: 6},
		{name: "Helm increase raises target", specPrimaries: 7, retained: 4, minimum: 3, want: 7},
		{name: "manual reset lowers to configured floor", specPrimaries: 2, retained: 6, minimum: 3, reset: true, want: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := newAutoscalingScheme(t)
			cluster := validAutoscalingCluster()
			cluster.Spec.Primaries = test.specPrimaries
			cluster.Status.Autoscaling.EffectivePrimaries = test.retained
			cluster.Spec.Autoscaling = &cachev1.RedisClusterAutoscalingSpec{
				Mode:         cachev1.AutoscalingModeDisabled,
				MinPrimaries: autoscalingInt32(test.minimum),
			}
			cluster.Annotations = map[string]string{"unrelated": "keep"}
			if test.reset {
				cluster.Annotations[constants.AutoscalingResetTarget] = "true"
			}
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(&cachev1.RedisCluster{}).
				Build()
			reconciler := newAutoscalingReconciler(scheme, k8sClient)

			_, err := reconciler.prepareAutoscaling(ctx, cluster)
			require.NoError(t, err)
			stored := &cachev1.RedisCluster{}
			require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored))
			assert.Equal(t, test.want, stored.Status.Autoscaling.EffectivePrimaries)
			assert.Equal(t, "keep", stored.Annotations["unrelated"])
			_, resetStillPresent := stored.Annotations[constants.AutoscalingResetTarget]
			assert.False(t, resetStillPresent, "the reset annotation must be acknowledged without deleting other annotations")
		})
	}
}

func TestPrepareAutoscalingDefersManualResetUntilReplicaMigrationIsRestored(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	redisCluster := validAutoscalingCluster()
	redisCluster.Spec.Primaries = 2
	redisCluster.Spec.Autoscaling = &cachev1.RedisClusterAutoscalingSpec{
		Mode:         cachev1.AutoscalingModeDisabled,
		MinPrimaries: autoscalingInt32(3),
	}
	redisCluster.Status.Autoscaling.EffectivePrimaries = 6
	redisCluster.Status.Autoscaling.ReplicaMigrationDisabled = true
	setAutoscalingCondition(
		&redisCluster.Status.Autoscaling,
		redisCluster.Generation,
		cachev1.AutoscalingConditionScaling,
		metav1.ConditionTrue,
		"RecommendationAccepted",
		"scaling",
	)
	redisCluster.Annotations = map[string]string{constants.AutoscalingResetTarget: "true"}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(redisCluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	preparation, err := reconciler.prepareAutoscaling(ctx, redisCluster)
	require.NoError(t, err)
	assert.True(t, preparation.manualResetPending)

	stored := &cachev1.RedisCluster{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(redisCluster), stored))
	assert.Equal(t, int32(6), stored.Status.Autoscaling.EffectivePrimaries)
	assert.True(t, stored.Status.Autoscaling.ReplicaMigrationDisabled)
	assert.Equal(t, "ManualTargetResetPendingReplicaMigrationRestore", stored.Status.Autoscaling.LastDecisionReason)
	scaling := apimeta.FindStatusCondition(stored.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScaling)
	require.NotNil(t, scaling)
	assert.Equal(t, metav1.ConditionTrue, scaling.Status)
	_, resetStillPresent := stored.Annotations[constants.AutoscalingResetTarget]
	assert.True(t, resetStillPresent)

	node := &redis_cluster.ClusterNode{IP: "10.0.0.1", Role: redis_cluster.PrimaryNode}
	cluster := &redis_cluster.Cluster{
		RedisCluster: stored,
		AllNodes:     []*redis_cluster.ClusterNode{node},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {node},
		},
	}
	restoreCalls := 0
	guard := newAutoscalingReplicaMigrationGuard(reconciler, logr.Discard())
	guard.setReplicaMigration = func(_ []*redis_cluster.ClusterNode, enabled bool) error {
		restoreCalls++
		assert.True(t, enabled)

		current := &cachev1.RedisCluster{}
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(redisCluster), current))
		assert.True(t, current.Status.Autoscaling.ReplicaMigrationDisabled,
			"the durable guard marker must remain set until Redis confirms restoration")
		condition := apimeta.FindStatusCondition(current.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScaling)
		require.NotNil(t, condition)
		assert.Equal(t, metav1.ConditionTrue, condition.Status,
			"manual reset must not clear Scaling before replica migration is restored")
		return nil
	}
	require.NoError(t, guard.restore(ctx, cluster))
	require.NoError(t, reconciler.completeManualAutoscalingReset(ctx, stored))
	assert.Equal(t, 1, restoreCalls)

	completed := &cachev1.RedisCluster{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(redisCluster), completed))
	assert.Equal(t, int32(3), completed.Status.Autoscaling.EffectivePrimaries)
	assert.False(t, completed.Status.Autoscaling.ReplicaMigrationDisabled)
	scaling = apimeta.FindStatusCondition(completed.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScaling)
	require.NotNil(t, scaling)
	assert.Equal(t, metav1.ConditionFalse, scaling.Status)
	_, resetStillPresent = completed.Annotations[constants.AutoscalingResetTarget]
	assert.False(t, resetStillPresent)
}

func TestReplicaMigrationGuardPersistsAcrossReconcilesAndRetriesRestore(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	redisCluster := validAutoscalingCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(redisCluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)
	node := &redis_cluster.ClusterNode{IP: "10.0.0.1", Role: redis_cluster.PrimaryNode}
	cluster := &redis_cluster.Cluster{
		RedisCluster: redisCluster,
		AllNodes:     []*redis_cluster.ClusterNode{node},
		Nodes: map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{
			redis_cluster.PrimaryNode: {node},
		},
	}

	disableGuard := newAutoscalingReplicaMigrationGuard(reconciler, logr.Discard())
	disableGuard.setReplicaMigration = func(_ []*redis_cluster.ClusterNode, enabled bool) error {
		assert.False(t, enabled)
		stored := &cachev1.RedisCluster{}
		require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(redisCluster), stored))
		assert.True(t, stored.Status.Autoscaling.ReplicaMigrationDisabled,
			"the guard marker must be durable before Redis migration is disabled")
		return nil
	}
	require.NoError(t, disableGuard.disable(ctx, cluster))
	assert.True(t, cluster.RedisCluster.Status.Autoscaling.ReplicaMigrationDisabled)

	restoreErr := errors.New("restore failed")
	restartedGuard := newAutoscalingReplicaMigrationGuard(reconciler, logr.Discard())
	restartedGuard.setReplicaMigration = func(_ []*redis_cluster.ClusterNode, enabled bool) error {
		assert.True(t, enabled)
		return restoreErr
	}
	require.ErrorIs(t, restartedGuard.restore(ctx, cluster), restoreErr)
	assert.True(t, cluster.RedisCluster.Status.Autoscaling.ReplicaMigrationDisabled,
		"a failed restore must retain the durable marker for the next reconcile")

	restartedGuard.setReplicaMigration = func(_ []*redis_cluster.ClusterNode, enabled bool) error {
		assert.True(t, enabled)
		return nil
	}
	require.NoError(t, restartedGuard.restore(ctx, cluster))
	assert.False(t, cluster.RedisCluster.Status.Autoscaling.ReplicaMigrationDisabled)
}

func TestPrepareAutoscalingWarnsWhenHelmTargetExceedsMaximum(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	cluster.Spec.Primaries = 9
	cluster.Status.Autoscaling.EffectivePrimaries = 4
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	assert.True(t, preparation.helmAboveMaximum)
	assert.Equal(t, int32(9), cluster.Status.Autoscaling.EffectivePrimaries)
	condition := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScalingBlocked)
	require.NotNil(t, condition)
	assert.Equal(t, "HelmTargetAboveMaximum", condition.Reason)
}

func healthyAutoscalingFixture(t *testing.T, primaries int32) (*RedisClusterReconciler, *redis_cluster.Cluster, client.Client) {
	return healthyAutoscalingFixtureWithReplicas(t, primaries, 1)
}

func healthyAutoscalingFixtureWithReplicas(t *testing.T, primaries, replicas int32) (*RedisClusterReconciler, *redis_cluster.Cluster, client.Client) {
	t.Helper()
	scheme := newAutoscalingScheme(t)
	redisCluster := validAutoscalingCluster()
	redisCluster.Status.Autoscaling.EffectivePrimaries = primaries
	redisCluster.Spec.Primaries = 3
	redisCluster.Spec.Replicas = replicas
	expectedPods := primaries * (1 + redisCluster.Spec.Replicas)
	redisCluster.Spec.StatefulsetTemplate.Spec.Replicas = autoscalingInt32(expectedPods)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: redisCluster.Name, Namespace: redisCluster.Namespace},
		Spec:       appsv1.StatefulSetSpec{Replicas: autoscalingInt32(expectedPods)},
		Status: appsv1.StatefulSetStatus{
			Replicas:          expectedPods,
			ReadyReplicas:     expectedPods,
			AvailableReplicas: expectedPods,
		},
	}
	objects := []client.Object{redisCluster, statefulSet}
	nodes := make([]*redis_cluster.ClusterNode, 0, expectedPods)
	grouped := map[redis_cluster.NodeRole][]*redis_cluster.ClusterNode{}
	for primaryIndex := int32(0); primaryIndex < primaries; primaryIndex++ {
		primaryID := fmt.Sprintf("primary-%d", primaryIndex)
		primary := &redis_cluster.ClusterNode{
			ID:          primaryID,
			PodName:     fmt.Sprintf("%s-%d", redisCluster.Name, primaryIndex),
			Role:        redis_cluster.PrimaryNode,
			SlotsRanges: []redis_cluster.SlotsRange{{First: int(primaryIndex), Last: int(primaryIndex)}},
			Info: redis_cluster.NodeInfo{
				ClusterStateIsOK: true,
				SlotsAssigned:    constants.TotalSlots,
			},
		}
		nodes = append(nodes, primary)
		grouped[redis_cluster.PrimaryNode] = append(grouped[redis_cluster.PrimaryNode], primary)
		for replicaIndex := int32(0); replicaIndex < redisCluster.Spec.Replicas; replicaIndex++ {
			podIndex := primaries + primaryIndex*redisCluster.Spec.Replicas + replicaIndex
			replica := &redis_cluster.ClusterNode{
				ID:        fmt.Sprintf("replica-%d-%d", primaryIndex, replicaIndex),
				PrimaryID: primaryID,
				PodName:   fmt.Sprintf("%s-%d", redisCluster.Name, podIndex),
				Role:      redis_cluster.ReplicaNode,
				Info: redis_cluster.NodeInfo{
					ClusterStateIsOK: true,
					SlotsAssigned:    constants.TotalSlots,
				},
			}
			nodes = append(nodes, replica)
			grouped[redis_cluster.ReplicaNode] = append(grouped[redis_cluster.ReplicaNode], replica)
		}
	}
	for podIndex := int32(0); podIndex < expectedPods; podIndex++ {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", redisCluster.Name, podIndex),
			Namespace: redisCluster.Namespace,
			Labels:    copyStringMap(redisCluster.Spec.StatefulsetTemplate.Labels),
		}, Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		}})
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)
	return reconciler, &redis_cluster.Cluster{
		RedisCluster: redisCluster,
		StatefulSet:  statefulSet,
		AllNodes:     nodes,
		Nodes:        grouped,
	}, k8sClient
}

func scaleUpPreparation(recommendation int32) autoscalingPreparation {
	return autoscalingPreparation{
		valid: true,
		policy: validatedAutoscalingPolicy{
			mode:            cachev1.AutoscalingModeScaleUp,
			minPrimaries:    3,
			maxPrimaries:    8,
			upscaleCooldown: 10 * time.Minute,
		},
		combined: autoscalingInt32(recommendation),
	}
}

func TestAutoscalingAcceptsExactlyOnePrimaryBeforeStatefulSetMutation(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixture(t, 3)
	statefulSetKey := client.ObjectKeyFromObject(cluster.StatefulSet)

	handled, requeue, err := reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), true, time.Unix(1000, 0))
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, constants.QuickRequeue, requeue)

	storedCluster := &cachev1.RedisCluster{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster.RedisCluster), storedCluster))
	assert.Equal(t, int32(4), storedCluster.Status.Autoscaling.EffectivePrimaries)
	assert.Equal(t, "RecommendationAccepted", storedCluster.Status.Autoscaling.LastDecisionReason)
	condition := apimeta.FindStatusCondition(storedCluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScaling)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)

	storedStatefulSet := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(ctx, statefulSetKey, storedStatefulSet))
	require.NotNil(t, storedStatefulSet.Spec.Replicas)
	assert.Equal(t, int32(6), *storedStatefulSet.Spec.Replicas,
		"the accepted target must be durable before the existing scaling path mutates the StatefulSet")
}

func TestAutoscalingStatusFailureDoesNotMutateStatefulSet(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, originalClient := healthyAutoscalingFixture(t, 3)
	scheme := reconciler.ClusterScheme
	storedCluster := &cachev1.RedisCluster{}
	require.NoError(t, originalClient.Get(ctx, client.ObjectKeyFromObject(cluster.RedisCluster), storedCluster))
	storedStatefulSet := &appsv1.StatefulSet{}
	require.NoError(t, originalClient.Get(ctx, client.ObjectKeyFromObject(cluster.StatefulSet), storedStatefulSet))
	pods := &corev1.PodList{}
	require.NoError(t, originalClient.List(ctx, pods, client.InNamespace(cluster.RedisCluster.Namespace)))
	objects := []client.Object{storedCluster, storedStatefulSet}
	for index := range pods.Items {
		objects = append(objects, &pods.Items[index])
	}
	statusErr := errors.New("status write failed")
	statusCalls := 0
	failingClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		WithInterceptorFuncs(interceptStatusUpdates([]error{statusErr}, &statusCalls)).
		Build()
	reconciler.Client = failingClient

	handled, _, err := reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), true, time.Unix(1000, 0))
	assert.True(t, handled)
	require.ErrorIs(t, err, statusErr)
	assert.Equal(t, 1, statusCalls)
	unchanged := &appsv1.StatefulSet{}
	require.NoError(t, failingClient.Get(ctx, client.ObjectKeyFromObject(storedStatefulSet), unchanged))
	assert.Equal(t, int32(6), *unchanged.Spec.Replicas)
}

func TestAutoscalingCompletionStartsCooldownAndSchedulesWakeup(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, _ := healthyAutoscalingFixture(t, 4)
	started := metav1.NewTime(time.Unix(500, 0))
	cluster.RedisCluster.Status.Autoscaling.ScaleStartedAt = &started
	setAutoscalingCondition(&cluster.RedisCluster.Status.Autoscaling, cluster.RedisCluster.Generation,
		cachev1.AutoscalingConditionScaling, metav1.ConditionTrue, "RecommendationAccepted", "scaling")
	now := time.Unix(1000, 0)

	handled, requeue, err := reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), true, now)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, constants.QuickRequeue, requeue)
	assert.Equal(t, "ScaleUpCompleted", cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
	require.NotNil(t, cluster.RedisCluster.Status.Autoscaling.ScaleCompletedAt)
	assert.Equal(t, now, cluster.RedisCluster.Status.Autoscaling.ScaleCompletedAt.Time)

	handled, requeue, err = reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), true, now.Add(time.Minute))
	require.NoError(t, err)
	assert.True(t, handled, "cooldown must schedule its own wake-up")
	assert.Equal(t, 9*time.Minute, requeue)
	assert.Equal(t, "UpscaleCooldown", cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
	assert.Equal(t, int32(4), cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries)
}

func TestAutoscalingModeSemantics(t *testing.T) {
	tests := []struct {
		name           string
		mode           cachev1.AutoscalingMode
		recommendation int32
		wantReason     string
	}{
		{name: "recommend never changes target", mode: cachev1.AutoscalingModeRecommend, recommendation: 8, wantReason: "RecommendationOnly"},
		{name: "ScaleUpAndDown is up-only in phase one", mode: cachev1.AutoscalingModeScaleUpAndDown, recommendation: 3, wantReason: "DownscaleUnavailablePhase2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconciler, cluster, _ := healthyAutoscalingFixture(t, 4)
			preparation := scaleUpPreparation(test.recommendation)
			preparation.policy.mode = test.mode
			handled, _, err := reconciler.reconcileAutoscalingAfterMaintenance(context.Background(), cluster, preparation, true, time.Unix(1000, 0))
			require.NoError(t, err)
			assert.False(t, handled)
			assert.Equal(t, int32(4), cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries)
			assert.Equal(t, test.wantReason, cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
		})
	}
}

func TestAutoscalingSafetyGates(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(context.Context, *redis_cluster.Cluster, client.Client)
		wantReason string
	}{
		{
			name: "blue green",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.RedisCluster.Status.BlueGreen.Phase = cachev1.BlueGreenPhaseCreatingGreen
			},
			wantReason: "blue/green",
		},
		{
			name: "rollout",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.RedisCluster.Status.RolloutInProgress = true
			},
			wantReason: "rollout",
		},
		{
			name: "StatefulSet desired size",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.StatefulSet.Spec.Replicas = autoscalingInt32(5)
			},
			wantReason: "desired replicas",
		},
		{
			name: "StatefulSet readiness",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.StatefulSet.Status.ReadyReplicas--
			},
			wantReason: "ready replicas",
		},
		{
			name: "node topology",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.AllNodes = cluster.AllNodes[:len(cluster.AllNodes)-1]
			},
			wantReason: "node count",
		},
		{
			name: "open slots",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.AllNodes[0].OpenSlots = []redis_cluster.OpenSlot{{Slot: 1, State: redis_cluster.OpenSlotMigrating}}
			},
			wantReason: "open or migrating slots",
		},
		{
			name: "draining node",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.AllNodes[0].IsTerminating = true
			},
			wantReason: "draining or terminating",
		},
		{
			name: "unhealthy slots",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.AllNodes[0].Info.SlotsAssigned--
			},
			wantReason: "full healthy slot coverage",
		},
		{
			name: "replica attachment",
			mutate: func(_ context.Context, cluster *redis_cluster.Cluster, _ client.Client) {
				cluster.Nodes[redis_cluster.ReplicaNode][0].PrimaryID = "missing-primary"
			},
			wantReason: "not attached",
		},
		{
			name: "pod readiness",
			mutate: func(ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				pod := &corev1.Pod{}
				require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: cluster.RedisCluster.Name + "-0"}, pod))
				pod.Status.Conditions = nil
				require.NoError(t, k8sClient.Status().Update(ctx, pod))
			},
			wantReason: "not Running and Ready",
		},
		{
			name: "NLA state",
			mutate: func(ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				pod := &corev1.Pod{}
				require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: cluster.RedisCluster.Name + "-0"}, pod))
				pod.Annotations = map[string]string{constants.NLAAnnotation: constants.NLAProcessing}
				require.NoError(t, k8sClient.Update(ctx, pod))
			},
			wantReason: "NLA state",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			reconciler, cluster, k8sClient := healthyAutoscalingFixture(t, 3)
			if test.name == "NLA state" {
				reconciler.LifecycleAdapter = DatadogNLAAdapter{}
			}
			test.mutate(ctx, cluster, k8sClient)
			blocker := reconciler.autoscalingSafetyBlocker(ctx, cluster, true)
			assert.True(t, strings.Contains(strings.ToLower(blocker), strings.ToLower(test.wantReason)),
				"blocker %q should contain %q", blocker, test.wantReason)
		})
	}
}

func TestAutoscalingDoesNotAcceptRecommendationUntilNLAConverges(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, _ := healthyAutoscalingFixture(t, 3)

	handled, _, err := reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), false, time.Unix(1000, 0))
	require.NoError(t, err)
	assert.False(t, handled)
	assert.Equal(t, int32(3), cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries)
	assert.Equal(t, "SafetyGateBlocked", cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
	condition := apimeta.FindStatusCondition(cluster.RedisCluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScalingBlocked)
	require.NotNil(t, condition)
	assert.Contains(t, condition.Message, "NLA processing has not converged")
}

func TestAutoscalingAllowsAcknowledgedNLAReplicaWithRedundancy(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
	reconciler.LifecycleAdapter = DatadogNLAAdapter{}
	candidate := cluster.Nodes[redis_cluster.ReplicaNode][0]
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, candidate, "done:100", time.Unix(100, 0))

	handled, requeue, err := reconciler.reconcileAutoscalingAfterMaintenance(ctx, cluster, scaleUpPreparation(6), true, time.Unix(1000, 0))
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, constants.QuickRequeue, requeue)
	assert.Equal(t, int32(4), cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries)
	assert.Equal(t, "RecommendationAccepted", cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
}

func TestAutoscalingExpandsBeforeRepairingAcknowledgedNLAReplicaRedundancy(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
	reconciler.LifecycleAdapter = DatadogNLAAdapter{}
	replicas := cluster.Nodes[redis_cluster.ReplicaNode]
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[0], "done:100", time.Unix(100, 0))
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[1], "done:100", time.Unix(100, 0))

	fullBlocker := reconciler.autoscalingSafetyBlocker(ctx, cluster, true)
	require.Contains(t, fullBlocker, "no healthy non-draining replica",
		"final convergence must still require repaired redundancy")
	require.Empty(t, reconciler.autoscalingExpansionSafetyBlocker(ctx, cluster, true),
		"StatefulSet expansion must be allowed to create the repair capacity")

	handled, requeue, err := reconciler.reconcileAutoscalingAfterMaintenance(
		ctx,
		cluster,
		scaleUpPreparation(6),
		true,
		time.Unix(1000, 0),
	)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, constants.QuickRequeue, requeue)
	assert.Equal(t, int32(4), cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries)
	assert.Equal(t, "RecommendationAccepted", cluster.RedisCluster.Status.Autoscaling.LastDecisionReason)
}

func TestNLAReplicaRepairTargetAndPrimaryOutsiderSelection(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
	reconciler.LifecycleAdapter = DatadogNLAAdapter{}
	replicas := cluster.Nodes[redis_cluster.ReplicaNode]
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[0], "done:100", time.Unix(100, 0))
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[1], "done:100", time.Unix(100, 0))

	pods := &corev1.PodList{}
	require.NoError(t, k8sClient.List(ctx, pods, client.InNamespace(cluster.RedisCluster.Namespace)))
	podsByName := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		podsByName[pods.Items[i].Name] = &pods.Items[i]
	}
	require.Same(t, cluster.Nodes[redis_cluster.PrimaryNode][0], reconciler.primaryNeedingNLAReplicaRepair(cluster, podsByName))

	setAutoscalingCondition(
		&cluster.RedisCluster.Status.Autoscaling,
		cluster.RedisCluster.Generation,
		cachev1.AutoscalingConditionScaling,
		metav1.ConditionTrue,
		"RecommendationAccepted",
		"scaling",
	)
	cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries = 4
	oldDrainOutsider := &redis_cluster.ClusterNode{ID: "old-drain", IP: "10.0.0.1", PodIndex: 1, HasDrainCandidateCond: true}
	newOutsiders := []*redis_cluster.ClusterNode{
		{ID: "new-0", IP: "10.0.0.9", PodIndex: 9},
		{ID: "new-1", IP: "10.0.0.10", PodIndex: 10},
		{ID: "new-2", IP: "10.0.0.11", PodIndex: 11},
	}
	cluster.Nodes[redis_cluster.OutsiderNode] = append([]*redis_cluster.ClusterNode{oldDrainOutsider}, newOutsiders...)

	assert.Equal(t, newOutsiders, nonDrainingOutsidersAtOrAboveOrdinal(cluster, 9))
	assert.Same(t, newOutsiders[0], nonDrainingPrimaryOutsider(cluster),
		"a drain candidate must never be selected as the new primary")
	newOutsiders[0].HasDrainCandidateCond = true
	assert.Same(t, newOutsiders[1], nonDrainingPrimaryOutsider(cluster))
}

func TestNLAReplicaRepairReservesCleanPrimaryGroupCapacity(t *testing.T) {
	ctx := context.Background()
	reconciler, cluster, k8sClient := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
	reconciler.LifecycleAdapter = DatadogNLAAdapter{}
	replicas := cluster.Nodes[redis_cluster.ReplicaNode]
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[0], "done:100", time.Unix(100, 0))
	markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[1], "done:100", time.Unix(100, 0))
	setAutoscalingCondition(
		&cluster.RedisCluster.Status.Autoscaling,
		cluster.RedisCluster.Generation,
		cachev1.AutoscalingConditionScaling,
		metav1.ConditionTrue,
		"RecommendationAccepted",
		"scaling",
	)
	cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries = 4
	cluster.Nodes[redis_cluster.OutsiderNode] = []*redis_cluster.ClusterNode{
		{ID: "new-primary", IP: "10.0.0.9", PodIndex: 9},
		{ID: "new-replica", IP: "10.0.0.10", PodIndex: 10},
	}

	returned, repaired, err := reconciler.repairAutoscalingNLAReplicaRedundancy(ctx, cluster, logr.Discard())
	require.ErrorIs(t, err, errWaitingForAutoscalingCapacity)
	assert.Same(t, cluster, returned)
	assert.False(t, repaired)
}

func TestNLAReplicaRepairCapacityTracksPromotionStage(t *testing.T) {
	_, cluster, _ := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
	cluster.RedisCluster.Status.Autoscaling.EffectivePrimaries = 4

	remaining, required, enough := cleanOutsiderCapacityAfterNLARepair(cluster, 2)
	assert.Equal(t, 1, remaining)
	assert.Equal(t, 2, required, "before promotion one clean pod is still needed for the primary and one for its missing replica")
	assert.False(t, enough)

	remaining, required, enough = cleanOutsiderCapacityAfterNLARepair(cluster, 3)
	assert.Equal(t, 2, remaining)
	assert.Equal(t, 2, required)
	assert.True(t, enough)

	newPrimary := &redis_cluster.ClusterNode{
		ID:          "primary-3",
		PodName:     "redis-cluster-test-9",
		Role:        redis_cluster.PrimaryNode,
		SlotsRanges: []redis_cluster.SlotsRange{{First: 3, Last: 3}},
	}
	cluster.Nodes[redis_cluster.PrimaryNode] = append(cluster.Nodes[redis_cluster.PrimaryNode], newPrimary)

	remaining, required, enough = cleanOutsiderCapacityAfterNLARepair(cluster, 2)
	assert.Equal(t, 1, remaining)
	assert.Equal(t, 1, required, "after promotion only the final missing replica still needs a clean outsider")
	assert.True(t, enough, "the two-outsider post-promotion state observed in staging must be repairable")

	remaining, required, enough = cleanOutsiderCapacityAfterNLARepair(cluster, 1)
	assert.Equal(t, 0, remaining)
	assert.Equal(t, 1, required)
	assert.False(t, enough)
}

func TestAutoscalingBlocksUnsafeNLAStates(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*testing.T, context.Context, *redis_cluster.Cluster, client.Client)
		wantReason string
	}{
		{
			name: "pending",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, cluster.Nodes[redis_cluster.ReplicaNode][0], "", time.Unix(100, 0))
			},
			wantReason: "active NLA state pending",
		},
		{
			name: "processing",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, cluster.Nodes[redis_cluster.ReplicaNode][0], constants.NLAProcessing, time.Unix(100, 0))
			},
			wantReason: "NLA state processing",
		},
		{
			name: "failed",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, cluster.Nodes[redis_cluster.ReplicaNode][0], constants.NLAFailed, time.Unix(100, 0))
			},
			wantReason: "NLA state failed",
		},
		{
			name: "retriggered",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, cluster.Nodes[redis_cluster.ReplicaNode][0], "done:100", time.Unix(101, 0))
			},
			wantReason: "retriggered NLA",
		},
		{
			name: "still primary",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, cluster.Nodes[redis_cluster.PrimaryNode][0], "done:100", time.Unix(100, 0))
			},
			wantReason: "still a primary",
		},
		{
			name: "terminating",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				candidate := cluster.Nodes[redis_cluster.ReplicaNode][0]
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, candidate, "done:100", time.Unix(100, 0))
				candidate.IsTerminating = true
			},
			wantReason: "draining or terminating",
		},
		{
			name: "no non-draining redundancy",
			mutate: func(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client) {
				replicas := cluster.Nodes[redis_cluster.ReplicaNode]
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[0], "done:100", time.Unix(100, 0))
				markAutoscalingNLACandidate(t, ctx, cluster, k8sClient, replicas[1], "done:100", time.Unix(100, 0))
			},
			wantReason: "no healthy non-draining replica",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			reconciler, cluster, k8sClient := healthyAutoscalingFixtureWithReplicas(t, 3, 2)
			reconciler.LifecycleAdapter = DatadogNLAAdapter{}
			test.mutate(t, ctx, cluster, k8sClient)
			blocker := reconciler.autoscalingSafetyBlocker(ctx, cluster, true)
			assert.Contains(t, strings.ToLower(blocker), strings.ToLower(test.wantReason), "blocker: %q", blocker)
			expansionBlocker := reconciler.autoscalingExpansionSafetyBlocker(ctx, cluster, true)
			if test.name == "no non-draining redundancy" {
				assert.Empty(t, expansionBlocker,
					"only acknowledged missing redundancy may be repaired after StatefulSet expansion")
			} else {
				assert.Contains(t, strings.ToLower(expansionBlocker), strings.ToLower(test.wantReason),
					"expansion blocker: %q", expansionBlocker)
			}
		})
	}
}

func markAutoscalingNLACandidate(t *testing.T, ctx context.Context, cluster *redis_cluster.Cluster, k8sClient client.Client, node *redis_cluster.ClusterNode, annotation string, transitionTime time.Time) {
	t.Helper()
	node.HasDrainCandidateCond = true
	pod := &corev1.Pod{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: node.PodName}, pod))
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[constants.NLAAnnotation] = annotation
	require.NoError(t, k8sClient.Update(ctx, pod))
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: node.PodName}, pod))
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodConditionType(constants.NLAConditionType),
		Status:             corev1.ConditionTrue,
		Reason:             constants.NLAConditionReason,
		LastTransitionTime: metav1.NewTime(transitionTime),
	})
	require.NoError(t, k8sClient.Status().Update(ctx, pod))
}
