package controller

import (
	"context"
	"errors"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type recordingAutoscalingProvider struct {
	recommendations AutoscalingRecommendations
	err             error
	calls           int
	cleanupCalls    int
	watchCalls      int
	watchErr        error
}

func (p *recordingAutoscalingProvider) AddWatches(_ ctrl.Manager, _ *builder.Builder) error {
	p.watchCalls++
	return p.watchErr
}

func (p *recordingAutoscalingProvider) Recommendations(context.Context, *cachev1.RedisCluster) (AutoscalingRecommendations, error) {
	p.calls++
	return p.recommendations, p.err
}

func (p *recordingAutoscalingProvider) Cleanup(context.Context, *cachev1.RedisCluster) error {
	p.cleanupCalls++
	return nil
}

func TestPrepareAutoscalingWithoutProviderReportsStableUnavailableStatus(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	assert.False(t, preparation.valid)
	assert.Nil(t, preparation.combined)

	stored := &cachev1.RedisCluster{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), stored))
	assert.Equal(t, providerUnavailableReason, stored.Status.Autoscaling.LastDecisionReason)
	expectedConditions := map[string]metav1.ConditionStatus{
		cachev1.AutoscalingConditionReady:                   metav1.ConditionFalse,
		cachev1.AutoscalingConditionRecommendationAvailable: metav1.ConditionFalse,
		cachev1.AutoscalingConditionMetricsDegraded:         metav1.ConditionTrue,
		cachev1.AutoscalingConditionScalingBlocked:          metav1.ConditionTrue,
	}
	for conditionType, conditionStatus := range expectedConditions {
		condition := apimeta.FindStatusCondition(stored.Status.Autoscaling.Conditions, conditionType)
		require.NotNil(t, condition)
		assert.Equal(t, providerUnavailableReason, condition.Reason)
		assert.Equal(t, conditionStatus, condition.Status)
		assert.NotEmpty(t, condition.Message)
	}

	first := stored.Status.Autoscaling.DeepCopy()
	_, err = reconciler.prepareAutoscaling(ctx, stored)
	require.NoError(t, err)
	assert.Equal(t, *first, stored.Status.Autoscaling, "unavailable-provider status must be stable across reconciles")
}

func TestUnavailableProviderDoesNotPreventStatefulSetReconciliation(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	_, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	statefulSet, err := reconciler.GetOrCreateStatefulSet(ctx, cluster)
	require.NoError(t, err)
	require.NotNil(t, statefulSet)

	stored := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(statefulSet), stored))
	assert.Equal(t, cluster.EffectivePodCount(), *stored.Spec.Replicas)
}

func TestAutoscalingProviderWatchRegistrationIsOptionalAndDelegated(t *testing.T) {
	controllerBuilder := &builder.Builder{}
	reconciler := &RedisClustersReconciler{}

	got, err := reconciler.addAutoscalingProviderWatches(nil, controllerBuilder)
	require.NoError(t, err)
	assert.Same(t, controllerBuilder, got)

	provider := &recordingAutoscalingProvider{}
	reconciler.AutoscalingProvider = provider
	got, err = reconciler.addAutoscalingProviderWatches(nil, controllerBuilder)
	require.NoError(t, err)
	assert.Same(t, controllerBuilder, got)
	assert.Equal(t, 1, provider.watchCalls)

	provider.watchErr = errors.New("watch registration failed")
	got, err = reconciler.addAutoscalingProviderWatches(nil, controllerBuilder)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "configure autoscaling provider watches")
}

func TestPrepareAutoscalingUsesProviderRecommendations(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	cpu, memory := int32(5), int32(7)
	provider := &recordingAutoscalingProvider{recommendations: AutoscalingRecommendations{
		CPU:    AutoscalingMetricRecommendation{Recommendation: &cpu},
		Memory: AutoscalingMetricRecommendation{Recommendation: &memory},
	}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)
	reconciler.AutoscalingProvider = provider

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	assert.True(t, preparation.valid)
	require.NotNil(t, preparation.combined)
	assert.Equal(t, int32(7), *preparation.combined)
	assert.Equal(t, 1, provider.calls)

	ready := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionReady)
	require.NotNil(t, ready)
	assert.Equal(t, metav1.ConditionTrue, ready.Status)
}

func TestPrepareAutoscalingIgnoresDegradedProviderRecommendation(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	cpu, staleMemory := int32(5), int32(8)
	provider := &recordingAutoscalingProvider{recommendations: AutoscalingRecommendations{
		CPU:    AutoscalingMetricRecommendation{Recommendation: &cpu},
		Memory: AutoscalingMetricRecommendation{Recommendation: &staleMemory, Degraded: true},
	}}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)
	reconciler.AutoscalingProvider = provider

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	require.NotNil(t, preparation.combined)
	assert.Equal(t, int32(5), *preparation.combined)
	assert.True(t, preparation.metricsDegraded)
	assert.Nil(t, cluster.Status.Autoscaling.MemoryRecommendation)

	condition := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionMetricsDegraded)
	require.NotNil(t, condition)
	assert.Equal(t, metav1.ConditionTrue, condition.Status)
	assert.Equal(t, "RecommendationDegraded", condition.Reason)
}

func TestPrepareAutoscalingProviderFailureIsStatusNotReconcileError(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	provider := &recordingAutoscalingProvider{err: errors.New("provider transport failed")}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)
	reconciler.AutoscalingProvider = provider

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	assert.False(t, preparation.valid)
	assert.Equal(t, providerErrorReason, cluster.Status.Autoscaling.LastDecisionReason)
	assert.NotContains(t, apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionReady).Message, "transport")
}

func TestPrepareAutoscalingClearsProviderOutageStatusAfterRecovery(t *testing.T) {
	for _, test := range []struct {
		name            string
		initialProvider AutoscalingProvider
	}{
		{name: "unavailable", initialProvider: nil},
		{name: "error", initialProvider: &recordingAutoscalingProvider{err: errors.New("provider failed")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := newAutoscalingScheme(t)
			cluster := validAutoscalingCluster()
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(cluster).
				WithStatusSubresource(&cachev1.RedisCluster{}).
				Build()
			reconciler := newAutoscalingReconciler(scheme, k8sClient)
			reconciler.AutoscalingProvider = test.initialProvider

			_, err := reconciler.prepareAutoscaling(ctx, cluster)
			require.NoError(t, err)

			recommendation := int32(5)
			reconciler.AutoscalingProvider = &recordingAutoscalingProvider{recommendations: AutoscalingRecommendations{
				CPU: AutoscalingMetricRecommendation{Recommendation: &recommendation},
			}}
			_, err = reconciler.prepareAutoscaling(ctx, cluster)
			require.NoError(t, err)

			assert.Equal(t, "ProviderAvailable", cluster.Status.Autoscaling.LastDecisionReason)
			blocked := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionScalingBlocked)
			require.NotNil(t, blocked)
			assert.Equal(t, metav1.ConditionFalse, blocked.Status)
			assert.Equal(t, "ProviderAvailable", blocked.Reason)
			ready := apimeta.FindStatusCondition(cluster.Status.Autoscaling.Conditions, cachev1.AutoscalingConditionReady)
			require.NotNil(t, ready)
			assert.Equal(t, metav1.ConditionTrue, ready.Status)
		})
	}
}

func TestDisabledAutoscalingDoesNotRequireProvider(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)
	cluster := validAutoscalingCluster()
	cluster.Spec.Autoscaling = nil
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cluster).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	preparation, err := reconciler.prepareAutoscaling(ctx, cluster)
	require.NoError(t, err)
	assert.True(t, preparation.valid)
	assert.Equal(t, "AutoscalingDisabled", cluster.Status.Autoscaling.LastDecisionReason)
}
