package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type autoscalingMetric string

const (
	autoscalingMetricCPU    autoscalingMetric = "cpu"
	autoscalingMetricMemory autoscalingMetric = "memory"

	invalidAutoscalingPolicyStatusMessage = "Autoscaling policy validation failed; inspect operator logs for details"
	providerUnavailableReason             = "ProviderUnavailable"
	providerErrorReason                   = "ProviderError"
)

type validatedAutoscalingPolicy struct {
	mode            cachev1.AutoscalingMode
	minPrimaries    int32
	maxPrimaries    int32
	upscaleCooldown time.Duration
	cpu             *cachev1.RedisClusterAutoscalingMetricPolicy
	memory          *cachev1.RedisClusterAutoscalingMetricPolicy
}

type autoscalingPreparation struct {
	policy             validatedAutoscalingPolicy
	valid              bool
	combined           *int32
	cpu                *int32
	memory             *int32
	metricsDegraded    bool
	helmAboveMaximum   bool
	manualResetPending bool
}

type autoscalingMetricResult struct {
	recommendation *int32
	degraded       bool
	reason         string
}

// AutoscalingProvider acquires recommendations without coupling the core
// controller to a particular metrics API or autoscaling CRD. Implementations
// own any provider-specific resources and must not put credentials or backend
// details in returned errors.
type AutoscalingProvider interface {
	Recommendations(context.Context, *cachev1.RedisCluster) (AutoscalingRecommendations, error)
}

// AutoscalingCleanupProvider optionally removes resources owned by a provider
// when autoscaling is disabled. Cleanup may run on every disabled reconcile and
// must therefore be idempotent.
type AutoscalingCleanupProvider interface {
	Cleanup(context.Context, *cachev1.RedisCluster) error
}

// AutoscalingMetricRecommendation is one provider's current primary-count
// recommendation. A nil recommendation means no current value is available.
type AutoscalingMetricRecommendation struct {
	Recommendation *int32
	Degraded       bool
}

// AutoscalingRecommendations contains the bounded recommendation set supported
// by the existing RedisCluster API contract.
type AutoscalingRecommendations struct {
	CPU    AutoscalingMetricRecommendation
	Memory AutoscalingMetricRecommendation
}

func validateAutoscalingPolicy(redisCluster *cachev1.RedisCluster) (validatedAutoscalingPolicy, error) {
	mode := redisCluster.EffectiveAutoscalingMode()
	policy := validatedAutoscalingPolicy{mode: mode}
	if mode == cachev1.AutoscalingModeDisabled {
		return policy, nil
	}

	spec := redisCluster.Spec.Autoscaling
	if spec == nil {
		return policy, fmt.Errorf("autoscaling configuration is missing")
	}
	switch mode {
	case cachev1.AutoscalingModeRecommend, cachev1.AutoscalingModeScaleUp, cachev1.AutoscalingModeScaleUpAndDown:
	default:
		return policy, fmt.Errorf("unsupported autoscaling mode %q", mode)
	}
	if spec.MinPrimaries == nil {
		return policy, fmt.Errorf("minPrimaries is required")
	}
	if *spec.MinPrimaries < 1 {
		return policy, fmt.Errorf("minPrimaries must be at least 1")
	}
	if spec.MaxPrimaries == nil {
		return policy, fmt.Errorf("maxPrimaries is required")
	}
	if *spec.MaxPrimaries < 1 {
		return policy, fmt.Errorf("maxPrimaries must be at least 1")
	}
	if *spec.MinPrimaries > *spec.MaxPrimaries {
		return policy, fmt.Errorf("minPrimaries cannot exceed maxPrimaries")
	}
	if spec.UpscaleCooldownSeconds == nil {
		return policy, fmt.Errorf("upscaleCooldownSeconds is required")
	}
	if *spec.UpscaleCooldownSeconds < 0 {
		return policy, fmt.Errorf("upscaleCooldownSeconds cannot be negative")
	}
	if spec.CPU == nil && spec.Memory == nil {
		return policy, fmt.Errorf("at least one of cpu or memory must be configured")
	}
	if err := validateAutoscalingMetricPolicy(autoscalingMetricCPU, spec.CPU); err != nil {
		return policy, err
	}
	if err := validateAutoscalingMetricPolicy(autoscalingMetricMemory, spec.Memory); err != nil {
		return policy, err
	}
	if redisCluster.Spec.Replicas < 0 {
		return policy, fmt.Errorf("replicas cannot be negative")
	}

	policy.minPrimaries = *spec.MinPrimaries
	policy.maxPrimaries = *spec.MaxPrimaries
	policy.upscaleCooldown = time.Duration(*spec.UpscaleCooldownSeconds) * time.Second
	policy.cpu = spec.CPU
	policy.memory = spec.Memory
	return policy, nil
}

func validateAutoscalingMetricPolicy(metric autoscalingMetric, policy *cachev1.RedisClusterAutoscalingMetricPolicy) error {
	if policy == nil {
		return nil
	}
	if policy.HighWatermark.Sign() <= 0 {
		return fmt.Errorf("%s.highWatermark must be greater than zero", metric)
	}
	if policy.UpscaleDelaySeconds == nil {
		return fmt.Errorf("%s.upscaleDelaySeconds is required", metric)
	}
	if *policy.UpscaleDelaySeconds < 0 {
		return fmt.Errorf("%s.upscaleDelaySeconds cannot be negative", metric)
	}
	return nil
}

func configuredMinimum(redisCluster *cachev1.RedisCluster) int32 {
	minimum := redisCluster.Spec.Primaries
	if spec := redisCluster.Spec.Autoscaling; spec != nil && spec.MinPrimaries != nil && *spec.MinPrimaries > minimum {
		minimum = *spec.MinPrimaries
	}
	return minimum
}

func (r *RedisClusterReconciler) initializeRetainedTarget(ctx context.Context, redisCluster *cachev1.RedisCluster, status *cachev1.RedisClusterAutoscalingStatus) (bool, error) {
	if status.EffectivePrimaries > 0 {
		return false, nil
	}

	// Bootstrap from what Helm requested or from a larger topology that is
	// already running. minPrimaries bounds recommendations, but must not resize
	// a cluster merely because autoscaling was enabled in Recommend or Disabled
	// mode. The explicit reset annotation is the only path that applies the
	// configured minimum directly to the retained target.
	target := redisCluster.Spec.Primaries
	groupSize := 1 + redisCluster.Spec.Replicas
	if groupSize > 0 {
		activeName := redisCluster.Status.ActiveClusterName
		if activeName == "" {
			activeName = redisCluster.Spec.StatefulsetTemplate.Name
		}
		if activeName != "" {
			statefulSet, err := r.GetStatefulSetByName(ctx, redisCluster.Namespace, activeName)
			if err != nil {
				return false, utils.WrapErr(err, "failed to inspect active StatefulSet while initializing autoscaling target")
			}
			if statefulSet != nil && statefulSet.Spec.Replicas != nil && *statefulSet.Spec.Replicas > 0 && *statefulSet.Spec.Replicas%groupSize == 0 {
				activePrimaries := *statefulSet.Spec.Replicas / groupSize
				if activePrimaries > target {
					target = activePrimaries
				}
			}
		}
	}
	if target < 1 {
		return false, fmt.Errorf("cannot initialize autoscaling target from non-positive primary count %d", target)
	}
	status.EffectivePrimaries = target
	status.LastDecisionReason = "RetainedTargetInitialized"
	return true, nil
}

func hasAutoscalingResetAnnotation(redisCluster *cachev1.RedisCluster) bool {
	if redisCluster.Annotations == nil {
		return false
	}
	_, found := redisCluster.Annotations[constants.AutoscalingResetTarget]
	return found
}

func (r *RedisClusterReconciler) removeAutoscalingResetAnnotation(ctx context.Context, redisCluster *cachev1.RedisCluster) error {
	isRetryable := func(err error) bool {
		return apierrors.IsConflict(err) || apierrors.IsServerTimeout(err) || apierrors.IsServiceUnavailable(err)
	}
	updateFn := func() error {
		latest := &cachev1.RedisCluster{}
		key := types.NamespacedName{Namespace: redisCluster.Namespace, Name: redisCluster.Name}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		if latest.Annotations == nil {
			return nil
		}
		if _, found := latest.Annotations[constants.AutoscalingResetTarget]; !found {
			return nil
		}
		delete(latest.Annotations, constants.AutoscalingResetTarget)
		if err := r.Update(ctx, latest); err != nil {
			return err
		}
		redisCluster.ResourceVersion = latest.ResourceVersion
		redisCluster.Annotations = latest.Annotations
		return nil
	}
	return utils.RetryWithBackoff(updateFn, isRetryable, 3, time.Second, r.Log)
}

func setAutoscalingCondition(status *cachev1.RedisClusterAutoscalingStatus, generation int64, conditionType string, conditionStatus metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	})
}

func applyManualAutoscalingReset(redisCluster *cachev1.RedisCluster, status *cachev1.RedisClusterAutoscalingStatus) {
	status.EffectivePrimaries = configuredMinimum(redisCluster)
	status.LastDecisionReason = "ManualTargetReset"
	setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScaling, metav1.ConditionFalse, "ManualTargetReset", "Automatic scaling is not in progress after the manual target reset")
}

func autoscalingIsScaling(status *cachev1.RedisClusterAutoscalingStatus) bool {
	condition := meta.FindStatusCondition(status.Conditions, cachev1.AutoscalingConditionScaling)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

func (r *RedisClusterReconciler) persistAutoscalingStatus(ctx context.Context, redisCluster *cachev1.RedisCluster, desired cachev1.RedisClusterAutoscalingStatus) error {
	if reflect.DeepEqual(redisCluster.Status.Autoscaling, desired) {
		return nil
	}
	return r.updateRedisClusterStatusWithRetry(ctx, redisCluster, func(status *cachev1.RedisClusterStatus) {
		desired.DeepCopyInto(&status.Autoscaling)
	})
}

func (r *RedisClusterReconciler) prepareAutoscaling(ctx context.Context, redisCluster *cachev1.RedisCluster) (autoscalingPreparation, error) {
	status := redisCluster.Status.Autoscaling.DeepCopy()
	resetRequested := hasAutoscalingResetAnnotation(redisCluster)
	initialized, err := r.initializeRetainedTarget(ctx, redisCluster, status)
	if err != nil {
		return autoscalingPreparation{}, err
	}

	manualResetPending := resetRequested && status.ReplicaMigrationDisabled
	if resetRequested && !manualResetPending {
		applyManualAutoscalingReset(redisCluster, status)
	} else if !initialized && redisCluster.Spec.Primaries > status.EffectivePrimaries {
		status.EffectivePrimaries = redisCluster.Spec.Primaries
		status.LastDecisionReason = "HelmPrimaryFloorRaised"
	}

	policy, validationErr := validateAutoscalingPolicy(redisCluster)
	preparation := autoscalingPreparation{
		policy:             policy,
		valid:              validationErr == nil,
		manualResetPending: manualResetPending,
	}
	if validationErr != nil {
		r.Log.Error(validationErr, "Autoscaling policy validation failed", "namespace", redisCluster.Namespace, "name", redisCluster.Name)
		status.CPURecommendation = nil
		status.MemoryRecommendation = nil
		status.CombinedRecommendation = nil
		status.LastDecisionReason = "InvalidPolicy"
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionReady, metav1.ConditionFalse, "InvalidPolicy", invalidAutoscalingPolicyStatusMessage)
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionFalse, "InvalidPolicy", "Recommendations are ignored until the policy is valid")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionFalse, "PolicyInvalid", "Metric health is not evaluated for an invalid policy")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, "InvalidPolicy", invalidAutoscalingPolicyStatusMessage)
	} else if policy.mode == cachev1.AutoscalingModeDisabled {
		if cleanupProvider, ok := r.AutoscalingProvider.(AutoscalingCleanupProvider); ok {
			if err := cleanupProvider.Cleanup(ctx, redisCluster); err != nil {
				r.Log.Error(err, "Failed to clean up disabled autoscaling provider resources")
			}
		}
		status.CPURecommendation = nil
		status.MemoryRecommendation = nil
		status.CombinedRecommendation = nil
		if status.LastDecisionReason == "" || status.LastDecisionReason == "RecommendationAccepted" {
			status.LastDecisionReason = "AutoscalingDisabled"
		}
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionReady, metav1.ConditionFalse, "Disabled", "Autoscaling is disabled")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionFalse, "Disabled", "Recommendation resources are disabled")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionFalse, "Disabled", "Metrics are not evaluated while autoscaling is disabled")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "Disabled", "Automatic scaling is disabled")
	} else if r.AutoscalingProvider == nil {
		preparation.valid = false
		status.CPURecommendation = nil
		status.MemoryRecommendation = nil
		status.CombinedRecommendation = nil
		status.LastDecisionReason = providerUnavailableReason
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionReady, metav1.ConditionFalse, providerUnavailableReason, "Autoscaling is configured but no recommendation provider is available")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionFalse, providerUnavailableReason, "No autoscaling recommendation provider is available")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionTrue, providerUnavailableReason, "Recommendation metrics are unavailable because no provider is configured")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, providerUnavailableReason, "Automatic scaling is blocked until a recommendation provider is configured")
	} else {
		providerRecommendations, providerErr := r.AutoscalingProvider.Recommendations(ctx, redisCluster)
		if providerErr != nil {
			r.Log.Error(providerErr, "Autoscaling recommendation provider failed")
			preparation.valid = false
			status.CPURecommendation = nil
			status.MemoryRecommendation = nil
			status.CombinedRecommendation = nil
			status.LastDecisionReason = providerErrorReason
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionReady, metav1.ConditionFalse, providerErrorReason, "The autoscaling recommendation provider failed; inspect operator logs for details")
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionFalse, providerErrorReason, "No recommendation is available because the provider failed")
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionTrue, providerErrorReason, "Recommendation metrics could not be evaluated")
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, providerErrorReason, "Automatic scaling is blocked while the recommendation provider is failing")
		} else {
			cpuResult := autoscalingResultForPolicy(autoscalingMetricCPU, policy.cpu, providerRecommendations.CPU)
			memoryResult := autoscalingResultForPolicy(autoscalingMetricMemory, policy.memory, providerRecommendations.Memory)
			preparation.cpu = cpuResult.recommendation
			preparation.memory = memoryResult.recommendation
			preparation.metricsDegraded = cpuResult.degraded || memoryResult.degraded
			preparation.combined = combineRecommendations(cpuResult.recommendation, memoryResult.recommendation, policy.minPrimaries, policy.maxPrimaries)
			status.CPURecommendation = copyInt32(preparation.cpu)
			status.MemoryRecommendation = copyInt32(preparation.memory)
			status.CombinedRecommendation = copyInt32(preparation.combined)

			providerRecovered := status.LastDecisionReason == providerUnavailableReason || status.LastDecisionReason == providerErrorReason
			if providerRecovered {
				status.LastDecisionReason = "ProviderAvailable"
			}
			if blocked := meta.FindStatusCondition(status.Conditions, cachev1.AutoscalingConditionScalingBlocked); blocked != nil &&
				(blocked.Reason == providerUnavailableReason || blocked.Reason == providerErrorReason) {
				setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "ProviderAvailable", "The autoscaling recommendation provider is available")
			}

			readyReason := "Ready"
			readyMessage := "The autoscaling recommendation provider is configured"
			if policy.mode == cachev1.AutoscalingModeScaleUpAndDown {
				readyReason = "Phase1UpOnly"
				readyMessage = "ScaleUpAndDown is accepted; automatic downscale is unavailable until Phase 2"
			}
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionReady, metav1.ConditionTrue, readyReason, readyMessage)
			if preparation.combined != nil {
				setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionTrue, "RecommendationAvailable", "At least one current recommendation is available")
			} else {
				setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionRecommendationAvailable, metav1.ConditionFalse, "RecommendationUnavailable", "No current recommendation is available")
			}
			if preparation.metricsDegraded {
				setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionTrue, "RecommendationDegraded", joinMetricReasons(cpuResult.reason, memoryResult.reason))
			} else {
				setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionMetricsDegraded, metav1.ConditionFalse, "MetricsHealthy", "All configured recommendation metrics are current")
			}
		}
	}

	if policy.maxPrimaries > 0 && status.EffectivePrimaries > policy.maxPrimaries {
		preparation.helmAboveMaximum = true
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, "HelmTargetAboveMaximum", "The retained Helm target exceeds maxPrimaries; maxPrimaries only limits automatic growth")
		_ = r.Metrics.CountAutoscalingDecision("blocked", "maximum_bound")
	}
	if manualResetPending {
		status.LastDecisionReason = "ManualTargetResetPendingReplicaMigrationRestore"
	}
	if !autoscalingIsScaling(status) {
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScaling, metav1.ConditionFalse, "NotScaling", "No automatic scale-up is in progress")
	}

	if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
		return autoscalingPreparation{}, utils.WrapErr(err, "failed to persist autoscaling status")
	}
	if resetRequested && !manualResetPending {
		if err := r.removeAutoscalingResetAnnotation(ctx, redisCluster); err != nil {
			return autoscalingPreparation{}, utils.WrapErr(err, "failed to acknowledge autoscaling target reset")
		}
	}
	r.reportAutoscalingGauges(&redisCluster.Status.Autoscaling)
	return preparation, nil
}

func autoscalingResultForPolicy(metric autoscalingMetric, policy *cachev1.RedisClusterAutoscalingMetricPolicy, recommendation AutoscalingMetricRecommendation) autoscalingMetricResult {
	if policy == nil {
		return autoscalingMetricResult{}
	}
	if recommendation.Recommendation == nil {
		return autoscalingMetricResult{degraded: true, reason: fmt.Sprintf("%s recommendation is unavailable", metric)}
	}
	if recommendation.Degraded {
		return autoscalingMetricResult{degraded: true, reason: fmt.Sprintf("%s recommendation metric is degraded", metric)}
	}
	if *recommendation.Recommendation <= 0 {
		return autoscalingMetricResult{degraded: true, reason: fmt.Sprintf("%s recommendation is invalid", metric)}
	}
	return autoscalingMetricResult{
		recommendation: copyInt32(recommendation.Recommendation),
	}
}

func (r *RedisClusterReconciler) completeManualAutoscalingReset(ctx context.Context, redisCluster *cachev1.RedisCluster) error {
	status := redisCluster.Status.Autoscaling.DeepCopy()
	applyManualAutoscalingReset(redisCluster, status)
	if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
		return utils.WrapErr(err, "failed to persist autoscaling target reset")
	}
	if err := r.removeAutoscalingResetAnnotation(ctx, redisCluster); err != nil {
		return utils.WrapErr(err, "failed to acknowledge autoscaling target reset")
	}
	return nil
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func combineRecommendations(cpu, memory *int32, minimum, maximum int32) *int32 {
	if cpu == nil && memory == nil {
		return nil
	}
	var combined int32
	if cpu != nil {
		combined = *cpu
	}
	if memory != nil && *memory > combined {
		combined = *memory
	}
	if combined < minimum {
		combined = minimum
	}
	if combined > maximum {
		combined = maximum
	}
	return &combined
}

func joinMetricReasons(reasons ...string) string {
	var nonempty []string
	for _, reason := range reasons {
		if reason != "" {
			nonempty = append(nonempty, reason)
		}
	}
	if len(nonempty) == 0 {
		return "One or more recommendations are unavailable"
	}
	return strings.Join(nonempty, "; ")
}

func (r *RedisClusterReconciler) reportAutoscalingGauges(status *cachev1.RedisClusterAutoscalingStatus) {
	_ = r.Metrics.Gauge(metrics.GaugeAutoscalingEffectivePrimaries, float64(status.EffectivePrimaries))
	_ = r.Metrics.Gauge(metrics.GaugeAutoscalingCombinedRecommendation, recommendationGauge(status.CombinedRecommendation))
	_ = r.Metrics.Gauge(metrics.GaugeAutoscalingCPURecommendation, recommendationGauge(status.CPURecommendation))
	_ = r.Metrics.Gauge(metrics.GaugeAutoscalingMemoryRecommendation, recommendationGauge(status.MemoryRecommendation))
}

func recommendationGauge(recommendation *int32) float64 {
	if recommendation == nil {
		return 0
	}
	return float64(*recommendation)
}

func (r *RedisClusterReconciler) reconcileAutoscalingAfterMaintenance(ctx context.Context, cluster *redis_cluster.Cluster, preparation autoscalingPreparation, nlaStable bool, now time.Time) (bool, time.Duration, error) {
	redisCluster := cluster.RedisCluster
	status := redisCluster.Status.Autoscaling.DeepCopy()

	if autoscalingIsScaling(status) {
		blocker := r.autoscalingSafetyBlocker(ctx, cluster, nlaStable)
		if blocker != "" {
			status.LastDecisionReason = "ScalingInProgress"
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, "ScalingInProgress", blocker)
			if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
				return true, constants.NormalRequeue, err
			}
			_ = r.Metrics.CountAutoscalingDecision("blocked", "scaling_in_progress")
			return true, constants.NormalRequeue, nil
		}

		completedAt := metav1.NewTime(now)
		status.ScaleCompletedAt = &completedAt
		status.LastDecisionReason = "ScaleUpCompleted"
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScaling, metav1.ConditionFalse, "ScaleUpCompleted", "The retained primary target has converged")
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "ScaleUpCompleted", "The automatic scale-up completed")
		if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
			return true, constants.QuickRequeue, err
		}
		_ = r.Metrics.CountAutoscalingDecision("accepted", "completed")
		return true, constants.QuickRequeue, nil
	}

	if !preparation.valid || preparation.policy.mode == cachev1.AutoscalingModeDisabled {
		return false, 0, nil
	}
	if preparation.policy.mode == cachev1.AutoscalingModeRecommend {
		status.LastDecisionReason = "RecommendationOnly"
		if !preparation.helmAboveMaximum {
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "RecommendationOnly", "Recommend mode never changes the retained target")
		}
		return false, 0, r.persistAutoscalingStatus(ctx, redisCluster, *status)
	}
	if preparation.combined == nil {
		status.LastDecisionReason = "RecommendationUnavailable"
		if !preparation.helmAboveMaximum {
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "RecommendationUnavailable", "No valid recommendation is available")
		}
		return false, 0, r.persistAutoscalingStatus(ctx, redisCluster, *status)
	}

	effectivePrimaries := redisCluster.EffectivePrimaries()
	if effectivePrimaries >= preparation.policy.maxPrimaries {
		reason := "MaximumBoundReached"
		message := "The retained target is at maxPrimaries"
		if preparation.helmAboveMaximum {
			reason = "HelmTargetAboveMaximum"
			message = "The retained Helm target exceeds maxPrimaries; maxPrimaries only limits automatic growth"
		}
		status.LastDecisionReason = reason
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, reason, message)
		if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
			return false, 0, err
		}
		_ = r.Metrics.CountAutoscalingDecision("blocked", "maximum_bound")
		return false, 0, nil
	}
	if *preparation.combined <= effectivePrimaries {
		reason := "RecommendationDoesNotRequireGrowth"
		message := "The combined recommendation does not exceed the retained target"
		if preparation.policy.mode == cachev1.AutoscalingModeScaleUpAndDown && *preparation.combined < effectivePrimaries {
			reason = "DownscaleUnavailablePhase2"
			message = "The recommendation is lower than the retained target; automatic downscale is unavailable until Phase 2"
		}
		status.LastDecisionReason = reason
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, reason, message)
		return false, 0, r.persistAutoscalingStatus(ctx, redisCluster, *status)
	}
	if status.ScaleCompletedAt != nil && preparation.policy.upscaleCooldown > 0 {
		elapsed := now.Sub(status.ScaleCompletedAt.Time)
		if elapsed < preparation.policy.upscaleCooldown {
			remaining := preparation.policy.upscaleCooldown - elapsed
			status.LastDecisionReason = "UpscaleCooldown"
			setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, "UpscaleCooldown", fmt.Sprintf("The completion-based upscale cooldown has %s remaining", remaining.Round(time.Second)))
			if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
				return false, 0, err
			}
			_ = r.Metrics.CountAutoscalingDecision("blocked", "cooldown")
			return true, remaining, nil
		}
	}
	if blocker := r.autoscalingExpansionSafetyBlocker(ctx, cluster, nlaStable); blocker != "" {
		status.LastDecisionReason = "SafetyGateBlocked"
		setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionTrue, "SafetyGateBlocked", blocker)
		if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
			return false, 0, err
		}
		_ = r.Metrics.CountAutoscalingDecision("blocked", "safety_gate")
		return false, 0, nil
	}

	nextTarget := effectivePrimaries + 1
	if nextTarget > preparation.policy.maxPrimaries {
		nextTarget = preparation.policy.maxPrimaries
	}
	startedAt := metav1.NewTime(now)
	status.EffectivePrimaries = nextTarget
	status.ScaleStartedAt = &startedAt
	status.ScaleCompletedAt = nil
	status.LastDecisionReason = "RecommendationAccepted"
	setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScaling, metav1.ConditionTrue, "RecommendationAccepted", fmt.Sprintf("Accepted one-primary scale-up from %d to %d; expanding capacity before topology changes", effectivePrimaries, nextTarget))
	setAutoscalingCondition(status, redisCluster.Generation, cachev1.AutoscalingConditionScalingBlocked, metav1.ConditionFalse, "RecommendationAccepted", "StatefulSet expansion safety gates passed; final topology convergence remains gated")
	if err := r.persistAutoscalingStatus(ctx, redisCluster, *status); err != nil {
		return true, constants.QuickRequeue, err
	}
	_ = r.Metrics.CountAutoscalingDecision("accepted", "recommendation")
	return true, constants.QuickRequeue, nil
}

func (r *RedisClusterReconciler) autoscalingSafetyBlocker(ctx context.Context, cluster *redis_cluster.Cluster, nlaStable bool) string {
	return r.autoscalingSafetyBlockerWithNLARedundancy(ctx, cluster, nlaStable, true)
}

// autoscalingExpansionSafetyBlocker permits StatefulSet expansion when the only
// remaining blocker is an acknowledged NLA replica that has no non-draining
// sibling. Expansion creates the capacity needed to repair that redundancy.
// Final convergence still uses autoscalingSafetyBlocker and therefore cannot
// complete until the repair has happened.
func (r *RedisClusterReconciler) autoscalingExpansionSafetyBlocker(ctx context.Context, cluster *redis_cluster.Cluster, nlaStable bool) string {
	return r.autoscalingSafetyBlockerWithNLARedundancy(ctx, cluster, nlaStable, false)
}

func (r *RedisClusterReconciler) autoscalingSafetyBlockerWithNLARedundancy(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	nlaStable bool,
	requireNLARedundancy bool,
) string {
	redisCluster := cluster.RedisCluster
	if !nlaStable {
		return "NLA processing has not converged"
	}
	if redisCluster.Status.BlueGreen.Phase != "" && redisCluster.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseNone {
		return fmt.Sprintf("blue/green phase %s is active", redisCluster.Status.BlueGreen.Phase)
	}
	if redisCluster.Status.RolloutInProgress {
		return "a StatefulSet rollout is active"
	}
	expectedPrimaries := redisCluster.EffectivePrimaries()
	expectedReplicas := expectedPrimaries * redisCluster.Spec.Replicas
	expectedPods := redisCluster.EffectivePodCount()
	if blocker := statefulSetConvergenceBlocker(cluster.StatefulSet, expectedPods); blocker != "" {
		return blocker
	}
	if int32(len(cluster.AllNodes)) != expectedPods {
		return fmt.Sprintf("Redis node count is %d, expected %d", len(cluster.AllNodes), expectedPods)
	}
	if int32(len(cluster.Nodes[redis_cluster.PrimaryNode])) != expectedPrimaries {
		return fmt.Sprintf("Redis primary count is %d, expected %d", len(cluster.Nodes[redis_cluster.PrimaryNode]), expectedPrimaries)
	}
	if int32(len(cluster.Nodes[redis_cluster.ReplicaNode])) != expectedReplicas {
		return fmt.Sprintf("Redis replica count is %d, expected %d", len(cluster.Nodes[redis_cluster.ReplicaNode]), expectedReplicas)
	}
	for _, role := range []redis_cluster.NodeRole{redis_cluster.OutsiderNode, redis_cluster.ZombieNode, redis_cluster.FailingNode, redis_cluster.OtherNode} {
		if len(cluster.Nodes[role]) > 0 {
			return fmt.Sprintf("cluster contains %d %s nodes", len(cluster.Nodes[role]), role)
		}
	}
	if len(cluster.OpenSlots()) > 0 {
		return "cluster has open or migrating slots"
	}
	for _, node := range cluster.AllNodes {
		if node.IsTerminating {
			return fmt.Sprintf("node %s is draining or terminating", node.PodName)
		}
		if !node.Info.ClusterStateIsOK || node.Info.SlotsAssigned != constants.TotalSlots || node.Info.SlotsPFail > 0 || node.Info.SlotsFail > 0 {
			return fmt.Sprintf("node %s does not report full healthy slot coverage", node.PodName)
		}
	}
	if blocker := replicaAttachmentBlocker(cluster, int(redisCluster.Spec.Replicas)); blocker != "" {
		return blocker
	}
	pods, err := r.GetRedisClusterPods(ctx, redisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to inspect Redis pods while evaluating autoscaling safety")
		return "could not inspect Redis pods"
	}
	if int32(len(pods.Items)) != expectedPods {
		return fmt.Sprintf("Redis pod count is %d, expected %d", len(pods.Items), expectedPods)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp != nil {
			return fmt.Sprintf("pod %s is terminating", pod.Name)
		}
		if pod.Status.Phase != corev1.PodRunning || !podHasReadyCondition(pod) {
			return fmt.Sprintf("pod %s is not Running and Ready", pod.Name)
		}
	}
	if blocker := r.autoscalingLifecycleBlocker(cluster, pods, requireNLARedundancy); blocker != "" {
		return blocker
	}
	return ""
}

func (r *RedisClusterReconciler) autoscalingLifecycleBlocker(cluster *redis_cluster.Cluster, pods *corev1.PodList, requireRedundancy bool) string {
	podsByName := make(map[string]*corev1.Pod, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		podsByName[pod.Name] = pod
	}
	if optionalProviderIsNil(r.LifecycleAdapter) {
		for _, node := range cluster.AllNodes {
			if node.IsDrainCandidate() {
				return fmt.Sprintf("node %s is a drain candidate", node.PodName)
			}
		}
		return ""
	}

	checkedNodes := make(map[string]struct{})
	for i := range pods.Items {
		pod := &pods.Items[i]
		state, err := r.LifecycleAdapter.PodState(pod)
		if err != nil {
			return fmt.Sprintf("lifecycle adapter could not inspect pod %s", pod.Name)
		}
		if state.Managed && state.BlocksScaling {
			return fmt.Sprintf("pod %s has %s", pod.Name, state.ScalingBlockReason)
		}
		if !state.Managed {
			continue
		}

		node := clusterNodeByPodName(cluster, pod.Name)
		if node == nil {
			return fmt.Sprintf("pod %s has an NLA drain trigger but no matching Redis node", pod.Name)
		}
		if blocker := r.acknowledgedNLACandidateBlocker(cluster, node, pod, podsByName, requireRedundancy); blocker != "" {
			return blocker
		}
		checkedNodes[node.PodName] = struct{}{}
	}

	// ClusterNode drain markers are populated from the pod snapshot used to build
	// the Redis topology. Require every marked node to have passed the fresh pod
	// checks above, even if the two snapshots somehow disagree.
	for _, node := range cluster.AllNodes {
		if !node.IsDrainCandidate() {
			continue
		}
		if _, checked := checkedNodes[node.PodName]; checked {
			continue
		}
		pod := podsByName[node.PodName]
		if pod == nil {
			return fmt.Sprintf("draining node %s has no matching Redis pod", node.PodName)
		}
		if blocker := r.acknowledgedNLACandidateBlocker(cluster, node, pod, podsByName, requireRedundancy); blocker != "" {
			return blocker
		}
	}
	return ""
}

func (r *RedisClusterReconciler) acknowledgedNLACandidateBlocker(
	cluster *redis_cluster.Cluster,
	node *redis_cluster.ClusterNode,
	pod *corev1.Pod,
	podsByName map[string]*corev1.Pod,
	requireRedundancy bool,
) string {
	if node.IsTerminating || pod.DeletionTimestamp != nil {
		return fmt.Sprintf("pod %s is terminating during NLA", pod.Name)
	}

	state, err := r.LifecycleAdapter.PodState(pod)
	if err != nil {
		return fmt.Sprintf("lifecycle adapter could not inspect pod %s", pod.Name)
	}
	if !state.Acknowledged {
		return fmt.Sprintf("pod %s has %s", pod.Name, state.ScalingBlockReason)
	}
	if state.Retriggered {
		return fmt.Sprintf("pod %s has a retriggered NLA drain request", pod.Name)
	}
	if node.RoleIs(redis_cluster.PrimaryNode) {
		return fmt.Sprintf("pod %s is still a primary after NLA acknowledgement", pod.Name)
	}
	if !node.RoleIs(redis_cluster.ReplicaNode) || node.PrimaryID == "" {
		return fmt.Sprintf("pod %s is not a healthy attached replica after NLA acknowledgement", pod.Name)
	}
	if requireRedundancy && r.healthyNonDrainingReplicaCount(cluster, node.PrimaryID, podsByName) == 0 {
		return fmt.Sprintf("primary %s has no healthy non-draining replica besides NLA candidate %s", node.PrimaryID, pod.Name)
	}
	return ""
}

func (r *RedisClusterReconciler) healthyNonDrainingReplicaCount(cluster *redis_cluster.Cluster, primaryID string, podsByName map[string]*corev1.Pod) int {
	count := 0
	for _, replica := range cluster.Nodes[redis_cluster.ReplicaNode] {
		if replica.PrimaryID != primaryID || replica.IsDrainCandidate() {
			continue
		}
		pod := podsByName[replica.PodName]
		if pod == nil {
			continue
		}
		adapterManaged := false
		if !optionalProviderIsNil(r.LifecycleAdapter) {
			state, err := r.LifecycleAdapter.PodState(pod)
			adapterManaged = err != nil || state.Managed
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podHasReadyCondition(pod) || redis_cluster.PodHasDrainCandidateCondition(pod) || adapterManaged {
			continue
		}
		if !replica.Info.ClusterStateIsOK || replica.Info.SlotsAssigned != constants.TotalSlots || replica.Info.SlotsPFail > 0 || replica.Info.SlotsFail > 0 {
			continue
		}
		count++
	}
	return count
}

func clusterNodeByPodName(cluster *redis_cluster.Cluster, podName string) *redis_cluster.ClusterNode {
	for _, node := range cluster.AllNodes {
		if node.PodName == podName {
			return node
		}
	}
	return nil
}

func podHasReadyCondition(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func statefulSetConvergenceBlocker(statefulSet *appsv1.StatefulSet, expectedPods int32) string {
	if statefulSet == nil || statefulSet.Spec.Replicas == nil {
		return "active StatefulSet or its desired replica count is unavailable"
	}
	if *statefulSet.Spec.Replicas != expectedPods {
		return fmt.Sprintf("StatefulSet desired replicas are %d, expected %d", *statefulSet.Spec.Replicas, expectedPods)
	}
	if statefulSet.Status.Replicas != expectedPods {
		return fmt.Sprintf("StatefulSet observed replicas are %d, expected %d", statefulSet.Status.Replicas, expectedPods)
	}
	if statefulSet.Status.ReadyReplicas != expectedPods {
		return fmt.Sprintf("StatefulSet ready replicas are %d, expected %d", statefulSet.Status.ReadyReplicas, expectedPods)
	}
	if statefulSet.Status.AvailableReplicas != expectedPods {
		return fmt.Sprintf("StatefulSet available replicas are %d, expected %d", statefulSet.Status.AvailableReplicas, expectedPods)
	}
	return ""
}

func replicaAttachmentBlocker(cluster *redis_cluster.Cluster, expectedReplicasPerPrimary int) string {
	primaries := make(map[string]struct{}, len(cluster.Nodes[redis_cluster.PrimaryNode]))
	replicaCounts := make(map[string]int, len(cluster.Nodes[redis_cluster.PrimaryNode]))
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if primary.ID == "" || len(primary.SlotsRanges) == 0 {
			return fmt.Sprintf("primary %s has no node ID or slots", primary.PodName)
		}
		primaries[primary.ID] = struct{}{}
	}
	for _, replica := range cluster.Nodes[redis_cluster.ReplicaNode] {
		if _, found := primaries[replica.PrimaryID]; !found {
			return fmt.Sprintf("replica %s is not attached to an expected primary", replica.PodName)
		}
		replicaCounts[replica.PrimaryID]++
	}
	for primaryID := range primaries {
		if replicaCounts[primaryID] != expectedReplicasPerPrimary {
			return fmt.Sprintf("primary %s has %d attached replicas, expected %d", primaryID, replicaCounts[primaryID], expectedReplicasPerPrimary)
		}
	}
	return ""
}
