package v1

// EffectiveAutoscalingMode returns Disabled for an omitted policy or mode.
func (r *RedisCluster) EffectiveAutoscalingMode() AutoscalingMode {
	if r.Spec.Autoscaling == nil || r.Spec.Autoscaling.Mode == "" {
		return AutoscalingModeDisabled
	}
	return r.Spec.Autoscaling.Mode
}

// EffectivePrimaries returns the retained primary target, falling back to the
// Helm bootstrap value until the operator has initialized status.
func (r *RedisCluster) EffectivePrimaries() int32 {
	if r.Status.Autoscaling.EffectivePrimaries > 0 {
		return r.Status.Autoscaling.EffectivePrimaries
	}
	return r.Spec.Primaries
}

// EffectivePodCount returns the desired number of Redis pods for the retained topology.
func (r *RedisCluster) EffectivePodCount() int32 {
	return r.EffectivePrimaries() * (1 + r.Spec.Replicas)
}
