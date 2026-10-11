package v1

import (
	"fmt"
	"math"
)

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

// ValidateTopology rejects values that cannot produce a valid StatefulSet size.
// The runtime check also protects clusters created before the CRD schema gained
// equivalent validation and status values retained by autoscaling.
func (r *RedisCluster) ValidateTopology() error {
	if r.Spec.Primaries < 1 {
		return fmt.Errorf("spec.primaries must be at least 1")
	}
	if r.Spec.Replicas < 0 {
		return fmt.Errorf("spec.replicas must be at least 0")
	}
	effectivePrimaries := int64(r.EffectivePrimaries())
	podCount := effectivePrimaries * (1 + int64(r.Spec.Replicas))
	if effectivePrimaries < 1 || podCount > math.MaxInt32 {
		return fmt.Errorf("effective topology produces invalid pod count %d", podCount)
	}
	return nil
}
