package v1

import "testing"

func TestEffectiveAutoscalingMode(t *testing.T) {
	tests := []struct {
		name string
		spec *RedisClusterAutoscalingSpec
		want AutoscalingMode
	}{
		{name: "omitted policy", want: AutoscalingModeDisabled},
		{name: "omitted mode", spec: &RedisClusterAutoscalingSpec{}, want: AutoscalingModeDisabled},
		{name: "recommend", spec: &RedisClusterAutoscalingSpec{Mode: AutoscalingModeRecommend}, want: AutoscalingModeRecommend},
		{name: "scale up", spec: &RedisClusterAutoscalingSpec{Mode: AutoscalingModeScaleUp}, want: AutoscalingModeScaleUp},
		{name: "scale up and down", spec: &RedisClusterAutoscalingSpec{Mode: AutoscalingModeScaleUpAndDown}, want: AutoscalingModeScaleUpAndDown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := &RedisCluster{Spec: RedisClusterSpec{Autoscaling: test.spec}}
			if got := cluster.EffectiveAutoscalingMode(); got != test.want {
				t.Fatalf("EffectiveAutoscalingMode() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestEffectiveTopologyUsesRetainedTarget(t *testing.T) {
	cluster := &RedisCluster{Spec: RedisClusterSpec{Primaries: 3, Replicas: 1}}
	if got := cluster.EffectivePrimaries(); got != 3 {
		t.Fatalf("EffectivePrimaries() before initialization = %d, want 3", got)
	}
	if got := cluster.EffectivePodCount(); got != 6 {
		t.Fatalf("EffectivePodCount() before initialization = %d, want 6", got)
	}

	cluster.Status.Autoscaling.EffectivePrimaries = 5
	if got := cluster.EffectivePrimaries(); got != 5 {
		t.Fatalf("EffectivePrimaries() after initialization = %d, want 5", got)
	}
	if got := cluster.EffectivePodCount(); got != 10 {
		t.Fatalf("EffectivePodCount() after initialization = %d, want 10", got)
	}
}
