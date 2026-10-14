// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package v1

import (
	"strings"
	"testing"
)

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

func TestValidateTopology(t *testing.T) {
	for _, tc := range []struct {
		name      string
		primaries int32
		replicas  int32
		effective int32
		wantErr   string
	}{
		{name: "valid", primaries: 3, replicas: 1},
		{name: "zero primaries", primaries: 0, replicas: 1, wantErr: "spec.primaries"},
		{name: "negative replicas", primaries: 1, replicas: -1, wantErr: "spec.replicas"},
		{name: "overflow", primaries: 1, replicas: 1, effective: 1 << 30, wantErr: "invalid pod count"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &RedisCluster{
				Spec:   RedisClusterSpec{Primaries: tc.primaries, Replicas: tc.replicas},
				Status: RedisClusterStatus{Autoscaling: RedisClusterAutoscalingStatus{EffectivePrimaries: tc.effective}},
			}
			err := cluster.ValidateTopology()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateTopology() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateTopology() error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}
