// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
)

// Skip while BG just completed — topology still settling.
func TestMaintainCluster_SkipsDuringBGGracePeriod(t *testing.T) {
	samples := []struct {
		name       string
		elapsedAgo time.Duration
	}{
		{"just completed", 0},
		{"midway in grace", constants.BlueGreenPostCompletionGracePeriod / 2},
		{"just before expiry", constants.BlueGreenPostCompletionGracePeriod - time.Second},
	}

	for _, tc := range samples {
		t.Run(tc.name, func(t *testing.T) {
			spy := &countingMetrics{}
			r := &RedisClusterReconciler{
				ClusterClient: redis_cluster.ClusterClient{
					Log:     logr.Discard(),
					Metrics: spy,
				},
			}
			cluster := &redis_cluster.Cluster{
				RedisCluster: &cachev1.RedisCluster{
					Status: cachev1.RedisClusterStatus{
						BlueGreen: cachev1.BlueGreenStatus{
							CompletedAt: time.Now().Add(-tc.elapsedAgo).Unix(),
						},
					},
				},
			}

			returned, err := r.maintainCluster(context.Background(), cluster)
			require.NoError(t, err)
			require.Same(t, cluster, returned)
			require.Zero(t, spy.gaugeCalls, "must skip without emitting gauges")
		})
	}
}

// Skip while BG rollout is mid-flight — must not race the BG handler.
func TestMaintainCluster_SkipsWhenBlueGreenPhaseActive(t *testing.T) {
	activePhases := []cachev1.BlueGreenPhase{
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

	for _, phase := range activePhases {
		t.Run(string(phase), func(t *testing.T) {
			spy := &countingMetrics{}
			r := &RedisClusterReconciler{
				ClusterClient: redis_cluster.ClusterClient{
					Log:     logr.Discard(),
					Metrics: spy,
				},
			}
			cluster := &redis_cluster.Cluster{
				RedisCluster: &cachev1.RedisCluster{
					Status: cachev1.RedisClusterStatus{
						BlueGreen: cachev1.BlueGreenStatus{Phase: phase},
					},
				},
			}

			returned, err := r.maintainCluster(context.Background(), cluster)
			require.NoError(t, err)
			require.Same(t, cluster, returned)
			require.Zero(t, spy.gaugeCalls, "must skip for phase %s", phase)
		})
	}
}

// countingMetrics counts Gauge calls; everything else falls through to Noop.
type countingMetrics struct {
	*metrics.NoopMetrics
	gaugeCalls int
}

func (s *countingMetrics) Gauge(name string, value float64, tags ...string) error {
	s.gaugeCalls++
	return nil
}
