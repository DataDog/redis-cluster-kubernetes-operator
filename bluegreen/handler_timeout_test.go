// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen_test

import (
	"context"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
)

// Drives Handle() with StartTime far in the past. Existing tests only
// check the exempt-set membership statically; these exercise the runtime.

// Pre-promotion phases past timeout → Failed with "timed out".
func TestHandle_OverallTimeout_FiresForSubjectPhases(t *testing.T) {
	for _, phase := range []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhaseCreatingGreen,
		cachev1.BlueGreenPhaseWaitingPods,
		cachev1.BlueGreenPhaseReplicating,
	} {
		t.Run(string(phase), func(t *testing.T) {
			rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
			rc.Status.BlueGreen = cachev1.BlueGreenStatus{
				Phase:     phase,
				StartTime: time.Now().Add(-2 * constants.BlueGreenOverallTimeout).Unix(),
			}
			h := newHandler(newTestDeps(t, rc))

			_, _, _ = h.Handle(context.Background(), &redis_cluster.Cluster{RedisCluster: rc})

			assert.Equal(t, cachev1.BlueGreenPhaseFailed, rc.Status.BlueGreen.Phase)
			assert.True(t, strings.Contains(rc.Status.BlueGreen.Message, "timed out"),
				"got: %q", rc.Status.BlueGreen.Message)
		})
	}
}

// Post-promotion phases past timeout → must NOT flip to Failed by timeout
// (data loss if it fires mid-promotion).
func TestHandle_OverallTimeout_ExemptsPostPromotionPhases(t *testing.T) {
	for _, phase := range []cachev1.BlueGreenPhase{
		cachev1.BlueGreenPhasePromoting,
		cachev1.BlueGreenPhaseSwitchingDNS,
		cachev1.BlueGreenPhaseFormingCluster,
		cachev1.BlueGreenPhaseScalingDownBlue,
		cachev1.BlueGreenPhaseWaitingSync,
	} {
		t.Run(string(phase), func(t *testing.T) {
			rc := newRedisCluster("redis-cluster-test", "default", 3, 1)
			rc.Status.BlueGreen = cachev1.BlueGreenStatus{
				Phase:            phase,
				StartTime:        time.Now().Add(-2 * constants.BlueGreenOverallTimeout).Unix(),
				SyncStartTime:    time.Now().Unix(),
				LastProgressTime: time.Now().Unix(),
				ShardMapping: []cachev1.ShardMappingEntry{
					{BluePrimaryID: "abc", BluePrimaryIP: "10.0.0.1", GreenPodName: "test-green-0", GreenPodIP: "10.0.0.10"},
				},
			}
			h := newHandler(newTestDeps(t, rc))

			_, _, _ = h.Handle(context.Background(), &redis_cluster.Cluster{RedisCluster: rc})

			assert.False(t, strings.Contains(rc.Status.BlueGreen.Message, "timed out"),
				"got: %q", rc.Status.BlueGreen.Message)
		})
	}
}
