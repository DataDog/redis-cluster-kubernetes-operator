// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
)

// QPS tests run the in-cluster rueidis loadgen continuously across a fault and
// assert a success-rate floor — the during-the-transition signal the toolbox
// seed→verify loadgen can't capture. They require the loadgen image
// (loadgen.LoadgenPodImage) kind-loaded first: ci/build-loadgen-image.sh.

// TestE2E_PrimaryKill_DataAndTopology kills a primary under live load and asserts
// the loadgen kept serving (success rate >= fault floor), no key was lost, and
// topology recovered.
func TestE2E_PrimaryKill_DataAndTopology(t *testing.T) {
	requireLoadgen(t)
	s := newCluster(t)
	g := s.StartRueidisLoadgen(t, clusterName)
	g.WaitForTraffic(t, loadgenWarmupTimeout)

	s.WriteStringData(t, "failover-data", dataIntegrityKeyCount, dataReadRetry)
	loadBefore := g.MustStats(t)

	s.DeleteRedisPod(t, s.PickPrimaryPod(t, clusterName, topologySettleTimeout))
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))

	// Gate the success-rate floor on real recovery: WaitReady's predicate can be
	// stale-true right after the kill (Status.StatefulSetReady not yet flipped),
	// so assert the loadgen floor only once topology/data have converged and the
	// full fault window is captured.
	s.RequireStringData(t, "failover-data", dataIntegrityKeyCount, dataReadRetry)
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	g.RequireSuccessRateSince(t, loadBefore, loadgenFaultMinSuccess)
}

// TestE2E_RestartOperatorDuringScale restarts the operator mid-scale under live
// load and asserts the data path stayed up and the cluster converged.
func TestE2E_RestartOperatorDuringScale(t *testing.T) {
	requireLoadgen(t)
	s := newCluster(t, redisbuilder.WithReplicas(1))
	g := s.StartRueidisLoadgen(t, clusterName)
	g.WaitForTraffic(t, loadgenWarmupTimeout)
	loadBefore := g.MustStats(t)

	s.PatchPrimaries(t, clusterName, scaleTargetPrimaries)
	want := redisbuilder.PodCount(scaleTargetPrimaries, redisbuilder.DefaultReplicas)
	s.EventuallyT(t, "STS spec reaches scale target", scaleConvergeTimeout, func() bool {
		sts := &appsv1.StatefulSet{}
		if err := s.Client.Get(s.Ctx, types.NamespacedName{Namespace: s.Namespace, Name: clusterName}, sts); err != nil {
			return false
		}
		return sts.Spec.Replicas != nil && *sts.Spec.Replicas == want
	})

	s.RestartManager(t)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireTopology(t, clusterName, scaleTargetPrimaries, 1, scaleConvergeTimeout)
	s.RequireIdle(t, clusterName)
	s.RequireReadyStatus(t, clusterName, scaleConvergeTimeout)
	g.RequireSuccessRateSince(t, loadBefore, loadgenFaultMinSuccess)
}

// TestE2E_ForceCutover applies the force-cutover annotation once green replicas
// are mapped and asserts the rollout still converges under live load.
func TestE2E_ForceCutover(t *testing.T) {
	requireLoadgen(t)
	requireSlow(t, "force-cutover pays a full blue-green rollout")
	s := newCluster(t, redisbuilder.WithReplicas(1))
	g := s.StartRueidisLoadgen(t, clusterName)
	g.WaitForTraffic(t, loadgenWarmupTimeout)
	loadBefore := g.MustStats(t)

	s.TriggerBlueGreen(t, clusterName)
	s.WaitBlueGreenPhase(t, clusterName, bgConvergeTimeout, func(rc *cachev1.RedisCluster) bool {
		return rc.Status.BlueGreen.Phase == cachev1.BlueGreenPhaseWaitingSync &&
			len(rc.Status.BlueGreen.ShardMapping) == int(rc.Spec.Primaries)
	})
	require.NoError(t, s.PatchCR(t, clusterName, func(rc *cachev1.RedisCluster) {
		if rc.Annotations == nil {
			rc.Annotations = map[string]string{}
		}
		rc.Annotations[constants.BlueGreenForceCutover] = "true"
	}))

	s.WaitBlueGreenComplete(t, clusterName, bgConvergeTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), clusterReadyTimeout))
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	g.RequireSuccessRateSince(t, loadBefore, blueGreenMinSuccess)
}
