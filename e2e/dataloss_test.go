//go:build kind_e2e

package e2e

import (
	"testing"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const seedKeyCount = 2000

// TestDataLoss_BlueGreenRollout verifies data, topology, and QPS through a rollout.
func TestDataLoss_BlueGreenRollout(t *testing.T) {
	requireLoadgen(t)
	requireSlow(t, "slow: full blue-green rollout (~10 min)")
	s := newCluster(t)
	g := s.StartRueidisLoadgen(t, clusterName)
	g.WaitForTraffic(t, loadgenWarmupTimeout)

	lg := s.StartLoadgen(t, seedKeyCount)
	typed := s.StartLoadgen(t, 1)
	s.WaitClusterHealthy(t, lg, clusterReadyTimeout)
	require.NoError(t, lg.SeedData(s.Ctx))
	require.NoError(t, typed.SeedTypedData(s.Ctx))
	loadBefore := g.MustStats(t)

	s.TriggerBlueGreen(t, clusterName)
	s.WaitBlueGreenStarted(t, clusterName, clusterReadyTimeout)
	s.WaitBlueGreenComplete(t, clusterName, bgConvergeTimeout)

	require.NoError(t, s.WaitReady(clusterObjectKey(s), clusterReadyTimeout))
	s.WaitClusterHealthy(t, lg, clusterReadyTimeout)

	assert.NoError(t, rckind.CheckSeedDataIntact(t.Context(), lg, seedKeyCount), "exact-2000-string-values")
	assert.NoError(t, rckind.CheckTypedDataIntact(t.Context(), typed, 3), "multi-type-values")
	if assert.NoError(t, s.CheckSlotCoverage(clusterName, topologySettleTimeout), "topology") {
		assert.NoError(t, s.CheckSingleActiveStatefulSet(clusterName), "topology")
	}
	assert.NoError(t, s.CheckServiceSelectsActive(clusterName), "service-selects-active")
	assert.NoError(t, g.CheckSuccessRateSince(loadBefore, blueGreenMinSuccess), "continuous-qps")
}

// TestDataLoss_OperatorRestartDuringBG verifies recovery and data after a mid-rollout restart.
func TestDataLoss_OperatorRestartDuringBG(t *testing.T) {
	requireLoadgen(t)
	requireSlow(t, "slow: full blue-green rollout (~10 min)")
	s := newCluster(t)
	g := s.StartRueidisLoadgen(t, clusterName)
	g.WaitForTraffic(t, loadgenWarmupTimeout)

	lg := s.StartLoadgen(t, seedKeyCount)
	s.WaitClusterHealthy(t, lg, clusterReadyTimeout)
	require.NoError(t, lg.SeedData(s.Ctx))
	loadBefore := g.MustStats(t)

	s.TriggerBlueGreen(t, clusterName)
	s.WaitBlueGreenStarted(t, clusterName, clusterReadyTimeout)
	s.RestartManager(t)

	s.WaitBlueGreenComplete(t, clusterName, bgConvergeTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), clusterReadyTimeout))
	s.WaitClusterHealthy(t, lg, clusterReadyTimeout)

	assert.NoError(t, rckind.CheckSeedDataIntact(t.Context(), lg, seedKeyCount), "exact-2000-string-values")
	if assert.NoError(t, s.CheckSlotCoverage(clusterName, topologySettleTimeout), "topology") {
		assert.NoError(t, s.CheckSingleActiveStatefulSet(clusterName), "topology")
	}
	assert.NoError(t, s.CheckServiceSelectsActive(clusterName), "service-selects-active")
	assert.NoError(t, g.CheckSuccessRateSince(loadBefore, blueGreenMinSuccess), "continuous-qps")
}
