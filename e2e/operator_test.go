//go:build kind_e2e

package e2e

import (
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
)

// TestE2E_OperatorRestart_Resumes verifies scaling after a manager restart.
func TestE2E_OperatorRestart_Resumes(t *testing.T) {
	s := newCluster(t, redisbuilder.WithReplicas(1))
	// Isolate restart recovery from bootstrap by proving Redis reached steady state first.
	s.RequireReadyStatus(t, clusterName, clusterReadyTimeout)
	s.RequireTopology(t, clusterName, redisbuilder.DefaultPrimaries,
		redisbuilder.DefaultReplicas, clusterReadyTimeout)
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)

	s.RestartManager(t)
	s.PatchPrimaries(t, clusterName, scaleTargetPrimaries)

	require.NoError(t, s.WaitReady(clusterObjectKey(s), clusterReadyTimeout))
	s.RequireTopology(t, clusterName, scaleTargetPrimaries, redisbuilder.DefaultReplicas, scaleConvergeTimeout)
	s.RequireReadyStatus(t, clusterName, scaleConvergeTimeout)
	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	s.RequireSingleActiveStatefulSet(t, clusterName)
}
