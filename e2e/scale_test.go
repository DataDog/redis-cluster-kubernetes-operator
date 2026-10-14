// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
)

// reshardBalanceTolerance bounds per-primary slot drift from the ideal share;
// Redis Cluster doesn't promise a perfectly even split, but no primary should
// drift more than ±30% — and every primary must own at least one slot.
const reshardBalanceTolerance = 0.30

// TestE2E_ScaleUp_ReshardDistribution scales primaries up and asserts every
// pre-scale key survives, the new primary picks up slots, and the per-primary
// slot distribution stays within tolerance of the ideal share.
func TestE2E_ScaleUp_ReshardDistribution(t *testing.T) {
	requireSlow(t, "slow: full scale-up + reshard convergence (~3-4 min)")
	s := newCluster(t)
	s.WriteStringData(t, "reshard-data", dataIntegrityKeyCount, dataReadRetry)

	preNodes, _, err := s.TryClusterView()
	require.NoError(t, err)
	prePrimaries := countPrimaries(preNodes)

	s.PatchPrimaries(t, clusterName, scaleTargetPrimaries)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireIdle(t, clusterName)

	// addPrimaries + reshard runs asynchronously after the STS is Ready and is
	// slow on a small VM; poll until the new primary exists AND owns a balanced
	// slot share.
	s.EventuallyErrorT(t, "reshard balances onto the new primary", scaleConvergeTimeout, func() error {
		nodes, raw, err := s.TryClusterView()
		if err != nil {
			return err
		}
		if got := countPrimaries(nodes); got <= prePrimaries {
			return fmt.Errorf("primary count still %d, want > %d", got, prePrimaries)
		}
		return checkBalancedSlotDistribution(nodes, raw)
	})

	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	s.RequireStringData(t, "reshard-data", dataIntegrityKeyCount, dataReadRetry)
}

// TestE2E_PrimaryDownscaleRetainsTarget scales primaries up, lowers the
// configured minimum, and asserts the retained target and data remain intact.
func TestE2E_PrimaryDownscaleRetainsTarget(t *testing.T) {
	requireSlow(t, "slow: scale-up then retained-target verification (~3 min)")
	s := newCluster(t)
	s.WriteStringData(t, "scale-data", dataIntegrityKeyCount, dataReadRetry)

	s.PatchPrimaries(t, clusterName, scaleTargetPrimaries)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireTopology(t, clusterName, scaleTargetPrimaries, redisbuilder.DefaultReplicas, scaleConvergeTimeout)
	s.RequireIdle(t, clusterName)

	s.PatchPrimaries(t, clusterName, redisbuilder.DefaultPrimaries)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), scaleConvergeTimeout))
	s.RequireTopology(t, clusterName, scaleTargetPrimaries, redisbuilder.DefaultReplicas, scaleConvergeTimeout)
	s.RequireIdle(t, clusterName)

	s.RequireSlotCoverage(t, clusterName, topologySettleTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	s.RequireStringData(t, "scale-data", dataIntegrityKeyCount, dataReadRetry)
}

// countPrimaries counts non-failed masters in a cluster view.
func countPrimaries(nodes []rckind.ClusterNode) int {
	n := 0
	for _, node := range nodes {
		if node.IsMaster && !node.Failed {
			n++
		}
	}
	return n
}

// checkBalancedSlotDistribution returns an error if any primary owns zero slots
// or drifts more than reshardBalanceTolerance from the ideal 16384/N share
// (non-fatal, for poll loops while the reshard is in flight).
func checkBalancedSlotDistribution(nodes []rckind.ClusterNode, raw string) error {
	slotsByPrimary := map[string]int{}
	for _, n := range nodes {
		if n.IsMaster && !n.Failed {
			slotsByPrimary[n.ID] = n.OwnedSlots()
		}
	}
	if len(slotsByPrimary) == 0 {
		return fmt.Errorf("no live primaries found\nCLUSTER NODES:\n%s", raw)
	}
	ideal := float64(constants.TotalSlots) / float64(len(slotsByPrimary))
	low := int(ideal * (1.0 - reshardBalanceTolerance))
	high := int(ideal * (1.0 + reshardBalanceTolerance))

	var detail strings.Builder
	for id, count := range slotsByPrimary {
		fmt.Fprintf(&detail, "  %s: %d slots\n", id, count)
	}
	for id, count := range slotsByPrimary {
		if count < low || count > high {
			return fmt.Errorf("primary %s slot count %d outside [%d,%d] (ideal=%.0f)\n%s", id, count, low, high, ideal, detail.String())
		}
	}
	return nil
}
