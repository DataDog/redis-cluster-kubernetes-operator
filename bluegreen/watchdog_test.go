// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"testing"

	"github.com/stretchr/testify/assert"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
)

// When green owns every slot, a stalled phase may only be skipped if doing so
// does not bypass a safety gate. In particular FormingCluster waits for green
// replicas to be attached and fully synced; skipping it to ScalingDownBlue
// deletes blue while it may hold the only fully synced copies.
func TestWatchdogFinishForwardTargetNeverSkipsGates(t *testing.T) {
	cases := []struct {
		phase  cachev1.BlueGreenPhase
		target cachev1.BlueGreenPhase
		skip   bool
	}{
		{cachev1.BlueGreenPhaseWaitingPods, cachev1.BlueGreenPhasePromoting, true},
		{cachev1.BlueGreenPhaseReplicating, cachev1.BlueGreenPhasePromoting, true},
		{cachev1.BlueGreenPhaseWaitingSync, cachev1.BlueGreenPhasePromoting, true},
		{cachev1.BlueGreenPhasePromoting, "", false},
		{cachev1.BlueGreenPhaseSwitchingDNS, "", false},
		{cachev1.BlueGreenPhaseFormingCluster, "", false},
		{cachev1.BlueGreenPhaseScalingDownBlue, cachev1.BlueGreenPhaseScalingDownBlue, true},
		{cachev1.BlueGreenPhaseTearingDown, cachev1.BlueGreenPhaseScalingDownBlue, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.phase), func(t *testing.T) {
			target, skip := watchdogFinishForwardTarget(tc.phase)
			assert.Equal(t, tc.skip, skip)
			assert.Equal(t, tc.target, target)
		})
	}
}
