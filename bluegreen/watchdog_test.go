// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/stretchr/testify/assert"
)

func TestSetPhaseResetsConsistencyGateAttempts(t *testing.T) {
	bgStatus := &cachev1.BlueGreenStatus{
		Phase:                   cachev1.BlueGreenPhasePromoting,
		ConsistencyGateAttempts: 2,
	}

	setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, "")
	assert.Equal(t, int32(2), bgStatus.ConsistencyGateAttempts, "same-phase Promoting retries are preserved")

	setPhase(bgStatus, cachev1.BlueGreenPhaseScalingDownBlue, "")
	assert.Zero(t, bgStatus.ConsistencyGateAttempts, "leaving Promoting resets consistency retries")

	bgStatus.ConsistencyGateAttempts = 2
	setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, "")
	assert.Zero(t, bgStatus.ConsistencyGateAttempts, "entering Promoting starts with a fresh consistency gate")
}

func TestConsistencyGateAttemptsDoNotEscalateFailoverMode(t *testing.T) {
	bgStatus := cachev1.BlueGreenStatus{
		ConsistencyGateAttempts: int32(constants.BlueGreenConsistencyGateAttempts),
		ResumeForwardAttempts:   0,
	}

	assert.Equal(t, int32(constants.BlueGreenConsistencyGateAttempts), bgStatus.ConsistencyGateAttempts)
	assert.False(t, bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenForceAttemptThreshold))
	assert.False(t, bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenTakeoverAttemptThreshold))
}
