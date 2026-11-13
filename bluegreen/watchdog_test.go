// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
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

// Repro for the original bug: FormingCluster stalls past its bound while green
// primaries own every slot. The watchdog must page but leave the phase alone
// instead of jumping to ScalingDownBlue and deleting blue.
func TestCheckProgressWatchdogHoldsStalledFormingCluster(t *testing.T) {
	const (
		greenA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		greenB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		blueA  = "cccccccccccccccccccccccccccccccccccccccc"
		blueB  = "dddddddddddddddddddddddddddddddddddddddd"
	)
	clusterNodes := strings.Join([]string{
		greenA + " 10.0.1.0:6379@16379 myself,master - 0 0 3 connected 0-8191",
		greenB + " 10.0.1.1:6379@16379 master - 0 0 4 connected 8192-16383",
		blueA + " 10.0.0.0:6379@16379 slave " + greenA + " 0 0 3 connected",
		blueB + " 10.0.0.1:6379@16379 slave " + greenB + " 0 0 4 connected",
	}, "\n") + "\n"

	ctx := context.Background()
	exec := &clusterNodesExecutor{stdout: clusterNodes}
	rc := &cachev1.RedisCluster{}
	rc.Status.BlueGreen = cachev1.BlueGreenStatus{
		Phase:          cachev1.BlueGreenPhaseFormingCluster,
		PhaseEntryTime: time.Now().Add(-constants.BlueGreenFormClusterStallTimeout - time.Minute).Unix(),
		ShardMapping: []cachev1.ShardMappingEntry{
			{BluePrimaryID: blueA, BluePrimaryIP: "10.0.0.0", GreenPodIP: "10.0.1.0", GreenNodeID: greenA, SlotsRanges: "0-8191"},
			{BluePrimaryID: blueB, BluePrimaryIP: "10.0.0.1", GreenPodIP: "10.0.1.1", GreenNodeID: greenB, SlotsRanges: "8192-16383"},
		},
	}
	cluster := &redis_cluster.Cluster{
		RedisCluster: rc,
		Toolbox:      redis_cluster.NewToolboxForTesting(ctx, exec, 6379),
	}
	deps := &watchdogDeps{}
	h := &Handler{deps: deps, log: logr.Discard(), metrics: metrics.CreateNoop()}

	_, _, handled, err := h.checkProgressWatchdog(ctx, cluster)

	require.NoError(t, err)
	require.Positive(t, exec.calls, "the watchdog must read the live cluster view")
	assert.False(t, handled, "the FormingCluster handler must keep running")
	assert.Equal(t, cachev1.BlueGreenPhaseFormingCluster, rc.Status.BlueGreen.Phase, "blue must not be scaled down")
	assert.Contains(t, rc.Status.BlueGreen.Message, "phase gate has not passed")
	assert.Positive(t, deps.statusUpdates, "the hold must be visible in status")
}

// clusterNodesExecutor answers every toolbox command with one CLUSTER NODES
// snapshot.
type clusterNodesExecutor struct {
	stdout string
	calls  int
}

func (e *clusterNodesExecutor) Exec(context.Context, string, string, []string, logr.Logger) (int, string, string, error) {
	e.calls++
	return 0, e.stdout, "", nil
}

func (e *clusterNodesExecutor) ExecWithInput(ctx context.Context, pod, container string, command []string, _ []byte, log logr.Logger) (int, string, string, error) {
	return e.Exec(ctx, pod, container, command, log)
}

// watchdogDeps records status writes; any other Deps call panics, which keeps
// the test honest about what the watchdog touches.
type watchdogDeps struct {
	Deps
	statusUpdates int
}

func (d *watchdogDeps) UpdateStatus(context.Context, *cachev1.RedisCluster) error {
	d.statusUpdates++
	return nil
}
