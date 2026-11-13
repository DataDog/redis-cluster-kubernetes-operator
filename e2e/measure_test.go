// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
)

// Measure tests record client-observed failover cost under open-loop load and
// write one JSON report per run. They assert data safety and convergence only;
// latency and error numbers are recorded, not asserted. See
// test/framework/kind/PLAN.md ("Failover measurements") and docs/failure-modes.md.

const (
	measureDefaultRuns      = 5
	measureDefaultQPS       = 1000
	measureDefaultPrimaries = 3
	// measureDefaultSeedKeys gives each migrating slot data to move.
	measureDefaultSeedKeys = 20_000
	// measureBaseline is the steady load recorded before the fault.
	measureBaseline = time.Minute
	// measureSettle is the steady load recorded after convergence.
	measureSettle = 15 * time.Second
	// migrationStartTimeout bounds the wait for the first open slot after a scale-up.
	migrationStartTimeout = 5 * time.Minute
	migrationPoll         = 200 * time.Millisecond
)

type measureConfig struct {
	runs, qps, seedKeys int
	primaries           int32
	out, commit         string
}

func requireMeasure(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_MEASURE") == "" {
		t.Skip("set E2E_MEASURE=1 (ci/run-e2e-lane.sh measure) to run failover measurements")
	}
}

func loadMeasureConfig(t *testing.T) measureConfig {
	t.Helper()
	cfg := measureConfig{
		runs:      envIntOr(t, "MEASURE_RUNS", measureDefaultRuns),
		qps:       envIntOr(t, "MEASURE_QPS", measureDefaultQPS),
		seedKeys:  envIntOr(t, "MEASURE_SEED_KEYS", measureDefaultSeedKeys),
		primaries: int32(envIntOr(t, "MEASURE_PRIMARIES", measureDefaultPrimaries)),
		out:       os.Getenv("MEASURE_OUT"),
		commit:    os.Getenv("MEASURE_COMMIT"),
	}
	if cfg.out == "" {
		cfg.out = filepath.Join(e2eSourceDir(t), "..", ".local", "measure")
	}
	return cfg
}

func envIntOr(t *testing.T, key string, fallback int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	require.NoError(t, err, "%s must be an integer", key)
	require.Positive(t, n, "%s must be positive", key)
	return n
}

// measureFault injects the scenario's fault, waits for the cluster to converge,
// and returns the primary count it converged to.
type measureFault func(t *testing.T, s *rckind.Suite, cfg measureConfig) int32

func runMeasure(t *testing.T, scenario string, fault measureFault) {
	t.Helper()
	cfg := loadMeasureConfig(t)
	for run := 1; run <= cfg.runs; run++ {
		t.Run(fmt.Sprintf("run%d", run), func(t *testing.T) {
			s := newCluster(t, redisbuilder.WithPrimaries(cfg.primaries), redisbuilder.WithReplicas(1))
			s.WriteStringData(t, "measure-seed", cfg.seedKeys, dataReadRetry)
			g := s.StartRueidisLoadgen(t, clusterName, rckind.WithOpenLoop(cfg.qps))
			g.WaitForTraffic(t, loadgenWarmupTimeout)
			time.Sleep(measureBaseline)

			start := time.Now()
			target := fault(t, s, cfg)
			time.Sleep(measureSettle)
			end := time.Now()

			require.NoError(t, g.Pause(), "pause loadgen")
			rows, err := g.Timeline()
			require.NoError(t, err, "fetch loadgen timeline")
			verify, err := g.Verify()
			require.NoError(t, err, "verify acknowledged writes")

			r := rckind.AnalyzeFault(rows, start, end)
			r.Scenario, r.Run = scenario, run
			r.Primaries, r.TargetPrimaries = cfg.primaries, target
			r.QPS, r.SeedKeys, r.Commit = cfg.qps, cfg.seedKeys, cfg.commit
			r.Verify, r.AckedLost = verify, verify.Lost()
			path := rckind.WriteMeasureReport(t, cfg.out, r)
			t.Logf("%s (%s)", r.Summary(), path)

			s.RequireStringData(t, "measure-seed", cfg.seedKeys, dataReadRetry)
			require.Zero(t, verify.Lost(), "acknowledged writes lost: %v", verify.Samples)
		})
	}
}

// TestMeasure_Baseline records steady load with no fault.
func TestMeasure_Baseline(t *testing.T) {
	requireLoadgen(t)
	requireMeasure(t)
	runMeasure(t, "Baseline", func(t *testing.T, _ *rckind.Suite, cfg measureConfig) int32 {
		time.Sleep(measureBaseline)
		return cfg.primaries
	})
}

// TestMeasure_ScaleUp records slot migration alone: P to 2P primaries.
func TestMeasure_ScaleUp(t *testing.T) {
	requireLoadgen(t)
	requireMeasure(t)
	runMeasure(t, "ScaleUp", func(t *testing.T, s *rckind.Suite, cfg measureConfig) int32 {
		target := 2 * cfg.primaries
		s.PatchPrimaries(t, clusterName, target)
		s.RequireTopology(t, clusterName, target, 1, scaleConvergeTimeout)
		s.RequireIdle(t, clusterName)
		return target
	})
}

// TestMeasure_PrimaryDelete records a failover alone: one primary pod deleted.
func TestMeasure_PrimaryDelete(t *testing.T) {
	requireLoadgen(t)
	requireMeasure(t)
	runMeasure(t, "PrimaryDelete", func(t *testing.T, s *rckind.Suite, cfg measureConfig) int32 {
		s.DeleteRedisPod(t, s.PickPrimaryPod(t, clusterName, topologySettleTimeout))
		require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))
		s.RequireTopology(t, clusterName, cfg.primaries, 1, failoverTimeout)
		return cfg.primaries
	})
}

// TestMeasure_PrimaryDeleteDuringScaleUp records a failover during slot
// migration: P to 2P, then the primary owning the most slots is deleted once
// the first slot is open. Without a PodDisruptionBudget an eviction is the
// same graceful delete.
func TestMeasure_PrimaryDeleteDuringScaleUp(t *testing.T) {
	requireLoadgen(t)
	requireMeasure(t)
	runMeasure(t, "PrimaryDeleteDuringScaleUp", func(t *testing.T, s *rckind.Suite, cfg measureConfig) int32 {
		target := 2 * cfg.primaries
		s.PatchPrimaries(t, clusterName, target)
		require.Eventually(t, func() bool {
			nodes, _, err := s.TryClusterView()
			return err == nil && rckind.SlotMigrationInProgress(nodes)
		}, migrationStartTimeout, migrationPoll, "slot migration did not start")
		s.DeleteRedisPod(t, s.BusiestPrimaryPod(t, clusterName))
		s.RequireTopology(t, clusterName, target, 1, scaleConvergeTimeout)
		s.RequireIdle(t, clusterName)
		return target
	})
}
