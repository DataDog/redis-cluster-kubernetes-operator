# Redis Cluster KIND E2E roadmap

The KIND suite is implemented. See [`../../../e2e/README.md`](../../../e2e/README.md)
for local commands, CI behavior, and test contracts.

## Current coverage

- **Default (9):** bootstrap, lifecycle, topology, maintenance, Kubernetes termination safety, and steady-state data.
- **Slow (6):** backup/restore, scale and reshard, stale green recovery, and drain pacing.
- **Blue-green stuck (3):** blank-node incident replay, DNS activation guard, and watchdog hold.
- **Loadgen (5):** continuous traffic across failover, scaling, restart, and blue-green rollout.

`e2e/lane_manifest_test.go` is the source of truth for all four lane manifests.
`e2e/README.md` documents the suite model and contributor workflow.
All lanes use the repository-local fixture in `test/framework/internal/kindharness`
and the operator-specific wrapper in `test/framework/kind`.

## Remaining work

- Deterministic data-size benchmarks with dataset validation, QPS, and phase timings.
- Remaining crash-phase and quorum-loss scenarios.
- Backup-promotion failure coverage.
- Blue-green stuck recovery for a blank replica in WaitingSync and
  FormingCluster, plus an explicit wall-clock watchdog-bound check.

## Failover measurements

Goal: publish client-observed failover cost, alone and during slot migration,
with numbers anyone can reproduce on KIND or EKS.

### Metrics (client-observed, per run)

| Metric | Definition |
| --- | --- |
| unavailability | first to last failed request, in seconds |
| error rate | failed / scheduled requests in the fault window |
| latency | p50, p99 and max per second, from the scheduled send time |
| acked-write loss | acknowledged SETs not readable after the run |

The operator's `failover_duration_ms` is not client downtime and is not used.

### Load

The rueidis loadgen gains an opt-in open-loop mode (`-open-loop`): requests
are scheduled at a fixed rate and latency is measured from the scheduled time,
so a stall shows as latency instead of fewer requests (no coordinated
omission). `/timeline` returns one JSON row per second. The default closed
mode is unchanged for the existing lanes.

### Scenarios (`measure` lane)

| Test | Fault |
| --- | --- |
| `TestMeasure_Baseline` | none |
| `TestMeasure_ScaleUp` | `spec.primaries` P → 2P, no pod fault |
| `TestMeasure_PrimaryDelete` | delete one primary pod |
| `TestMeasure_PrimaryEvictDuringScaleUp` | P → 2P, then evict the primary exporting the most slots once migration starts |

Each test repeats `MEASURE_RUNS` times (default 5) and writes one JSON report
per run to `MEASURE_OUT` (default `.local/measure`). Tests assert only data
safety and convergence; latency is recorded, not asserted.

Knobs: `MEASURE_PRIMARIES` (3), `MEASURE_QPS` (1000), `MEASURE_SEED_KEYS`
(data per shard, drives migration time), `MEASURE_RUNS`.

### Results

`docs/failure-modes.md` lists each scenario with median and worst run, the
environment (operator commit, Redis version, client, node type, data size),
and the command to rerun it. Known gaps link to issues.
