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
