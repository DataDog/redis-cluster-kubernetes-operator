# Redis Cluster KIND E2E suite

This suite runs the real `RedisClustersReconciler` in-process against real Redis
pods in a KIND cluster. Its 23 scenarios cover lifecycle, maintenance, scaling,
backup/restore, Kubernetes termination safety, blue-green rollout, data
integrity, and availability during faults.

This is regression and diagnostic coverage for reconciliation behavior. It does
not install the public Helm chart or run the packaged operator Deployment, so it
does not qualify the public installation. The independent deployed-operator
suite is documented in `test/blackbox/README.md` and runs through
`ci/run-cleanroom.sh`.

## Mental model

```text
Local: ci/dev-e2e.sh -> create or reuse KIND -> one go test process
CI:    Go test jobs  -> create one KIND       -> serialized test lanes

One test process (local run or CI lane)
└── selected scenarios run serially
    └── each scenario gets a fresh namespace
        ├── real RedisClustersReconciler in-process (no operator pod)
        ├── RedisCluster CR + headless Service -> StatefulSet -> real Redis pods
        ├── toolbox pod -> redis-cli data and topology checks
        └── optional Rueidis pod -> continuous traffic during a fault
```

`rckind.NewSuite(t)` creates the namespace, toolbox, client, and
namespace-scoped manager. `newCluster(t)` also creates the `RedisCluster` and
waits for Ready. The default topology is three primaries with one replica each:
six Redis pods. Tests run serially and clean up their namespace.

## Coverage

[`lane_manifest_test.go`](lane_manifest_test.go) is the source of truth for exact membership.

| Lane | Count | Local gate | Covers |
| --- | ---: | --- | --- |
| Default | 9 | none | bootstrap, CR deletion, backup rotation, maintenance repair, pod termination, manager restart, pod-IP churn |
| Slow | 6 | `E2E_SLOW=1` | backup/restore, scale and reshard, maintenance rebalance, stale-green recovery, DNS drain pacing |
| Blue-green stuck | 3 | `E2E_SLOW=1` | blank replica, blank primary, loss of both shard copies |
| Loadgen | 5 | `E2E_LOADGEN=1`; some also need `E2E_SLOW=1` | primary loss, manager restart during scale, force cutover, blue-green rollout and restart |

The fast tests in `lane_manifest_test.go` do not start KIND. They verify that
every scenario belongs to exactly one compatible lane, that the CI manifest
matches it, and that the workflow runs smoke plus all four scheduled lanes.

## What we validate and measure

| Layer | Validation |
| --- | --- |
| Reconciliation | Ready status, requested topology, phase progression, recovery after manager restart |
| Kubernetes | StatefulSet and pod lifecycle, ownership, active Service selection, ready endpoints |
| Redis | full slot coverage, primary/replica relationships, gossip convergence, stale-node removal |
| Data | exact string and typed values before and after destructive operations |
| Availability | completed operations, failures, value mismatches, and aggregate success rate during a fault |

Most scenarios use the toolbox pod and `redis-cli` to seed and verify data.
Loadgen scenarios additionally run a Rueidis pod. They snapshot counters before
the disruption and validate the aggregate window through recovery:

- at least one operation completed;
- no value mismatch occurred;
- success rate is at least 90% for primary-loss/scale faults and 95% for
  blue-green operations.

The current loadgen target is 200 operations/second with two workers. This is a
configured target, not measured throughput. The suite does not currently assert
achieved QPS, latency, maximum outage duration, or per-phase error rate.

## Run tests

Run local commands from the repository root:

```sh
# Default lane behavior: runs 9 scenarios and skips gated scenarios.
ci/dev-e2e.sh

# One default scenario.
ci/dev-e2e.sh -run '^TestE2E_Bootstrap$'

# One slow or blue-green-stuck scenario.
E2E_SLOW=1 ci/dev-e2e.sh -run '^TestE2E_BlankReplicaDuringPromoting$'

# One loadgen scenario; builds and loads the Rueidis image.
E2E_BUILD_LOADGEN=1 ci/dev-e2e.sh -run '^TestE2E_PrimaryKill_DataAndTopology$'

# A slow loadgen scenario.
E2E_SLOW=1 E2E_BUILD_LOADGEN=1 ci/dev-e2e.sh -run '^TestE2E_ForceCutover$'
```

The wrapper requires Docker, KIND, `kubectl`, and Go. It reuses or creates the
named KIND cluster, applies the CRD, and defaults to the public Redis 8.6 image.

Run the fast lane-wiring guard from the repository root:

```sh
go test ./e2e
```

Local and CI execution use ordinary `go test` commands. Pull requests run the
small in-process `smoke` lane as a diagnostic alongside the required packaged
operator clean-room smoke. The daily schedule and manual workflow run the four
complete in-process lanes independently against Kubernetes 1.33 and Redis 8.6;
separate jobs run every deployed-operator clean-room lane in fresh clusters.

## Add a test

Every new scenario follows the same contract:

1. Add one top-level `Test...` function to the closest `e2e/*_test.go` file.
   A new file needs the `//go:build kind_e2e` tag.
2. Use `newCluster` for standard setup or `rckind.NewSuite` when setup must
   happen before creating the CR. Do not use `t.Parallel`.
3. Prove the baseline, introduce one change or fault, wait with bounded helpers,
   then assert externally visible data and topology.
4. Add the exact test name once to the matching lane in `lane_manifest_test.go`.
5. Call `requireSlow` or `requireLoadgen` when required by the lane.
6. Run the manifest guard, the focused scenario, and the on-demand CI pipeline.

Generic Kubernetes helpers belong in `test/framework/internal/kindharness`.
Redis topology, data, pod lifecycle, and loadgen helpers belong in
`test/framework/kind`. Keep `support_test.go` limited to scenario constants,
gates, and standard cluster creation.

### Maintenance

Add maintenance behavior to [`maintain_test.go`](maintain_test.go):

1. Establish the Redis role and topology that the repair depends on.
2. Inject drift, for example a reset node, a stale node ID, or a topology change.
3. Call `TriggerReconcile` and wait for Ready/convergence.
4. Verify slot coverage, replica links, stale-node removal, and data as applicable.
5. Use the default lane for short repair; use the slow lane for rebalance or
   topology growth.

### Blue-green

| Behavior | File | Lane |
| --- | --- | --- |
| Successful rollout, phase transition, DNS or endpoint behavior | [`bluegreen_test.go`](bluegreen_test.go) | Slow |
| Safety hold or recovery from a destructive fault | [`bg_stuck_test.go`](bg_stuck_test.go) | Blue-green stuck |
| Client availability or data integrity during rollout | [`qps_test.go`](qps_test.go) or [`dataloss_test.go`](dataloss_test.go) | Loadgen |

For a blue-green safety test:

1. Trigger blue-green and wait for the exact target phase.
2. Introduce and prove the real Redis fault before changing phase-control time.
3. Assert the safety invariant while reconciliation continues: preserve blue,
   hold the phase, or keep an unsafe replacement inactive.
4. If recovery is expected, also verify final topology, active Service, and data.

Track future scenarios in
[`test/framework/kind/PLAN.md`](../test/framework/kind/PLAN.md), not in this
guide.
