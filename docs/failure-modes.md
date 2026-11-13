# Failure modes

What clients see when a Redis pod, node or the operator fails, what the
operator does about it, and measured numbers you can reproduce.

Guarantees describe operator behavior. Measurements are client-observed
results on one environment; rerun them on yours before relying on them.

## Events and operator response

| Event | Operator response | Data safety |
| --- | --- | --- |
| Primary pod deleted, evicted or drained (graceful) | `CLUSTER FAILOVER` to a replica that is not draining, then `CLUSTER FORGET` of the old node ([`controller/terminating_pods.go`](../controller/terminating_pods.go)) | Planned failover: the replica catches up before it takes over, so acknowledged writes are kept |
| Replica pod terminated | `CLUSTER FORGET` of the old node; the StatefulSet recreates the pod and it rejoins as a replica | No data path impact |
| Pod marked `NodeLifecycle`/`NodeDrainCandidate` | Moves the primary role to a sibling that is not marked, as soon as it sees the mark | Same as planned failover |
| Redis process or node crash (no grace) | Redis Cluster elects a replica after `cluster-node-timeout`; the operator repairs topology afterwards | Unplanned failover: writes not yet replicated can be lost (Redis replication is asynchronous) |
| `spec.primaries` increased | New primaries join, then slots are rebalanced onto them | Keys move with their slots; clients follow `MOVED`/`ASK` |
| Operator restarted | Resumes from Kubernetes and Redis state | Data path unaffected |
| Pod-template change | Blue/green rollout to a second StatefulSet ([`docs/operations.md`](operations.md)) | Covered by the `loadgen` E2E lane |

## Measured results

Client: in-cluster [rueidis](https://github.com/redis/rueidis) open-loop
loadgen, 1,000 requests/s (70% GET, 25% SET, 5% MGET), latency measured from
each request's scheduled send time. Cluster: 3 primaries with 1 replica each,
20,000 seed keys, Redis 8.6.0, KIND (Kubernetes 1.33) on a 4-CPU Docker VM.

| Scenario | Unavailable | Error rate | p99 baseline → fault | Max latency | Acked writes lost |
| --- | --- | --- | --- | --- | --- |
| No fault | 0 s | 0% | 1.6 → 1.8 ms | 4 ms | 0 |
| Primary pod deleted | 1 s | 0.16% | 1.6 → 292 ms | 338 ms | 0 |
| Scale-up 3 → 6 primaries | 0 s | 0% | 1.4 → 90 ms | 102 ms | 0 |
| Primary deleted during scale-up 3 → 6 | ~6 s stall | 0.23% | 1.4 → 5,595 ms | 5.6 s | **410** |

Status: one validation run per scenario on `f28eaaf`. Five runs per scenario,
with median and worst, replace this table before it is quoted.

Definitions:

- **Unavailable**: first to last second with a failed request.
- **Error rate**: failed / scheduled requests between the fault and convergence.
- **Acked writes lost**: acknowledged SETs that do not read back afterwards.

## Known limitations

- **Termination during a scale-up rebalance can lose acknowledged writes.** The
  rebalance after a scale-up is one blocking call
  ([`controller/maintain.go`](../controller/maintain.go)), so a primary that
  terminates while it runs gets no planned failover. In the validation run, 410
  of 35,514 acknowledged writes were lost; all were written during the ~6 s
  stall after the delete. A primary delete without a rebalance lost none. The
  exact mechanism is under investigation.
- **No PodDisruptionBudget.** The operator does not create one. Add one with
  `maxUnavailable: 1` so a node drain cannot take a primary and its replica at
  the same time.
- **Unplanned failover can lose writes.** See the crash row above.

## Reproduce

```sh
KIND_CLUSTER_NAME=rco-measure ci/run-e2e-lane.sh measure
go run ./test/framework/kind/cmd/measuresummary .local/measure
```

Knobs: `MEASURE_RUNS` (5), `MEASURE_QPS` (1000), `MEASURE_PRIMARIES` (3),
`MEASURE_SEED_KEYS` (20000), `MEASURE_OUT` (`.local/measure`). Each run writes
a JSON report with the per-second timeline and the first error message of each
kind. The scenarios live in [`e2e/measure_test.go`](../e2e/measure_test.go).
