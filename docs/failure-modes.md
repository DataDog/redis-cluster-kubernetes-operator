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

Five runs per scenario. Unavailable and error rate are median / worst; p99 is
the median run; max latency is the worst run.

| Scenario | Unavailable | Error rate | p99 baseline → fault | Max latency | Acked writes lost |
| --- | --- | --- | --- | --- | --- |
| No fault | 0 s / 0 s | 0% / 0% | 1.4 → 2.1 ms | 12 ms | 0 |
| Primary pod deleted | 2 s / 2 s | 0.32% / 0.57% | 1.4 → 5,346 ms | 5.6 s | 0 |
| Scale-up 3 → 6 primaries | 0 s / 0 s | 0% / 0% | 1.4 → 157 ms | 184 ms | 0 |
| Primary deleted during scale-up 3 → 6 | 13 s / 18 s | 0.16% / 0.17% | 1.4 → 5,810 ms | 6.0 s | 0 |

The multi-second p99 after a primary delete is the client, not Redis: requests
already routed to the deleted pod's old IP wait out the client's dial timeout
(`dial tcp … i/o timeout`; 5 s by default in rueidis) before following the new
slot owner. A shorter client dial timeout shortens it.

Runs on the operator with [#40](https://github.com/DataDog/redis-cluster-kubernetes-operator/pull/40);
without it, a primary deleted during scale-up lost 372–441 acknowledged writes
per run.

Definitions:

- **Unavailable**: first to last second with a failed request.
- **Error rate**: failed / scheduled requests between the fault and convergence.
- **Acked writes lost**: acknowledged SETs that do not read back afterwards.

## Known limitations

- **Termination during a scale-up rebalance gets no planned failover.** The
  rebalance after a scale-up is one blocking call
  ([`controller/maintain.go`](../controller/maintain.go)), so a primary that
  terminates while it runs is not failed over first. The pod restarts with the
  same node ID (from `nodes.conf` on its PVC) and rejoins as the primary of its
  slots. Clients see errors spread over 13–18 s instead of 2 s. Acknowledged
  writes are kept since #40: before it, the operator mistook the restarted
  primary for a zombie and forced its replica over, discarding them.
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
