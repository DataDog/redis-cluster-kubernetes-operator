# Sizing

Pick values from five inputs: dataset size, peak operations per second,
whether writes are heavy, whether the cache must survive a pod loss, and
whether data must survive a restart. Start from the table, then measure on a test cluster.

## Inputs to values

| Input | Field | Rule |
| --- | --- | --- |
| Must survive a pod loss | `spec.replicas` | `1`. With `0`, losing a primary takes its slots offline until the pod returns. |
| Dataset size `D` | `spec.primaries` (`P`) | Each primary holds `D / P`. Start at `3` (the example, chart default and tested shape) and add primaries before one primary grows past what a pod can restart and resync in your recovery budget. |
| Dataset size `D` | `--maxmemory` | `D / P` plus growth headroom: start at 20%, more if the dataset is still growing. Always set it, and set `--maxmemory-policy` (`allkeys-lru` for a cache). Without it Redis grows until the pod is OOMKilled. |
| Writes are heavy | memory request and limit | `2 × maxmemory` for write-heavy caches (counters, keys overwritten often), `1.5 × maxmemory` otherwise; if unsure, `2 ×`. Forks for RDB saves and replica full syncs copy every page written while they run. Set the request equal to the limit: under node memory pressure Kubernetes evicts pods using more than their request first. |
| Peak ops/s | `spec.primaries`, CPU | Redis runs commands on one main thread, so one primary uses at most about one core for commands. Request `1` CPU for busy primaries and add primaries for throughput. For low traffic start from the chart default (`100m` request) and raise it from measured usage. The chart's `1` CPU limit stays unless removed (`cpu: null`), because Helm merges `resources`; a CPU limit throttles the main thread in bursts, which shows as latency, so remove it for busy primaries. |
| Data must survive restart | `appendonly` (chart) or `--appendonly`, `--save`, PVC size | Persistent: `appendonly: true` and a PVC of at least `2 × maxmemory` (AOF rewrite and RDB write a full copy). Pure cache: `appendonly: false` and `--save ""` so pods do not snapshot; keep the chart's `1Gi` PVC, which holds `nodes.conf`. |

Pod count is `P × (1 + replicas)`. Every replica uses the same memory as its
primary.

Units: Redis `gb`/`mb` are binary, so `6gb` = `6Gi`. Round the memory limit up
to a whole `Mi`/`Gi` value.

`replicas: 1` survives losing a pod. To survive losing a node, spread primaries
and their replicas across nodes with pod anti-affinity (chart `affinity`) or
topology spread constraints; neither is set by default.

## Rollouts double capacity

A pod-template change starts a blue/green rollout: the operator creates a
second StatefulSet of the same size, moves the data, then removes the first.
During a rollout the cluster needs room for `2 × P × (1 + replicas)` pods.

Changing `spec.primaries` reshards in place without a second StatefulSet.

## Measure

Size from measurements, not estimates:

1. Load a representative sample and read `used_memory` from `INFO memory`.
   Scale it to the full key count to get `D`.
2. Run `redis-benchmark` or `memtier_benchmark` against a test cluster with
   your value sizes and command mix. Add primaries until peak ops/s is met with
   each primary below about 70% of one core.
3. Watch for `OOMKilled` pods and `evicted_keys` in `INFO stats` after the
   first full day.

## Example

A 30 GiB write-heavy cache that must survive a pod loss and does not need
persistence:

| Field | Value | Why |
| --- | --- | --- |
| `spec.primaries` | `6` | 5 GiB per primary |
| `spec.replicas` | `1` | survives a pod loss |
| `--maxmemory` | `6gb` | 5 GiB plus 20% growth |
| `--maxmemory-policy` | `allkeys-lru` | cache |
| memory request and limit | `12Gi` | `2 × maxmemory`, write-heavy |
| CPU request | `1` | one busy main thread |
| CPU limit | none (`cpu: null`) | no throttling of the main thread |
| `appendonly`, `--save` | `false`, `""` | cache, no snapshots |

That is 12 pods, and room for 24 during a rollout.

With the Helm chart (`deploy/redis-cluster`):

```yaml
primaries: 6
replicas: 1
appendonly: false
extraArgs: ["--maxmemory", "6gb", "--maxmemory-policy", "allkeys-lru", "--save", ""]
resources:
  requests: {cpu: "1", memory: 12Gi}
  limits: {cpu: null, memory: 12Gi}
```
