# RedisCluster API reference

The public API is `cache.datadoghq.com/v1`, kind `RedisCluster`. The resource is
namespaced. Use the canonical manifest in
[`examples/redis-cluster.yaml`](../examples/redis-cluster.yaml) as the starting
point.

## Spec

| Field | Required | Meaning |
| --- | --- | --- |
| `primaries` | yes | Desired primary count. The public beta supports scale-up; primary downscale is retained rather than applied. |
| `replicas` | yes | Replica count per primary. The reference topology uses one. |
| `restoreFrom` | yes | Backup path used to seed a cluster. Set to an empty string when unused. |
| `backupEnabled` | yes | Enables scheduled backup processing. Keep `false` for the basic installation. |
| `backupFrequency` | yes | Backup interval in seconds. Use `0` when backups are disabled. |
| `backupRetention` | no | Deprecated compatibility field. It is ignored. |
| `backupBackend` | no | `s3`, `gcs`, or `azure`; defaults to `s3`. |
| `statefulsetTemplate` | yes | StatefulSet desired state used to create Redis pods. It must follow the [workload contract](workload-contract.md). |
| `disableReconciliation` | no | Stops controller changes for this resource while `true`. |
| `reconcileTimeoutSeconds` | no | Overrides the size-based reconciliation deadline. `0` uses the automatic value. |
| `autoscaling` | no | Configures recommendation observation or automatic primary scale-up. No recommendation provider is wired by the public startup. |

`statefulsetTemplate.spec.replicas` is normalized by the controller to
`effectivePrimaries * (1 + replicas)`.

## Autoscaling

| Field | Meaning |
| --- | --- |
| `mode` | `Disabled`, `Recommend`, `ScaleUp`, or `ScaleUpAndDown`. Public automatic behavior is scale-up only. |
| `minPrimaries` | Lower bound for the retained target. |
| `maxPrimaries` | Upper bound for automatic growth. |
| `upscaleCooldownSeconds` | Delay after a completed automatic scale-up. |
| `cpu` / `memory` | Recommendation policies containing `highWatermark` and optional `upscaleDelaySeconds`. |

Configuring autoscaling without a recommendation provider records
`ProviderUnavailable` in `status.autoscaling` and does not change the topology.

## Status

The main fields are:

| Field | Meaning |
| --- | --- |
| `nodes` | Last observed Redis node identities, roles, addresses, slots, and Redis versions. |
| `statefulset_ready` | Initial StatefulSet bootstrap completed. This is not sufficient by itself to prove Redis health. |
| `blueGreen` | Current blue-green phase, progress, mapping, and recovery bookkeeping. |
| `rollout_in_progress` and related rollout fields | Compatibility rollout state. |
| `last_backup_*` / `backup_restored` | Last backup and restore observations. |
| `autoscaling` | Retained primary target, latest recommendations, timestamps, and autoscaling conditions. |

Use `CLUSTER INFO` and `CLUSTER NODES` for the final Redis topology and data-path
check. See the [operations guide](operations.md).
