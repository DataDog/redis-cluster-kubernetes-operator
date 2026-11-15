# Operations guide

The commands below use the public example namespace and resource name. Set the
dedicated local kubeconfig first when following the local quick start:

```sh
export KUBECONFIG="$PWD/.local/kubeconfig"
```

## Check health

Inspect Kubernetes state:

```sh
kubectl get rediscluster cache -n redis-cluster-example -o yaml
kubectl get statefulset,pods,pvc -n redis-cluster-example -o wide
```

Inspect Redis from more than one pod:

```sh
kubectl exec -n redis-cluster-example cache-0 -c redis -- \
  redis-cli --raw CLUSTER INFO
kubectl exec -n redis-cluster-example cache-1 -c redis -- \
  redis-cli --raw CLUSTER NODES
```

The operational health gates are:

- `cluster_state:ok`;
- all 16,384 slots assigned and healthy;
- three primaries and three replicas for the reference topology;
- no `fail`, `fail?`, migrating, or importing entries left in `CLUSTER NODES`;
- every Kubernetes Redis pod is Running and Ready.

`status.statefulset_ready` records completion of the initial StatefulSet
bootstrap. It is not a complete Redis health signal by itself.

## Read operator logs

```sh
kubectl logs -n redis-cluster-example \
  -l app.kubernetes.io/component=operator \
  --all-containers=true --since=15m --tail=-1
```

To follow a recovery:

```sh
kubectl logs -n redis-cluster-example \
  -l app.kubernetes.io/component=operator \
  --all-containers=true -f
```

## Replace a Redis pod

Use `CLUSTER NODES` to identify the pod role, then delete the pod normally:

```sh
kubectl delete pod -n redis-cluster-example cache-4
kubectl get pods -n redis-cluster-example -w
```

After recreation, verify Redis health and application data. Do not delete its
PVC for an ordinary restart; the PVC contains Redis data and node identity.

## Restart the operator

```sh
kubectl rollout restart deployment -n redis-cluster-example \
  -l app.kubernetes.io/component=operator
kubectl rollout status deployment -n redis-cluster-example \
  -l app.kubernetes.io/component=operator --timeout=5m
```

The operator should resume reconciliation from the persisted Kubernetes and
Redis state.

## Scale up primaries

Patch the owning resource:

```sh
kubectl patch rediscluster cache -n redis-cluster-example \
  --type merge --patch '{"spec":{"primaries":4}}'
```

With one replica per primary, the controller expands the StatefulSet from six
to eight pods and assigns slots to the new primary. Watch operator logs and
verify topology after convergence.

Do not scale the child StatefulSet directly. Primary downscale is not supported
in the public beta; lowering `spec.primaries` does not remove primaries.

## Pause reconciliation

For diagnosis only:

```sh
kubectl patch rediscluster cache -n redis-cluster-example \
  --type merge --patch '{"spec":{"disableReconciliation":true}}'
```

Resume with:

```sh
kubectl patch rediscluster cache -n redis-cluster-example \
  --type merge --patch '{"spec":{"disableReconciliation":false}}'
```

Redis and Kubernetes can continue changing while reconciliation is paused.
Always recheck the full topology after resuming.

## Unstick a blue/green rollout held in FormingCluster

In `FormingCluster`, green primaries already own every slot and the operator is
attaching green replicas. It moves on to `ScalingDownBlue` only after every
green replica is Running, attached to its expected primary, and fully synced.
Until then the blue pods may hold the only fully synced copy of each shard, so
this phase has no overall timeout and the progress watchdog never skips it.
Past the phase's stall bound (five minutes) the watchdog pages on every
reconcile instead:

- `redis_cluster_operator_bad_situations_total{situation="bluegreen_watchdog_hold"}`
  keeps increasing;
- `redis_cluster_operator_bluegreen_progress_stalled_seconds` keeps growing;
- `status.blueGreen.message` alternates between the watchdog hold message and
  what the phase is waiting for.

The hold clears by itself as soon as the gate passes. Find out what it is
waiting for:

```sh
kubectl get rediscluster cache -n redis-cluster-example \
  -o jsonpath='{.status.blueGreen.message}{"\n"}'
kubectl get pods -n redis-cluster-example -o wide
kubectl logs -n redis-cluster-example \
  -l app.kubernetes.io/component=operator --all-containers=true --since=15m |
  grep -E 'Waiting for green|Green replica is not|Green cluster'
```

Then fix the cause:

- **`Waiting for green replica <pod> to be running` or a green pod is
  `Pending`:** fix scheduling (node capacity, PVC binding, quota) the same way
  as for any other pod. The phase continues once the pod runs.
- **`Green replica is not fully synchronized`:** the full sync is still in
  progress or keeps restarting. Check it on the replica:

  ```sh
  kubectl exec -n redis-cluster-example <green-replica-pod> -c redis -- \
    redis-cli --raw INFO replication
  ```

  `master_sync_in_progress:1` on a large dataset only needs time. A sync that
  restarts repeatedly usually points at the primary's
  `client-output-buffer-limit replica` or `repl-timeout` being too small for
  the dataset.
- **`Green replica is not attached to its expected primary`:** delete that
  green replica pod. It holds no data the cluster still needs, and the
  operator attaches the recreated pod again on the next reconcile. Never
  delete a green primary in this phase.

Do not edit `status.blueGreen.phase` by hand to skip ahead, and do not delete
the blue StatefulSet or its pods: either removes the only fully synced copy
of data that green has not yet replicated.

## Unstick a blue/green rollout held in Promoting or SwitchingDNS

`Promoting` and `SwitchingDNS` run after green has taken over slots, so like
`FormingCluster` they have no overall timeout. Once green owns every slot, the
progress watchdog no longer skips them past their stall bound (five minutes
for `Promoting`, two for `SwitchingDNS`). It pages with the same metrics as
`FormingCluster` (`bluegreen_watchdog_hold` and the stalled-seconds gauge;
`status.blueGreen.phase` tells you which phase) and lets the phase keep
retrying. The hold clears by itself as soon as the phase's own gate passes.

Find out what the phase is waiting for:

```sh
kubectl get rediscluster cache -n redis-cluster-example \
  -o jsonpath='{.status.blueGreen.phase}{"\n"}{.status.blueGreen.message}{"\n"}'
kubectl get rediscluster cache -n redis-cluster-example \
  -o jsonpath='{range .status.blueGreen.shardMapping[*]}{.greenPodName}{" "}{.greenPodIP}{" "}{.greenNodeID}{" "}{.slotsRanges}{"\n"}{end}'
kubectl logs -n redis-cluster-example \
  -l app.kubernetes.io/component=operator --all-containers=true --since=15m |
  grep -E 'Topology not converged|Deferring DNS switch|Refusing role=active|Deferring promotion'
```

**`Promoting`: `All promoted — waiting for topology convergence`.** Every
green primary has been promoted, but at least one green primary does not yet
see all of them as healthy masters. Run `CLUSTER NODES` on each green primary
from the shard mapping:

```sh
kubectl exec -n redis-cluster-example <green-primary-pod> -c redis -- \
  redis-cli CLUSTER NODES
```

- **A green primary pod does not answer, or is restarting:** fix the pod
  (scheduling, OOM, readiness) as for any other pod. The check includes every
  mapped green primary, so one unreachable pod holds the phase.
- **A green node ID is missing, or flagged `fail` or `fail?`:** the nodes
  cannot reach each other on the cluster bus port (Redis port + 10000, 16379
  by default). Check NetworkPolicies and security groups between green pods.
  The operator sends `CLUSTER MEET` on every retry, so the phase continues
  once the bus is reachable.

**`SwitchingDNS`: `<n> green primary(ies) do not own their slots — deferring`,
or `Deferring DNS switch` in the logs.** Before marking green primaries
`role=active`, the operator checks that each one still has the node ID from the
shard mapping and owns its slots as a healthy master.

- **`Deferring DNS switch: cannot read cluster view`:** no green node
  returned a usable `CLUSTER NODES`. Fix the green pods' availability first.
- **`Refusing role=active` for a pod:** compare its current ID with
  `greenNodeID` in the shard mapping:

  ```sh
  kubectl exec -n redis-cluster-example <green-primary-pod> -c redis -- \
    redis-cli CLUSTER MYID
  ```

  If the ID matches, the pod is restarting or is briefly not a healthy master
  for its slots. It clears once the pod is back. If the ID differs, or another
  node now owns those slots, the gate cannot pass on its own: that shard's
  green copy changed identity after promotion. Escalate with the shard
  mapping, the `CLUSTER NODES` output from every green pod, and the operator
  logs, rather than forcing the rollout forward.

As with `FormingCluster`, do not edit `status.blueGreen.phase` by hand, do not
use the `bluegreen.force-reset` annotation to get past these phases (it keeps
green and drops blue), and do not delete the blue StatefulSet or its pods.

## Remove the local installation

For the local KIND environment, remove the whole dedicated cluster:

```sh
make local-down
```

For a shared cluster, decide whether Redis data must be retained before deleting
the `RedisCluster`, namespace, or PVCs. PVC deletion destroys the stored Redis
data and `nodes.conf`.
