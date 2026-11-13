# Redis workload contract

A `RedisCluster` contains the StatefulSet template used to create Redis pods.
The controller coordinates Redis topology and replaces or creates the
StatefulSet; it does not adopt an unrelated StatefulSet.

Start from [`examples/redis-cluster.yaml`](../examples/redis-cluster.yaml).
Several requirements below are runtime requirements and are not all rejected by
the CRD schema yet. A malformed template can therefore be accepted by the API
server and fail during reconciliation.

## Resource identity

- The `RedisCluster`, headless Service, and generated StatefulSet use the same
  name in the reference configuration.
- Any valid Kubernetes resource name is accepted. No `redis-cluster-` prefix is
  required.
- The `RedisCluster` needs a `chart_version` label. The operator copies it to
  the StatefulSet and uses it when coordinating template updates.
- The StatefulSet selector must contain `app`, `chart_name`, and
  `redis_cluster_name`. The pod template must use the same values.
- Selector labels are immutable after StatefulSet creation.

## StatefulSet behavior

- Use `updateStrategy.type: OnDelete`. The operator coordinates Redis role
  changes before replacing pods.
- Preserve stable StatefulSet ordinals. Redis node identity and persisted
  `nodes.conf` are associated with each ordinal.
- Set the initial replica count to `primaries * (1 + replicas)`. The controller
  recalculates this value while scaling.
- Use one persistent volume claim mounted by the Redis container. The public
  example calls it `data` and mounts it at `/data`.
- Store both Redis persistence files and `/data/nodes.conf` on that volume.
- Do not directly scale or update the generated StatefulSet. Change the owning
  `RedisCluster` resource.

## Redis container

- The Redis container must be named `redis`.
- The qualified images are `redis:8.6.0` and `valkey/valkey:8.1.11`. The
  operator keeps its Redis API names for compatibility; the official Valkey
  image supplies the `redis-server` and `redis-cli` command aliases used by the
  reference workload and toolbox.
- Redis Cluster must be enabled.
- The cluster configuration file must be persistent, for example
  `--cluster-config-file /data/nodes.conf`.
- Expose the Redis client port as `redis` and the cluster bus port as `gossip`.
  The default ports are 6379 and 16379.
- Set `REDIS_SERVICE_NAME` to the headless Service name.
- Supply `POD_NAME` and `POD_NAMESPACE` through the Kubernetes downward API
  when using the reference hostname announcement.
- Readiness must mean that the Redis process accepts commands. Pod readiness is
  used as a gate before topology changes.
- Keep blue-green rollouts within one engine. Changing a running Redis cluster
  to Valkey, or Valkey to Redis, is not a qualified migration path.

## Headless Service

The workload requires a headless Service with:

- `clusterIP: None`;
- `publishNotReadyAddresses: true`, so Redis gossip can form before every pod is
  ready;
- a selector matching `redis_cluster_name`;
- the Redis client and gossip ports.

The Service is also the client entry point. Redis Cluster clients must run on a
network that can reach the node addresses returned by Redis and must follow
`MOVED` and `ASK` redirects. Use cluster mode in `redis-cli` with `-c`.

## Supported reference topology

The public beta reference uses three primaries and one replica per primary.
This is six Redis pods. Primary downscale is not supported. Backups and external
autoscaling providers require additional configuration and are outside the
basic workload contract.
