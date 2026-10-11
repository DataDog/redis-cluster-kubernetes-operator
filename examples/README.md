# Redis Cluster example

[`redis-cluster.yaml`](redis-cluster.yaml) is the canonical public workload. It
creates a `RedisCluster` named `cache`, demonstrating that resource names do not
need a `redis-cluster-` prefix.

The example creates three primaries with one replica each, persists Redis data
and `nodes.conf` on one volume claim per pod, uses an `OnDelete` StatefulSet
strategy, and leaves backups disabled. The `RedisCluster` API remains
`cache.datadoghq.com/v1` for compatibility.

For the complete local path, including Colima, KIND, image build, Helm install,
topology verification, writes, pod replacement, logs, and cleanup, run:

```sh
make local-up
export KUBECONFIG="$PWD/.local/kubeconfig"
```

See the [local quick start](../docs/getting-started.md). To apply only the
workload to an operator already installed in `redis-cluster-example`:

```sh
kubectl apply -f examples/redis-cluster.yaml
```

The embedded StatefulSet is part of the operator API. Read the
[workload contract](../docs/workload-contract.md) before changing its labels,
Service, Redis container, ports, update strategy, or storage layout.

For authentication, create a Secret in the operator namespace, configure Redis
with a matching ACL user, and set `operator.redisAuth.secretName`,
`usernameKey`, and `passwordKey` during Helm installation. The chart mounts the
Secret only into the toolbox; it does not copy credentials into arguments,
URLs, logs, ConfigMaps, or the example manifest.

Authentication also has to be configured in Redis itself. The repository does
not yet provide a complete authenticated Redis workload example, so the
unauthenticated manifest above remains the supported getting-started example.
