# Redis Cluster example

Install the operator into the example namespace, then apply the workload:

```sh
helm install redis-operator . \
  --namespace redis-cluster-example \
  --create-namespace
kubectl apply -f examples/redis-cluster.yaml
```

The example creates three primaries with one replica each, persists Redis data
and `nodes.conf` on a volume claim per pod, and leaves backups disabled. The
`RedisCluster` API remains `cache.datadoghq.com/v1` for compatibility.

For authentication, create a Secret in the operator namespace, configure Redis
with a matching ACL user, and set `operator.redisAuth.secretName`,
`usernameKey`, and `passwordKey` during Helm installation. The chart mounts the
Secret only into the toolbox; it does not copy credentials into arguments,
URLs, logs, ConfigMaps, or the example manifest.
