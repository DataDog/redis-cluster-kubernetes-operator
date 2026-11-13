# Client TLS

Redis serves clients over TLS on a second port while the plaintext port, the
cluster bus and replication stay plaintext. The operator keeps using the
plaintext port, so it needs no TLS configuration.

Redis advertises ports per connection: a TLS client sees the TLS port in
`MOVED`, `ASK`, `CLUSTER SLOTS` and `CLUSTER NODES`, and a plaintext client
sees the plaintext port. Cluster-aware clients connected over TLS therefore
stay on TLS after redirects.

TLS is configured in the `statefulsetTemplate`; the `RedisCluster` API has no
TLS fields.

## Certificate Secret

Create a Secret with `tls.crt`, `tls.key` and `ca.crt` in the cluster
namespace. This is the layout cert-manager writes. Include the Service names
that clients use:

```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: cache-tls
  namespace: redis-cluster-example
spec:
  secretName: cache-tls
  issuerRef:
    name: my-ca-issuer
    kind: Issuer
  dnsNames:
    - cache.redis-cluster-example.svc
    - "*.cache.redis-cluster-example.svc"
```

## Workload changes

Starting from [`examples/redis-cluster.yaml`](../examples/redis-cluster.yaml),
add the Secret volume and the TLS arguments to the `redis` container:

```yaml
      template:
        spec:
          volumes:
            - name: tls
              secret:
                secretName: cache-tls
          containers:
            - name: redis
              args:
                # ...existing arguments, including --port 6379...
                - --tls-port
                - "6380"
                - --tls-cert-file
                - /tls/tls.crt
                - --tls-key-file
                - /tls/tls.key
                - --tls-ca-cert-file
                - /tls/ca.crt
                - --tls-cluster
                - "no"
                - --tls-replication
                - "no"
                - --tls-auth-clients
                - optional
              ports:
                # ...redis and gossip...
                - name: redis-tls
                  containerPort: 6380
              volumeMounts:
                # ...data...
                - name: tls
                  mountPath: /tls
                  readOnly: true
```

Add the port to the headless Service:

```yaml
    - name: redis-tls
      port: 6380
      targetPort: redis-tls
```

Set `--tls-auth-clients yes` to require client certificates (mTLS). It applies
only to the TLS port.

Changing the pod template on an existing cluster starts a blue/green rollout.

## Connect

```sh
kubectl exec -n redis-cluster-example cache-0 -c redis -- \
  redis-cli --tls --cacert /tls/ca.crt -p 6380 -c SET greeting hello
```

## Limits

- The plaintext port must stay enabled; the operator, probes and backups use
  it. Restrict it with a NetworkPolicy if clients must not reach it.
- Redis loads certificates at startup. After rotating the Secret, roll the
  pods by changing an env var on the `redis` container, for example
  `TLS_CERT_REVISION`. Pod-template annotations do not work: the CRD drops
  pod-template metadata, so they never reach the operator.
- The cluster bus and replication are not encrypted (`tls-cluster no`,
  `tls-replication no`).

`TestE2E_ClientTLS` in `e2e/tls_test.go` covers this setup on KIND.
