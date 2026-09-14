# redis-cluster-kubernetes-operator

A Kubernetes operator for coordinating the lifecycle and topology of Redis Cluster on Kubernetes.

The Redis operators we evaluated did not provide the same combination of Kubernetes lifecycle coordination, Redis Cluster topology management, and failure recovery capabilities.

> **Beta:** the initial compatibility target is Redis 8.6 on Kubernetes 1.33 only. Other versions have not been qualified.

## Features

- Redis Cluster creation, topology reconciliation, failover, and safe handling of terminating pods.
- Namespace-scoped installation and ordinary Kubernetes Service DNS by default.
- Optional Secret-backed Redis ACL authentication without placing credentials in process arguments, URLs, logs, or ConfigMaps.
- Prometheus metrics and independent controller-runtime health endpoints.
- Provider-neutral autoscaling and lifecycle interfaces. No external provider is enabled by the public startup path.
- S3, Google Cloud Storage, and Azure Blob backup contracts with versioned, hashed manifests.

The API remains `cache.datadoghq.com/v1`, including its existing JSON fields, and the leader-election identity remains unchanged for compatibility.

## Install

Prerequisites are Helm 3 and a Kubernetes 1.33 cluster. Until a release image is published, build and load the operator image into your registry or local cluster:

```sh
docker build -t redis-cluster-kubernetes-operator:dev .
kind load docker-image redis-cluster-kubernetes-operator:dev
helm install redis-cluster-kubernetes-operator . \
  --namespace redis-operator \
  --create-namespace \
  --set operator.image.repository=redis-cluster-kubernetes-operator \
  --set operator.image.tag=dev \
  --set operator.image.pullPolicy=Never
```

The chart installs one namespace-scoped operator and one toolbox Deployment. They use separate ServiceAccounts; the toolbox has no Kubernetes API permissions. The operator retains only the pod-exec permission required by the Phase 1 typed toolbox.

Apply the durable reference workload after installing the chart into its namespace:

```sh
helm install redis-cluster-kubernetes-operator . \
  --namespace redis-cluster-example \
  --create-namespace \
  --set operator.image.repository=redis-cluster-kubernetes-operator \
  --set operator.image.tag=dev \
  --set operator.image.pullPolicy=Never
kubectl apply -f examples/redis-cluster.yaml
```

See `examples/README.md` for its PVC-backed `nodes.conf` layout. Backups are disabled by default.

## Redis authentication

The public startup supports either no authentication (the default) or a same-namespace Secret. Configure Redis itself with the same ACL user, then create the Secret and reference its keys:

```sh
kubectl -n redis-operator create secret generic redis-auth \
  --from-literal=username=operator \
  --from-literal=password='replace-me'

helm upgrade --install redis-cluster-kubernetes-operator . \
  --namespace redis-operator \
  --set operator.image.repository=redis-cluster-kubernetes-operator \
  --set operator.image.tag=dev \
  --set operator.image.pullPolicy=Never \
  --set operator.redisAuth.secretName=redis-auth \
  --set operator.redisAuth.usernameKey=username \
  --set operator.redisAuth.passwordKey=password
```

The Secret is mounted only into the toolbox. A fixed helper reads it inside the pod; credentials are not sent in exec arguments.

## Metrics and health

Prometheus metrics are served on port `8080` by default. The optional metrics Service can be annotated through `operator.metrics.service.annotations`. Metric labels are bounded and do not include cluster, namespace, or pod names.

Liveness and readiness use a separate listener on port `8081`:

- `/healthz` reports process liveness.
- `/readyz` becomes successful after the controller manager has started its post-cache-sync runnables. It does not report the health of any Redis cluster.

Metrics can be disabled independently without disabling health probes.

## Autoscaling and lifecycle providers

The public process does not register DatadogMetric, WPA, or other external CRD schemes. If autoscaling is configured without an injected recommendation provider, reconciliation continues and reports `ProviderUnavailable` in status. Kubernetes termination and drain safety remain active; provider-specific lifecycle acknowledgement is opt-in and is not wired by the public startup.

## Backups and restore

Backups are beta and must remain disabled unless all prerequisites are supplied:

- Set `backup.bucketName` and `spec.backupBackend` to `s3`, `gcs`, or `azure`.
- Add a `backup` sidecar to the Redis StatefulSet template with the corresponding `aws`, `gcloud`, or `az` CLI and cloud identity/credentials.
- Mount the Redis data volume at the same path in the Redis and backup containers.
- Grant the workload identity permission to read, write, copy, list, and delete objects in the selected bucket or container.
- Complete the required security review of toolbox, authentication, backup artifact integrity, and credential handling.

Successful backups rotate exactly two generations: the new complete backup is promoted to `latest`, and the former `latest` becomes `previous`. Restore tries `latest` first and can fall back to `previous`. `spec.backupRetention` is a deprecated compatibility field: it is unsupported and ignored, so it does not change the fixed current-plus-previous retention behavior.

Backup manifests are versioned and artifacts carry size and SHA-256 metadata; malformed or mismatched data fails closed. The manifest authentication decision (signature or MAC) is not frozen and remains a security-review gate.

## Beta limitations

- Only Redis 8.6 and Kubernetes 1.33 are claimed as compatible.
- The repository does not publish an image or chart during Phase 1.
- No autoscaling recommendation provider or provider-specific lifecycle adapter is enabled by public startup.
- Backup deployment is not provided by the reference workload and requires the prerequisites above.
- The Phase 1 toolbox remains present. Its Redis operations are typed, but removal of the toolbox is future work.
- The CRD is cluster-scoped while each controller watches one namespace. Coordinate CRD lifecycle when installing multiple releases.

## Development

```sh
go test ./...
go vet ./...
helm lint . --strict --kube-version 1.33.0
helm template redis-cluster-kubernetes-operator . --namespace redis-operator --kube-version 1.33.0
```

See `CONTRIBUTING.md` and `SECURITY.md` before proposing changes. The project is licensed under Apache-2.0; third-party production dependencies are listed in `LICENSE-3rdparty.csv`.
