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

The public startup supports either no authentication (the default) or a
same-namespace Secret. Configure Redis itself with the same ACL user, then
create the Secret and reference its keys. The following example reads the
values without putting either credential in a process argument or a committed
file; `jq` passes the generated manifest to `kubectl` over standard input:

```sh
read -r -p 'Redis ACL username: ' REDIS_USERNAME
read -r -s -p 'Redis ACL password: ' REDIS_PASSWORD
printf '\n'
REDIS_USERNAME="$REDIS_USERNAME" REDIS_PASSWORD="$REDIS_PASSWORD" jq -n '{
  apiVersion: "v1",
  kind: "Secret",
  metadata: {name: "redis-auth", namespace: "redis-operator"},
  type: "Opaque",
  stringData: {username: env.REDIS_USERNAME, password: env.REDIS_PASSWORD}
}' | kubectl apply -f -
unset REDIS_USERNAME REDIS_PASSWORD

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

## Integration defaults and embedding

The reference binary supplies usable defaults when an embedding application
does not override either integration:

| `mains.ManageOptions` field | Default when omitted |
| --- | --- |
| `Metrics` | Prometheus on `MetricsAddr`, or no-op when `MetricsAddr` is `"0"` |
| `BackupBackendFactory` | The built-in S3, GCS, and Azure registry; an omitted `spec.backupBackend` selects AWS S3 |

An embedding application can also wire those defaults explicitly:

```go
reporter, cleanup, err := metrics.CreatePrometheusWithCleanup(ctrlmetrics.Registry)
if err != nil {
	return err
}
defer cleanup()

err = mains.ManageWithOptions(scheme, log, mains.ManageOptions{
	Context:              ctx,
	MetricsAddr:          ":8080",
	HealthProbeAddr:      ":8081",
	Metrics:              reporter,
	BackupBackendFactory: controller.NewBackupBackendRegistry(),
})
```

To add a backup implementation while retaining the built-ins, call
`Register` on `NewBackupBackendRegistry` during process startup and pass the
registry as `BackupBackendFactory`. The constructor has the signature
`func(location string) (controller.BackupProvider, error)`. To replace
metrics, pass any implementation of `metrics.Metrics`. The compile-checked
example in `mains/example_test.go` shows the complete explicit-default setup.

## Metrics and health

Prometheus metrics are served on port `8080` by default. The optional metrics Service can be annotated through `operator.metrics.service.annotations`. Metric labels are bounded and do not include cluster, namespace, or pod names. Applications embedding the manager can supply any `metrics.Metrics` implementation and their own cancellation context through `mains.ManageWithOptions`, so Datadog, Prometheus, OpenTelemetry, StatsD, or another backend can use the same controller contract.

Liveness and readiness use a separate listener on port `8081`:

- `/healthz` reports process liveness.
- `/readyz` becomes successful after the controller manager has started its post-cache-sync runnables. It does not report the health of any Redis cluster.

Metrics can be disabled independently without disabling health probes.

## Autoscaling and lifecycle providers

The public process does not register DatadogMetric, WPA, or other external CRD schemes. If autoscaling is configured without an injected recommendation provider, reconciliation continues and reports `ProviderUnavailable` in status. Kubernetes termination and drain safety remain active; provider-specific lifecycle acknowledgement is opt-in and is not wired by the public startup.

## Backups and restore

Backups are beta and must remain disabled unless all prerequisites are supplied:

- For a built-in provider, set `backup.bucketName` and `spec.backupBackend`. The reference binary supports `s3`, `gcs`, and `azure`; AWS S3 is the default when the provider is omitted.
- Add a `backup` sidecar to the Redis StatefulSet template with the corresponding `aws`, `gcloud`, or `az` CLI and cloud identity/credentials.
- Mount the Redis data volume at the same path in the Redis and backup containers.
- Grant the workload identity permission to read, write, copy, list, and delete objects in the selected bucket or container.
- Complete the required security review of toolbox, authentication, backup artifact integrity, and credential handling.

Applications embedding the controller may inject a `controller.BackupBackendFactory` or use `controller.NewBackupBackendRegistry` to add another provider while retaining S3, GCS, and Azure. The factory returns a trusted in-process `controller.BackupProvider`; binding it to the pod executor produces a typed `controller.BackupStore`. The controller calls operations such as `UploadFile`, `DownloadFile`, `CopyPrefix`, `DeletePrefix`, and `Exists`, without accepting command templates from the RedisCluster resource. CLI-backed stores may translate those operations internally through the fixed-argv pod executor, which never adds shell wrapping or logs its arguments. Credentials must be supplied through mounted files or stdin, not argv.

Before using a custom provider name on an existing installation, apply the updated RedisCluster CRD. Helm does not replace CRDs from the chart's `crds/` directory during a normal upgrade.

Successful backups rotate exactly two generations: the new complete backup is promoted to `latest`, and the former `latest` becomes `previous`. Restore tries `latest` first and can fall back to `previous`. `spec.backupRetention` is a deprecated compatibility field: it is unsupported and ignored, so it does not change the fixed current-plus-previous retention behavior.

Backup manifests are versioned and artifacts carry size and SHA-256 metadata; malformed or mismatched data fails closed. The manifest authentication decision (signature or MAC) is not frozen and remains a security-review gate.

Backup validation currently consists of unit and in-process integration
coverage. Backups are not qualified through the deployed-operator clean-room
or EKS suites in this cycle, so the project does not yet claim deployed backup
portability.

## Beta limitations

- Only Redis 8.6 and Kubernetes 1.33 are claimed as compatible.
- The repository does not publish an image or chart during Phase 1.
- No autoscaling recommendation provider or provider-specific lifecycle adapter is enabled by public startup.
- Primary downscale is not supported in the initial public release. Lowering `spec.primaries` does not lower the retained effective target or remove Redis pods.
- Blue-green rollout qualification covers image changes. Resource-only pod template changes are not currently included in the rollout hash.
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

## Clean-room qualification

The public qualification path builds the operator from this repository,
creates a fresh Kubernetes 1.33 cluster, installs only this repository's
chart, and reconciles Redis 8.6 through the packaged operator Deployment. It
does not use dd-source, an internal chart, a private registry, or a
pre-existing operator installation.

Local prerequisites are Docker, Go, KIND, `kubectl`, and Helm 3. From the repository root, the complete smoke path is one command:

```sh
ci/run-cleanroom.sh kind smoke
```

The runner creates a dedicated four-node KIND cluster, builds and loads the
operator image, installs the chart and reference workload, verifies Redis
topology and durable data, and deletes the cluster even when a check fails.
Run all deployed-operator scenarios with `ci/run-cleanroom.sh kind full`, or
select `packaging`, `lifecycle`, `topology`, or `security` in place of `full`.

The tests under `e2e/` remain useful reconciler regression and diagnostic
tests, but they run the manager inside the Go test process. They are not
evidence that the packaged chart and operator Deployment work. See
`test/blackbox/README.md` and `e2e/README.md` for the exact coverage and CI
model.

The EKS 1.33 portability gate is manual during Phase 1 because it creates
chargeable resources. Kubernetes 1.33 is in EKS extended support and incurs
extended-support charges; check the current lifecycle before running it:
https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html

The workflow creates a run-scoped EKS cluster, VPC, managed node group,
EBS-backed storage, and private immutable ECR repository. Both the runner and
an independent final job invoke ownership-checked cleanup. Configure the
protected `cleanroom-eks` GitHub environment, repository variable
`CLEANROOM_AWS_ACCOUNT_ID`, and repository OIDC role secret
`CLEANROOM_AWS_ROLE_ARN`, then manually dispatch **EKS qualification**. The AWS
configuration is repository-level so the unconditional cleanup job can
authenticate without a second environment approval. The role needs the scoped
EKS, CloudFormation, EC2/VPC, IAM pass-role and Pod Identity, ECR, EBS CSI,
STS, and resource-tagging permissions used by the runner and cleanup audit.

Qualification reports configured workload and operation totals only. It makes no achieved-QPS, latency, or maximum-outage claim.

See `CONTRIBUTING.md` and `SECURITY.md` before proposing changes. The project is licensed under Apache-2.0; third-party production dependencies are listed in `LICENSE-3rdparty.csv`.
