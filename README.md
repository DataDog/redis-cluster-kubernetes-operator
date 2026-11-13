# redis-cluster-kubernetes-operator

A Kubernetes operator for coordinating the lifecycle and topology of Redis Cluster on Kubernetes.

The Redis operators we evaluated did not provide the same combination of Kubernetes lifecycle coordination, Redis Cluster topology management, and failure recovery capabilities.

> **Beta:** the initial compatibility target is Redis 8.6 on Kubernetes 1.33 only. Other versions have not been qualified.

## Features

- Redis Cluster creation, topology reconciliation, failover, and safe handling of terminating pods.
- Namespace-scoped installation and ordinary Kubernetes Service DNS by default.
- Optional Secret-backed Redis ACL authentication without placing the password in process arguments, URLs, logs, or ConfigMaps.
- Prometheus metrics and independent controller-runtime health endpoints.
- Provider-neutral autoscaling, lifecycle, and primary endpoint interfaces. The reference binary enables no external autoscaling or lifecycle provider.
- S3, Google Cloud Storage, and Azure Blob backup contracts with versioned, hashed manifests.

The API remains `cache.datadoghq.com/v1`, including its existing JSON fields, and the leader-election identity remains unchanged for compatibility.

## Local quick start

On macOS, start a Docker runtime with Colima, then create the local development
cluster:

```sh
colima start --cpu 4 --memory 8 --disk 30
go install sigs.k8s.io/kind@v0.30.0
export PATH="$(go env GOPATH)/bin:$PATH"
make local-up
export KUBECONFIG="$PWD/.local/kubeconfig"
```

The command builds the current checkout, installs the chart into a dedicated
one-node KIND cluster, applies the six-pod `cache` example, and waits for a
healthy Redis topology. It never uses the current context from the default
kubeconfig. See [`docs/getting-started.md`](docs/getting-started.md) for data,
restart, log, qualification, and cleanup commands.

## Set up with an AI agent

Clone this repository and ask a coding agent (Claude Code, Codex, Cursor):

> Install the redis-cluster operator from this repository using
> `skills/redis-cluster-setup/SKILL.md`. Create a cluster for a 10 GiB cache
> that must survive a pod loss, and verify it.

The agent confirms the kubeconfig context first, runs
`scripts/preflight.sh`, sizes the cluster with [docs/sizing.md](docs/sizing.md),
shows you the values before applying them, and finishes only when
`scripts/verify-cluster.sh` passes. For a read-only check of existing clusters
use `skills/redis-cluster-health/SKILL.md`.

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

The reference binary supports either no authentication (the default) or a
same-namespace Secret. Configure Redis itself with the same ACL user, then
create the Secret and reference its keys. Redis ACL usernames are identifiers,
not secret values; the helper passes the username to `redis-cli --user` and
supplies only the password through `REDISCLI_AUTH`. The following example reads
the values without putting the password in a process argument or either value
in a committed file; `jq` passes the generated manifest to `kubectl` over
standard input:

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

The Secret is mounted only into the toolbox. A fixed helper reads it inside the pod; the password is not sent in exec arguments.

## Integration defaults and embedding

The reference binary supplies the following defaults when an embedding
application omits an integration:

| `mains.ManageOptions` field | Default when omitted |
| --- | --- |
| `Metrics` | Prometheus on `MetricsAddr`, or no-op when `MetricsAddr` is `"0"` |
| `BackupBackendFactory` | The built-in S3, GCS, and Azure registry; an omitted `spec.backupBackend` selects AWS S3 |
| `AutoscalingProvider` | None; configured autoscaling reports `ProviderUnavailable` |
| `LifecycleAdapter` | None; the controller retains its provider-neutral Kubernetes termination safety |
| `PrimaryEndpointResolver` | Kubernetes Service DNS |

An embedding application can also wire those defaults explicitly:

```go
reporter, cleanup, err := metrics.CreatePrometheusWithCleanup(ctrlmetrics.Registry)
if err != nil {
	return err
}
defer cleanup()

err = mains.ManageWithOptions(scheme, log, mains.ManageOptions{
	Context:                 ctx,
	MetricsAddr:             ":8080",
	HealthProbeAddr:         ":8081",
	Metrics:                 reporter,
	BackupBackendFactory:    controller.NewBackupBackendRegistry(),
	PrimaryEndpointResolver: controller.KubernetesServiceDNSResolver{},
})
```

To add a backup implementation while retaining the built-ins, call
`Register` on `NewBackupBackendRegistry` during process startup and pass the
registry as `BackupBackendFactory`. The constructor has the signature
`func(location string) (controller.BackupProvider, error)`. To replace
metrics, pass any implementation of `metrics.Metrics`. The compile-checked
example in `mains/example_test.go` shows the complete explicit-default setup.

`AutoscalingProvider` supplies primary-count recommendations. An implementation
may also implement `AutoscalingWatchProvider` to register watches for resources
that it owns. `LifecycleAdapter` handles an optional external lifecycle
acknowledgement protocol. `PrimaryEndpointResolver` selects the hostname used by
the primary identity stability check; the built-in resolver uses
`<pod>.<service>.<namespace>.svc`. A resolver can disable hostname validation
when clients use pod IPs.

These are process startup integrations supplied through `ManageOptions`.
RedisCluster resources cannot provide implementation code or command templates.

## Metrics and health

Prometheus metrics are served on port `8080` by default. The optional metrics Service can be annotated through `operator.metrics.service.annotations`. Metric labels are bounded and do not include cluster, namespace, or pod names. Applications embedding the manager can supply any `metrics.Metrics` implementation and their own cancellation context through `mains.ManageWithOptions`, so Datadog, Prometheus, OpenTelemetry, StatsD, or another backend can use the same controller contract.

Liveness and readiness use a separate listener on port `8081`:

- `/healthz` reports process liveness.
- `/readyz` becomes successful after the controller manager has started its post-cache-sync runnables. It does not report the health of any Redis cluster.

Metrics can be disabled independently without disabling health probes.

## Autoscaling, lifecycle, and endpoint providers

The reference binary does not register DatadogMetric, WPA, or other external CRD schemes. An embedding application can register provider CRDs in the runtime scheme passed to `ManageWithOptions` and set `ManageOptions.AutoscalingProvider`. If autoscaling is configured without a provider, reconciliation continues and reports `ProviderUnavailable` in status. Kubernetes termination and drain safety remain active without `ManageOptions.LifecycleAdapter`; an adapter adds an external acknowledgement protocol. `ManageOptions.PrimaryEndpointResolver` supports environments whose Redis clients use a hostname other than Kubernetes Service DNS, or use pod IPs only.

## Backups and restore

Backups are beta and must remain disabled unless all prerequisites are supplied:

- For a built-in provider, set `backup.bucketName` and `spec.backupBackend`. The reference binary supports `s3`, `gcs`, and `azure`; AWS S3 is the default when the provider is omitted.
- Add a `backup` sidecar to the Redis StatefulSet template with the corresponding `aws`, `gcloud`, or `az` CLI and cloud identity/credentials.
- Mount the Redis data volume at the same path in the Redis and backup containers.
- Configure Redis with `appendonly no` before using RDB restore. The operator
  rejects restore before activation when AOF is enabled because Redis would load
  the AOF instead of the validated `dump.rdb`.
- Grant the workload identity permission to read, write, copy, list, and delete objects in the selected bucket or container.
- Complete the required security review of toolbox, authentication, backup artifact integrity, and credential handling.

Applications embedding the controller may inject a `controller.BackupBackendFactory` or use `controller.NewBackupBackendRegistry` to add another provider while retaining S3, GCS, and Azure. The factory returns a trusted in-process `controller.BackupProvider`; binding it to the pod executor produces a typed `controller.BackupStore`. The controller calls operations such as `UploadFile`, `DownloadFile`, `CopyPrefix`, `DeletePrefix`, and `Exists`, without accepting command templates from the RedisCluster resource. CLI-backed stores may translate those operations internally through the fixed-argv pod executor, which never adds shell wrapping or logs its arguments. Credentials must be supplied through mounted files or stdin, not argv.

Before using a custom provider name on an existing installation, apply the updated RedisCluster CRD. Helm does not replace CRDs from the chart's `crds/` directory during a normal upgrade.

Successful backups rotate exactly two generations: the new complete backup is promoted to `latest`, and the former `latest` becomes `previous`. Restore tries `latest` first and can fall back to `previous`. `spec.backupRetention` is a deprecated compatibility field: it is unsupported and ignored, so it does not change the fixed current-plus-previous retention behavior.

Object prefixes use `<namespace>/<RedisCluster name>/<generation>` so releases
in different namespaces cannot overwrite each other's backups. Backups created
by prerelease builds under the old name-only prefix are not discovered
automatically; set `spec.restoreFrom` to that exact legacy prefix for a one-time
restore, then let the operator write subsequent backups to the namespaced path.

Backup manifests are versioned and artifacts carry size and SHA-256 metadata; malformed or mismatched data fails closed. The manifest authentication decision (signature or MAC) is not frozen and remains a security-review gate.

Backup validation currently consists of unit and in-process integration
coverage. Backups are not qualified through the deployed-operator clean-room
or EKS suites in this cycle, so the project does not yet claim deployed backup
portability.

## Beta limitations

- Only Redis 8.6 and Kubernetes 1.33 are claimed as compatible.
- The repository does not publish an image or chart during Phase 1.
- The reference binary enables no autoscaling recommendation provider or provider-specific lifecycle adapter.
- Primary downscale is not supported in the initial public release. Lowering `spec.primaries` does not lower the retained effective target or remove Redis pods.
- Blue-green rollout qualification covers image changes. Other pod-template
  changes are detected and trigger the same rollout path, but do not yet have
  dedicated deployed-operator qualification scenarios.
- Backup deployment is not provided by the reference workload and requires the prerequisites above.
- The Phase 1 toolbox remains present. Its Redis operations are typed, but removal of the toolbox is future work.
- `RedisCluster` resources are namespaced. The CRD definition is cluster-wide and shared by all releases, so coordinate CRD upgrades when installing multiple namespace-scoped controllers.

## Documentation

- [Local quick start](docs/getting-started.md)
- [Redis workload contract](docs/workload-contract.md)
- [Operations guide](docs/operations.md)
- [RedisCluster API reference](docs/api-reference.md)
- [Sizing](docs/sizing.md)
- [Agent skills](AGENTS.md)
- [Example workload](examples/README.md)
- [Deployed-operator clean-room tests](test/blackbox/README.md)
- [In-process KIND tests](e2e/README.md)

## Development

```sh
go test ./...
go vet ./...
helm lint . --strict --kube-version 1.33.0
helm template redis-cluster-kubernetes-operator . --namespace redis-operator --kube-version 1.33.0
make verify-license-inventory
```

## Clean-room qualification

The public qualification path builds the operator from this repository,
creates a fresh Kubernetes 1.33 cluster, installs only this repository's
chart, and reconciles Redis 8.6 through the packaged operator Deployment. It
does not use dd-source, an internal chart, a private registry, or a
pre-existing operator installation.

Local prerequisites are Docker, Go, KIND v0.30.0 or newer, `kubectl`, and Helm 3. Install the same KIND version used by CI with `go install sigs.k8s.io/kind@v0.30.0`. From the repository root, the complete smoke path is one command:

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

See `CONTRIBUTING.md` and `SECURITY.md` before proposing changes. The project is licensed under Apache-2.0; third-party Go module dependencies used by repository packages are listed in `LICENSE-3rdparty.csv`.
