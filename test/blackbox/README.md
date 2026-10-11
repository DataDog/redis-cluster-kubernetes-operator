# Deployed-operator clean-room qualification

This suite verifies the artifact an outside user installs. It starts with a fresh Kubernetes cluster, builds the operator image from this repository, installs only the root public Helm chart, applies the Redis 8.6 reference workload, and makes assertions through Kubernetes and Redis interfaces. It never starts a controller manager inside the test process.

The suite requires a dedicated kubeconfig created by the provider runner. Tests refuse ambient Kubernetes configuration. Failure collection requests an explicit allowlist of resource types, never Kubernetes Secrets or a namespace-wide dump, and redacts credential-shaped fields before CI considers an upload.

## KIND

Install Docker, Go, KIND v0.30.0 or newer, `kubectl`, and Helm 3. To match CI exactly, run `go install sigs.k8s.io/kind@v0.30.0`. Run commands from the repository root:

```sh
# Canonical public example, topology, durable data, and pod recreation.
ci/run-cleanroom.sh kind smoke

# Every lane below, sequentially, in one fresh cluster.
ci/run-cleanroom.sh kind full

# Independently runnable lanes; each command creates and deletes a fresh cluster.
ci/run-cleanroom.sh kind packaging
ci/run-cleanroom.sh kind lifecycle
ci/run-cleanroom.sh kind topology
ci/run-cleanroom.sh kind security
```

The runner creates one control-plane and three worker nodes on Kubernetes 1.33, builds and loads the operator image, installs the chart and reference workload, runs the selected tests with a bounded timeout, collects sanitized evidence after failure, and deletes the cluster through an exit trap. It refuses to reuse a cluster with the generated name.

Lane membership is defined in `ci/cleanroom-lanes.txt`:

| Lane | Deployed-operator behavior |
| --- | --- |
| `smoke` | Exact reference install, six Redis pods and PVCs, complete slot topology, typed data, persisted `nodes.conf`, and Redis pod recovery |
| `packaging` | Namespace isolation, namespace-scoped RBAC, leader-election failover, Helm upgrade and uninstall behavior |
| `lifecycle` | Primary and replica failure, terminating-primary and drain-candidate safety, reconciliation after operator restart |
| `topology` | Scale-up and retained-target downscale safety, blue-green availability and data, unavailable autoscaling provider status, standard Service DNS |
| `security` | Generated Secret authentication, fail-closed invalid authentication, probes, Prometheus metrics, service-account and RBAC boundaries, credential non-disclosure |

The `e2e/` suite is intentionally different: it runs the reconciler in the Go test process and remains valuable fast regression coverage. Passing it does not prove the chart or operator image installs. A clean-room pass always exercises the deployed operator pod built by that run.

## Continuous integration

Pull requests run Helm schema/lint checks, unit tests, the in-process smoke diagnostic, and `ci/run-cleanroom.sh kind smoke`. The daily schedule and manual **Test** workflow execute every clean-room KIND lane in a fresh cluster while retaining the in-process lanes as separately labeled regressions.

Failure artifacts are retained for 14 days only after an additional CI credential scan. They include bounded Kubernetes resources, events, pod descriptions and logs, Helm manifests and values, Redis topology observations, and test output. Secret objects and decoded Secret data are excluded. A failed scan suppresses the upload.

## EKS portability

The **EKS qualification** workflow is manual during Phase 1. It is not a pull-request or scheduled trigger. The qualification job is pinned to the protected `cleanroom-eks` environment; configure its required reviewers before dispatching the workflow.

Repository configuration must provide:

- `CLEANROOM_AWS_ACCOUNT_ID`: the exact 12-digit target account as a repository variable;
- `CLEANROOM_AWS_ROLE_ARN`: an AWS OIDC role ARN as a repository secret.
- an approved AWS region with at least three available zones, selected at dispatch.

These values are repository-level so the unconditional cleanup job can authenticate without waiting for a second protected-environment approval. The OIDC role needs the scoped EKS, CloudFormation, EC2/VPC, IAM pass-role and Pod Identity, ECR, EBS CSI, STS, and resource-tagging permissions exercised by `ci/cleanroom/eks.sh` and `ci/cleanroom/cleanup-eks.sh`. No application-level AWS permissions are needed because backup qualification is deferred.

The workflow creates an EKS 1.33 cluster with three AL2023 workers, EBS CSI and encrypted `gp3` storage, plus a private immutable ECR repository containing the exact commit image. It verifies install and EBS persistence, primary recovery, operator restart, blue-green rollout, authentication, health, metrics, and RBAC. The primary runner cleans through an exit trap; an independent `if: always()` job reconstructs the exact run state and invokes the ownership-checked cleanup script again. Cleanup refuses mismatched account, names, or ownership tags and fails if run-owned tagged resources remain.

EKS 1.33 is in extended support and has an extended-support charge. Confirm the current lifecycle before dispatching: https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html

## Qualification boundaries

- Redis 8.6 and Kubernetes 1.33 are the only claimed versions.
- Backups retain unit and in-process test coverage, but are not deployed-operator, KIND clean-room, or EKS qualified in this cycle.
- The tests report configured load, completed operations, failures, and value mismatches. They do not claim achieved QPS, latency, or maximum outage duration.
- No dd-source checkout, internal chart, private Datadog registry, DatadogMetric, WPA, Fabric hostname, DD.AUTH, or StatsD service is required.
