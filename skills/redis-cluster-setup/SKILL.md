---
name: redis-cluster-setup
description: Install the Datadog redis-cluster-kubernetes-operator and create a Redis Cluster on Kubernetes, sized from the user's requirements, then prove it serves traffic. Use when the user asks to install, set up, deploy or try this operator or a RedisCluster.
---

# Set up the operator and a Redis Cluster

Work from a checkout of
https://github.com/DataDog/redis-cluster-kubernetes-operator. Every step is
idempotent; on failure fix the cause and rerun the same step.

## 0. Confirm the target

Run `kubectl config current-context` and show it to the user. Do not continue,
and never switch context, until the user confirms it. Then agree on:

- namespace (operator and cache share it; the operator watches only its own namespace)
- cluster name
- sizing inputs: dataset size, peak ops/s, write-heavy (yes/no/unsure), survive a pod loss (yes/no), persist data (yes/no)
- client TLS (yes/no)

## 1. Preflight

```sh
scripts/preflight.sh --namespace NS --json
```

`fail` blocks the install: report `detail` and `fix` to the user and stop.
`todo` items are the install steps below. `warn` needs the user's decision.

## 2. CRD

Skip when preflight reports `crd` as `pass`.

```sh
kubectl apply --server-side -f crds/redis-cluster-v1.yaml
```

Helm never upgrades CRDs, so rerun this on every operator upgrade.

## 3. Operator

From a release, when `helm show chart oci://ghcr.io/datadog/charts/redis-cluster-kubernetes-operator`
succeeds. A `403` or `not found` means no public release: build from source.

```sh
helm upgrade --install redis-cluster-operator \
  oci://ghcr.io/datadog/charts/redis-cluster-kubernetes-operator \
  --namespace NS --create-namespace --wait --timeout 10m
```

Without a release, build the image and make it pullable by the cluster. Ask
the user for a registry; never push to one they did not name. On KIND use
`kind load docker-image` instead of a registry. Use the commit as `TAG`
(`TAG=$(git rev-parse --short HEAD)`) so each build gets its own tag.

```sh
docker build -t REGISTRY/redis-cluster-kubernetes-operator:TAG .
docker push REGISTRY/redis-cluster-kubernetes-operator:TAG
helm upgrade --install redis-cluster-operator . --namespace NS --create-namespace --wait --timeout 10m \
  --set-string operator.image.repository=REGISTRY/redis-cluster-kubernetes-operator \
  --set-string operator.image.tag=TAG
```

On KIND, skip the registry and name the KIND cluster:

```sh
docker build -t redis-cluster-kubernetes-operator:TAG .
kind load docker-image --name KIND_CLUSTER redis-cluster-kubernetes-operator:TAG
helm upgrade --install redis-cluster-operator . --namespace NS --create-namespace --wait --timeout 10m \
  --set-string operator.image.repository=redis-cluster-kubernetes-operator \
  --set-string operator.image.tag=TAG \
  --set-string operator.image.pullPolicy=Never
```

If the operator pod reports `ErrImageNeverPull`, check the image reached the
node: `docker exec KIND_CLUSTER-control-plane crictl images | grep redis-cluster`.

Rerun preflight; `operator` must be `pass`. The chart also runs a toolbox
Deployment (`app.kubernetes.io/component=toolbox`), so expect two pods.

## 4. Size

Turn the sizing inputs into values with `docs/sizing.md`. Always set
`--maxmemory` and `--maxmemory-policy` through `extraArgs`. Write the values
to `NAME-values.yaml` (not `values.yaml`, which belongs to the operator
chart), show it to the user with the pod count and the rollout peak
(`2 × primaries × (1 + replicas)` pods), and wait for approval.

For client TLS, create the certificate Secret first (`docs/tls.md`) and set
`tls.enabled=true` and `tls.secretName`.

## 5. Create the cluster

```sh
helm upgrade --install NAME deploy/redis-cluster --namespace NS -f NAME-values.yaml
```

## 6. Verify

```sh
scripts/verify-cluster.sh --namespace NS --name NAME --json
```

It waits up to 10 minutes (`--timeout`); a new cluster usually converges in
1–2. `ok: true` is the only proof of success. Otherwise report the first non-`pass`
check with its `detail` and `fix`. Do not retry by deleting resources.

## 7. Hand over

Give the user the Service name `NAME.NS.svc`, port `6379` (and the TLS port if
enabled), and that clients must be cluster-aware (`redis-cli -c`, or a Redis
Cluster client). Point them to `docs/operations.md`.

## Rules

- Never delete a RedisCluster, PVC, StatefulSet or namespace, and never edit
  `status`. Those lose data or fight the operator.
- Change the cache through the RedisCluster (or the chart), never by editing
  the StatefulSets or pods the operator owns.
- A pod-template change starts a blue/green rollout that doubles pods; tell the
  user before applying one. Changing `primaries` reshards in place.
- The CRD drops pod-template annotations and labels. To force a rollout,
  change an env var on the `redis` container.
- Run `scripts/verify-cluster.sh` after every change.
