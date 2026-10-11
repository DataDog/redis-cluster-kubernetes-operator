# Local quick start

This guide creates a one-node KIND cluster, builds the operator from the current
checkout, installs the Helm chart, and creates a durable Redis Cluster with
three primaries and one replica per primary. The Kubernetes cluster has one
node to keep local resource use small; Redis still runs six pods so primary
replacement and replication can be exercised.

The helper uses a dedicated kubeconfig at `.local/kubeconfig`. It does not use
or modify the current context in the default kubeconfig.

## Prerequisites

- Go 1.25 or newer
- Docker, either through Colima or Docker Desktop
- KIND v0.30.0 or newer
- Helm 3
- `kubectl`
- at least 4 CPUs, 8 GiB of memory, and 20 GiB of free disk for the container VM

On macOS, Colima is sufficient:

```sh
colima start --cpu 4 --memory 8 --disk 30
docker info
```

Install the KIND version used by CI:

```sh
go install sigs.k8s.io/kind@v0.30.0
export PATH="$(go env GOPATH)/bin:$PATH"
kind version
```

## Create the cluster

From the repository root:

```sh
make local-up
export KUBECONFIG="$PWD/.local/kubeconfig"
```

Run `make local-up` again after code changes. It reuses the named KIND cluster,
rebuilds and reloads the local operator image, upgrades the Helm release,
reapplies the example, and waits for all 16,384 slots and the expected six
Redis nodes. If you changed the example topology, use `make local-down` first
to reset its retained topology state.

The helper records ownership under `.local/` when it creates the cluster. It
refuses to reuse or delete a same-name cluster created outside this checkout.
Set `LOCAL_KIND_CLUSTER` to another name instead of adopting an existing
cluster.

The expected resources are:

```sh
kubectl get rediscluster,statefulset,pods,pvc -n redis-cluster-example
```

The `RedisCluster` is named `cache`. Resource names do not require a
`redis-cluster-` prefix.

## Use Redis

Run `redis-cli` inside a Redis pod. The `-c` option follows Redis Cluster
`MOVED` and `ASK` redirects:

```sh
REDIS_POD="$(kubectl get pods -n redis-cluster-example \
  -l redis_cluster_name=cache \
  --field-selector status.phase=Running \
  -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
  | awk -F'|' '$2 == "" && $3 == "True" {print $1; exit}')"

kubectl exec -n redis-cluster-example "$REDIS_POD" -c redis -- \
  redis-cli -c SET hello world

kubectl exec -n redis-cluster-example "$REDIS_POD" -c redis -- \
  redis-cli -c GET hello
```

Inspect the topology:

```sh
kubectl exec -n redis-cluster-example "$REDIS_POD" -c redis -- \
  redis-cli --raw CLUSTER INFO

kubectl exec -n redis-cluster-example "$REDIS_POD" -c redis -- \
  redis-cli --raw CLUSTER NODES
```

A healthy reference cluster reports `cluster_state:ok`,
`cluster_slots_assigned:16384`, `cluster_known_nodes:6`, and `cluster_size:3`.

A simple port-forward is insufficient for Redis Cluster because Redis redirects
clients to individual node addresses. Run the test client inside Kubernetes, or
provide network and DNS access to every advertised Redis node.

## Exercise pod replacement

First inspect `CLUSTER NODES` to see which ordinals are primaries and replicas.
Then delete one pod and watch it return:

```sh
kubectl delete pod -n redis-cluster-example "$REDIS_POD"
kubectl get pods -n redis-cluster-example -w
```

After the replacement becomes ready, run the `REDIS_POD` discovery command
again before verifying the key and topology. Follow the operator while testing:

```sh
kubectl logs -n redis-cluster-example \
  -l app.kubernetes.io/component=operator \
  --all-containers=true -f
```

## Run release qualification

The interactive cluster above is intended for development. The clean-room
runner creates a fresh four-node cluster, executes assertions against the
packaged operator, and deletes the cluster afterward:

```sh
make cleanroom-smoke
```

Run every packaged-operator lane with:

```sh
ci/run-cleanroom.sh kind full
```

## Delete the local cluster

```sh
make local-down
colima stop
```

`colima stop` is optional when Colima is used for other local containers.

## Troubleshooting local setup

If the helper reports that Docker is unavailable, verify Colima and the Docker
context:

```sh
colima status
docker context show
docker info
```

If it rejects the KIND version, make sure the Go binary directory appears
before older package-manager installations in `PATH`:

```sh
export PATH="$(go env GOPATH)/bin:$PATH"
kind version
```

For Pending Redis pods, inspect events and confirm the container VM has enough
CPU, memory, and disk:

```sh
kubectl get events -n redis-cluster-example --sort-by=.lastTimestamp
kubectl describe pods -n redis-cluster-example -l redis_cluster_name=cache
```

Reset only the dedicated local environment with `make local-down`, then run
`make local-up` again.
