#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CLUSTER_NAME="${LOCAL_KIND_CLUSTER:-redis-operator-dev}"
readonly KIND_NODE_IMAGE="${LOCAL_KIND_NODE_IMAGE:-kindest/node:v1.33.1}"
readonly NAMESPACE="redis-cluster-example"
readonly RELEASE="${LOCAL_HELM_RELEASE:-redis-operator}"
readonly IMAGE_REPOSITORY="redis-cluster-kubernetes-operator"
readonly IMAGE_TAG="${LOCAL_IMAGE_TAG:-dev-$(date -u +%Y%m%d%H%M%S)}"
readonly OPERATOR_IMAGE="$IMAGE_REPOSITORY:$IMAGE_TAG"
readonly KUBECONFIG_PATH="${LOCAL_KUBECONFIG:-$REPO_ROOT/.local/kubeconfig}"
readonly REDIS_CLUSTER_NAME="cache"
readonly EXPECTED_PODS=6

redis_topology_is_ready() {
  awk '
    BEGIN { valid = 1 }
    NF < 8 { valid = 0; next }
    {
      node_id = $1
      flags = $3
      primary_id = $4
      link_state = $8
      is_primary = 0
      is_replica = 0

      flag_count = split(flags, node_flags, ",")
      for (i = 1; i <= flag_count; i++) {
        if (node_flags[i] == "master" || node_flags[i] == "primary") {
          is_primary = 1
        } else if (node_flags[i] == "slave" || node_flags[i] == "replica") {
          is_replica = 1
        } else if (node_flags[i] == "fail" || node_flags[i] == "fail?") {
          valid = 0
        }
      }

      if (link_state != "connected" || is_primary == is_replica) {
        valid = 0
      } else if (is_primary) {
        primary_count++
        primaries[node_id] = 1
        if (primary_id != "-") {
          valid = 0
        }
      } else {
        replica_count++
        replica_primary[node_id] = primary_id
      }
      node_count++
    }
    END {
      for (replica in replica_primary) {
        primary = replica_primary[replica]
        if (!(primary in primaries)) {
          valid = 0
        } else {
          replicas_per_primary[primary]++
        }
      }
      for (primary in primaries) {
        if (replicas_per_primary[primary] != 1) {
          valid = 0
        }
      }
      exit !(valid && node_count == 6 && primary_count == 3 && replica_count == 3)
    }
  '
}

# shellcheck source=../ci/cleanroom/common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

cleanroom_require_commands docker go helm kind kubectl
cleanroom_require_kind_version 0 30 0
docker info >/dev/null 2>&1 || cleanroom_die "Docker is unavailable; start Colima or Docker Desktop"

mkdir -p "$(dirname "$KUBECONFIG_PATH")"

cluster_exists=false
while IFS= read -r existing; do
  if [[ "$existing" == "$CLUSTER_NAME" ]]; then
    cluster_exists=true
    break
  fi
done < <(kind get clusters 2>/dev/null || true)

if [[ "$cluster_exists" == false ]]; then
  kind_config="$(mktemp "${TMPDIR:-/tmp}/redis-operator-kind.XXXXXX")"
  trap 'rm -f "$kind_config"' EXIT
  cat >"$kind_config" <<'KIND_CONFIG'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
KIND_CONFIG
  kind create cluster \
    --name "$CLUSTER_NAME" \
    --image "$KIND_NODE_IMAGE" \
    --config "$kind_config" \
    --kubeconfig "$KUBECONFIG_PATH"
else
  kind export kubeconfig --name "$CLUSTER_NAME" --kubeconfig "$KUBECONFIG_PATH"
fi

export KUBECONFIG="$KUBECONFIG_PATH"
cleanroom_verify_context "$KUBECONFIG_PATH" "kind-$CLUSTER_NAME"

docker build --tag "$OPERATOR_IMAGE" "$REPO_ROOT"
kind load docker-image --name "$CLUSTER_NAME" "$OPERATOR_IMAGE"

helm upgrade --install "$RELEASE" "$REPO_ROOT" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --set-string operator.image.repository="$IMAGE_REPOSITORY" \
  --set-string operator.image.tag="$IMAGE_TAG" \
  --set-string operator.image.pullPolicy=Never \
  --wait \
  --timeout 10m

kubectl apply -f "$REPO_ROOT/examples/redis-cluster.yaml"

pod_deadline=$((SECONDS + 600))
while ((SECONDS < pod_deadline)); do
  pod_count="$(kubectl get pods --namespace "$NAMESPACE" \
    --selector "redis_cluster_name=$REDIS_CLUSTER_NAME" \
    --output name 2>/dev/null | wc -l | tr -d ' ')"
  if [[ "$pod_count" == "$EXPECTED_PODS" ]]; then
    break
  fi
  sleep 2
done

[[ "${pod_count:-0}" == "$EXPECTED_PODS" ]] || cleanroom_die \
  "expected $EXPECTED_PODS Redis pods, found ${pod_count:-0}"

kubectl wait --namespace "$NAMESPACE" \
  --for=condition=Ready \
  --selector "redis_cluster_name=$REDIS_CLUSTER_NAME" \
  pod \
  --timeout 10m

redis_pod="$(kubectl get pods --namespace "$NAMESPACE" \
  --selector "redis_cluster_name=$REDIS_CLUSTER_NAME" \
  --field-selector status.phase=Running \
  --output jsonpath='{range .items[*]}{.metadata.name}{"|"}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
  | awk -F'|' '$2 == "" && $3 == "True" {print $1; exit}')"

[[ -n "$redis_pod" ]] || cleanroom_die "could not find a current Ready Redis pod"

topology_deadline=$((SECONDS + 600))
topology_ready=false
while ((SECONDS < topology_deadline)); do
  cluster_info="$(kubectl exec --namespace "$NAMESPACE" "$redis_pod" \
    --container redis -- redis-cli --raw CLUSTER INFO 2>/dev/null | tr -d '\r' || true)"
  cluster_nodes="$(kubectl exec --namespace "$NAMESPACE" "$redis_pod" \
    --container redis -- redis-cli --raw CLUSTER NODES 2>/dev/null | tr -d '\r' || true)"
  cluster_state="$(awk -F: '$1 == "cluster_state" {print $2}' <<<"$cluster_info")"
  assigned_slots="$(awk -F: '$1 == "cluster_slots_assigned" {print $2}' <<<"$cluster_info")"
  known_nodes="$(awk -F: '$1 == "cluster_known_nodes" {print $2}' <<<"$cluster_info")"
  cluster_size="$(awk -F: '$1 == "cluster_size" {print $2}' <<<"$cluster_info")"
  if [[ "$cluster_state" == ok && "$assigned_slots" == 16384 && "$known_nodes" == 6 && "$cluster_size" == 3 ]] \
    && redis_topology_is_ready <<<"$cluster_nodes"; then
    topology_ready=true
    break
  fi
  sleep 2
done

if [[ "$topology_ready" != true ]]; then
  kubectl get pods --namespace "$NAMESPACE" --output wide >&2 || true
  printf '%s\n' "${cluster_nodes:-}" >&2
  cleanroom_die "Redis topology did not become ready within 10 minutes"
fi

cat <<EOF
Local Redis Cluster is ready.

Use this dedicated kubeconfig:
  export KUBECONFIG="$KUBECONFIG_PATH"

Inspect the workload:
  kubectl get rediscluster,pods,pvc -n $NAMESPACE
  kubectl exec -n $NAMESPACE $redis_pod -c redis -- redis-cli --raw CLUSTER INFO
  kubectl exec -n $NAMESPACE $redis_pod -c redis -- redis-cli --raw CLUSTER NODES

Write and read through Redis Cluster redirection:
  kubectl exec -n $NAMESPACE $redis_pod -c redis -- redis-cli -c SET hello world
  kubectl exec -n $NAMESPACE $redis_pod -c redis -- redis-cli -c GET hello

Follow operator logs:
  kubectl logs -n $NAMESPACE -l app.kubernetes.io/component=operator --all-containers=true -f

Delete the local cluster:
  make local-down
EOF
