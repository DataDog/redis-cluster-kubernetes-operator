#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly LANE="${1:?clean-room lane is required}"
readonly MANIFEST="$REPO_ROOT/ci/cleanroom-lanes.txt"
readonly KIND_NODE_IMAGE="${CLEANROOM_KIND_NODE_IMAGE:-kindest/node:v1.33.1}"
readonly RUN_ID="${CLEANROOM_RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}"
readonly CLUSTER_NAME="redis-cleanroom-$RUN_ID"
readonly CONTEXT="kind-$CLUSTER_NAME"
readonly NAMESPACE="redis-cluster-example"
readonly RELEASE="redis-operator"
readonly IMAGE_REPOSITORY="redis-cluster-kubernetes-operator"
readonly IMAGE_TAG="$RUN_ID"
readonly OPERATOR_IMAGE="$IMAGE_REPOSITORY:$IMAGE_TAG"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/redis-cleanroom-kind.XXXXXX")"
readonly KUBECONFIG_PATH="$TEMP_DIR/kubeconfig"
readonly RESOURCE_STATE="$TEMP_DIR/resource-state"
readonly ARTIFACT_DIR="${CLEANROOM_ARTIFACT_DIR:-$REPO_ROOT/cleanroom-artifacts/$RUN_ID}"
cluster_owned=false

# shellcheck source=common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

cleanup() {
  local status=$?
  local cleanup_status=0
  trap - EXIT
  set +e
  if ((status != 0)) && [[ -f "$KUBECONFIG_PATH" ]]; then
    cleanroom_collect_failure_artifacts "$KUBECONFIG_PATH" "$ARTIFACT_DIR" "$NAMESPACE" || \
      echo "WARNING: failure artifact collection did not complete" >&2
    echo "Clean-room failure artifacts: $ARTIFACT_DIR" >&2
  fi
  if [[ "$cluster_owned" == true ]] && command -v kind >/dev/null 2>&1 && kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; then
    kind delete cluster --name "$CLUSTER_NAME" || cleanup_status=$?
  fi
  rm -f "$KUBECONFIG_PATH" "$TEMP_DIR/kind.yaml" "$RESOURCE_STATE"
  rmdir "$TEMP_DIR" 2>/dev/null || true
  if ((cleanup_status != 0)); then
    echo "ERROR: failed to delete KIND cluster '$CLUSTER_NAME'" >&2
    exit 1
  fi
  exit "$status"
}
trap cleanup EXIT

cleanroom_validate_run_id "$RUN_ID"
cleanroom_require_commands docker go helm kind kubectl
cleanroom_require_kind_version 0 30 0
"$REPO_ROOT/ci/cleanroom/verify-lanes.sh"

if kind get clusters 2>/dev/null | grep -Fxq "$CLUSTER_NAME"; then
  cleanroom_die "refusing to reuse existing KIND cluster '$CLUSTER_NAME'"
fi

cat >"$TEMP_DIR/kind.yaml" <<KIND_CONFIG
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
  - role: worker
KIND_CONFIG

kind create cluster \
  --name "$CLUSTER_NAME" \
  --image "$KIND_NODE_IMAGE" \
  --config "$TEMP_DIR/kind.yaml" \
  --kubeconfig "$KUBECONFIG_PATH"
# A failed create can lose a same-name race, so claim ownership only on success.
cluster_owned=true

export KUBECONFIG="$KUBECONFIG_PATH"
cleanroom_verify_context "$KUBECONFIG_PATH" "$CONTEXT"

docker build --tag "$OPERATOR_IMAGE" "$REPO_ROOT"
kind load docker-image --name "$CLUSTER_NAME" "$OPERATOR_IMAGE"
cleanroom_install_public_chart "$REPO_ROOT" "$NAMESPACE" "$RELEASE" "$IMAGE_REPOSITORY" "$IMAGE_TAG" Never

printf '%s\n' \
  "provider=kind" \
  "run_id=$RUN_ID" \
  "cluster_name=$CLUSTER_NAME" \
  "context=$CONTEXT" \
  "operator_image=$OPERATOR_IMAGE" \
  >"$RESOURCE_STATE"

export CLEANROOM_PROVIDER=kind
export CLEANROOM_RUN_ID="$RUN_ID"
export CLEANROOM_NAMESPACE="$NAMESPACE"
export CLEANROOM_RELEASE="$RELEASE"
export CLEANROOM_OPERATOR_IMAGE="$OPERATOR_IMAGE"
export CLEANROOM_ARTIFACT_DIR="$ARTIFACT_DIR"
export CLEANROOM_REPO_ROOT="$REPO_ROOT"
export CLEANROOM_RESOURCE_STATE="$RESOURCE_STATE"

if [[ "$LANE" == full ]]; then
  for lane_name in smoke packaging lifecycle topology security; do
    cleanroom_run_tests "$REPO_ROOT" "$(cleanroom_lane_selector "$MANIFEST" "$lane_name")"
  done
else
  cleanroom_run_tests "$REPO_ROOT" "$(cleanroom_lane_selector "$MANIFEST" "$LANE")"
fi
