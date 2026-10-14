#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CLUSTER_NAME="${LOCAL_KIND_CLUSTER:-redis-operator-dev}"
readonly KUBECONFIG_PATH="${LOCAL_KUBECONFIG:-$REPO_ROOT/.local/kubeconfig}"
readonly OWNERSHIP_PATH="${LOCAL_KIND_OWNERSHIP_FILE:-$REPO_ROOT/.local/kind-owner}"

# shellcheck source=local-kind-ownership.sh
source "$REPO_ROOT/scripts/local-kind-ownership.sh"

for command_name in docker kind; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "ERROR: required command '$command_name' is not on PATH" >&2
    exit 1
  fi
done

cluster_exists=false
while IFS= read -r existing; do
  if [[ "$existing" == "$CLUSTER_NAME" ]]; then
    cluster_exists=true
    break
  fi
done < <(kind get clusters 2>/dev/null || true)

if [[ "$cluster_exists" == false ]]; then
  rm -f "$KUBECONFIG_PATH" "$OWNERSHIP_PATH"
  echo "Local KIND cluster '$CLUSTER_NAME' does not exist."
  exit 0
fi

if ! local_kind_is_owned "$CLUSTER_NAME" "$OWNERSHIP_PATH"; then
  echo "ERROR: refusing to delete unowned KIND cluster '$CLUSTER_NAME'" >&2
  exit 1
fi

kind delete cluster --name "$CLUSTER_NAME"
rm -f "$KUBECONFIG_PATH" "$OWNERSHIP_PATH"
echo "Deleted local KIND cluster '$CLUSTER_NAME'."
