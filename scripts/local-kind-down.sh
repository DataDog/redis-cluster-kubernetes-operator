#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CLUSTER_NAME="${LOCAL_KIND_CLUSTER:-redis-operator-dev}"
readonly KUBECONFIG_PATH="${LOCAL_KUBECONFIG:-$REPO_ROOT/.local/kubeconfig}"

if ! command -v kind >/dev/null 2>&1; then
  echo "ERROR: required command 'kind' is not on PATH" >&2
  exit 1
fi

kind delete cluster --name "$CLUSTER_NAME"
rm -f "$KUBECONFIG_PATH"
echo "Deleted local KIND cluster '$CLUSTER_NAME'."
