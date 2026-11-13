#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly RELEASE="chart-strategy-test"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/redis-chart-test.XXXXXX")"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Prints the operator Deployment's strategy type, or "default" when unset.
operator_strategy() {
  awk '
    /^kind: Deployment$/ { in_deployment = 1; operator = 0; next }
    /^---$/ { in_deployment = 0; operator = 0 }
    in_deployment && /^  name: chart-strategy-test-redis-cluster-kubernetes-operator$/ { operator = 1 }
    operator && /^  strategy:$/ { strategy = 1; next }
    operator && strategy && /^    type: / { print $2; found = 1; exit }
    END { if (!found) print "default" }
  ' "$1"
}

helm template "$RELEASE" "$REPO_ROOT" --kube-version 1.33.0 >"$TEMP_DIR/default.yaml"
got="$(operator_strategy "$TEMP_DIR/default.yaml")"
if [[ "$got" != "Recreate" ]]; then
  echo "operator without leader election must use Recreate, got: $got" >&2
  exit 1
fi

helm template "$RELEASE" "$REPO_ROOT" --kube-version 1.33.0 \
  --set operator.replicaCount=2 \
  --set operator.leaderElection.enabled=true \
  >"$TEMP_DIR/leader-election.yaml"
got="$(operator_strategy "$TEMP_DIR/leader-election.yaml")"
if [[ "$got" != "default" ]]; then
  echo "operator with leader election should keep the default rolling update, got: $got" >&2
  exit 1
fi
