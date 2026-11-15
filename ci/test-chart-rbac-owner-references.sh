#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# The operator sets controller ownerReferences (blockOwnerDeletion: true) from
# each RedisCluster to the StatefulSets and Services it creates. Clusters that
# enable the OwnerReferencesPermissionEnforcement admission plugin (OpenShift
# by default) reject those objects unless the operator can update the owner's
# finalizers subresource.

set -euo pipefail

# shellcheck disable=SC2155 # keep readonly-at-assignment; splitting it would change set -e failure behavior.
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly RELEASE="chart-rbac-test"
# shellcheck disable=SC2155 # keep readonly-at-assignment; splitting it would change set -e failure behavior.
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/redis-chart-rbac.XXXXXX")"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Prints the verbs of the operator Role rule for redisclusters/finalizers.
finalizer_verbs() {
  awk '
    /^kind: Role$/ { in_role = 1; next }
    /^---$/ { in_role = 0 }
    in_role && /resources: \["redisclusters\/finalizers"\]/ { rule = 1; next }
    in_role && rule && /verbs:/ { print; exit }
  ' "$1"
}

check() {
  local name="$1"
  shift
  helm template "$RELEASE" "$REPO_ROOT" --kube-version 1.33.0 "$@" >"$TEMP_DIR/$name.yaml"
  local verbs
  verbs="$(finalizer_verbs "$TEMP_DIR/$name.yaml")"
  if [[ "$verbs" != *'"update"'* ]]; then
    echo "$name: operator Role must allow update on redisclusters/finalizers, got: ${verbs:-<no rule>}" >&2
    exit 1
  fi
}

check default
check leader-election --set operator.replicaCount=2 --set operator.leaderElection.enabled=true
