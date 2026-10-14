#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly DIGEST="sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
readonly REPOSITORY="ghcr.io/datadog/redis-cluster-kubernetes-operator"
readonly RELEASE="chart-image-test"
readonly NAMESPACE="chart-image-test"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/redis-chart-test.XXXXXX")"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

helm template "$RELEASE" "$REPO_ROOT" \
  --namespace "$NAMESPACE" \
  --kube-version 1.33.0 \
  --set-string operator.image.digest="$DIGEST" \
  --set-string operator.image.tag=must-not-render \
  --set operator.imagePullSecrets='{redis-operator-ghcr,secondary-pull-secret}' \
  >"$TEMP_DIR/rendered.yaml"

grep -Fq "image: \"$REPOSITORY@$DIGEST\"" "$TEMP_DIR/rendered.yaml"
if grep -Fq "$REPOSITORY:must-not-render" "$TEMP_DIR/rendered.yaml"; then
  echo "operator tag rendered even though a digest was configured" >&2
  exit 1
fi

awk '
  /^kind: Deployment$/ { in_deployment = 1; operator = 0; secrets = 0; next }
  /^---$/ { in_deployment = 0; operator = 0 }
  in_deployment && /^  name: chart-image-test-redis-cluster-kubernetes-operator$/ { operator = 1 }
  operator && /^      imagePullSecrets:$/ { secrets = 1; next }
  secrets && /^        - name: "redis-operator-ghcr"$/ { first = 1; next }
  secrets && /^        - name: "secondary-pull-secret"$/ { second = 1; next }
  END { exit !(first && second) }
' "$TEMP_DIR/rendered.yaml"

awk '
  /^kind: Deployment$/ { in_deployment = 1; toolbox = 0; next }
  /^---$/ { in_deployment = 0; toolbox = 0 }
  in_deployment && /^  name: chart-image-test-redis-cluster-kubernetes-operator-toolbox$/ { toolbox = 1 }
  toolbox && /^[[:space:]]*imagePullSecrets:$/ { found = 1 }
  END { exit found ? 1 : 0 }
' "$TEMP_DIR/rendered.yaml"

helm template "$RELEASE" "$REPO_ROOT" \
  --namespace "$NAMESPACE" \
  --kube-version 1.33.0 \
  >"$TEMP_DIR/default.yaml"
if grep -q '^[[:space:]]*imagePullSecrets:' "$TEMP_DIR/default.yaml"; then
  echo "imagePullSecrets rendered when no pull Secret was configured" >&2
  exit 1
fi

if helm template invalid-digest "$REPO_ROOT" \
  --kube-version 1.33.0 \
  --set-string operator.image.digest=sha256:not-a-digest \
  >"$TEMP_DIR/invalid-digest.out" 2>&1; then
  echo "invalid operator image digest passed schema validation" >&2
  exit 1
fi

if helm template invalid-secret "$REPO_ROOT" \
  --kube-version 1.33.0 \
  --set operator.imagePullSecrets='{Invalid_Secret}' \
  >"$TEMP_DIR/invalid-secret.out" 2>&1; then
  echo "invalid image pull Secret name passed schema validation" >&2
  exit 1
fi
