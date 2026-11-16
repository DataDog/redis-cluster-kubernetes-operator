#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail
: "${VERSION:?}" "${DIGEST:?}" "${RUNNER_TEMP:?}"

# Package a copy so the checkout stays untouched on retries.
chart_dir="$(mktemp -d "$RUNNER_TEMP/operator-chart.XXXXXX")"
trap 'rm -rf "$chart_dir"' EXIT
helm package . --version "$VERSION" --app-version "$VERSION" --destination "$chart_dir"
tar -xzf "$chart_dir/redis-cluster-kubernetes-operator-$VERSION.tgz" -C "$chart_dir"
python3 - "$chart_dir/redis-cluster-kubernetes-operator/values.yaml" <<'PY'
import os, sys, yaml
path = sys.argv[1]
with open(path) as source:
    values = yaml.safe_load(source)
values["operator"]["image"].update(tag=os.environ["VERSION"], digest=os.environ["DIGEST"])
with open(path, "w") as output:
    yaml.safe_dump(values, output, sort_keys=False)
PY
mkdir -p "$RUNNER_TEMP/charts"
helm package "$chart_dir/redis-cluster-kubernetes-operator" --destination "$RUNNER_TEMP/charts"
helm package deploy/redis-cluster --version "$VERSION" --destination "$RUNNER_TEMP/charts"
cp crds/redis-cluster-v1.yaml "$RUNNER_TEMP/crds.yaml"
{
  cat crds/redis-cluster-v1.yaml
  echo "---"
  helm template redis-cluster-operator "$RUNNER_TEMP/charts/redis-cluster-kubernetes-operator-$VERSION.tgz" \
    --namespace redis-cluster-operator --set namespace.create=true --kube-version 1.33.0
} > "$RUNNER_TEMP/install.yaml"
