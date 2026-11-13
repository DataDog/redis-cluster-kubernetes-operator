#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Validate deploy/redis-cluster and keep its default render identical to
# examples/redis-cluster.yaml, so the chart and the raw example cannot drift.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CHART="$REPO_ROOT/deploy/redis-cluster"
readonly EXAMPLE="$REPO_ROOT/examples/redis-cluster.yaml"

helm lint "$CHART" --strict --kube-version 1.33.0
helm lint "$CHART" --strict --kube-version 1.33.0 \
  --set tls.enabled=true --set tls.secretName=cache-tls --set primaries=4
if helm template missing-tls-secret "$CHART" --kube-version 1.33.0 --set tls.enabled=true >/dev/null 2>&1; then
  echo "tls.enabled without tls.secretName passed validation" >&2
  exit 1
fi

helm template cache "$CHART" --namespace redis-cluster-example --kube-version 1.33.0 \
  | python3 -c '
import sys, yaml

def objects(stream):
    out = {}
    for doc in yaml.safe_load_all(stream):
        if not doc or doc["kind"] not in ("Service", "RedisCluster"):
            continue
        meta = doc["metadata"]
        meta.pop("namespace", None)
        labels = meta.get("labels", {})
        for key in ("app.kubernetes.io/managed-by", "helm.sh/chart"):
            labels.pop(key, None)
        if not labels:
            meta.pop("labels", None)
        out[doc["kind"]] = doc
    return out

rendered = objects(sys.stdin)
with open(sys.argv[1]) as f:
    example = objects(f)
if rendered != example:
    sys.exit("deploy/redis-cluster default render differs from " + sys.argv[1])
' "$EXAMPLE"
echo "instance chart OK"
