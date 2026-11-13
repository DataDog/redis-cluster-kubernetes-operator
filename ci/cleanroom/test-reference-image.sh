#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/reference-image-test.XXXXXX")"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# shellcheck source=common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

cleanroom_render_reference_workload \
  "$REPO_ROOT/examples/redis-cluster.yaml" \
  "valkey/valkey:8.1.11" \
  >"$TEMP_DIR/valkey.yaml"

grep -F -q 'image: valkey/valkey:8.1.11' "$TEMP_DIR/valkey.yaml"
if grep -F -q 'image: redis:8.6.0' "$TEMP_DIR/valkey.yaml"; then
  echo "rendered Valkey workload retained the Redis image" >&2
  exit 1
fi

cleanroom_render_reference_workload \
  "$REPO_ROOT/examples/redis-cluster.yaml" \
  "redis:8.6.0" \
  >"$TEMP_DIR/redis.yaml"
cmp -s "$REPO_ROOT/examples/redis-cluster.yaml" "$TEMP_DIR/redis.yaml" || {
  echo "rendering the default Redis image changed the reference manifest" >&2
  exit 1
}

if (cleanroom_render_reference_workload \
  "$REPO_ROOT/examples/redis-cluster.yaml" \
  'invalid image' \
  >"$TEMP_DIR/invalid.yaml" 2>"$TEMP_DIR/invalid.stderr"); then
  echo "invalid workload image was accepted" >&2
  exit 1
fi
grep -F -q 'invalid CLEANROOM_REDIS_IMAGE' "$TEMP_DIR/invalid.stderr"

echo "Reference workload image tests passed"
