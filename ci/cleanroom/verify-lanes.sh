#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly MANIFEST="$REPO_ROOT/ci/cleanroom-lanes.txt"
readonly EXPECTED_LANES="smoke packaging lifecycle topology security"

actual_lanes="$(awk '!/^#/ && NF {print $1}' "$MANIFEST" | tr '\n' ' ' | sed 's/ $//')"
[[ "$actual_lanes" == "$EXPECTED_LANES" ]] || {
  echo "unexpected clean-room lanes: $actual_lanes" >&2
  exit 1
}

duplicate_lane="$(awk '!/^#/ && NF {print $1}' "$MANIFEST" | sort | uniq -d | head -n 1)"
[[ -z "$duplicate_lane" ]] || { echo "duplicate lane: $duplicate_lane" >&2; exit 1; }

duplicate_test="$(awk '!/^#/ && NF {for (i=2; i<=NF; i++) print $i}' "$MANIFEST" | sort | uniq -d | head -n 1)"
[[ -z "$duplicate_test" ]] || { echo "test appears in multiple lanes: $duplicate_test" >&2; exit 1; }

while IFS= read -r test_name; do
  [[ "$test_name" =~ ^TestCleanroom_[A-Za-z0-9_]+$ ]] || { echo "invalid test: $test_name" >&2; exit 1; }
  if [[ -d "$REPO_ROOT/test/blackbox" ]] && ! grep -R -E -q "^func[[:space:]]+$test_name\\(" "$REPO_ROOT/test/blackbox"; then
    echo "manifest test does not exist: $test_name" >&2
    exit 1
  fi
done < <(awk '!/^#/ && NF {for (i=2; i<=NF; i++) print $i}' "$MANIFEST")
