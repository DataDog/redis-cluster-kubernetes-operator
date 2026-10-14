#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Run one repository-defined KIND lane with public tools and images.

set -euo pipefail

OP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$OP_DIR/ci/e2e-lanes.txt"
LANE="${1:-}"

case "$LANE" in
  smoke)
    export E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-40m}"
    ;;
  default)
    export E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-75m}"
    ;;
  slow|bg-stuck)
    export E2E_SLOW=1
    export E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-85m}"
    ;;
  loadgen)
    export E2E_SLOW=1
    export E2E_BUILD_LOADGEN=1
    export E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-85m}"
    ;;
  *)
    echo "usage: $0 {smoke|default|slow|bg-stuck|loadgen}" >&2
    exit 2
    ;;
esac

read -r -a tests <<<"$(awk -v lane="$LANE" '$1 == lane {$1=""; sub(/^ /, ""); print; exit}' "$MANIFEST")"
if [[ ${#tests[@]} -eq 0 ]]; then
  echo "ERROR: lane '$LANE' has no tests in $MANIFEST" >&2
  exit 1
fi

selector="^("
for test_name in "${tests[@]}"; do
  if [[ "$selector" != "^(" ]]; then
    selector+="|"
  fi
  selector+="$test_name"
done
selector+=")$"

exec "$OP_DIR/ci/dev-e2e.sh" -run "$selector"
