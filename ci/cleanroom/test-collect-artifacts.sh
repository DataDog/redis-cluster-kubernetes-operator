#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/collect-artifacts-test.XXXXXX")"
readonly FAKE_ROOT="$TEMP_DIR/repo"
readonly FAKE_BIN="$TEMP_DIR/bin"
readonly LOG="$TEMP_DIR/commands.log"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$FAKE_ROOT/ci/cleanroom" "$FAKE_BIN"
cp "$SOURCE_ROOT/ci/cleanroom/collect-artifacts.sh" "$FAKE_ROOT/ci/cleanroom/collect-artifacts.sh"

cat >"$FAKE_BIN/kubectl" <<'KUBECTL'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$CLEANROOM_TEST_COMMAND_LOG"
if [[ "$*" == *"jsonpath="* ]]; then
  exit 0
fi
printf 'kind: List\nitems: []\n'
KUBECTL

cat >"$FAKE_BIN/helm" <<'HELM'
#!/usr/bin/env bash
exit 0
HELM

chmod +x "$FAKE_ROOT/ci/cleanroom/collect-artifacts.sh" "$FAKE_BIN/kubectl" "$FAKE_BIN/helm"

PATH="$FAKE_BIN:$PATH" \
  CLEANROOM_TEST_COMMAND_LOG="$LOG" \
  CLEANROOM_KUBECTL_REQUEST_TIMEOUT_SECONDS=7 \
  "$FAKE_ROOT/ci/cleanroom/collect-artifacts.sh" "$TEMP_DIR/artifacts" test-namespace

if [[ ! -s "$LOG" ]]; then
  echo "collector did not invoke kubectl" >&2
  exit 1
fi
if grep -F -v -- '--request-timeout=7s' "$LOG" >/dev/null; then
  echo "collector issued a kubectl request without the configured deadline" >&2
  exit 1
fi

cat >"$FAKE_ROOT/ci/cleanroom/collect-artifacts.sh" <<'COLLECTOR'
#!/usr/bin/env bash
printf '%s\n' "$$" >"$CLEANROOM_TEST_PID_FILE"
exec sleep 30
COLLECTOR
chmod +x "$FAKE_ROOT/ci/cleanroom/collect-artifacts.sh"

REPO_ROOT="$FAKE_ROOT"
# shellcheck source=common.sh
source "$SOURCE_ROOT/ci/cleanroom/common.sh"

SECONDS=0
if CLEANROOM_TEST_PID_FILE="$TEMP_DIR/collector.pid" \
  CLEANROOM_ARTIFACT_TIMEOUT_SECONDS=1 \
  CLEANROOM_ARTIFACT_KILL_GRACE_SECONDS=1 \
  cleanroom_collect_failure_artifacts "$TEMP_DIR/kubeconfig" "$TEMP_DIR/timed-out" test-namespace; then
  echo "timed-out collector returned success" >&2
  exit 1
fi
if ((SECONDS > 4)); then
  echo "collector watchdog exceeded its timeout and grace period" >&2
  exit 1
fi
collector_pid="$(cat "$TEMP_DIR/collector.pid")"
if kill -0 "$collector_pid" 2>/dev/null; then
  echo "collector process survived the watchdog" >&2
  exit 1
fi

echo "collect-artifacts tests passed"
