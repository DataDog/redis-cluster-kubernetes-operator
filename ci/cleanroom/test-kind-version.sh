#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kind-version-test.XXXXXX")"
readonly FAKE_BIN="$TEMP_DIR/bin"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/kind" <<'KIND'
#!/usr/bin/env bash
[[ "${1:-}" == version ]] || exit 2
printf '%s\n' "$CLEANROOM_TEST_KIND_VERSION"
KIND
chmod +x "$FAKE_BIN/kind"

for command_name in docker go helm kubectl; do
  cat >"$FAKE_BIN/$command_name" <<'COMMAND'
#!/usr/bin/env bash
exit 99
COMMAND
  chmod +x "$FAKE_BIN/$command_name"
done

# shellcheck source=common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

expect_accepted() {
  local version="$1"
  (
    PATH="$FAKE_BIN:$PATH"
    CLEANROOM_TEST_KIND_VERSION="$version"
    export PATH CLEANROOM_TEST_KIND_VERSION
    cleanroom_require_kind_version 0 30 0
  )
}

expect_rejected() {
  local version="$1"
  local stderr="$TEMP_DIR/stderr"
  if (
    PATH="$FAKE_BIN:$PATH"
    CLEANROOM_TEST_KIND_VERSION="$version"
    export PATH CLEANROOM_TEST_KIND_VERSION
    cleanroom_require_kind_version 0 30 0
  ) 2>"$stderr"; then
    echo "unsupported KIND version was accepted: $version" >&2
    exit 1
  fi
  grep -F -q 'KIND v0.30.0 or newer is required' "$stderr" || {
    echo "unsupported KIND version did not produce the expected error: $version" >&2
    exit 1
  }
}

expect_accepted 'kind v0.30.0 go1.24.1 darwin/arm64'
expect_accepted 'kind v0.33.0 go1.26.4 linux/amd64'
expect_accepted 'kind v1.0.0 go1.26.4 linux/amd64'
expect_rejected 'kind v0.29.9 go1.23.0 linux/amd64'
expect_rejected 'kind v0.30.0-alpha go1.24.0 linux/amd64'
expect_rejected 'unexpected output'

runner_stderr="$TEMP_DIR/runner-stderr"
if PATH="$FAKE_BIN:$PATH" \
  CLEANROOM_TEST_KIND_VERSION='kind v0.29.9 go1.23.0 linux/amd64' \
  CLEANROOM_RUN_ID='version-guard-test' \
  TMPDIR="$TEMP_DIR" \
  "$REPO_ROOT/ci/run-cleanroom.sh" kind smoke 2>"$runner_stderr"; then
  echo "clean-room runner accepted an unsupported KIND version" >&2
  exit 1
fi
grep -F -q 'KIND v0.30.0 or newer is required; found v0.29.9' "$runner_stderr" || {
  echo "clean-room runner did not enforce the KIND version guard" >&2
  exit 1
}

echo "KIND version tests passed"
