#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly SCANNER="$REPO_ROOT/ci/cleanroom/verify-artifacts.sh"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/verify-artifacts-test.XXXXXX")"
readonly HEX36="0123456789abcdef0123456789abcdef0123"

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

expect_rejected_without_disclosure() {
  local name="$1"
  local secret="$2"
  local contents="$3"
  local fixture="$TEMP_DIR/$name"
  local output="$TEMP_DIR/$name.github-output"
  local stdout="$TEMP_DIR/$name.stdout"
  local stderr="$TEMP_DIR/$name.stderr"

  mkdir -p "$fixture"
  printf '%s\n' "$contents" >"$fixture/artifact.txt"
  if GITHUB_OUTPUT="$output" "$SCANNER" "$fixture" >"$stdout" 2>"$stderr"; then
    echo "unsafe artifact passed verification: $name" >&2
    exit 1
  fi
  if grep -F -q -- "$secret" "$stdout" "$stderr"; then
    echo "artifact verification disclosed rejected content: $name" >&2
    exit 1
  fi
  if [[ -s "$output" ]]; then
    echo "unsafe artifact was marked safe: $name" >&2
    exit 1
  fi
}

expect_missing_scanner_fails_closed() {
  local secret="missing-scanner-secret-$HEX36"
  local fixture="$TEMP_DIR/missing-scanner"
  local output="$TEMP_DIR/missing-scanner.github-output"
  local stdout="$TEMP_DIR/missing-scanner.stdout"
  local stderr="$TEMP_DIR/missing-scanner.stderr"
  local empty_path="$TEMP_DIR/empty-path"

  mkdir -p "$fixture" "$empty_path"
  printf '%s\n' "password=$secret" >"$fixture/artifact.txt"
  if PATH="$empty_path" GITHUB_OUTPUT="$output" /bin/bash "$SCANNER" "$fixture" >"$stdout" 2>"$stderr"; then
    echo "artifact verification passed without its scanner" >&2
    exit 1
  fi
  if grep -F -q -- "$secret" "$stdout" "$stderr"; then
    echo "missing-scanner failure disclosed rejected content" >&2
    exit 1
  fi
  if [[ -s "$output" ]]; then
    echo "artifact was marked safe without its scanner" >&2
    exit 1
  fi
  grep -F -x -q 'artifact safety scan failed' "$stderr" || {
    echo "missing-scanner failure was not reported" >&2
    exit 1
  }
}

expect_missing_scanner_fails_closed

expect_rejected_without_disclosure \
  "credential-field" \
  "field-secret-$HEX36" \
  "password=field-secret-$HEX36"

expect_rejected_without_disclosure \
  "json-credential-field" \
  "json-secret-$HEX36" \
  "{\"password\":\"json-secret-$HEX36\"}"

expect_rejected_without_disclosure \
  "multiline-json-credential-field" \
  "multiline-json-secret-$HEX36" \
  $'{\n  "password":\n  "multiline-json-secret-'"$HEX36"$'"\n}'

expect_rejected_without_disclosure \
  "multiline-yaml-credential-field" \
  "multiline-yaml-secret-$HEX36" \
  $'rediscli_auth:\n  multiline-yaml-secret-'"$HEX36"

expect_rejected_without_disclosure \
  "mixed-redacted-and-unredacted-credential-fields" \
  "real-secret" \
  "user=x password=[REDACTED] password=real-secret"

for prefix in cleanroom- p- wrong- missing-; do
  expect_rejected_without_disclosure \
    "canary-${prefix%-}" \
    "$prefix$HEX36" \
    "unlabelled value $prefix$HEX36"
done

expect_rejected_without_disclosure \
  "kubernetes-secret" \
  "kind: Secret" \
  $'apiVersion: v1\nkind: Secret\nmetadata:\n  name: forbidden'

safe_fixture="$TEMP_DIR/safe"
safe_output="$TEMP_DIR/safe.github-output"
mkdir -p "$safe_fixture"
printf '%s\n' \
  'password=[REDACTED]' \
  'REDISCLI_AUTH=[REDACTED]' \
  '"password":"[REDACTED]",' \
  "rediscli_auth: '[REDACTED]'" \
  'ordinary diagnostic content' \
  >"$safe_fixture/artifact.txt"
GITHUB_OUTPUT="$safe_output" "$SCANNER" "$safe_fixture"
grep -F -x -q 'safe=true' "$safe_output" || {
  echo "safe artifact was not marked safe" >&2
  exit 1
}

echo "verify-artifacts tests passed"
