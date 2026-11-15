#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Exercise ci/nightly-alert.sh against recorded API fixtures. gh and curl are
# replaced by PATH shims that serve the fixtures and record write calls.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly FIXTURES="$REPO_ROOT/ci/testdata/nightly-alert"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/nightly-alert-test.XXXXXX")"
readonly SHIM_BIN="$TEMP_DIR/bin"
readonly REPO='DataDog/redis-cluster-kubernetes-operator'
trap 'rm -rf "$TEMP_DIR"' EXIT

mkdir -p "$SHIM_BIN"
cat >"$SHIM_BIN/gh" <<'SHIM'
#!/usr/bin/env bash
set -euo pipefail
case "$1 ${2:-}" in
  "api --paginate")
    id="${3#*/runs/}"; id="${id%%/*}"
    cat "$FIXTURES/jobs-$id.json" ;;
  "api "*)
    cat "$FIXTURES/run-${2##*/}.json" ;;
  "issue list")
    cat "${FAKE_ISSUES:-/dev/null}" 2>/dev/null || true
    [[ -n "${FAKE_ISSUES:-}" ]] || echo '[]' ;;
  "label create"|"issue create"|"issue comment")
    printf '%s\n' "$*" >>"$CALLS"
    prev=''
    for arg in "$@"; do
      [[ "$prev" == --body-file ]] && cat "$arg" >>"$CALLS"
      prev="$arg"
    done ;;
  *) echo "unexpected gh call: $*" >&2; exit 1 ;;
esac
SHIM
cat >"$SHIM_BIN/curl" <<'SHIM'
#!/usr/bin/env bash
set -euo pipefail
out=''
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) out="$2"; shift 2 ;;
    --header) shift 2 ;;
    *) url="$1"; shift ;;
  esac
done
job="${url%/logs}"; job="${job##*/}"
cp "$FIXTURES/log-$job.txt" "$out"
SHIM
chmod +x "$SHIM_BIN/gh" "$SHIM_BIN/curl"

export PATH="$SHIM_BIN:$PATH" FIXTURES GH_TOKEN=fake-token RUNNER_TEMP="$TEMP_DIR"

fail() { echo "FAIL: $*" >&2; exit 1; }

run_alert() {
  CALLS="$TEMP_DIR/calls" "$REPO_ROOT/ci/nightly-alert.sh" --repo "$REPO" "$@"
}

expected_body() {
  cat <<'EOF'
Scheduled run #130 of the Test workflow failed on `main`.

- Run: https://github.com/DataDog/redis-cluster-kubernetes-operator/actions/runs/43672223271 (attempt 1)
- Started: 2026-11-15T03:24:32Z
- Commit: `f28eaaf`

Failing jobs:

- [In-process reconciler regression (loadgen)](https://github.com/DataDog/redis-cluster-kubernetes-operator/actions/runs/43672223271/job/132684572175), step `Run diagnostic regression lane`: `TestE2E_PrimaryKill_DataAndTopology/seed_and_verify`, `TestE2E_PrimaryKill_DataAndTopology`
EOF
}

assert_no_log_text() {
  local file="$1"
  ! grep -Eq 'ghp_|SECRET|pwned|touch|##\[|812\.34s|suite\.go' "$file" || fail "log text leaked into $file"
}

export CALLS="$TEMP_DIR/calls"

# 1. Scheduled failure on main, dry run: exact output, no writes.
: >"$CALLS"
run_alert --run-id 43672223271 --dry-run >"$TEMP_DIR/out"
{ echo 'DRY RUN: would create issue labeled ci-flake'; echo 'Title: Nightly Test workflow failing on main'; echo; expected_body; } >"$TEMP_DIR/want"
diff -u "$TEMP_DIR/want" "$TEMP_DIR/out" || fail "dry-run output mismatch"
[[ ! -s "$CALLS" ]] || fail "dry run made write calls: $(cat "$CALLS")"
assert_no_log_text "$TEMP_DIR/out"

# 2. Non-scheduled run on a feature branch: guard exits 0, nothing touched.
: >"$CALLS"
run_alert --run-id 43753044412 >"$TEMP_DIR/out"
grep -q 'nothing to report' "$TEMP_DIR/out" || fail "guard did not skip run 43753044412"
[[ ! -s "$CALLS" ]] || fail "guard run made write calls"

# 2b. Synthetic manual run on main (fixture run-1001): still not scheduled.
run_alert --run-id 1001 >"$TEMP_DIR/out"
grep -q 'nothing to report' "$TEMP_DIR/out" || fail "guard did not skip a manual run on main"
[[ ! -s "$CALLS" ]] || fail "guard run made write calls"

# 2c. Synthetic scheduled failure on another branch (fixture run-1002).
run_alert --run-id 1002 >"$TEMP_DIR/out"
grep -q 'nothing to report' "$TEMP_DIR/out" || fail "guard did not skip a scheduled run off main"
[[ ! -s "$CALLS" ]] || fail "guard run made write calls"

# 3. No open issue: create one with the label and body.
: >"$CALLS"
run_alert --run-id 43672223271 >/dev/null
grep -q '^label create ci-flake' "$CALLS" || fail "label not ensured"
grep -q "^issue create --repo $REPO --title Nightly Test workflow failing on main --label ci-flake" "$CALLS" || fail "issue not created: $(cat "$CALLS")"
grep -qF 'TestE2E_PrimaryKill_DataAndTopology' "$CALLS" || fail "issue body missing test name"
! grep -q '^issue comment' "$CALLS" || fail "unexpected comment"
assert_no_log_text "$CALLS"

# 4. Open ci-flake issue with the same title: comment instead of creating.
echo '[{"number":7,"title":"Something else"},{"number":12,"title":"Nightly Test workflow failing on main"}]' >"$TEMP_DIR/issues.json"
: >"$CALLS"
FAKE_ISSUES="$TEMP_DIR/issues.json" run_alert --run-id 43672223271 >/dev/null
grep -q "^issue comment 12 --repo $REPO" "$CALLS" || fail "did not comment on #12: $(cat "$CALLS")"
! grep -q '^issue create' "$CALLS" || fail "created a duplicate issue"
assert_no_log_text "$CALLS"

echo "nightly-alert tests passed"
