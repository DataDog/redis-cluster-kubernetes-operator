#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Offline regression tests for scripts/preflight.sh and scripts/verify-cluster.sh.
# Runs the real scripts with only kubectl and helm replaced by mocks on PATH.
#
#   scripts/test-agent-scripts.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly REPO_ROOT
TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/agent-scripts-test.XXXXXX")"
readonly TEST_DIR
trap 'rm -rf "$TEST_DIR"' EXIT
readonly BOUND_SECONDS=10
readonly AUTH_HELPER="/usr/local/bin/redis-operator-redis-cli"

failures=0
fail() {
  echo "FAIL: $*" >&2
  failures=$((failures + 1))
}
pass() {
  echo "ok: $*"
}

mkdir -p "$TEST_DIR/bin"
# Mock kubectl: logs every call to $MOCK_LOG; behavior from MOCK_NS
# (ok|forbidden|notfound) and MOCK_ACL (true|false).
cat >"$TEST_DIR/bin/kubectl" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$MOCK_LOG"
args=" $* "
case "$args" in
  *" version -o json "*) printf '{\n "serverVersion": {\n  "gitVersion": "v1.33.1",\n }\n}\n' ;;
  *" config current-context "*) echo kind-test ;;
  *" auth can-i "*) exit 0 ;;
  *" get crd "* | *" diff "*) exit 0 ;;
  *" get storageclass "*) echo standard ;;
  *" get namespace "*)
    case "${MOCK_NS:-ok}" in
      forbidden) echo 'Error from server (Forbidden): namespaces "ns" is forbidden' >&2; exit 1 ;;
      notfound) echo 'Error from server (NotFound): namespaces "ns" not found' >&2; exit 1 ;;
    esac ;;
  *" get deployment "*) echo 'operator|1|1' ;;
  *" get rediscluster "*) echo '1|0||app=cache,' ;;
  *" toolbox"*)
    [[ "${MOCK_ACL:-false}" == true ]] && echo toolbox-0 ;;
  *" get pods "*) echo '|True|cache-0' ;;
  *" get pod "*) echo 10.0.0.1 ;;
  *" exec "*" test -x "*) [[ "${MOCK_ACL:-false}" == true ]] ;;
  *" exec "*)
    if [[ "${MOCK_ACL:-false}" == true && "$args" != *redis-operator-redis-cli* ]]; then
      echo 'NOAUTH Authentication required.'; exit 0
    fi
    case "$args" in
      *" CLUSTER INFO "*) printf 'cluster_state:ok\ncluster_slots_assigned:16384\ncluster_known_nodes:1\ncluster_size:1\n' ;;
      *" CLUSTER NODES "*) echo 'id1 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383' ;;
      *" SET "*) set -- $*; for ((i = 1; i <= $#; i++)); do [[ "${!i}" == SET ]] && { j=$((i + 2)); echo "${!j}" >"$MOCK_LOG.value"; }; done; echo OK ;;
      *" GET "*) cat "$MOCK_LOG.value" ;;
    esac ;;
esac
exit 0
MOCK
printf '#!/usr/bin/env bash\nexit 0\n' >"$TEST_DIR/bin/helm"
chmod +x "$TEST_DIR/bin/kubectl" "$TEST_DIR/bin/helm"
export PATH="$TEST_DIR/bin:$PATH"

# Runs a script with a wall-clock bound; sets rc (124 on timeout) and out.
run_bounded() {
  export MOCK_LOG="$TEST_DIR/calls.$RANDOM"
  : >"$MOCK_LOG"
  "$@" >"$TEST_DIR/out" 2>&1 &
  local pid=$! waited=0
  while kill -0 "$pid" 2>/dev/null && ((waited < BOUND_SECONDS * 10)); do
    sleep 0.1
    waited=$((waited + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    rc=124
  else
    rc=0
    wait "$pid" || rc=$?
  fi
  out="$(cat "$TEST_DIR/out")"
}

preflight="$REPO_ROOT/scripts/preflight.sh"
verify="$REPO_ROOT/scripts/verify-cluster.sh"

# Trailing value-taking options exit 2 with usage instead of looping.
for argv in "$preflight --namespace" "$verify --namespace" "$verify --namespace ns --name" \
  "$verify --namespace ns --name cache --timeout" "$verify -n"; do
  # shellcheck disable=SC2086
  run_bounded $argv
  if [[ "$rc" == 2 && "$out" == *usage:* ]]; then
    pass "${argv#"$REPO_ROOT"/} exits 2 with usage"
  else
    fail "${argv#"$REPO_ROOT"/} rc=$rc output: $out"
  fi
done

# Namespace GET: only NotFound means absent.
check_namespace() {
  local mode="$1" want="$2"
  MOCK_NS="$mode" run_bounded "$preflight" --namespace ns --json
  if grep -q "\"check\":\"namespace\",\"status\":\"$want\"" <<<"$out"; then
    pass "namespace GET $mode -> $want"
  else
    fail "namespace GET $mode: want $want, got $out"
  fi
}
check_namespace ok pass
check_namespace forbidden warn
check_namespace notfound todo

# verify-cluster.sh: SET issued only without --read-only; ACL uses the helper.
check_verify() {
  local label="$1" acl="$2" want_set="$3"
  shift 3
  MOCK_ACL="$acl" run_bounded "$verify" --namespace ns --name cache --timeout 0 --json "$@"
  local sets helper_calls direct_calls
  sets="$(grep -c ' SET ' "$MOCK_LOG" || true)"
  helper_calls="$(grep -c -- "-- $AUTH_HELPER " "$MOCK_LOG" || true)"
  direct_calls="$(grep -c -- '-- redis-cli ' "$MOCK_LOG" || true)"
  if [[ "$rc" != 0 || "$out" != *'"ok":true'* ]]; then
    fail "$label: rc=$rc output: $out"
  elif [[ "$want_set" == true && "$sets" == 0 ]] || [[ "$want_set" == false && "$sets" != 0 ]]; then
    fail "$label: $sets SET calls, want_set=$want_set"
  elif [[ "$acl" == true && ("$helper_calls" == 0 || "$direct_calls" != 0) ]]; then
    fail "$label: helper=$helper_calls direct=$direct_calls redis-cli calls on an ACL cluster"
  else
    pass "$label"
  fi
}
check_verify "write probe issues SET" false true
check_verify "--read-only issues no SET" false false --read-only
check_verify "ACL cluster uses the toolbox auth helper" true true
check_verify "ACL cluster --read-only issues no SET" true false --read-only

if ((failures > 0)); then
  echo "$failures check(s) failed" >&2
  exit 1
fi
echo "all checks passed"
