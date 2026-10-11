#!/usr/bin/env bash
# Shared clean-room runner functions. This file is sourced by provider scripts.

cleanroom_die() {
  echo "ERROR: $*" >&2
  exit 1
}

cleanroom_require_commands() {
  local command_name
  for command_name in "$@"; do
    command -v "$command_name" >/dev/null 2>&1 || cleanroom_die "required command '$command_name' is not on PATH"
  done
}

cleanroom_require_kind_version() {
  local required_major="$1"
  local required_minor="$2"
  local required_patch="$3"
  local version_output major minor patch

  version_output="$(kind version 2>/dev/null)" || cleanroom_die \
    "KIND v${required_major}.${required_minor}.${required_patch} or newer is required"
  [[ "$version_output" =~ ^kind[[:space:]]v([0-9]+)\.([0-9]+)\.([0-9]+)([[:space:]]|$) ]] || cleanroom_die \
    "KIND v${required_major}.${required_minor}.${required_patch} or newer is required; could not parse: $version_output"

  major="${BASH_REMATCH[1]}"
  minor="${BASH_REMATCH[2]}"
  patch="${BASH_REMATCH[3]}"
  if ((major < required_major ||
       (major == required_major && minor < required_minor) ||
       (major == required_major && minor == required_minor && patch < required_patch))); then
    cleanroom_die \
      "KIND v${required_major}.${required_minor}.${required_patch} or newer is required; found v${major}.${minor}.${patch}"
  fi
}

cleanroom_validate_run_id() {
  local run_id="$1"
  [[ "$run_id" =~ ^[a-z0-9][a-z0-9-]{2,30}$ ]] || cleanroom_die \
    "CLEANROOM_RUN_ID must match ^[a-z0-9][a-z0-9-]{2,30}$"
}

cleanroom_lane_selector() {
  local manifest="$1"
  local lane="$2"
  local -a tests=()
  local test_name selector="^("

  read -r -a tests <<<"$(awk -v lane="$lane" '$1 == lane {$1=""; sub(/^ /, ""); print; exit}' "$manifest")"
  ((${#tests[@]} > 0)) || cleanroom_die "lane '$lane' has no tests in $manifest"
  for test_name in "${tests[@]}"; do
    [[ "$test_name" =~ ^TestCleanroom_[A-Za-z0-9_]+$ ]] || cleanroom_die \
      "invalid test name '$test_name' in $manifest"
    [[ "$selector" == "^(" ]] || selector+="|"
    selector+="$test_name"
  done
  selector+=")$"
  printf '%s\n' "$selector"
}

cleanroom_verify_context() {
  local kubeconfig="$1"
  local expected_context="$2"
  local actual_context

  [[ -f "$kubeconfig" ]] || cleanroom_die "dedicated kubeconfig was not created: $kubeconfig"
  actual_context="$(KUBECONFIG="$kubeconfig" kubectl config current-context)"
  [[ "$actual_context" == "$expected_context" ]] || cleanroom_die \
    "refusing Kubernetes context '$actual_context'; expected '$expected_context'"
}

cleanroom_collect_failure_artifacts() {
  local kubeconfig="$1"
  local destination="$2"
  local namespace="$3"
  local timeout_seconds="${CLEANROOM_ARTIFACT_TIMEOUT_SECONDS:-60}"
  local kill_grace_seconds="${CLEANROOM_ARTIFACT_KILL_GRACE_SECONDS:-5}"
  local collector_pid watchdog_pid status=0

  [[ "$timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    echo "ERROR: CLEANROOM_ARTIFACT_TIMEOUT_SECONDS must be a positive integer" >&2
    return 2
  }
  [[ "$kill_grace_seconds" =~ ^[1-9][0-9]*$ ]] || {
    echo "ERROR: CLEANROOM_ARTIFACT_KILL_GRACE_SECONDS must be a positive integer" >&2
    return 2
  }

  KUBECONFIG="$kubeconfig" "$REPO_ROOT/ci/cleanroom/collect-artifacts.sh" "$destination" "$namespace" &
  collector_pid=$!
  (
    sleep "$timeout_seconds"
    if kill -0 "$collector_pid" 2>/dev/null; then
      echo "WARNING: failure artifact collection exceeded ${timeout_seconds}s; terminating it" >&2
      kill -TERM "$collector_pid" 2>/dev/null || exit 0
      sleep "$kill_grace_seconds"
      kill -KILL "$collector_pid" 2>/dev/null || true
    fi
  ) &
  watchdog_pid=$!

  wait "$collector_pid" || status=$?
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$status"
}

cleanroom_install_public_chart() {
  local repo_root="$1"
  local namespace="$2"
  local release="$3"
  local image_repository="$4"
  local image_tag="$5"
  local pull_policy="$6"

  kubectl create namespace "$namespace" --dry-run=client -o yaml | kubectl apply -f -
  helm upgrade --install "$release" "$repo_root" \
    --namespace "$namespace" \
    --set-string operator.image.repository="$image_repository" \
    --set-string operator.image.tag="$image_tag" \
    --set-string operator.image.pullPolicy="$pull_policy" \
    --wait \
    --timeout "${CLEANROOM_HELM_TIMEOUT:-10m}"
  kubectl apply -f "$repo_root/examples/redis-cluster.yaml"
}

cleanroom_run_tests() {
  local repo_root="$1"
  local selector="$2"
  local timeout="${CLEANROOM_TEST_TIMEOUT:-90m}"

  (
    cd "$repo_root"
    go test -tags=blackbox_e2e -count=1 ./test/blackbox -run "$selector" -timeout "$timeout"
  )
}
