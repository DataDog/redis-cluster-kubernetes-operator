#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Wait until a RedisCluster serves traffic: no rollout in progress, every pod
# Ready, all slots assigned, the topology matches spec.primaries/spec.replicas,
# and a write is read back through cluster redirects. Exits 1 on timeout.
#
#   scripts/verify-cluster.sh --namespace NS --name NAME [--timeout SECONDS] [--json]

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly REDIS_CONTAINER="redis"
readonly HASH_SLOTS=16384
readonly POLL_SECONDS=5
readonly DEFAULT_TIMEOUT_SECONDS=600
readonly PROBE_TTL_SECONDS=60
readonly PHASE_FAILED="Failed"

usage() {
  echo "usage: $0 --namespace NAMESPACE --name NAME [--timeout SECONDS] [--json]" >&2
  exit 2
}

namespace=""
name=""
timeout="$DEFAULT_TIMEOUT_SECONDS"
json=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace | -n) namespace="${2:-}"; shift 2 ;;
    --name) name="${2:-}"; shift 2 ;;
    --timeout) timeout="${2:-}"; shift 2 ;;
    --json) json=true; shift ;;
    *) usage ;;
  esac
done
[[ -n "$namespace" && -n "$name" && "$timeout" =~ ^[0-9]+$ ]] || usage

REPORT_JSON="$json"
# shellcheck source=lib/report.sh
source "$REPO_ROOT/scripts/lib/report.sh"
REPORT_SUBJECT="\"namespace\":\"$(report_json_escape "$namespace")\",\"name\":\"$(report_json_escape "$name")\","

# Results of the latest attempt; checks stop at the first one that is not pass.
results=()
fatal=false
record() {
  results+=("$1"$'\t'"$2"$'\t'"$3"$'\t'"${4:-}")
}

# Exits 0 when every CLUSTER NODES line is healthy and each of PRIMARIES
# primaries has exactly REPLICAS replicas.
topology_matches() {
  awk -v want_primaries="$1" -v want_replicas="$2" '
    BEGIN { valid = 1 }
    NF < 8 { valid = 0; next }
    {
      is_primary = 0; is_replica = 0
      n = split($3, flags, ",")
      for (i = 1; i <= n; i++) {
        if (flags[i] == "master" || flags[i] == "primary") is_primary = 1
        else if (flags[i] == "slave" || flags[i] == "replica") is_replica = 1
        else if (flags[i] == "fail" || flags[i] == "fail?") valid = 0
      }
      if ($8 != "connected" || is_primary == is_replica) valid = 0
      else if (is_primary) { primaries[$1] = 1; if ($4 != "-") valid = 0 }
      else replica_of[$1] = $4
      nodes++
    }
    END {
      for (r in replica_of) {
        if (!(replica_of[r] in primaries)) valid = 0
        else per_primary[replica_of[r]]++
      }
      for (p in primaries) {
        primary_count++
        if (per_primary[p] + 0 != want_replicas) valid = 0
      }
      exit !(valid && primary_count == want_primaries && nodes == want_primaries * (1 + want_replicas))
    }
  '
}

redis_cli() {
  kubectl exec --namespace "$namespace" "$redis_pod" --container "$REDIS_CONTAINER" -- \
    redis-cli --raw "$@" 2>/dev/null | tr -d '\r'
}

attempt() {
  results=()
  local spec
  if ! spec="$(kubectl get rediscluster "$name" --namespace "$namespace" -o go-template='{{.spec.primaries}}|{{.spec.replicas}}|{{or .status.blueGreen.phase ""}}|{{range $k, $v := .spec.statefulsetTemplate.spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}' 2>&1)"; then
    record resource fail "RedisCluster '$name' not readable: $spec" "kubectl get rediscluster --namespace $namespace"
    return
  fi
  local primaries replicas phase selector
  IFS='|' read -r primaries replicas phase selector <<<"$spec"
  selector="${selector%,}"
  local expected_pods=$((primaries * (1 + replicas)))
  record resource pass "$primaries primaries x $replicas replicas ($expected_pods pods)"

  if [[ "$phase" == "$PHASE_FAILED" ]]; then
    fatal=true
    record rollout fail "blue/green rollout is Failed" "kubectl get rediscluster $name --namespace $namespace -o jsonpath='{.status.blueGreen}'"
    return
  elif [[ -n "$phase" ]]; then
    record rollout fail "blue/green rollout in progress (phase $phase)" "wait; rollouts replace every pod"
    return
  fi
  record rollout pass "no rollout in progress"

  local ready
  ready="$(kubectl get pods --namespace "$namespace" --selector "$selector" \
    -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"|"}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | awk -F'|' '$1 == "" && $2 == "True" {print $3}')"
  local ready_count
  ready_count="$(grep -c . <<<"$ready")"
  if [[ "$ready_count" != "$expected_pods" ]]; then
    record pods fail "$ready_count/$expected_pods pods Ready" "kubectl get pods --namespace $namespace --selector $selector"
    return
  fi
  record pods pass "$ready_count/$expected_pods pods Ready"
  redis_pod="$(head -1 <<<"$ready")"

  local info state slots known size
  info="$(redis_cli CLUSTER INFO)"
  state="$(awk -F: '$1 == "cluster_state" {print $2}' <<<"$info")"
  slots="$(awk -F: '$1 == "cluster_slots_assigned" {print $2}' <<<"$info")"
  known="$(awk -F: '$1 == "cluster_known_nodes" {print $2}' <<<"$info")"
  size="$(awk -F: '$1 == "cluster_size" {print $2}' <<<"$info")"
  if [[ "$state" != ok || "$slots" != "$HASH_SLOTS" || "$known" != "$expected_pods" || "$size" != "$primaries" ]]; then
    record cluster-info fail "state=${state:-?} slots=${slots:-?}/$HASH_SLOTS nodes=${known:-?}/$expected_pods primaries=${size:-?}/$primaries" \
      "kubectl exec --namespace $namespace $redis_pod -c $REDIS_CONTAINER -- redis-cli CLUSTER INFO"
    return
  fi
  record cluster-info pass "state ok, $slots slots, $known nodes"

  if ! redis_cli CLUSTER NODES | topology_matches "$primaries" "$replicas"; then
    record topology fail "CLUSTER NODES does not show $primaries healthy primaries with $replicas replicas each" \
      "kubectl exec --namespace $namespace $redis_pod -c $REDIS_CONTAINER -- redis-cli CLUSTER NODES"
    return
  fi
  record topology pass "$primaries primaries, $replicas replicas each, all links connected"

  local key="rco:verify:$(date +%s)" value="ok-$$"
  if [[ "$(redis_cli -c SET "$key" "$value" EX "$PROBE_TTL_SECONDS")" != OK \
    || "$(redis_cli -c GET "$key")" != "$value" ]]; then
    record data fail "SET/GET of $key through cluster redirects failed" \
      "kubectl exec --namespace $namespace $redis_pod -c $REDIS_CONTAINER -- redis-cli -c PING"
    return
  fi
  record data pass "wrote and read $key (expires in ${PROBE_TTL_SECONDS}s)"
}

deadline=$((SECONDS + timeout))
while :; do
  attempt
  last="${results[${#results[@]} - 1]}"
  [[ "$(cut -f2 <<<"$last")" == pass || "$fatal" == true || SECONDS -ge deadline ]] && break
  sleep "$POLL_SECONDS"
done

for row in "${results[@]}"; do
  IFS=$'\t' read -r check status detail fix <<<"$row"
  report_add "$check" "$status" "$detail" "$fix"
done
report_finish
