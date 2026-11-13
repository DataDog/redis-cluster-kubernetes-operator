# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Check reporting shared by preflight.sh and verify-cluster.sh. Set REPORT_JSON
# and REPORT_SUBJECT, call report_add per check, then report_finish.
# Statuses: pass, todo (install step not done yet), warn, fail.

REPORT_JSON="${REPORT_JSON:-false}"
REPORT_SUBJECT="${REPORT_SUBJECT:-}"
report_checks=()
report_failed=false

report_json_escape() {
  local s="${1//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\t'/ }"
  s="${s//$'\r'/}"
  s="${s//$'\n'/ }"
  printf '%s' "$s"
}

# report_add CHECK STATUS DETAIL [FIX]
report_add() {
  [[ "$2" == fail ]] && report_failed=true
  if [[ "$REPORT_JSON" == true ]]; then
    report_checks+=("{\"check\":\"$1\",\"status\":\"$2\",\"detail\":\"$(report_json_escape "$3")\",\"fix\":\"$(report_json_escape "${4:-}")\"}")
  else
    printf '%-20s %-5s %s\n' "$1" "$2" "$3"
    [[ -n "${4:-}" ]] && printf '%-20s %-5s fix: %s\n' "" "" "$4"
  fi
  return 0
}

report_finish() {
  if [[ "$REPORT_JSON" == true ]]; then
    local IFS=,
    printf '{%s"ok":%s,"checks":[%s]}\n' "$REPORT_SUBJECT" \
      "$([[ "$report_failed" == true ]] && echo false || echo true)" "${report_checks[*]}"
  fi
  [[ "$report_failed" == true ]] && exit 1
  exit 0
}
