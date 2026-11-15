#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Open or update a public GitHub issue when the scheduled Test workflow fails
# on main. Only sanitized facts leave the runner: lane (job) names, failed step
# names, Go test names matched by a strict pattern, and links. Job logs are read
# to find test names and deleted right away; log text is never printed.
#
# Usage: ci/nightly-alert.sh --repo OWNER/NAME --run-id ID [--dry-run] [--force]
#   --dry-run  print the issue title/body (or comment target); make no writes
#   --force    skip the "scheduled failure on main" guard (manual dispatch)
# Requires gh, curl, and jq; GH_TOKEN must be set in the environment.

set -euo pipefail

readonly TITLE='Nightly Test workflow failing on main'
readonly LABEL='ci-flake'
readonly MAX_TESTS_PER_JOB=10

repo=''
run_id=''
dry_run=false
force=false

usage() {
  echo "usage: $0 --repo OWNER/NAME --run-id ID [--dry-run] [--force]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) repo="${2:-}"; shift 2 ;;
    --run-id) run_id="${2:-}"; shift 2 ;;
    --dry-run) dry_run=true; shift ;;
    --force) force=true; shift ;;
    *) usage ;;
  esac
done
[[ "$repo" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || usage
[[ "$run_id" =~ ^[0-9]+$ ]] || usage
[[ -n "${GH_TOKEN:-}" ]] || { echo 'GH_TOKEN must be set' >&2; exit 2; }

work_dir="$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/nightly-alert.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# A Go test name as printed in a failure record: a Test/Fuzz/Example/Benchmark
# top-level name with optional /subtest segments, capped in length.
readonly TEST_NAME_RE='^(Test|Fuzz|Example|Benchmark)[A-Za-z0-9_]*(/[A-Za-z0-9_.-]+)*$'
readonly MAX_TEST_NAME_LEN=200
readonly MARKER_TIMED_OUT='(test binary timed out)'
readonly MARKER_NOT_FOUND='(no test name found; see job log)'

valid_test_name() {
  [[ ${#1} -le $MAX_TEST_NAME_LEN && "$1" =~ $TEST_NAME_RE ]]
}

# extract_tests LOG_FILE: print up to MAX_TESTS_PER_JOB unique failing test
# names, or a fixed marker when none can be found. Only complete Go failure
# records count: after the Actions timestamp prefix, the line must be optional
# whitespace, "--- FAIL: ", a test name, and " (<seconds>s)" at end of line.
extract_tests() {
  local log_file="$1" names='' line count=0
  while IFS= read -r line; do
    valid_test_name "$line" || continue
    [[ $'\n'"$names" == *$'\n'"$line"$'\n'* ]] && continue
    names+="$line"$'\n'
    count=$((count + 1))
    [[ $count -lt $MAX_TESTS_PER_JOB ]] || break
  done < <(sed -E \
    -e 's/\r$//' \
    -e 's/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z //' \
    "$log_file" \
    | sed -nE 's/^[[:space:]]*--- FAIL: ([^ ]+) \([0-9]+(\.[0-9]+)?s\)$/\1/p')
  if [[ -n "$names" ]]; then
    printf '%s' "$names"
  elif grep -q 'panic: test timed out' "$log_file"; then
    echo "$MARKER_TIMED_OUT"
  else
    echo "$MARKER_NOT_FOUND"
  fi
}

# format_tests: join validated test names from stdin with ", ", wrapping each
# in backticks. Fixed markers pass through as plain text; anything else is
# dropped.
format_tests() {
  local line out=''
  while IFS= read -r line; do
    if valid_test_name "$line"; then
      line="\`$line\`"
    elif [[ "$line" != "$MARKER_TIMED_OUT" && "$line" != "$MARKER_NOT_FOUND" ]]; then
      continue
    fi
    out+="${out:+, }$line"
  done
  printf '%s' "$out"
}

# render_body RUN_JSON FAILURES_TSV: print the issue/comment markdown.
# FAILURES_TSV columns: job name, job URL, failed step, tests (comma-separated).
render_body() {
  local run_json="$1" failures="$2"
  jq -r --arg repo "$repo" '
    "Scheduled run #\(.run_number) of the Test workflow failed on `main`.",
    "",
    "- Run: https://github.com/\($repo)/actions/runs/\(.id) (attempt \(.run_attempt))",
    "- Started: \(.run_started_at)",
    "- Commit: `\(.head_sha[0:7])`",
    "",
    "Failing jobs:",
    ""' "$run_json"
  local name url step tests
  while IFS=$'\t' read -r name url step tests; do
    echo "- [$name]($url), step \`$step\`: $tests"
  done <"$failures"
}

run_json="$work_dir/run.json"
gh api "repos/$repo/actions/runs/$run_id" >"$run_json"

if [[ "$force" != true ]]; then
  if ! jq -e '.event == "schedule" and .head_branch == "main" and .conclusion == "failure"' "$run_json" >/dev/null; then
    echo "Run $run_id is not a failed scheduled run on main; nothing to report."
    exit 0
  fi
fi

jobs_tsv="$work_dir/jobs.tsv"
gh api --paginate "repos/$repo/actions/runs/$run_id/jobs?per_page=100" \
  | jq -rs '[.[].jobs[]]
      | map(select(.conclusion == "failure" or .conclusion == "timed_out" or .conclusion == "cancelled"))
      | .[]
      | [(.id | tostring), .name, .html_url,
         ((.steps // []) | map(select(.conclusion == "failure" or .conclusion == "timed_out" or .conclusion == "cancelled")) | .[0].name // "(unknown step)")]
      | @tsv' >"$jobs_tsv"

if [[ ! -s "$jobs_tsv" ]]; then
  echo "Run $run_id has no failed jobs; nothing to report."
  exit 0
fi

failures="$work_dir/failures.tsv"
: >"$failures"
while IFS=$'\t' read -r job_id name url step; do
  log_file="$work_dir/job-$job_id.log"
  # Plain curl: the logs endpoint redirects to raw text, which may contain
  # terminal escape sequences that some gh builds refuse to print.
  if curl --fail --silent --show-error --location \
    --header "Authorization: Bearer $GH_TOKEN" \
    --output "$log_file" \
    "${GITHUB_API_URL:-https://api.github.com}/repos/$repo/actions/jobs/$job_id/logs" 2>/dev/null; then
    tests="$(extract_tests "$log_file" | format_tests)"
  else
    tests='(job log unavailable)'
  fi
  rm -f "$log_file"
  printf '%s\t%s\t%s\t%s\n' "$name" "$url" "$step" "$tests" >>"$failures"
done <"$jobs_tsv"

body_file="$work_dir/body.md"
render_body "$run_json" "$failures" >"$body_file"

existing="$(gh issue list --repo "$repo" --label "$LABEL" --state open --json number,title \
  | jq -r --arg t "$TITLE" 'map(select(.title == $t)) | .[0].number // empty')"

if [[ "$dry_run" == true ]]; then
  if [[ -n "$existing" ]]; then
    echo "DRY RUN: would comment on issue #$existing"
  else
    echo "DRY RUN: would create issue labeled $LABEL"
  fi
  echo "Title: $TITLE"
  echo
  cat "$body_file"
  exit 0
fi

gh label create "$LABEL" --repo "$repo" --force \
  --color B60205 --description 'Scheduled Test workflow failure on main' >/dev/null
if [[ -n "$existing" ]]; then
  gh issue comment "$existing" --repo "$repo" --body-file "$body_file"
else
  gh issue create --repo "$repo" --title "$TITLE" --label "$LABEL" --body-file "$body_file"
fi
