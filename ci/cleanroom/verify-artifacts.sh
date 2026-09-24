#!/usr/bin/env bash
# Fail closed before uploading clean-room failure artifacts.

set -euo pipefail

readonly ARTIFACT_DIR="${1:-cleanroom-artifacts}"

quiet_match() {
  local status=0

  grep -E -r -a -q "$@" -- "$ARTIFACT_DIR" >/dev/null 2>&1 || status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *)
      echo "artifact safety scan failed" >&2
      exit 1
      ;;
  esac
}

contains_unredacted_credential() {
  local -a statuses
  local credential_field
  credential_field="(^|[^[:alnum:]_])[\"']?(password|rediscli_auth)[\"']?[[:space:]]*[=:][[:space:]]*"

  # Use tools and POSIX regular expressions available on standard CI runners.
  # awk consumes the complete stream, so a detected match cannot leave grep
  # with a misleading broken-pipe status.
  set +e
  grep -E -r -a -h -i "$credential_field" -- "$ARTIFACT_DIR" 2>/dev/null |
    awk -v field="$credential_field" '
      BEGIN {
        redacted = field "[\"\047]?[[]redacted[]][\"\047]?([,}]|[[:space:]]|$)"
        credential = field "[^[:space:]]+"
      }
      {
        line = tolower($0)
        while (match(line, redacted)) {
          line = substr(line, 1, RSTART - 1) substr(line, RSTART + RLENGTH)
        }
        if (line ~ credential) {
          unsafe = 1
        }
      }
      END { exit unsafe ? 0 : 1 }
    ' >/dev/null 2>&1
  statuses=("${PIPESTATUS[@]}")
  set -e

  if ((statuses[0] > 1 || statuses[1] > 1)); then
    echo "artifact safety scan failed" >&2
    exit 1
  fi
  [[ "${statuses[0]}" -eq 0 && "${statuses[1]}" -eq 0 ]]
}

if [[ ! -d "$ARTIFACT_DIR" ]]; then
  echo "safe=true" >>"${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
  exit 0
fi

if contains_unredacted_credential; then
  echo "refusing to upload an artifact containing an unredacted credential field" >&2
  exit 1
fi
if quiet_match -i '(cleanroom-|p-|wrong-|missing-)[[:xdigit:]]{36}'; then
  echo "refusing to upload an artifact containing a generated credential" >&2
  exit 1
fi
if quiet_match '^[[:space:]]*kind:[[:space:]]*Secret[[:space:]]*$'; then
  echo "refusing to upload a Kubernetes Secret object" >&2
  exit 1
fi

echo "safe=true" >>"$GITHUB_OUTPUT"
