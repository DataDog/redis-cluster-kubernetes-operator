#!/usr/bin/env bash
# Fail closed before uploading clean-room failure artifacts.

set -euo pipefail

readonly ARTIFACT_DIR="${1:-cleanroom-artifacts}"

quiet_match() {
  local status=0

  rg --quiet --hidden "$@" -- "$ARTIFACT_DIR" >/dev/null 2>&1 || status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *)
      echo "artifact safety scan failed" >&2
      exit 1
      ;;
  esac
}

if [[ ! -d "$ARTIFACT_DIR" ]]; then
  echo "safe=true" >>"${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
  exit 0
fi

if quiet_match --pcre2 -i '(^|[^[:alnum:]_])(password|rediscli_auth)[=:][[:space:]]*(?>["\x27]?)(?!\[REDACTED\](?>["\x27]?)(?:[,}]|[[:space:]]|$))[^[:space:]]+'; then
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
