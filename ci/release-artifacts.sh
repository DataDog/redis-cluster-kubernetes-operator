#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Only an explicit missing manifest means absent; auth/network errors stop publishing.
registry_digest() {
  local result error_file
  error_file="$(mktemp)"
  if result="$(docker buildx imagetools inspect "$1" --format '{{.Manifest.Digest}}' 2>"$error_file")"; then
    rm -f "$error_file"
    [[ "$result" =~ ^sha256:[a-f0-9]{64}$ ]] || return 1
    printf '%s\n' "$result"
  elif grep -Eiq 'manifest unknown|name unknown' "$error_file" \
    || grep -Fq "$1: not found" "$error_file"; then
    rm -f "$error_file"
  else
    cat "$error_file" >&2
    rm -f "$error_file"
    return 1
  fi
}

publish_chart() {
  local package="$1" name="$2" existing temp_dir status=0
  existing="$(registry_digest "${CHART_REPO#oci://}/$name:$VERSION")" || return 1
  if [[ -z "$existing" ]]; then
    helm push "$package" "$CHART_REPO"
    return
  fi
  # Compare contents on retries, ignoring archive timestamps. Never overwrite.
  temp_dir="$(mktemp -d)"
  if ! helm pull "$CHART_REPO/$name" --version "$VERSION" --destination "$temp_dir"; then
    rm -rf "$temp_dir"
    return 1
  fi
  python3 - "$package" "$temp_dir/$name-$VERSION.tgz" <<'PY' || status=$?
import sys, tarfile

def contents(path):
    with tarfile.open(path) as archive:
        return {m.name: archive.extractfile(m).read() for m in archive if m.isfile()}

if contents(sys.argv[1]) != contents(sys.argv[2]):
    sys.exit("refusing to overwrite an existing chart with different contents")
PY
  rm -rf "$temp_dir"
  return "$status"
}

promote_image() {
  local existing
  existing="$(registry_digest "$IMAGE:$VERSION")" || return 1
  if [[ -n "$existing" ]]; then
    [[ "$existing" == "$DIGEST" ]] || {
      echo "refusing to overwrite $IMAGE:$VERSION ($existing) with $DIGEST" >&2
      return 1
    }
  else
    docker buildx imagetools create --tag "$IMAGE:$VERSION" "$IMAGE@$DIGEST" || return 1
  fi
  [[ "$(registry_digest "$IMAGE:$VERSION")" == "$DIGEST" ]]
}
