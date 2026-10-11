#!/usr/bin/env bash
# Collect failure evidence without reading Kubernetes Secrets or dumping namespaces.

set -euo pipefail

readonly DESTINATION="${1:?artifact destination is required}"
readonly NAMESPACE="${2:?namespace is required}"
readonly KUBECTL_REQUEST_TIMEOUT_SECONDS="${CLEANROOM_KUBECTL_REQUEST_TIMEOUT_SECONDS:-15}"
[[ "$KUBECTL_REQUEST_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || {
  echo "ERROR: CLEANROOM_KUBECTL_REQUEST_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
}
readonly KUBECTL_REQUEST_TIMEOUT="${KUBECTL_REQUEST_TIMEOUT_SECONDS}s"
active_pid=""
temporary=""
mkdir -p "$DESTINATION"

cleanup() {
  if [[ -n "$active_pid" ]]; then
    kill -TERM "$active_pid" 2>/dev/null || true
    wait "$active_pid" 2>/dev/null || true
  fi
  [[ -z "$temporary" ]] || rm -f "$temporary"
}
trap cleanup EXIT

capture() {
  local output="$1"
  shift
  temporary="$(mktemp "${TMPDIR:-/tmp}/cleanroom-artifact.XXXXXX")"
  "$@" >"$temporary" 2>&1 &
  active_pid=$!
  wait "$active_pid" 2>/dev/null || true
  active_pid=""
  sed -E \
    -e 's#(redis(s)?://[^:/@[:space:]]+):[^@/[:space:]]+@#\1:[REDACTED]@#g' \
    -e 's#(([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd]|[Uu][Ss][Ee][Rr][Nn][Aa][Mm][Ee]|REDISCLI_AUTH)[=:][[:space:]]*)[^[:space:]]+#\1[REDACTED]#g' \
    "$temporary" >"$DESTINATION/$output"
  rm -f "$temporary"
  temporary=""
}

# These are explicit resource types; do not replace them with `get all` or a
# namespace dump. Secret objects are intentionally never requested.
capture redisclusters.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get redisclusters.cache.datadoghq.com -n "$NAMESPACE" -o yaml
capture statefulsets.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get statefulsets.apps -n "$NAMESPACE" -o yaml
capture pods.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get pods -n "$NAMESPACE" -o yaml
capture services.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get services -n "$NAMESPACE" -o yaml
capture endpointslices.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get endpointslices.discovery.k8s.io -n "$NAMESPACE" -o yaml
capture pvcs.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get persistentvolumeclaims -n "$NAMESPACE" -o yaml
capture pvs.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get persistentvolumes -o yaml
capture leases.yaml kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get leases.coordination.k8s.io -n "$NAMESPACE" -o yaml
capture events.txt kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get events -n "$NAMESPACE" --sort-by=.metadata.creationTimestamp

while IFS= read -r pod_name; do
  [[ -n "$pod_name" ]] || continue
  safe_name="${pod_name//[^A-Za-z0-9_.-]/_}"
  capture "pod-${safe_name}.describe.txt" kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" describe pod -n "$NAMESPACE" "$pod_name"
  capture "pod-${safe_name}.log.txt" kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" logs -n "$NAMESPACE" "$pod_name" --all-containers=true --tail=500
done < <(kubectl --request-timeout="$KUBECTL_REQUEST_TIMEOUT" get pods -n "$NAMESPACE" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)

if command -v helm >/dev/null 2>&1; then
  while IFS= read -r release_name; do
    [[ -n "$release_name" ]] || continue
    safe_name="${release_name//[^A-Za-z0-9_.-]/_}"
    capture "helm-${safe_name}.manifest.yaml" helm get manifest "$release_name" -n "$NAMESPACE"
    capture "helm-${safe_name}.values.yaml" helm get values "$release_name" -n "$NAMESPACE"
  done < <(helm list -n "$NAMESPACE" -q 2>/dev/null || true)
fi
