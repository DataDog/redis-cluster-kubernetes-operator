#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Check that the current kubeconfig context can run the operator in NAMESPACE.
# Read-only. Each check reports pass, todo (install step not done yet), warn
# or fail, with a fix. Exits 1 when any check fails.
#
#   scripts/preflight.sh --namespace redis [--json]

set -uo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CRD_NAME="redisclusters.cache.datadoghq.com"
readonly CRD_FILE="$REPO_ROOT/crds/redis-cluster-v1.yaml"
readonly OPERATOR_SELECTOR="app.kubernetes.io/name=redis-cluster-kubernetes-operator,app.kubernetes.io/component=operator"
readonly DEFAULT_SC_ANNOTATION="storageclass.kubernetes.io/is-default-class"

namespace=""
json=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace | -n)
      [[ $# -ge 2 ]] || { echo "usage: $0 --namespace NAMESPACE [--json]" >&2; exit 2; }
      namespace="$2"; shift 2 ;;
    --json) json=true; shift ;;
    *) echo "usage: $0 --namespace NAMESPACE [--json]" >&2; exit 2 ;;
  esac
done
[[ -n "$namespace" ]] || { echo "usage: $0 --namespace NAMESPACE [--json]" >&2; exit 2; }

REPORT_JSON="$json"
# shellcheck source=lib/report.sh
source "$REPO_ROOT/scripts/lib/report.sh"
REPORT_SUBJECT="\"namespace\":\"$(report_json_escape "$namespace")\","

can_i() {
  kubectl auth can-i "$@" >/dev/null 2>&1
}

for tool in kubectl helm; do
  if command -v "$tool" >/dev/null 2>&1; then
    report_add "tool-$tool" pass "$(command -v "$tool")"
  else
    report_add "tool-$tool" fail "$tool not found on PATH" "install $tool"
  fi
done
[[ "$report_failed" == true ]] && report_finish

server_version="$(kubectl version -o json 2>/dev/null \
  | awk '/"serverVersion"/ {s = 1} s && /"gitVersion"/ {gsub(/[",]/, "", $2); print $2; exit}')"
if [[ -z "$server_version" ]]; then
  report_add api fail "cannot reach the API server of context '$(kubectl config current-context 2>/dev/null)'" \
    "set KUBECONFIG or kubectl config use-context"
  report_finish
fi
report_add api pass "context '$(kubectl config current-context)', server $server_version"

# Helm enforces the chart's kubeVersion, so the supported range lives only in Chart.yaml.
if gate="$(helm template preflight "$REPO_ROOT" --kube-version "$server_version" 2>&1 >/dev/null)"; then
  report_add kubernetes-version pass "$server_version is supported"
else
  report_add kubernetes-version fail "$gate" "use a supported Kubernetes minor"
fi

if ! can_i get customresourcedefinitions; then
  report_add crd warn "cannot read CRDs, so cannot tell whether $CRD_NAME is installed" \
    "ask a cluster admin to confirm crds/redis-cluster-v1.yaml is applied"
elif kubectl get crd "$CRD_NAME" >/dev/null 2>&1; then
  if kubectl diff --server-side --force-conflicts -f "$CRD_FILE" >/dev/null 2>&1; then
    report_add crd pass "$CRD_NAME matches $CRD_FILE"
  else
    report_add crd warn "$CRD_NAME differs from $CRD_FILE" \
      "kubectl apply --server-side -f crds/redis-cluster-v1.yaml (Helm does not upgrade CRDs)"
  fi
elif can_i create customresourcedefinitions; then
  report_add crd todo "$CRD_NAME not installed" "kubectl apply --server-side -f crds/redis-cluster-v1.yaml"
else
  report_add crd fail "$CRD_NAME not installed and you cannot create CRDs" \
    "ask a cluster admin to apply crds/redis-cluster-v1.yaml"
fi

default_sc="$(kubectl get storageclass \
  -o jsonpath="{range .items[?(@.metadata.annotations.${DEFAULT_SC_ANNOTATION//./\\.}==\"true\")]}{.metadata.name}{\"\\n\"}{end}" 2>/dev/null | head -1)"
if ! can_i list storageclasses; then
  report_add storageclass warn "cannot list StorageClasses" \
    "ask a cluster admin which StorageClass to set in the RedisCluster volumeClaimTemplates"
elif [[ -n "$default_sc" ]]; then
  report_add storageclass pass "default StorageClass '$default_sc'"
else
  report_add storageclass warn "no default StorageClass" \
    "set storageClassName in the RedisCluster volumeClaimTemplates"
fi

# Only NotFound means absent; a namespace-scoped installer may lack get namespaces.
if ns_err="$(kubectl get namespace "$namespace" 2>&1 >/dev/null)"; then
  report_add namespace pass "namespace '$namespace' exists"
elif [[ "$ns_err" != *NotFound* ]]; then
  report_add namespace warn "cannot read namespace '$namespace', so cannot tell whether it exists" \
    "confirm it exists; the permissions check below still applies"
elif can_i create namespaces; then
  report_add namespace todo "namespace '$namespace' does not exist" "helm install ... --create-namespace"
else
  report_add namespace fail "namespace '$namespace' does not exist and you cannot create it" \
    "ask a cluster admin to create it"
fi

# The chart installs a namespaced Role, so the installer needs these verbs here.
missing=()
for want in "create roles.rbac.authorization.k8s.io" "create rolebindings.rbac.authorization.k8s.io" \
  "create deployments.apps" "create serviceaccounts" "create redisclusters.cache.datadoghq.com"; do
  # shellcheck disable=SC2086
  can_i $want --namespace "$namespace" || missing+=("${want#create }")
done
if [[ ${#missing[@]} -eq 0 ]]; then
  report_add permissions pass "can install the operator and create RedisClusters in '$namespace'"
else
  report_add permissions fail "cannot create: ${missing[*]}" "ask for these permissions in '$namespace'"
fi

operator="$(kubectl get deployment --namespace "$namespace" --selector "$OPERATOR_SELECTOR" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.readyReplicas}{"|"}{.spec.replicas}{"\n"}{end}' 2>/dev/null | head -1)"
if [[ -z "$operator" ]]; then
  report_add operator todo "operator not installed in '$namespace'" \
    "install the operator image first: skills/redis-cluster-setup/SKILL.md step 3 (release, registry or KIND)"
else
  IFS='|' read -r name ready want <<<"$operator"
  if [[ "${ready:-0}" -ge 1 && "${ready:-0}" == "$want" ]]; then
    report_add operator pass "deployment '$name' ready ($ready/$want)"
  else
    report_add operator fail "deployment '$name' not ready (${ready:-0}/$want)" \
      "kubectl describe deployment $name --namespace $namespace"
  fi
fi

report_finish
