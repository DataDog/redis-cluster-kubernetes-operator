#!/usr/bin/env bash
# Idempotently delete only the EKS and ECR resources recorded by one run.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

readonly STATE_FILE="${1:?resource-state file is required}"
[[ -f "$STATE_FILE" ]] || { echo "ERROR: state file not found: $STATE_FILE" >&2; exit 1; }

RUN_ID=""
AWS_ACCOUNT_ID=""
AWS_REGION_VALUE=""
CLUSTER_NAME=""
ECR_REPOSITORY=""
OWNER_TAG_KEY=""
OWNER_TAG_VALUE=""
KUBECONFIG_PATH=""
declare -a RECORDED_EBS_VOLUME_IDS=()

while IFS='=' read -r key value; do
  case "$key" in
    run_id) RUN_ID="$value" ;;
    aws_account_id) AWS_ACCOUNT_ID="$value" ;;
    aws_region) AWS_REGION_VALUE="$value" ;;
    cluster_name) CLUSTER_NAME="$value" ;;
    ecr_repository) ECR_REPOSITORY="$value" ;;
    owner_tag_key) OWNER_TAG_KEY="$value" ;;
    owner_tag_value) OWNER_TAG_VALUE="$value" ;;
    kubeconfig) KUBECONFIG_PATH="$value" ;;
    ebs_volume_id) RECORDED_EBS_VOLUME_IDS+=("$value") ;;
  esac
done <"$STATE_FILE"

[[ "$RUN_ID" =~ ^[a-z0-9][a-z0-9-]{2,30}$ ]] || { echo "ERROR: unsafe run id in state file" >&2; exit 1; }
[[ "$AWS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]] || { echo "ERROR: unsafe AWS account id in state file" >&2; exit 1; }
[[ "$AWS_REGION_VALUE" =~ ^[a-z]{2}-[a-z]+-[0-9]$ ]] || { echo "ERROR: unsafe AWS region in state file" >&2; exit 1; }
[[ "$CLUSTER_NAME" == "redis-op-$RUN_ID" ]] || { echo "ERROR: cluster name does not match run id" >&2; exit 1; }
[[ "$ECR_REPOSITORY" == "redis-op-cleanroom-$RUN_ID" ]] || { echo "ERROR: ECR repository does not match run id" >&2; exit 1; }
[[ "$OWNER_TAG_KEY" == "redis-cluster-kubernetes-operator-cleanroom" ]] || { echo "ERROR: unsafe owner tag key" >&2; exit 1; }
[[ "$OWNER_TAG_VALUE" == "$RUN_ID" ]] || { echo "ERROR: owner tag value does not match run id" >&2; exit 1; }
if [[ -n "$KUBECONFIG_PATH" && "$KUBECONFIG_PATH" != /* ]]; then
  echo "ERROR: kubeconfig path in state file must be absolute" >&2
  exit 1
fi
for volume_id in "${RECORDED_EBS_VOLUME_IDS[@]}"; do
  [[ "$volume_id" =~ ^vol-[0-9a-f]+$ ]] || { echo "ERROR: unsafe EBS volume id in state file" >&2; exit 1; }
done

cleanroom_require_commands aws eksctl

actual_account="$(aws sts get-caller-identity --query Account --output text)"
[[ "$actual_account" == "$AWS_ACCOUNT_ID" ]] || {
  echo "ERROR: AWS account changed: expected $AWS_ACCOUNT_ID, got $actual_account" >&2
  exit 1
}

cleanup_status=0
cluster_present=false
cluster_owned=false
if cluster_owner="$(aws eks describe-cluster --name "$CLUSTER_NAME" --region "$AWS_REGION_VALUE" \
  --query "cluster.tags.\"$OWNER_TAG_KEY\"" --output text 2>/dev/null)"; then
  cluster_present=true
  [[ "$cluster_owner" == "$RUN_ID" ]] && cluster_owned=true
elif stack_owner="$(aws cloudformation describe-stacks --stack-name "eksctl-$CLUSTER_NAME-cluster" \
  --region "$AWS_REGION_VALUE" \
  --query "Stacks[0].Tags[?Key=='$OWNER_TAG_KEY'].Value | [0]" --output text 2>/dev/null)"; then
  cluster_present=true
  [[ "$stack_owner" == "$RUN_ID" ]] && cluster_owned=true
fi

record_volume_id() {
  local candidate="$1"
  local existing
  [[ "$candidate" =~ ^vol-[0-9a-f]+$ ]] || return 1
  for existing in "${RECORDED_EBS_VOLUME_IDS[@]}"; do
    [[ "$existing" == "$candidate" ]] && return 0
  done
  RECORDED_EBS_VOLUME_IDS+=("$candidate")
  printf 'ebs_volume_id=%s\n' "$candidate" >>"$STATE_FILE"
}

teardown_kubernetes_storage() {
  local expected_context="cleanroom-$CLUSTER_NAME"
  local current_context namespace namespace_output pvc_name pvc_output pv_name driver volume_id
  local -a namespaces=("redis-cluster-example")
  local -a persistent_volumes=()

  command -v kubectl >/dev/null 2>&1 || {
    echo "ERROR: recorded kubeconfig exists but kubectl is unavailable; cannot delete PVCs before EKS teardown" >&2
    return 1
  }
  current_context="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl config current-context 2>/dev/null)" || {
    echo "ERROR: cannot read the recorded EKS kubeconfig context" >&2
    return 1
  }
  [[ "$current_context" == "$expected_context" ]] || {
    echo "ERROR: refusing Kubernetes context '$current_context'; expected '$expected_context'" >&2
    return 1
  }
  KUBECONFIG="$KUBECONFIG_PATH" kubectl get --raw=/readyz >/dev/null 2>&1 || {
    echo "ERROR: recorded kubeconfig cannot reach the EKS API" >&2
    return 1
  }

  namespace_output="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get namespaces \
    -l "cleanroom.redis-operator/run=$RUN_ID" -o name)" || return 1
  while IFS= read -r namespace; do
    namespace="${namespace#namespace/}"
    [[ -n "$namespace" ]] || continue
    [[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
      echo "ERROR: unsafe run-owned namespace returned by Kubernetes: $namespace" >&2
      return 1
    }
    namespaces+=("$namespace")
  done <<<"$namespace_output"

  for namespace in "${namespaces[@]}"; do
    if ! KUBECONFIG="$KUBECONFIG_PATH" kubectl get namespace "$namespace" >/dev/null 2>&1; then
      continue
    fi
    pvc_output="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get persistentvolumeclaims \
      -n "$namespace" -o name)" || return 1
    while IFS= read -r pvc_name; do
      [[ -n "$pvc_name" ]] || continue
      pvc_name="${pvc_name#persistentvolumeclaim/}"
      pv_name="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get persistentvolumeclaim "$pvc_name" \
        -n "$namespace" -o jsonpath='{.spec.volumeName}' 2>/dev/null)" || continue
      [[ -n "$pv_name" ]] || continue
      driver="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get persistentvolume "$pv_name" \
        -o jsonpath='{.spec.csi.driver}' 2>/dev/null)" || continue
      [[ "$driver" == "ebs.csi.aws.com" ]] || continue
      volume_id="$(KUBECONFIG="$KUBECONFIG_PATH" kubectl get persistentvolume "$pv_name" \
        -o jsonpath='{.spec.csi.volumeHandle}' 2>/dev/null)" || continue
      if ! record_volume_id "$volume_id"; then
        echo "ERROR: EBS CSI returned unsafe volume handle '$volume_id'" >&2
        return 1
      fi
      persistent_volumes+=("$pv_name")
    done <<<"$pvc_output"
  done

  for namespace in "${namespaces[@]}"; do
    if KUBECONFIG="$KUBECONFIG_PATH" kubectl get namespace "$namespace" >/dev/null 2>&1; then
      KUBECONFIG="$KUBECONFIG_PATH" kubectl delete namespace "$namespace" --wait=false || return 1
      KUBECONFIG="$KUBECONFIG_PATH" kubectl wait --for=delete "namespace/$namespace" --timeout=10m || return 1
    fi
  done
  for pv_name in "${persistent_volumes[@]}"; do
    KUBECONFIG="$KUBECONFIG_PATH" kubectl wait --for=delete "persistentvolume/$pv_name" --timeout=10m || return 1
  done

  for volume_id in "${RECORDED_EBS_VOLUME_IDS[@]}"; do
    local deadline=$((SECONDS + 600))
    local count
    while ((SECONDS < deadline)); do
      count="$(aws ec2 describe-volumes --region "$AWS_REGION_VALUE" \
        --filters "Name=volume-id,Values=$volume_id" --query 'length(Volumes)' --output text)" || return 1
      [[ "$count" == "0" ]] && break
      sleep 5
    done
    if [[ "${count:-1}" != "0" ]]; then
      echo "ERROR: EBS volume '$volume_id' was not deleted before the storage timeout" >&2
      return 1
    fi
  done
}

if [[ "$cluster_present" == true ]]; then
  if [[ "$cluster_owned" != true ]]; then
    echo "ERROR: refusing to delete EKS cluster '$CLUSTER_NAME' without matching ownership tag" >&2
    cleanup_status=1
  else
    if [[ -n "$KUBECONFIG_PATH" && -f "$KUBECONFIG_PATH" ]] && ! teardown_kubernetes_storage; then
      echo "ERROR: failed to remove run-owned Kubernetes storage before EKS teardown" >&2
      cleanup_status=1
    fi
    if ! eksctl delete cluster --name "$CLUSTER_NAME" --region "$AWS_REGION_VALUE" --wait; then
      echo "ERROR: failed to delete EKS cluster '$CLUSTER_NAME'" >&2
      cleanup_status=1
    fi
  fi
fi

if repository_arn="$(aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION_VALUE" \
  --query 'repositories[0].repositoryArn' --output text 2>/dev/null)"; then
  ecr_owner="$(aws ecr list-tags-for-resource --resource-arn "$repository_arn" --region "$AWS_REGION_VALUE" \
    --query "tags[?Key=='$OWNER_TAG_KEY'].Value | [0]" --output text)"
  if [[ "$ecr_owner" != "$RUN_ID" ]]; then
    echo "ERROR: refusing to delete ECR repository '$ECR_REPOSITORY' without matching ownership tag" >&2
    cleanup_status=1
  elif ! aws ecr delete-repository --repository-name "$ECR_REPOSITORY" --region "$AWS_REGION_VALUE" --force >/dev/null; then
    echo "ERROR: failed to delete ECR repository '$ECR_REPOSITORY'" >&2
    cleanup_status=1
  fi
fi

if ! remaining_resources="$(aws resourcegroupstaggingapi get-resources \
  --region "$AWS_REGION_VALUE" \
  --tag-filters "Key=$OWNER_TAG_KEY,Values=$RUN_ID" \
  --query 'ResourceTagMappingList[].ResourceARN' \
  --output text)"; then
  echo "ERROR: could not audit run-owned AWS resources" >&2
  cleanup_status=1
elif [[ -n "$remaining_resources" && "$remaining_resources" != None ]]; then
  echo "ERROR: run-owned AWS resources remain after cleanup:" >&2
  tr '\t' '\n' <<<"$remaining_resources" >&2
  cleanup_status=1
fi

if ! remaining_ebs_volumes="$(aws ec2 describe-volumes \
  --region "$AWS_REGION_VALUE" \
  --filters \
    "Name=tag:$OWNER_TAG_KEY,Values=$RUN_ID" \
    "Name=tag:ebs.csi.aws.com/cluster,Values=true" \
  --query 'Volumes[].VolumeId' \
  --output text)"; then
  echo "ERROR: could not audit run-owned EBS CSI volumes" >&2
  cleanup_status=1
elif [[ -n "$remaining_ebs_volumes" && "$remaining_ebs_volumes" != None ]]; then
  echo "ERROR: run-owned EBS CSI volumes remain after cleanup:" >&2
  tr '\t' '\n' <<<"$remaining_ebs_volumes" >&2
  cleanup_status=1
fi

for volume_id in "${RECORDED_EBS_VOLUME_IDS[@]}"; do
  if ! recorded_volume_count="$(aws ec2 describe-volumes --region "$AWS_REGION_VALUE" \
    --filters "Name=volume-id,Values=$volume_id" --query 'length(Volumes)' --output text)"; then
    echo "ERROR: could not audit recorded EBS volume '$volume_id'" >&2
    cleanup_status=1
  elif [[ "$recorded_volume_count" != "0" ]]; then
    echo "ERROR: recorded EBS volume remains after cleanup: $volume_id" >&2
    cleanup_status=1
  fi
done

exit "$cleanup_status"
