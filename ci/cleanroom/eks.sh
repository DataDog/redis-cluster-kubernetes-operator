#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# shellcheck source=common.sh
source "$REPO_ROOT/ci/cleanroom/common.sh"

readonly RUN_ID="${CLEANROOM_RUN_ID:?CLEANROOM_RUN_ID is required for EKS qualification}"
readonly AWS_ACCOUNT_ID="${CLEANROOM_AWS_ACCOUNT_ID:?CLEANROOM_AWS_ACCOUNT_ID is required}"
readonly AWS_REGION_VALUE="${CLEANROOM_AWS_REGION:?CLEANROOM_AWS_REGION is required}"
readonly CLUSTER_NAME="redis-op-$RUN_ID"
readonly CONTEXT="cleanroom-$CLUSTER_NAME"
readonly ECR_REPOSITORY="redis-op-cleanroom-$RUN_ID"
readonly OWNER_TAG_KEY="redis-cluster-kubernetes-operator-cleanroom"
readonly OWNER_TAG_VALUE="$RUN_ID"
readonly NAMESPACE="redis-cluster-example"
readonly RELEASE="redis-operator"
readonly TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/redis-cleanroom-eks.XXXXXX")"
readonly KUBECONFIG_PATH="$TEMP_DIR/kubeconfig"
readonly ARTIFACT_DIR="${CLEANROOM_ARTIFACT_DIR:-$REPO_ROOT/cleanroom-artifacts/$RUN_ID}"
readonly RESOURCE_STATE="${CLEANROOM_RESOURCE_STATE:-${TMPDIR:-/tmp}/redis-cleanroom-eks-$RUN_ID.state}"
readonly IMAGE_REVISION="${CLEANROOM_IMAGE_REVISION:?CLEANROOM_IMAGE_REVISION is required for EKS qualification}"
readonly IMAGE_TAG="$IMAGE_REVISION-$RUN_ID"
readonly PORTABILITY_SELECTOR='^(TestCleanroom_ReferenceInstall|TestCleanroom_PrimaryAndReplicaRecovery|TestCleanroom_OperatorRestartResume|TestCleanroom_BlueGreenAvailability|TestCleanroom_SecretAuthentication|TestCleanroom_ObservabilityAndRBAC)$'

cleanup() {
  local status=$?
  local cleanup_status=0
  trap - EXIT
  set +e
  if ((status != 0)) && [[ -f "$KUBECONFIG_PATH" ]]; then
    cleanroom_collect_failure_artifacts "$KUBECONFIG_PATH" "$ARTIFACT_DIR" "$NAMESPACE" || \
      echo "WARNING: failure artifact collection did not complete" >&2
    echo "Clean-room failure artifacts: $ARTIFACT_DIR" >&2
  fi
  if [[ -f "$RESOURCE_STATE" ]]; then
    "$REPO_ROOT/ci/cleanroom/cleanup-eks.sh" "$RESOURCE_STATE" || cleanup_status=$?
  fi
  rm -f "$KUBECONFIG_PATH" "$TEMP_DIR/eksctl.yaml" "$TEMP_DIR/gp3.yaml"
  rmdir "$TEMP_DIR" 2>/dev/null || true
  if ((cleanup_status != 0)); then
    echo "ERROR: EKS cleanup failed; retained resource state: $RESOURCE_STATE" >&2
    exit 1
  fi
  exit "$status"
}
trap cleanup EXIT

cleanroom_validate_run_id "$RUN_ID"
[[ "$AWS_ACCOUNT_ID" =~ ^[0-9]{12}$ ]] || cleanroom_die "CLEANROOM_AWS_ACCOUNT_ID must be a 12-digit account id"
[[ "$AWS_REGION_VALUE" =~ ^[a-z]{2}-[a-z]+-[0-9]$ ]] || cleanroom_die "invalid CLEANROOM_AWS_REGION"
[[ "$IMAGE_REVISION" =~ ^[0-9a-f]{7,40}$ ]] || cleanroom_die "CLEANROOM_IMAGE_REVISION must be a 7-40 character lowercase Git SHA"
[[ "$IMAGE_TAG" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || cleanroom_die "unsafe ECR image tag"
cleanroom_require_commands aws docker eksctl go helm kubectl

actual_account="$(aws sts get-caller-identity --query Account --output text)"
[[ "$actual_account" == "$AWS_ACCOUNT_ID" ]] || cleanroom_die \
  "AWS identity is account '$actual_account', expected '$AWS_ACCOUNT_ID'"

if aws eks describe-cluster --name "$CLUSTER_NAME" --region "$AWS_REGION_VALUE" >/dev/null 2>&1; then
  cleanroom_die "refusing to reuse existing EKS cluster '$CLUSTER_NAME'"
fi
if aws ecr describe-repositories --repository-names "$ECR_REPOSITORY" --region "$AWS_REGION_VALUE" >/dev/null 2>&1; then
  cleanroom_die "refusing to reuse existing ECR repository '$ECR_REPOSITORY'"
fi

printf '%s\n' \
  "run_id=$RUN_ID" \
  "aws_account_id=$AWS_ACCOUNT_ID" \
  "aws_region=$AWS_REGION_VALUE" \
  "cluster_name=$CLUSTER_NAME" \
  "ecr_repository=$ECR_REPOSITORY" \
  "owner_tag_key=$OWNER_TAG_KEY" \
  "owner_tag_value=$OWNER_TAG_VALUE" \
  "kubeconfig=$KUBECONFIG_PATH" \
  >"$RESOURCE_STATE"
echo "Clean-room resource state: $RESOURCE_STATE"

repository_uri="$(aws ecr create-repository \
  --repository-name "$ECR_REPOSITORY" \
  --region "$AWS_REGION_VALUE" \
  --image-tag-mutability IMMUTABLE \
  --image-scanning-configuration scanOnPush=true \
  --encryption-configuration encryptionType=AES256 \
  --tags "Key=$OWNER_TAG_KEY,Value=$OWNER_TAG_VALUE" \
  --query repository.repositoryUri \
  --output text)"

aws ecr get-login-password --region "$AWS_REGION_VALUE" | \
  docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION_VALUE.amazonaws.com" >/dev/null
docker build --platform linux/amd64 --tag "$repository_uri:$IMAGE_TAG" "$REPO_ROOT"
docker push "$repository_uri:$IMAGE_TAG"
image_digest="$(aws ecr describe-images \
  --repository-name "$ECR_REPOSITORY" \
  --region "$AWS_REGION_VALUE" \
  --image-ids imageTag="$IMAGE_TAG" \
  --query 'imageDetails[0].imageDigest' \
  --output text)"
[[ "$image_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || cleanroom_die "ECR did not report a valid image digest"
printf 'image_repository=%s\nimage_tag=%s\nimage_digest=%s\n' \
  "$repository_uri" "$IMAGE_TAG" "$image_digest" >>"$RESOURCE_STATE"

read -r availability_zone_1 availability_zone_2 availability_zone_3 extra_zone < <(aws ec2 describe-availability-zones \
  --region "$AWS_REGION_VALUE" \
  --filters Name=state,Values=available \
  --query 'AvailabilityZones[0:3].ZoneName' \
  --output text)
[[ -n "$availability_zone_1" && -n "$availability_zone_2" && -n "$availability_zone_3" && -z "${extra_zone:-}" ]] || \
  cleanroom_die "region '$AWS_REGION_VALUE' does not expose three available zones"

cat >"$TEMP_DIR/eksctl.yaml" <<EKS_CONFIG
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: $CLUSTER_NAME
  region: $AWS_REGION_VALUE
  version: "1.33"
availabilityZones:
  - $availability_zone_1
  - $availability_zone_2
  - $availability_zone_3
upgradePolicy:
  supportType: EXTENDED
tags:
  $OWNER_TAG_KEY: $OWNER_TAG_VALUE
  redis-cluster-kubernetes-operator-cleanroom-owned: "true"
iam:
  withOIDC: true
addonsConfig:
  autoApplyPodIdentityAssociations: true
addons:
  - name: eks-pod-identity-agent
  - name: aws-ebs-csi-driver
managedNodeGroups:
  - name: cleanroom-workers
    amiFamily: AmazonLinux2023
    instanceType: m6i.large
    desiredCapacity: 3
    minSize: 3
    maxSize: 3
    availabilityZones:
      - $availability_zone_1
      - $availability_zone_2
      - $availability_zone_3
    privateNetworking: true
    labels:
      cleanroom.redis-operator.datadoghq.com/worker: "true"
    tags:
      $OWNER_TAG_KEY: $OWNER_TAG_VALUE
EKS_CONFIG

eksctl create cluster --config-file "$TEMP_DIR/eksctl.yaml" --kubeconfig "$KUBECONFIG_PATH"
export KUBECONFIG="$KUBECONFIG_PATH"
generated_context="$(kubectl config current-context)"
[[ -n "$generated_context" ]] || cleanroom_die "eksctl did not create a current context"
[[ "$(kubectl config get-contexts -o name | wc -l | tr -d ' ')" == 1 ]] || cleanroom_die \
  "dedicated EKS kubeconfig contains more than one context"
kubectl config rename-context "$generated_context" "$CONTEXT" >/dev/null
cleanroom_verify_context "$KUBECONFIG_PATH" "$CONTEXT"

while IFS= read -r storage_class; do
  [[ -n "$storage_class" && "$storage_class" != cleanroom-gp3 ]] || continue
  kubectl annotate storageclass "$storage_class" storageclass.kubernetes.io/is-default-class- --overwrite
done < <(kubectl get storageclass -o jsonpath='{range .items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{"\n"}{end}')

cat >"$TEMP_DIR/gp3.yaml" <<STORAGE_CLASS
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: cleanroom-gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters:
  type: gp3
  encrypted: "true"
  tagSpecification_1: "$OWNER_TAG_KEY=$OWNER_TAG_VALUE"
STORAGE_CLASS
kubectl apply -f "$TEMP_DIR/gp3.yaml"
[[ "$(kubectl get storageclass cleanroom-gp3 -o jsonpath='{.provisioner}')" == ebs.csi.aws.com ]] || \
  cleanroom_die "default gp3 StorageClass does not use ebs.csi.aws.com"

cleanroom_install_public_chart "$REPO_ROOT" "$NAMESPACE" "$RELEASE" "$repository_uri" "$IMAGE_TAG" IfNotPresent
operator_pod="$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=operator \
  -o jsonpath='{.items[0].metadata.name}')"
[[ -n "$operator_pod" ]] || cleanroom_die "operator pod was not created"
running_image_id="$(kubectl get pod -n "$NAMESPACE" "$operator_pod" -o jsonpath='{.status.containerStatuses[0].imageID}')"
[[ "$running_image_id" == *"@$image_digest" ]] || cleanroom_die \
  "running operator image digest does not match the ECR image"

export CLEANROOM_PROVIDER=eks
export CLEANROOM_RUN_ID="$RUN_ID"
export CLEANROOM_NAMESPACE="$NAMESPACE"
export CLEANROOM_RELEASE="$RELEASE"
export CLEANROOM_OPERATOR_IMAGE="$repository_uri:$IMAGE_TAG"
export CLEANROOM_ARTIFACT_DIR="$ARTIFACT_DIR"
export CLEANROOM_REPO_ROOT="$REPO_ROOT"
export CLEANROOM_RESOURCE_STATE="$RESOURCE_STATE"

cleanroom_run_tests "$REPO_ROOT" "$PORTABILITY_SELECTOR"
