#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cleanup-eks-test.XXXXXX")"
trap 'rm -rf "$TEST_DIR"' EXIT

mkdir -p "$TEST_DIR/bin"

cat >"$TEST_DIR/bin/aws" <<'MOCK_AWS'
#!/usr/bin/env bash
set -euo pipefail
printf 'aws %s\n' "$*" >>"$MOCK_LOG"
case "$1:$2" in
  sts:get-caller-identity)
    echo 123456789012
    ;;
  eks:describe-cluster)
    [[ "$MOCK_MODE" == deletion-failure || "$MOCK_MODE" == storage-teardown ]] || exit 255
    echo test-run
    ;;
  cloudformation:describe-stacks | ecr:describe-repositories)
    exit 255
    ;;
  resourcegroupstaggingapi:get-resources)
    if [[ "$MOCK_MODE" == deletion-failure ]]; then
      echo arn:aws:cloudformation:us-east-2:123456789012:stack/residual
    fi
    ;;
  ec2:describe-volumes)
    if [[ "$MOCK_MODE" == deletion-failure ]]; then
      echo vol-0123456789abcdef0
    elif [[ "$*" == *"Name=volume-id"* ]]; then
      echo 0
    fi
    ;;
  *)
    echo "unexpected aws call: $*" >&2
    exit 2
    ;;
esac
MOCK_AWS

cat >"$TEST_DIR/bin/eksctl" <<'MOCK_EKSCTL'
#!/usr/bin/env bash
set -euo pipefail
printf 'eksctl %s\n' "$*" >>"$MOCK_LOG"
[[ "$MOCK_MODE" != deletion-failure ]]
MOCK_EKSCTL

cat >"$TEST_DIR/bin/kubectl" <<'MOCK_KUBECTL'
#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"$MOCK_LOG"
case "$*" in
  "config current-context") echo cleanroom-redis-op-test-run ;;
  "get --raw=/readyz") echo ok ;;
  "get namespaces "*) echo namespace/redis-cluster-scenario-test-run ;;
  "get namespace "*) ;;
  "get persistentvolumeclaims "*) echo persistentvolumeclaim/data-0 ;;
  "get persistentvolumeclaim "*) echo pv-test ;;
  *"jsonpath={.spec.csi.driver}"*) echo ebs.csi.aws.com ;;
  *"jsonpath={.spec.csi.volumeHandle}"*) echo vol-0123456789abcdef0 ;;
  "delete namespace "* | "wait --for=delete "*) ;;
  *) echo "unexpected kubectl call: $*" >&2; exit 2 ;;
esac
MOCK_KUBECTL
chmod 0755 "$TEST_DIR/bin/aws" "$TEST_DIR/bin/eksctl" "$TEST_DIR/bin/kubectl"

cat >"$TEST_DIR/state" <<'STATE'
run_id=test-run
aws_account_id=123456789012
aws_region=us-east-2
cluster_name=redis-op-test-run
ecr_repository=redis-op-cleanroom-test-run
owner_tag_key=redis-cluster-kubernetes-operator-cleanroom
owner_tag_value=test-run
STATE

cp "$TEST_DIR/state" "$TEST_DIR/state-with-kubeconfig"
printf 'kubeconfig=%s\n' "$TEST_DIR/kubeconfig" >>"$TEST_DIR/state-with-kubeconfig"
: >"$TEST_DIR/kubeconfig"

export PATH="$TEST_DIR/bin:$PATH"
export MOCK_LOG="$TEST_DIR/calls.log"

export MOCK_MODE=deletion-failure
set +e
failure_output="$("$REPO_ROOT/ci/cleanroom/cleanup-eks.sh" "$TEST_DIR/state" 2>&1)"
failure_status=$?
set -e
[[ "$failure_status" -ne 0 ]] || { echo "cleanup unexpectedly succeeded after mocked deletion failure" >&2; exit 1; }
grep -q '^aws resourcegroupstaggingapi get-resources ' "$MOCK_LOG"
grep -q '^aws ec2 describe-volumes ' "$MOCK_LOG"

: >"$MOCK_LOG"
export MOCK_MODE=storage-teardown
"$REPO_ROOT/ci/cleanroom/cleanup-eks.sh" "$TEST_DIR/state-with-kubeconfig"
grep -q '^kubectl delete namespace redis-cluster-example --wait=false$' "$MOCK_LOG"
grep -q '^kubectl delete namespace redis-cluster-scenario-test-run --wait=false$' "$MOCK_LOG"
grep -q '^kubectl wait --for=delete persistentvolume/pv-test --timeout=10m$' "$MOCK_LOG"
grep -q '^eksctl delete cluster --name redis-op-test-run --region us-east-2 --wait$' "$MOCK_LOG"
grep -q '^ebs_volume_id=vol-0123456789abcdef0$' "$TEST_DIR/state-with-kubeconfig"
grep -q 'run-owned AWS resources remain after cleanup' <<<"$failure_output"
grep -q 'run-owned EBS CSI volumes remain after cleanup' <<<"$failure_output"

: >"$MOCK_LOG"
export MOCK_MODE=already-absent
"$REPO_ROOT/ci/cleanroom/cleanup-eks.sh" "$TEST_DIR/state"
if grep -q '^eksctl ' "$MOCK_LOG"; then
  echo "idempotent cleanup invoked eksctl for an absent cluster" >&2
  exit 1
fi
grep -q '^aws resourcegroupstaggingapi get-resources ' "$MOCK_LOG"
grep -q '^aws ec2 describe-volumes ' "$MOCK_LOG"
