#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.


set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/local-kind-ownership-test.XXXXXX")"
readonly FAKE_BIN="$TEST_DIR/bin"
readonly MOCK_LOG="$TEST_DIR/commands.log"
readonly CLUSTER_NAME="redis-operator-dev"
readonly OWNERSHIP_PATH="$TEST_DIR/kind-owner"
readonly KUBECONFIG_PATH="$TEST_DIR/kubeconfig"

cleanup() {
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT

mkdir -p "$FAKE_BIN"
cat >"$FAKE_BIN/kind" <<'KIND'
#!/usr/bin/env bash
set -euo pipefail
printf 'kind %s\n' "$*" >>"$MOCK_LOG"
case "${1:-}:${2:-}" in
  version:)
    echo 'kind v0.30.0 go1.25.0 linux/amd64'
    ;;
  get:clusters)
    echo "$CLUSTER_NAME"
    ;;
  get:nodes)
    echo "$CLUSTER_NAME-control-plane"
    ;;
  delete:cluster)
    ;;
  *)
    echo "unexpected kind call: $*" >&2
    exit 2
    ;;
esac
KIND
chmod 0755 "$FAKE_BIN/kind"

cat >"$FAKE_BIN/docker" <<'DOCKER'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %s\n' "$*" >>"$MOCK_LOG"
case "${1:-}" in
  info)
    ;;
  inspect)
    echo "${MOCK_CLUSTER_ID:-current-cluster-id}"
    ;;
  *)
    echo "unexpected docker call: $*" >&2
    exit 2
    ;;
esac
DOCKER
chmod 0755 "$FAKE_BIN/docker"

for command_name in go helm kubectl; do
  cat >"$FAKE_BIN/$command_name" <<'COMMAND'
#!/usr/bin/env bash
exit 99
COMMAND
  chmod 0755 "$FAKE_BIN/$command_name"
done

export PATH="$FAKE_BIN:$PATH"
export CLUSTER_NAME MOCK_LOG

if LOCAL_KIND_CLUSTER="$CLUSTER_NAME" \
  LOCAL_KIND_OWNERSHIP_FILE="$OWNERSHIP_PATH" \
  LOCAL_KUBECONFIG="$KUBECONFIG_PATH" \
  "$REPO_ROOT/scripts/local-kind-up.sh" 2>"$TEST_DIR/up-stderr"; then
  echo "local-kind-up reused an unowned cluster" >&2
  exit 1
fi
grep -F -q "refusing to reuse unowned KIND cluster '$CLUSTER_NAME'" "$TEST_DIR/up-stderr"

if LOCAL_KIND_CLUSTER="$CLUSTER_NAME" \
  LOCAL_KIND_OWNERSHIP_FILE="$OWNERSHIP_PATH" \
  LOCAL_KUBECONFIG="$KUBECONFIG_PATH" \
  "$REPO_ROOT/scripts/local-kind-down.sh" 2>"$TEST_DIR/down-stderr"; then
  echo "local-kind-down deleted an unowned cluster" >&2
  exit 1
fi
grep -F -q "refusing to delete unowned KIND cluster '$CLUSTER_NAME'" "$TEST_DIR/down-stderr"
if grep -F -q "kind delete cluster" "$MOCK_LOG"; then
  echo "local-kind-down called kind delete for an unowned cluster" >&2
  exit 1
fi

printf '%s\t%s\n' "$CLUSTER_NAME" "different-cluster-id" >"$OWNERSHIP_PATH"
if LOCAL_KIND_CLUSTER="$CLUSTER_NAME" \
  LOCAL_KIND_OWNERSHIP_FILE="$OWNERSHIP_PATH" \
  LOCAL_KUBECONFIG="$KUBECONFIG_PATH" \
  "$REPO_ROOT/scripts/local-kind-down.sh" 2>"$TEST_DIR/stale-stderr"; then
  echo "local-kind-down accepted a stale ownership marker" >&2
  exit 1
fi
grep -F -q "refusing to delete unowned KIND cluster '$CLUSTER_NAME'" "$TEST_DIR/stale-stderr"

printf '%s\t%s\n' "$CLUSTER_NAME" "current-cluster-id" >"$OWNERSHIP_PATH"
: >"$KUBECONFIG_PATH"
LOCAL_KIND_CLUSTER="$CLUSTER_NAME" \
  LOCAL_KIND_OWNERSHIP_FILE="$OWNERSHIP_PATH" \
  LOCAL_KUBECONFIG="$KUBECONFIG_PATH" \
  "$REPO_ROOT/scripts/local-kind-down.sh" >"$TEST_DIR/owned-stdout"
grep -F -q "kind delete cluster --name $CLUSTER_NAME" "$MOCK_LOG"
[[ ! -e "$OWNERSHIP_PATH" && ! -e "$KUBECONFIG_PATH" ]]

echo "Local KIND ownership tests passed"
