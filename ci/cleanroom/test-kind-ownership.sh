#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kind-ownership-test.XXXXXX")"
readonly FAKE_BIN="$TEST_DIR/bin"
readonly CLUSTER_NAME="redis-cleanroom-ownership-collision"
readonly CLUSTER_STATE="$TEST_DIR/cluster-exists"
readonly MOCK_LOG="$TEST_DIR/kind.log"

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
    [[ ! -f "$CLUSTER_STATE" ]] || echo "$CLUSTER_NAME"
    ;;
  create:cluster)
    # Simulate another run winning the same-name create race.
    : >"$CLUSTER_STATE"
    exit 1
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

for command_name in docker go helm kubectl; do
  cat >"$FAKE_BIN/$command_name" <<'COMMAND'
#!/usr/bin/env bash
exit 99
COMMAND
  chmod 0755 "$FAKE_BIN/$command_name"
done

export PATH="$FAKE_BIN:$PATH"
export CLUSTER_NAME CLUSTER_STATE MOCK_LOG

runner_stderr="$TEST_DIR/runner-stderr"
if CLEANROOM_RUN_ID=ownership-collision \
  TMPDIR="$TEST_DIR" \
  "$REPO_ROOT/ci/run-cleanroom.sh" kind smoke 2>"$runner_stderr"; then
  echo "clean-room runner unexpectedly succeeded after the mocked create collision" >&2
  exit 1
fi

grep -F -q "kind create cluster --name $CLUSTER_NAME " "$MOCK_LOG"
if grep -F -q "kind delete cluster --name $CLUSTER_NAME" "$MOCK_LOG"; then
  echo "clean-room runner deleted a same-name cluster it did not create" >&2
  exit 1
fi

echo "KIND ownership tests passed"
