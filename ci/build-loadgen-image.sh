#!/usr/bin/env bash
#
# Build the image used by continuous-QPS E2E tests and optionally KIND-load it.
#
# Cross-compiles the static binary, bakes it into a minimal public base image,
# and loads it into the selected KIND cluster.
#
# Usage:
#   ci/build-loadgen-image.sh                       # build + load into rc-e2e-dev
#   E2E_BUILD_ONLY=1 ci/build-loadgen-image.sh      # build; harness loads it later
#   LOADGEN_IMAGE=foo:bar ci/build-loadgen-image.sh # custom tag
#   KIND_CLUSTER_NAME=foo ci/build-loadgen-image.sh # custom cluster

set -euo pipefail

OP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${LOADGEN_IMAGE:-rco-loadgen:test}"
CLUSTER="${KIND_CLUSTER_NAME:-rc-e2e-dev}"

for bin in go docker kind; do
  command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: '$bin' not on PATH" >&2; exit 1; }
done

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# KIND nodes are linux; match the local (VM) arch so kind load doesn't hit the
# multi-arch manifest bug.
ARCH="$(go env GOARCH)"
echo "Compiling loadgen for linux/${ARCH}"
( cd "$OP_DIR" && CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
    go build -trimpath -ldflags="-s -w" -o "$TMP/loadgen" ./test/framework/kind/loadgen/cmd/loadgen )

echo "Building image ${IMAGE}"
docker build -f "$OP_DIR/test/framework/kind/loadgen/Dockerfile" -t "$IMAGE" "$TMP"

if [[ "${E2E_BUILD_ONLY:-0}" == "1" ]]; then
  echo "OK: ${IMAGE} built; KIND harness will load it after cluster creation"
  exit 0
fi

echo "Loading ${IMAGE} into kind cluster ${CLUSTER}"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "OK: ${IMAGE} available in kind cluster ${CLUSTER}"
