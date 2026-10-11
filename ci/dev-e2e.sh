#!/usr/bin/env bash
#
# Local KIND e2e runner for redis-cluster-kubernetes-operator.
#
# Boots or reuses a standard KIND cluster, installs the RedisCluster CRD, and
# runs the kind_e2e suite with ordinary public Go and Kubernetes tooling.
#
# Usage:
#   ci/dev-e2e.sh                              # run the default kind_e2e tier
#   ci/dev-e2e.sh -run '^TestE2E_Bootstrap$'   # one test
#   E2E_SLOW=1 ci/dev-e2e.sh -run StaleGreen   # one slow-gated test
#   E2E_BUILD_LOADGEN=1 ci/dev-e2e.sh -run PrimaryKill # one loadgen test
#   E2E_SLOW=1 E2E_BUILD_LOADGEN=1 ci/dev-e2e.sh -run ForceCutover
#   KIND_CLUSTER_NAME=foo ci/dev-e2e.sh        # name the cluster
#
# By default the repository-local harness creates and removes an isolated KIND
# cluster. Set E2E_CREATE_KIND_CLUSTER=false with KIND_CLUSTER_NAME to reuse one.

set -euo pipefail

OP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export E2E_CREATE_KIND_CLUSTER="${E2E_CREATE_KIND_CLUSTER:-true}"
export KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.33.1}"
TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-30m}"
export KIND_E2E_REDIS_IMAGE="${KIND_E2E_REDIS_IMAGE:-redis:8.6.0}"

command -v go >/dev/null 2>&1 || { echo "ERROR: 'go' not on PATH" >&2; exit 1; }

if [[ "${E2E_BUILD_LOADGEN:-0}" == "1" ]]; then
	 echo "Building the rueidis loadgen image"
	 E2E_BUILD_ONLY=1 bash "$OP_DIR/ci/build-loadgen-image.sh"
	 export E2E_LOADGEN=1
fi

cd "$OP_DIR"
echo "Running isolated kind_e2e suite with $KIND_NODE_IMAGE and $KIND_E2E_REDIS_IMAGE (timeout=$TEST_TIMEOUT)"
exec go test -tags kind_e2e -count=1 ./e2e/ -timeout "$TEST_TIMEOUT" "$@"
