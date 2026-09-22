#!/usr/bin/env bash
# Build, install, and qualify the public operator in a clean Kubernetes cluster.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly PROVIDER="${1:-}"
readonly LANE="${2:-}"

usage() {
  cat >&2 <<'USAGE'
usage:
  ci/run-cleanroom.sh kind {smoke|full|packaging|lifecycle|topology|security}
  ci/run-cleanroom.sh eks portability
USAGE
}

case "$PROVIDER:$LANE" in
  kind:smoke|kind:full|kind:packaging|kind:lifecycle|kind:topology|kind:security)
    exec "$REPO_ROOT/ci/cleanroom/kind.sh" "$LANE"
    ;;
  eks:portability)
    exec "$REPO_ROOT/ci/cleanroom/eks.sh"
    ;;
  *)
    usage
    exit 2
    ;;
esac
