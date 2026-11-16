#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

set -euo pipefail
cd "$(dirname "$0")/.."
readonly TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
export VERSION=9.8.7 DIGEST="sha256:$(printf '%064d' 1)" RUNNER_TEMP="$TEMP_DIR"
export IMAGE=ghcr.io/test/operator CHART_REPO=oci://ghcr.io/test/charts
export SOURCE_DIGEST="$DIGEST" STATE="$TEMP_DIR/state" CALLS="$TEMP_DIR/calls"
ci/prepare-release.sh
cp "$RUNNER_TEMP/charts/redis-cluster-$VERSION.tgz" "$TEMP_DIR/original.tgz"
python3 - <<'PY'
import os, pathlib, tarfile, yaml
root = pathlib.Path(os.environ["RUNNER_TEMP"])
docs = list(yaml.safe_load_all((root / "install.yaml").read_text()))
images = [c["image"] for d in docs if d and d["kind"] == "Deployment"
          for c in d["spec"]["template"]["spec"]["containers"] if c["name"] == "operator"]
assert images and all(i.endswith("@" + os.environ["DIGEST"]) for i in images), images
assert all(d["metadata"]["namespace"] == "redis-cluster-operator"
           for d in docs if d and d["kind"] in ("Deployment", "Role", "RoleBinding", "ServiceAccount"))
with tarfile.open(root / "charts" / ("redis-cluster-kubernetes-operator-" + os.environ["VERSION"] + ".tgz")) as chart:
    assert not any("/deploy/" in m.name for m in chart)
PY
# Retry preparation must produce identical chart contents without changing source values.
ci/prepare-release.sh
# shellcheck source=ci/release-artifacts.sh
source ci/release-artifacts.sh
mkdir "$TEMP_DIR/bin"
cat > "$TEMP_DIR/bin/docker" <<'MOCK'
#!/usr/bin/env bash
set -eu
case "$3" in
  inspect)
    case "$(cat "$STATE")" in
      missing) echo 'manifest unknown' >&2; exit 1 ;;
      denied) echo 'unauthorized' >&2; exit 1 ;;
      network) echo 'connection refused' >&2; exit 1 ;;
      malformed) echo 'invalid digest' ;;
      conflict) printf 'sha256:%064d\n' 2 ;;
      *) echo "$SOURCE_DIGEST" ;;
    esac ;;
  create) echo create >> "$CALLS"; echo present > "$STATE" ;;
  *) exit 1 ;;
esac
MOCK
cat > "$TEMP_DIR/bin/helm" <<'MOCK'
#!/usr/bin/env bash
set -eu
case "$1" in
  push) echo push >> "$CALLS" ;;
  pull) cp "$RUNNER_TEMP/original.tgz" "$6/redis-cluster-$VERSION.tgz" ;;
  *) exit 1 ;;
esac
MOCK
chmod +x "$TEMP_DIR/bin/"*
export PATH="$TEMP_DIR/bin:$PATH"
echo missing > "$STATE"
[[ -z "$(registry_digest "$IMAGE:test")" ]]
for failure in denied network malformed; do
  echo "$failure" > "$STATE"
  if registry_digest "$IMAGE:test"; then echo "accepted $failure" >&2; exit 1; fi
done
echo missing > "$STATE"
promote_image
[[ "$(cat "$CALLS")" == create ]]
promote_image
[[ "$(wc -l < "$CALLS" | tr -d ' ')" == 1 ]]
echo conflict > "$STATE"
if promote_image; then echo 'overwrote conflicting image' >&2; exit 1; fi
echo missing > "$STATE"
publish_chart "$RUNNER_TEMP/charts/redis-cluster-$VERSION.tgz" redis-cluster
echo present > "$STATE"
publish_chart "$RUNNER_TEMP/charts/redis-cluster-$VERSION.tgz" redis-cluster
[[ "$(wc -l < "$CALLS" | tr -d ' ')" == 2 ]]
# A different package at the same version must fail, without a second push.
if publish_chart "$RUNNER_TEMP/charts/redis-cluster-kubernetes-operator-$VERSION.tgz" redis-cluster; then
  echo 'accepted conflicting chart' >&2; exit 1
fi
[[ "$(wc -l < "$CALLS" | tr -d ' ')" == 2 ]]
# Exercise the ancestry check with both annotated main tags and an off-main tag.
git init -q --initial-branch=release-test "$TEMP_DIR/repo"
git -C "$TEMP_DIR/repo" config commit.gpgsign false
git -C "$TEMP_DIR/repo" config tag.gpgsign false
git -C "$TEMP_DIR/repo" config core.hooksPath /dev/null
git -C "$TEMP_DIR/repo" -c user.name=Test -c user.email=test@example.com commit -qm base --allow-empty
git -C "$TEMP_DIR/repo" branch main
git -C "$TEMP_DIR/repo" -c user.name=Test -c user.email=test@example.com tag -a v9.8.7 -m release
git -C "$TEMP_DIR/repo" merge-base --is-ancestor 'v9.8.7^{commit}' main
git -C "$TEMP_DIR/repo" -c user.name=Test -c user.email=test@example.com commit -qm off-main --allow-empty
if git -C "$TEMP_DIR/repo" merge-base --is-ancestor HEAD main; then
  echo 'accepted off-main commit' >&2; exit 1
fi
echo 'release preparation and retry checks OK'
