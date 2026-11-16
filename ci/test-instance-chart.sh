#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Validate deploy/redis-cluster and keep its default render identical to
# examples/redis-cluster.yaml, so the chart and the raw example cannot drift.

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CHART="$REPO_ROOT/deploy/redis-cluster"
readonly EXAMPLE="$REPO_ROOT/examples/redis-cluster.yaml"

helm lint "$CHART" --strict --kube-version 1.33.0
helm lint "$CHART" --strict --kube-version 1.33.0 \
  --set tls.enabled=true --set tls.secretName=cache-tls --set primaries=4
if helm template missing-tls-secret "$CHART" --kube-version 1.33.0 --set tls.enabled=true >/dev/null 2>&1; then
  echo "tls.enabled without tls.secretName passed validation" >&2
  exit 1
fi

helm template cache "$CHART" --namespace redis-cluster-example --kube-version 1.33.0 \
  | python3 -c '
import sys, yaml

def objects(stream):
    out = {}
    for doc in yaml.safe_load_all(stream):
        if not doc or doc["kind"] not in ("Service", "RedisCluster"):
            continue
        meta = doc["metadata"]
        meta.pop("namespace", None)
        labels = meta.get("labels", {})
        for key in ("app.kubernetes.io/managed-by", "helm.sh/chart"):
            labels.pop(key, None)
        if not labels:
            meta.pop("labels", None)
        out[doc["kind"]] = doc
    return out

rendered = objects(sys.stdin)
with open(sys.argv[1]) as f:
    example = objects(f)
if rendered != example:
    sys.exit("deploy/redis-cluster default render differs from " + sys.argv[1])
' "$EXAMPLE"
# The instance chart and the operator chart must accept the same Kubernetes
# versions, including the staging qualification version and its boundaries.
for kube_version in 1.33.0 1.36.0; do
  helm template cache "$CHART" --kube-version "$kube_version" >/dev/null
done
for kube_version in 1.32.9 1.34.0 1.35.0 1.37.0; do
  if helm template cache "$CHART" --kube-version "$kube_version" >/dev/null 2>&1; then
    echo "instance chart accepted unsupported Kubernetes $kube_version" >&2
    exit 1
  fi
done

# Probe the TLS listener, including when Redis requires client certificates.
# A custom port must be used consistently by redis-server, the container and
# both probes, rather than silently probing the always-enabled plaintext port.
for auth_clients in no optional yes; do
  helm template cache "$CHART" --kube-version 1.33.0 \
    --set tls.enabled=true --set tls.secretName=cache-tls \
    --set tls.port=6381 --set tls.authClients="$auth_clients" \
    | python3 -c '
import sys, yaml
cluster = next(doc for doc in yaml.safe_load_all(sys.stdin) if doc and doc["kind"] == "RedisCluster")
container = cluster["spec"]["statefulsetTemplate"]["spec"]["template"]["spec"]["containers"][0]
args = container["args"]
assert args[args.index("--tls-port") + 1] == "6381"
assert args[args.index("--tls-auth-clients") + 1] == sys.argv[1]
assert {port["name"]: port["containerPort"] for port in container["ports"]}["redis-tls"] == 6381
expected = ["redis-cli", "-e", "-h", "127.0.0.1", "-p", "6381", "--tls",
            "--cacert", "/tls/ca.crt"]
if sys.argv[1] == "yes":
    expected += ["--cert", "/tls/tls.crt", "--key", "/tls/tls.key"]
expected += ["ping"]
for probe in ("readinessProbe", "livenessProbe"):
    assert container[probe]["exec"]["command"] == expected, probe + " must check the TLS listener"
' "$auth_clients"
done
# Helm omits NOTES.txt from template output. Render the same source through tpl
# in a temporary chart so the emitted shell commands can be exercised offline.
notes_chart="$(mktemp -d)"
trap 'rm -rf "$notes_chart"' EXIT
cp -R "$CHART/." "$notes_chart/"
cp "$CHART/templates/NOTES.txt" "$notes_chart/connection-notes.txt"
cat > "$notes_chart/templates/notes-test.yaml" <<'NOTES_TEMPLATE'
apiVersion: v1
kind: ConfigMap
metadata:
  name: connection-notes-test
data:
  notes: |-
{{ tpl (.Files.Get "connection-notes.txt") . | indent 4 }}
NOTES_TEMPLATE
for auth_clients in disabled no optional yes; do
  tls_enabled=true
  [[ "$auth_clients" != disabled ]] || tls_enabled=false
  helm template notes-release "$notes_chart" --namespace notes-namespace --kube-version 1.33.0 \
    --set fullnameOverride=notes-cache --set tls.enabled="$tls_enabled" \
    --set tls.secretName=cache-tls --set tls.port=6381 \
    --set tls.authClients="${auth_clients/disabled/optional}" \
    | python3 -c '
import json, os, pathlib, subprocess, sys, tempfile, yaml
notes = next(doc["data"]["notes"] for doc in yaml.safe_load_all(sys.stdin)
             if doc and doc["kind"] == "ConfigMap" and doc["metadata"]["name"] == "connection-notes-test")
commands = [line.strip() for line in notes.splitlines()
            if line.startswith(("  ACTIVE_STS=", "  REDIS_POD=", "  kubectl exec "))]
mode = sys.argv[1]
with tempfile.TemporaryDirectory() as directory:
    root = pathlib.Path(directory)
    kubectl = root / "kubectl"
    kubectl.write_text("""#!/usr/bin/env python3
import json, os, sys
if sys.argv[1:] == ["get", "rediscluster", "notes-cache", "-n", "notes-namespace", "-o", "jsonpath={.status.activeClusterName}"]:
    print(os.environ["TEST_ACTIVE_STS"], end="")
elif sys.argv[1] == "exec":
    with open(os.environ["TEST_CALLS"], "a") as output:
        output.write(json.dumps(sys.argv[1:]) + "\\n")
else:
    sys.exit("unexpected kubectl arguments: " + repr(sys.argv[1:]))
""")
    kubectl.chmod(0o755)
    for active in ("", "notes-cache-alt"):
        calls = root / "calls.jsonl"
        calls.write_text("")
        env = dict(os.environ, PATH=directory + os.pathsep + os.environ["PATH"],
                   TEST_ACTIVE_STS=active, TEST_CALLS=str(calls))
        subprocess.run(["bash", "-eu", "-c", "\n".join(commands)], env=env, check=True)
        actual = [json.loads(line) for line in calls.read_text().splitlines()]
        prefix = ["exec", "-n", "notes-namespace", (active or "notes-cache") + "-0", "-c", "redis", "--", "redis-cli"]
        expected = [prefix + ["-c", "PING"]]
        if mode != "disabled":
            tls = ["--tls", "--cacert", "/tls/ca.crt"]
            if mode == "yes":
                tls += ["--cert", "/tls/tls.crt", "--key", "/tls/tls.key"]
            expected.append(prefix + tls + ["-p", "6381", "-c", "PING"])
        assert actual == expected, (mode, active, actual)
' "$auth_clients"
done
echo "instance chart OK"
