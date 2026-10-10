#!/usr/bin/env bash

set -euo pipefail

readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly INVENTORY="$REPO_ROOT/LICENSE-3rdparty.csv"

expected="$(mktemp "${TMPDIR:-/tmp}/redis-operator-license-modules.XXXXXX")"
trap 'rm -f "$expected"' EXIT

cd "$REPO_ROOT"
go list -deps -f '{{with .Module}}{{if .Version}}{{.Path}}|{{.Version}}{{end}}{{end}}' ./... \
  | sort -u >"$expected"

python3 - "$INVENTORY" "$expected" <<'PY'
import csv
import re
import sys

inventory_path, expected_path = sys.argv[1:]
required_fields = ["Component", "Origin", "License", "Copyright"]

with open(expected_path, encoding="utf-8") as source:
    expected = dict(line.rstrip("\n").split("|", 1) for line in source if line.strip())

with open(inventory_path, newline="", encoding="utf-8") as source:
    reader = csv.DictReader(source)
    if reader.fieldnames != required_fields:
        raise SystemExit(
            f"{inventory_path}: expected header {required_fields}, got {reader.fieldnames}"
        )
    rows = list(reader)

actual = {}
errors = []
origin_pattern = re.compile(r"^https://pkg\.go\.dev/(.+)@([^?]+)\?tab=licenses$")
for row in rows:
    component = row["Component"]
    if not component or not row["License"]:
        errors.append(f"{component or '<empty component>'}: component and license are required")
        continue
    if component in actual:
        errors.append(f"{component}: duplicate component")
        continue
    match = origin_pattern.match(row["Origin"])
    if not match or match.group(1) != component:
        errors.append(f"{component}: origin must be its versioned pkg.go.dev license URL")
        continue
    actual[component] = match.group(2)

for component in sorted(expected.keys() - actual.keys()):
    errors.append(f"{component}: missing from license inventory")
for component in sorted(actual.keys() - expected.keys()):
    errors.append(f"{component}: not used by repository packages")
for component in sorted(expected.keys() & actual.keys()):
    if actual[component] != expected[component]:
        errors.append(
            f"{component}: inventory has {actual[component]}, expected {expected[component]}"
        )

if errors:
    raise SystemExit("license inventory validation failed:\n- " + "\n- ".join(errors))

print(f"license inventory covers {len(expected)} Go modules")
PY
