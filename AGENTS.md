# AGENTS.md

Guidance for coding agents (and humans) changing this repository. To *install
or run* the operator with an agent, see [README.md](README.md#set-up-with-an-ai-agent)
and [llms.txt](llms.txt) instead.

## Layout

- `apiv1/` RedisCluster API types; `controller/`, `bluegreen/`, `redis_cluster/` reconcile logic.
- `crds/redis-cluster-v1.yaml` the CRD; `templates/`, `values.yaml` the Helm chart.
- `e2e/` in-process KIND tests; `test/blackbox/`, `ci/cleanroom/` packaged-operator tests.
- `scripts/` user-facing tooling; `skills/` user agent skills (Claude Code plugin);
  `.claude/skills/` maintainer skills.

## Rules

- The CRD is a sha-pinned compatibility contract. Never regenerate or reformat
  `crds/redis-cluster-v1.yaml`; `make verify-crd-contract` must pass. API changes
  need an issue and maintainer agreement first.
- Regenerate deepcopy with `make generate-deepcopy`. Never hand-edit
  `zz_generated.*` files.
- Dependency changes (`go.mod`) must update `LICENSE-3rdparty.csv`;
  `make verify-license-inventory` must pass.
- New files start with the Apache-2.0 header used by existing files of the same type.
- Shell scripts in `ci/` and `scripts/` must pass `shellcheck -x`.
- Never commit credentials, private endpoints, internal deployment names or kubeconfigs.
- Never run or dispatch the **EKS qualification** workflow; it creates chargeable AWS resources.

## Test

Unit tests need Kubernetes 1.33 envtest binaries (same as `.github/workflows/test.yml`):

```sh
export KUBEBUILDER_ASSETS="$(go run sigs.k8s.io/controller-runtime/tools/setup-envtest@v0.0.0-20250626154428-7fd020cb5fc3 use 1.33.x -p path)"
go test ./...
go vet ./...
helm lint . --strict --kube-version 1.33.0
make verify-crd-contract verify-license-inventory
```

E2E runs one lane on a throwaway KIND cluster (Docker, KIND v0.30.0+). Lanes and
their tests are listed in `ci/e2e-lanes.txt`; run only the lane you touched:

```sh
ci/run-e2e-lane.sh smoke
```

A new e2e test must be added to a lane in `ci/e2e-lanes.txt`
(`e2e/lane_manifest_test.go` enforces it).

## Operator safety invariants

Breaking these loses data. Keep the tests that pin them.

- Never `CLUSTER FORGET` a live node, and never forget the node ID of a pod
  that was recreated; only forget IDs no running pod owns.
- Never force a failover onto a replica while Redis still reports its primary
  healthy. Treat the `nofailover` flag as a flag, not a failure.
- Blue/green phase safety gates (sync, drain, blank-primary checks) must not be
  skipped, including by watchdogs or timeouts.
- Backup and restore fail closed: an unreadable, invalid or missing backup stops
  the operation instead of continuing with empty data.

## AI-assisted contributions

See [CONTRIBUTING.md](CONTRIBUTING.md#ai-assisted-contributions). In short: the
human submitter is responsible for every line, non-trivial PRs need an issue
first, never fabricate links, issues or test results, and disclose AI use in
the PR template.
