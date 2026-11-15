---
name: investigate-nightly-failure
description: Triage a failed scheduled "In-process reconciler regression" lane of the Test workflow. Use when a nightly e2e lane fails or flakes.
---

# Investigate a nightly failure

The nightly is the `scheduled-regression` job in `.github/workflows/test.yml`
(cron `17 3 * * *`), one matrix entry per lane of `ci/e2e-lanes.txt`.

1. Find the run and the failing lane:
   `gh run list --workflow Test --event schedule --limit 5`, then
   `gh run view RUN_ID --log-failed`.
2. Name the first failing test (`--- FAIL: TestE2E_...`) and its file:
   `rg -n 'func TestE2E_NAME' e2e/`.
3. Check whether it is new: `gh run list --workflow Test --event schedule --limit 14`
   and compare the same lane. Failing once in many green runs is a flake; say so.
4. Read the operator log lines around the failure in the job log. Map the
   symptom to the reconcile path (`controller/`, `bluegreen/`, `redis_cluster/`)
   and to the safety invariants in `AGENTS.md`.
5. Reproduce only that lane locally: `ci/run-e2e-lane.sh LANE` (Docker + KIND
   v0.30.0). To narrow to one test, check how the lane script passes tests and
   run `ci/dev-e2e.sh -run '^TestE2E_NAME$'`.
6. Report: run link, lane, test, first error line, new or flaky, suspected
   commit (`git log --since` the last green run), and a proposed fix with a
   regression test. Do not quarantine or delete tests without a maintainer.
