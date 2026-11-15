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
   and compare the same lane, noting the commit SHA of each run
   (`gh run view RUN_ID --json headSha`). A single failure among green runs is
   not proof of a flake: compare the failing SHA with the last green SHA and
   look for repeated evidence (the same test failing the same way on several
   runs or on reruns of one SHA). Without that, report the cause as
   unconfirmed instead of calling it flaky.
4. Read the operator log lines around the failure in the job log. Map the
   symptom to the reconcile path (`controller/`, `bluegreen/`, `redis_cluster/`)
   and to the safety invariants in `AGENTS.md`.
5. Reproduce only that lane locally: `ci/run-e2e-lane.sh LANE` (Docker + KIND
   v0.30.0). To narrow to one test, call `ci/dev-e2e.sh` directly with the
   environment `ci/run-e2e-lane.sh` sets for that lane, or the test skips or
   times out differently:

   | Lane | Environment |
   | --- | --- |
   | `smoke` | `E2E_TEST_TIMEOUT=40m` |
   | `default` | `E2E_TEST_TIMEOUT=75m` |
   | `slow`, `bg-stuck` | `E2E_SLOW=1 E2E_TEST_TIMEOUT=85m` |
   | `loadgen` | `E2E_SLOW=1 E2E_BUILD_LOADGEN=1 E2E_TEST_TIMEOUT=85m` |

   ```sh
   E2E_SLOW=1 E2E_TEST_TIMEOUT=85m ci/dev-e2e.sh -v -run '^TestE2E_NAME$'
   ```

   Keep `-v`: without it `go test` prints no per-test lines. Confirm the test
   actually ran: the `-v` output must show `--- PASS` or
   `--- FAIL` for it, not `--- SKIP` or `no tests to run`. A skipped test is
   not a reproduction.
6. Report: run link, lane, test, first error line, new, repeated or unconfirmed (with the SHAs compared), suspected
   commit (`git log --since` the last green run), and a proposed fix with a
   regression test. Do not quarantine or delete tests without a maintainer.
