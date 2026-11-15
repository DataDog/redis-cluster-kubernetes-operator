---
name: add-e2e-test
description: Add an in-process KIND e2e test and register it in an e2e lane. Use when adding or moving a test under e2e/.
---

# Add a test to an e2e lane

1. Put the test in the `e2e/*_test.go` file for its area (`backup`, `bluegreen`,
   `scale`, `termination`, ...). Name it `TestE2E_Area_Behavior`; reuse helpers
   from `e2e/support_test.go` and `e2e/main_test.go` instead of new ones.
2. Assert the behavior the fix guarantees (topology, data, slot ownership), and
   check the test fails without the fix.
3. Add the exact test name to one lane in `ci/e2e-lanes.txt`:
   - `smoke`: fast, every PR
   - `default`: core behavior, under ~75 min for the lane
   - `slow`, `bg-stuck`, `loadgen`: long or load tests (`E2E_SLOW=1`)
4. Verify the manifest and the lane:
   ```sh
   go test ./e2e -run 'TestLaneManifest|TestCILaneManifest'
   ci/run-e2e-lane.sh LANE
   ```
   (`e2e/lane_manifest_test.go` fails when a test is in no lane or a lane names
   a missing test.)
5. New files need the Apache-2.0 header.
