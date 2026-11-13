// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind/loadgen"
)

func TestAnalyzeFaultWindow(t *testing.T) {
	start, end := time.Unix(110, 0), time.Unix(120, 0)
	rows := []loadgen.TimelineRow{
		{Unix: 100, OK: 100, P99Ms: 2},
		{Unix: 101, OK: 100, P99Ms: 3},
		{Unix: 102, OK: 100, P99Ms: 4},
		{Unix: 112, OK: 90, Fail: 10, P99Ms: 900, MaxMs: 1000, Errors: map[string]string{"dial tcp": "dial tcp: lookup a"}},
		{Unix: 113, OK: 100, P99Ms: 5},
		{Unix: 115, OK: 95, Fail: 5, P99Ms: 40, MaxMs: 60, Errors: map[string]string{"dial tcp": "dial tcp: lookup b"}},
		{Unix: 130, OK: 0, Fail: 100}, // after the window
	}

	r := AnalyzeFault(rows, start, end)
	assert.Equal(t, 3.0, r.BaselineP99Ms, "median of pre-fault p99")
	assert.Equal(t, int64(4), r.UnavailableSeconds, "first to last failing second, inclusive")
	assert.Equal(t, int64(300), r.Scheduled)
	assert.Equal(t, int64(15), r.Failed)
	assert.InDelta(t, 0.05, r.ErrorRate, 1e-9)
	assert.Equal(t, 900.0, r.FaultP99Ms)
	assert.Equal(t, 1000.0, r.FaultMaxMs)
	assert.Equal(t, "dial tcp: lookup a", r.Errors["dial tcp"], "first sample per kind is kept")
}

func TestAnalyzeFaultWithoutErrors(t *testing.T) {
	r := AnalyzeFault([]loadgen.TimelineRow{{Unix: 10, OK: 50, P99Ms: 1}}, time.Unix(10, 0), time.Unix(11, 0))
	assert.Zero(t, r.UnavailableSeconds)
	assert.Zero(t, r.ErrorRate)
	assert.Zero(t, r.BaselineP99Ms, "no pre-fault seconds")
}

func TestSlotMigrationInProgress(t *testing.T) {
	assert.False(t, SlotMigrationInProgress([]ClusterNode{{Slots: []string{"0-5460"}}}))
	assert.True(t, SlotMigrationInProgress([]ClusterNode{{Slots: []string{"0-5460", "[5461->-abc]"}}}))
}
