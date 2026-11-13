// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuantileMsNearestRank(t *testing.T) {
	var lat []time.Duration
	for i := 1; i <= 100; i++ {
		lat = append(lat, time.Duration(i)*time.Millisecond)
	}
	assert.Equal(t, 50.0, quantileMs(lat, 0.50))
	assert.Equal(t, 99.0, quantileMs(lat, 0.99))
	assert.Equal(t, 100.0, quantileMs(lat, 1))
	assert.Zero(t, quantileMs(nil, 0.99))
}

func TestTimelineBucketsByScheduledSecond(t *testing.T) {
	tl := newTimeline()
	base := time.Unix(1_000, 0)
	tl.record(base, time.Millisecond, nil)
	// A request scheduled in second 1000 that completes late still belongs to 1000.
	tl.record(base.Add(900*time.Millisecond), 2*time.Second, errors.New("dial tcp: lookup host-1: no such host"))
	tl.record(base.Add(950*time.Millisecond), time.Second, errors.New("dial tcp: lookup host-2: no such host"))
	tl.record(base.Add(980*time.Millisecond), time.Second, errors.New("dial tcp 10.244.0.30:6379: connect: connection refused"))
	tl.record(base.Add(990*time.Millisecond), time.Second, errors.New("dial tcp 10.244.0.31:6379: connect: connection refused"))
	tl.record(base.Add(time.Second), 3*time.Millisecond, nil)

	rows := tl.rows()
	require.Len(t, rows, 2)
	assert.Equal(t, int64(1_000), rows[0].Unix)
	assert.Equal(t, int64(1), rows[0].OK)
	assert.Equal(t, int64(4), rows[0].Fail)
	assert.Equal(t, 2000.0, rows[0].MaxMs)
	require.Len(t, rows[0].Errors, 1, "errors differing only by host or IP share a kind")
	assert.Equal(t, "dial tcp: lookup host-1: no such host", rows[0].Errors["dial tcp"])
	assert.Equal(t, int64(1_001), rows[1].Unix)
}
