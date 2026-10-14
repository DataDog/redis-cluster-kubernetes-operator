// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package utils

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RetryWithBackoff sleeps base * attempt — linear, despite the name.

var (
	errTransient = errors.New("transient")
	errFatal     = errors.New("fatal")
)

// All N attempts fail: elapsed = base*(1+2+...+N-1), no sleep after last.
func TestSynctest_RetryWithBackoff_LinearScheduleAcrossAllAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		f := func() error { attempts++; return errTransient }

		start := time.Now()
		err := RetryWithBackoff(f, func(error) bool { return true }, 4, 100*time.Millisecond, logr.Discard())
		elapsed := time.Since(start)

		require.ErrorIs(t, err, errTransient)
		assert.Equal(t, 4, attempts)
		assert.Equal(t, 600*time.Millisecond, elapsed) // 100 + 200 + 300
	})
}

// Non-retryable error → stop, no further attempts or sleeps.
func TestSynctest_RetryWithBackoff_StopsImmediatelyOnNonRetryableError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		f := func() error {
			attempts++
			if attempts == 1 {
				return errTransient
			}
			return errFatal
		}
		isRetryable := func(e error) bool { return errors.Is(e, errTransient) }

		start := time.Now()
		err := RetryWithBackoff(f, isRetryable, 10, 100*time.Millisecond, logr.Discard())
		elapsed := time.Since(start)

		require.ErrorIs(t, err, errFatal)
		assert.Equal(t, 2, attempts)
		assert.Equal(t, 100*time.Millisecond, elapsed)
	})
}

// Success on attempt 3 → no trailing sleep.
func TestSynctest_RetryWithBackoff_SuccessSkipsTrailingSleep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		f := func() error {
			attempts++
			if attempts == 3 {
				return nil
			}
			return errTransient
		}

		start := time.Now()
		err := RetryWithBackoff(f, func(error) bool { return true }, 5, 100*time.Millisecond, logr.Discard())
		elapsed := time.Since(start)

		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
		assert.Equal(t, 300*time.Millisecond, elapsed) // 100 + 200
	})
}
