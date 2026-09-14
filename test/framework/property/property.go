// Package property wraps pgregory.net/rapid with helpers for the operator's
// recurring shapes (slot ranges, node roles, and blue-green phases).
package property

import (
	"testing"

	"pgregory.net/rapid"
)

// Check runs property fn against many random inputs, failing the test on the first counterexample.
func Check(t *testing.T, fn func(*rapid.T)) {
	t.Helper()
	rapid.Check(t, fn)
}
