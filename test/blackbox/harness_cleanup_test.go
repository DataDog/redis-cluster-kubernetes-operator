//go:build blackbox_e2e

package blackbox

import (
	"reflect"
	"strings"
	"testing"
)

func TestScenarioCleanupOrdering(t *testing.T) {
	t.Run("failure collects before teardown", func(t *testing.T) {
		var calls []string
		runScenarioCleanup(true, func() {
			calls = append(calls, "collect")
		}, func() {
			calls = append(calls, "teardown")
		})
		if want := []string{"collect", "teardown"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("cleanup calls=%v, want %v", calls, want)
		}
	})

	t.Run("success only tears down", func(t *testing.T) {
		var calls []string
		runScenarioCleanup(false, func() {
			calls = append(calls, "collect")
		}, func() {
			calls = append(calls, "teardown")
		})
		if want := []string{"teardown"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("cleanup calls=%v, want %v", calls, want)
		}
	})
}

func TestArtifactSafeNameDisambiguatesTruncatedNamespaces(t *testing.T) {
	left := "redis-cluster-isolation-a-identical-prefix"
	right := "redis-cluster-isolation-b-identical-prefix"
	if sanitizeDNS(left) != sanitizeDNS(right) {
		t.Fatal("test inputs must collide after the human-readable truncation")
	}
	leftName := artifactSafeName(left)
	rightName := artifactSafeName(right)
	if leftName == rightName {
		t.Fatalf("artifact names collide: %q", leftName)
	}
	if !strings.HasPrefix(leftName, sanitizeDNS(left)+"-") {
		t.Fatalf("artifact name %q lost its readable namespace prefix", leftName)
	}
	if leftName != artifactSafeName(left) {
		t.Fatalf("artifact name %q is not stable", leftName)
	}
}
