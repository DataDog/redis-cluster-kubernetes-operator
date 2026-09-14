package kind

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// poll returns the fixture's configured poll cadence (default defaultReadyPollInterval).
func (f *Fixture) poll() time.Duration {
	if f.cfg.pollEvery > 0 {
		return f.cfg.pollEvery
	}
	return defaultReadyPollInterval
}

// Get fetches a named object from the fixture's namespace.
func (f *Fixture) Get(name string, obj client.Object) error {
	return f.Client.Get(f.Ctx, types.NamespacedName{Namespace: f.Namespace, Name: name}, obj)
}

// Eventually fails the test if cond does not hold within timeout.
func (f *Fixture) Eventually(msg string, timeout time.Duration, cond func() bool) {
	f.T.Helper()
	f.EventuallyT(f.T, msg, timeout, cond)
}

// EventuallyT fails t if cond does not hold within timeout.
func (f *Fixture) EventuallyT(t testing.TB, msg string, timeout time.Duration, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, timeout, f.poll(), msg)
}

// EventuallyError retries check and returns its last error on timeout.
func (f *Fixture) EventuallyError(msg string, timeout time.Duration, check func() error) error {
	var lastErr error
	ctx, cancel := context.WithTimeout(f.Ctx, timeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, f.poll(), timeout, true, func(context.Context) (bool, error) {
		lastErr = check()
		return lastErr == nil, nil
	})
	if err != nil {
		if lastErr != nil {
			return fmt.Errorf(
				"%s did not settle within %s; last error: %w; poll error: %w",
				msg, timeout, lastErr, err,
			)
		}
		return fmt.Errorf("%s did not settle within %s: %w", msg, timeout, err)
	}
	return nil
}

// EventuallyErrorT retries check and reports its last error on timeout.
func (f *Fixture) EventuallyErrorT(t testing.TB, msg string, timeout time.Duration, check func() error) {
	t.Helper()
	require.NoError(t, f.EventuallyError(msg, timeout, check))
}

// SetPodCondition sets (or replaces) a status condition on a named pod and pushes
// the status update, retrying until it sticks — for readiness gates that no
// controller sets in a KIND test.
func (f *Fixture) SetPodCondition(name string, cond corev1.PodCondition, timeout time.Duration) {
	f.T.Helper()
	f.Eventually("set condition "+string(cond.Type)+" on pod "+name, timeout, func() bool {
		pod := &corev1.Pod{}
		if f.Get(name, pod) != nil {
			return false
		}
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == cond.Type {
				pod.Status.Conditions[i] = cond
				return f.Client.Status().Update(f.Ctx, pod) == nil
			}
		}
		pod.Status.Conditions = append(pod.Status.Conditions, cond)
		return f.Client.Status().Update(f.Ctx, pod) == nil
	})
}
