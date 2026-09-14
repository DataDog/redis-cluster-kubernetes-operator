package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestPrimaryEndpointReadinessTrackerRequiresSpacedSuccesses(t *testing.T) {
	tracker := &primaryEndpointReadinessTracker{}
	now := time.Unix(1_000, 0)

	stability := tracker.Observe("default/redis-0", "identity", true, now)
	require.Equal(t, 1, stability.Successes)
	require.False(t, stability.Ready)

	stability = tracker.Observe("default/redis-0", "identity", true, now.Add(time.Millisecond))
	require.Equal(t, 1, stability.Successes)
	require.False(t, stability.Ready)

	stability = tracker.Observe(
		"default/redis-0",
		"identity",
		true,
		now.Add(primaryEndpointSuccessInterval),
	)
	require.Equal(t, 2, stability.Successes)
	require.False(t, stability.Ready)

	stability = tracker.Observe(
		"default/redis-0",
		"identity",
		true,
		now.Add(2*primaryEndpointSuccessInterval),
	)
	require.Equal(t, primaryEndpointRequiredSuccesses, stability.Successes)
	require.True(t, stability.Ready)
}

func TestPrimaryEndpointReadinessTrackerResetsOnFailureOrIdentityChange(t *testing.T) {
	tracker := &primaryEndpointReadinessTracker{}
	now := time.Unix(1_000, 0)
	podKey := "default/redis-0"

	tracker.Observe(podKey, "first-identity", true, now)
	stability := tracker.Observe(
		podKey,
		"first-identity",
		true,
		now.Add(primaryEndpointSuccessInterval),
	)
	require.Equal(t, 2, stability.Successes)

	stability = tracker.Observe(
		podKey,
		"first-identity",
		false,
		now.Add(2*primaryEndpointSuccessInterval),
	)
	require.Zero(t, stability.Successes)
	require.False(t, stability.Ready)

	stability = tracker.Observe(
		podKey,
		"first-identity",
		true,
		now.Add(3*primaryEndpointSuccessInterval),
	)
	require.Equal(t, 1, stability.Successes)

	stability = tracker.Observe(
		podKey,
		"new-pod-uid-or-ip",
		true,
		now.Add(4*primaryEndpointSuccessInterval),
	)
	require.Equal(t, 1, stability.Successes)
	require.False(t, stability.Ready)
}

func TestResolvedIPsMatchPodRejectsMissingOrStaleAddresses(t *testing.T) {
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			PodIP:  "10.0.0.27",
			PodIPs: []corev1.PodIP{{IP: "10.0.0.27"}, {IP: "2001:db8::27"}},
		},
	}

	require.True(t, resolvedIPsMatchPod([]string{"10.0.0.27"}, pod))
	require.True(t, resolvedIPsMatchPod([]string{"10.0.0.27", "2001:db8::27"}, pod))
	require.False(t, resolvedIPsMatchPod(nil, pod))
	require.False(t, resolvedIPsMatchPod([]string{"2001:db8::27"}, pod),
		"the primary PodIP must be among the answers")
	require.False(t, resolvedIPsMatchPod([]string{"10.0.0.27", "10.0.0.99"}, pod),
		"an additional stale address could route clients to the wrong pod")
}
