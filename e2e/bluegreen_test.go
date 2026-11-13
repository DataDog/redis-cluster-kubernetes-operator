// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// endpointSampleInterval is the cadence the SwitchingDNS endpoint sampler polls.
const endpointSampleInterval = 100 * time.Millisecond

// TestE2E_StaleGreenStatefulSetRecovery pre-creates a stale zero-replica green
// StatefulSet at the deterministic alternate name, then asserts a blue-green
// rollout replaces it (new UID, grown replicas) and converges to a single
// active StatefulSet with the Service selecting the active role.
func TestE2E_StaleGreenStatefulSetRecovery(t *testing.T) {
	requireSlow(t, "stale green recovery pays a full blue-green rollout")
	s := newCluster(t, redisbuilder.WithReplicas(1))

	greenName := inactiveStatefulSetName(t, s)
	staleUID := createStaleGreenStatefulSet(t, s, greenName)

	s.TriggerBlueGreen(t, clusterName)
	s.WaitBlueGreenStarted(t, clusterName, clusterReadyTimeout)
	waitStatefulSetRecreated(t, s, greenName, staleUID)
	s.WaitBlueGreenComplete(t, clusterName, bgConvergeTimeout)

	require.NoError(t, s.WaitReady(clusterObjectKey(s), clusterReadyTimeout))
	s.RequireSingleActiveStatefulSet(t, clusterName)
	s.RequireServiceSelectsActive(t, clusterName)
}

// TestE2E_BG_SwitchingDNS_DrainPacing samples the headless Service's ready
// EndpointSlice addresses every 100ms across a blue-green rollout and asserts
// the count never hits zero — no data-plane outage during the SwitchingDNS drain.
func TestE2E_BG_SwitchingDNS_DrainPacing(t *testing.T) {
	requireSlow(t, "switching-DNS drain pacing pays a full blue-green rollout")
	s := newCluster(t, redisbuilder.WithReplicas(1))

	stopSampling := observeEndpointsCount(t, s)

	s.TriggerBlueGreen(t, clusterName)
	// Block for the whole rollout so the sampler spans it. WaitBlueGreenComplete's
	// sawActive guard waits for the phase to leave None before requiring the return
	// to None, so sampling cannot stop before SwitchingDNS starts.
	s.WaitBlueGreenComplete(t, clusterName, bgConvergeTimeout)
	require.Positive(t, stopSampling(),
		"headless Service hit 0 ready addresses during SwitchingDNS — data-plane outage")
}

// activeStatefulSetName returns the current serving StatefulSet name.
func activeStatefulSetName(t *testing.T, s *rckind.Suite) string {
	t.Helper()
	rc := s.GetCluster(t, clusterName)
	active := rc.Status.ActiveClusterName
	if active == "" {
		active = clusterName
	}
	return active
}

// inactiveStatefulSetName returns the deterministic alternate StatefulSet name.
func inactiveStatefulSetName(t *testing.T, s *rckind.Suite) string {
	t.Helper()
	return constants.AlternateSTSName(activeStatefulSetName(t, s))
}

// createStaleGreenStatefulSet fabricates a zero-replica green STS owned by the
// CR, simulating a leaked STS from a prior aborted rollout.
func createStaleGreenStatefulSet(t *testing.T, s *rckind.Suite, name string) types.UID {
	t.Helper()
	rc := s.GetCluster(t, clusterName)
	template := rc.Spec.StatefulsetTemplate.DeepCopy()
	replicas := int32(0)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace, Labels: template.Labels},
		Spec:       *template.Spec.DeepCopy(),
	}
	sts.Spec.Replicas = &replicas
	// The compatibility CRD prunes pod-template labels; restore the selector
	// labels as the operator does, or the API server rejects the StatefulSet.
	sts.Spec.Template.Labels = maps.Clone(sts.Spec.Selector.MatchLabels)
	require.NoError(t, ctrl.SetControllerReference(rc, sts, s.Client.Scheme()))
	require.NoError(t, s.Client.Create(s.Ctx, sts))
	return sts.UID
}

// waitStatefulSetRecreated blocks until the stale STS is replaced (new UID) and
// grown to the expected replica count.
func waitStatefulSetRecreated(t *testing.T, s *rckind.Suite, name string, staleUID types.UID) {
	t.Helper()
	rc := s.GetCluster(t, clusterName)
	expected := redisbuilder.PodCount(rc.Spec.Primaries, rc.Spec.Replicas)
	s.EventuallyT(t, "stale green StatefulSet recreated", bgConvergeTimeout, func() bool {
		var sts appsv1.StatefulSet
		if err := s.Client.Get(s.Ctx, types.NamespacedName{Namespace: s.Namespace, Name: name}, &sts); err != nil {
			return false
		}
		return sts.UID != staleUID && sts.Spec.Replicas != nil && *sts.Spec.Replicas == expected
	})
}

// observeEndpointsCount returns a function that stops sampling and returns the minimum.
func observeEndpointsCount(t *testing.T, s *rckind.Suite) func() int32 {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	var stopOnce sync.Once
	var minReady atomic.Int32
	minReady.Store(1<<31 - 1)
	go func() {
		defer close(done)
		tick := time.NewTicker(endpointSampleInterval)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				ready := countReadyEndpoints(s)
				for {
					cur := minReady.Load()
					if ready >= cur || minReady.CompareAndSwap(cur, ready) {
						break
					}
				}
			}
		}
	}()
	stopSampling := func() int32 {
		stopOnce.Do(func() { close(stop) })
		<-done
		return minReady.Load()
	}
	t.Cleanup(func() { stopSampling() })
	return stopSampling
}

// countReadyEndpoints sums Ready endpoints across the CR's headless-Service slices.
func countReadyEndpoints(s *rckind.Suite) int32 {
	var slices discoveryv1.EndpointSliceList
	if err := s.Client.List(s.Ctx, &slices, client.InNamespace(s.Namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: clusterName}); err != nil {
		return 0
	}
	var ready int32
	for _, sl := range slices.Items {
		for _, e := range sl.Endpoints {
			if e.Conditions.Ready == nil || *e.Conditions.Ready {
				ready++
			}
		}
	}
	return ready
}
