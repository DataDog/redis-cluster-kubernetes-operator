// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build blackbox_e2e

package blackbox

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCleanroom_ScaleUpDown(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("scale-up-down")
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	h.resize(s, 4, 1)
	h.waitReady(s, 4, 1)
	h.verifyData(s)
	h.resize(s, 3, 1)
	h.eventually("retained target prevents unsupported downscale", 3*time.Minute, func(ctx context.Context) error {
		rc := h.cluster(s)
		if rc.Status.Autoscaling.EffectivePrimaries != 4 || rc.Status.Autoscaling.LastDecisionReason != "HelmPrimaryFloorRaised" {
			return fmt.Errorf("effectivePrimaries=%d lastDecisionReason=%q", rc.Status.Autoscaling.EffectivePrimaries, rc.Status.Autoscaling.LastDecisionReason)
		}
		statefulSet := &appsv1.StatefulSet{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, statefulSet); err != nil {
			return err
		}
		if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != 8 {
			return fmt.Errorf("StatefulSet replicas=%v, want retained 8", statefulSet.Spec.Replicas)
		}
		return nil
	})
	h.waitReady(s, 4, 1)
	h.verifyData(s)
}

func (h *harness) resize(s scenario, primaries, replicas int32) {
	h.t.Helper()
	h.patchCluster(s, func(rc *cachev1.RedisCluster) {
		rc.Spec.Primaries = primaries
		rc.Spec.Replicas = replicas
		pods := primaries * (1 + replicas)
		rc.Spec.StatefulsetTemplate.Spec.Replicas = &pods
	})
}

func TestCleanroom_BlueGreenAvailability(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("blue-green")
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	h.eventually("rollout baseline hash", 2*time.Minute, func(ctx context.Context) error {
		if h.cluster(s).Status.LastAppliedSpecHash == "" {
			return fmt.Errorf("lastAppliedSpecHash is empty")
		}
		return nil
	})
	trigger := fmt.Sprintf("cleanroom-%d", time.Now().UnixNano())
	type probeResult struct {
		output string
		err    error
	}
	probeCtx, cancelProbe := context.WithCancel(h.ctx)
	probeResults := make(chan probeResult, 1)
	go func() {
		output, err := h.serviceAvailabilityProbe(probeCtx, s)
		probeResults <- probeResult{output: output, err: err}
	}()
	defer cancelProbe()
	h.patchCluster(s, func(rc *cachev1.RedisCluster) {
		if rc.Labels == nil {
			rc.Labels = map[string]string{}
		}
		rc.Labels["chart_version"] = trigger
		redisContainer := &rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0]
		redisContainer.Env = append(redisContainer.Env, corev1.EnvVar{
			Name:  "CLEANROOM_BLUE_GREEN_TRIGGER",
			Value: trigger,
		})
	})

	deadline := time.Now().Add(readyTimeout)
	sawActive := false
	for time.Now().Before(deadline) {
		select {
		case result := <-probeResults:
			t.Fatalf("Service availability probe ended before blue-green completed: %v (samples=%d)", result.err, len(strings.Fields(result.output)))
		default:
		}
		rc := h.cluster(s)
		if rc.Status.BlueGreen.Phase == cachev1.BlueGreenPhaseFailed {
			t.Fatalf("blue-green failed: %s", rc.Status.BlueGreen.Message)
		}
		if rc.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseNone {
			sawActive = true
		}
		if sawActive && rc.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseNone {
			var endpoints discoveryv1.EndpointSliceList
			if err := h.client.List(h.ctx, &endpoints, client.InNamespace(s.Namespace), client.MatchingLabels{discoveryv1.LabelServiceName: s.Name}); err != nil {
				t.Fatal(err)
			}
			readyEndpoints := 0
			for _, endpointSlice := range endpoints.Items {
				for _, endpoint := range endpointSlice.Endpoints {
					if endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready {
						readyEndpoints++
					}
				}
			}
			if readyEndpoints == 0 {
				t.Fatal("blue-green rollout published zero ready Service endpoints")
			}
		}
		if sawActive && rc.Status.BlueGreen.Phase == cachev1.BlueGreenPhaseNone && !rc.Status.RolloutInProgress {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !sawActive {
		t.Fatal("blue-green rollout never entered an active phase")
	}
	cancelProbe()
	var result probeResult
	select {
	case result = <-probeResults:
	case <-time.After(30 * time.Second):
		t.Fatal("Service availability probe did not stop after cancellation")
	}
	categories := map[string]int{}
	for _, sample := range strings.Fields(result.output) {
		categories[sample]++
	}
	attempts := categories["ok"] + categories["exec-error"] + categories["empty"] + categories["redis-error"]
	successes := categories["ok"]
	if attempts == 0 || float64(successes)/float64(attempts) < 0.95 {
		t.Fatalf("Redis availability=%d/%d, want at least 95%%; categories=%v; probeErr=%v", successes, attempts, categories, result.err)
	}
	h.waitReady(s, 3, 1)
	var sets appsv1.StatefulSetList
	if err := h.client.List(h.ctx, &sets, client.InNamespace(s.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(sets.Items) != 1 {
		t.Fatalf("active StatefulSet count=%d, want 1", len(sets.Items))
	}
	h.assertBlueGreenTemplateApplied(s, trigger)
	h.verifyData(s)
}

func (h *harness) assertBlueGreenTemplateApplied(s scenario, trigger string) {
	h.t.Helper()
	rc := h.cluster(s)
	active := rc.Status.ActiveClusterName
	if active == "" {
		active = s.Name
	}
	statefulSet := &appsv1.StatefulSet{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: s.Namespace, Name: active}, statefulSet); err != nil {
		h.t.Fatalf("get active StatefulSet %s/%s: %v", s.Namespace, active, err)
	}
	h.assertRedisTemplate(statefulSet.Name, statefulSet.Spec.Template.Spec.Containers, trigger)

	pods := h.pods(s)
	serving := 0
	for _, pod := range pods {
		if !podReady(pod) {
			continue
		}
		serving++
		h.assertRedisTemplate(pod.Name, pod.Spec.Containers, trigger)
	}
	if serving == 0 {
		h.t.Fatal("active Redis workload has no serving pods")
	}
}

func (h *harness) assertRedisTemplate(resourceName string, containers []corev1.Container, trigger string) {
	h.t.Helper()
	for _, container := range containers {
		if container.Name != "redis" {
			continue
		}
		if container.Image != h.redisImage {
			h.t.Fatalf("%s Redis image=%q, want %q", resourceName, container.Image, h.redisImage)
		}
		for _, env := range container.Env {
			if env.Name == "CLEANROOM_BLUE_GREEN_TRIGGER" && env.Value == trigger {
				return
			}
		}
		h.t.Fatalf("%s Redis container does not have CLEANROOM_BLUE_GREEN_TRIGGER=%q", resourceName, trigger)
	}
	h.t.Fatalf("%s does not have a Redis container", resourceName)
}

func TestCleanroom_AutoscalingProviderUnavailable(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("autoscaling-unavailable")
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	min, max, cooldown, delay := int32(3), int32(5), int32(0), int32(0)
	h.patchCluster(s, func(rc *cachev1.RedisCluster) {
		rc.Spec.Autoscaling = &cachev1.RedisClusterAutoscalingSpec{
			Mode: cachev1.AutoscalingModeRecommend, MinPrimaries: &min, MaxPrimaries: &max,
			UpscaleCooldownSeconds: &cooldown,
			CPU: &cachev1.RedisClusterAutoscalingMetricPolicy{
				HighWatermark: resource.MustParse("0.75"), UpscaleDelaySeconds: &delay,
			},
		}
	})
	h.eventually("ProviderUnavailable autoscaling status", 3*time.Minute, func(ctx context.Context) error {
		rc := h.cluster(s)
		if rc.Status.Autoscaling.LastDecisionReason != "ProviderUnavailable" ||
			!conditionReason(rc.Status.Autoscaling.Conditions, "ProviderUnavailable") {
			return fmt.Errorf("lastDecisionReason=%q conditions=%+v", rc.Status.Autoscaling.LastDecisionReason, rc.Status.Autoscaling.Conditions)
		}
		if !rc.Status.StatefulSetReady {
			return fmt.Errorf("ordinary reconciliation is not ready")
		}
		return nil
	})
	h.waitReady(s, 3, 1)
	h.verifyData(s)

	for _, gvk := range []struct{ group, version, resource string }{
		{"datadoghq.com", "v1alpha1", "datadogmetrics"},
		{"datadoghq.com", "v1alpha1", "watermarkpodautoscalers"},
	} {
		resources, err := h.clientset.Discovery().ServerResourcesForGroupVersion(gvk.group + "/" + gvk.version)
		if err != nil {
			continue
		}
		for _, resource := range resources.APIResources {
			if resource.Name == gvk.resource {
				t.Fatalf("external autoscaling API %s/%s is installed; clean-room startup must not require it", gvk.group, gvk.resource)
			}
		}
	}
}

func TestCleanroom_ServiceDNS(t *testing.T) {
	h := newHarness(t)
	s := h.reference()
	h.waitReady(s, 3, 1)
	host := fmt.Sprintf("%s.%s.svc.cluster.local", s.Name, s.Namespace)
	for attempt := 1; attempt <= 3; attempt++ {
		got, err := h.toolbox(h.ctx, s.Namespace, false, "-h", host, "-p", "6379", "PING")
		if err != nil || strings.TrimSpace(got) != "PONG" {
			t.Fatalf("standard Service DNS identity gate attempt %d: output=%q err=%v", attempt, got, err)
		}
		time.Sleep(time.Second)
	}
}
