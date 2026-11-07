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
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCleanroom_ReferenceInstall(t *testing.T) {
	h := newHarness(t)
	s := h.reference()
	h.waitReady(s, 3, 1)

	pods := h.pods(s)
	if len(pods) != 6 {
		t.Fatalf("Redis pod count=%d, want 6", len(pods))
	}
	for _, pod := range pods {
		if pod.Spec.Containers[0].Image != h.redisImage {
			t.Errorf("pod %s image=%q, want %q", pod.Name, pod.Spec.Containers[0].Image, h.redisImage)
		}
		size, err := h.exec(h.ctx, pod.Namespace, pod.Name, "redis", []string{"stat", "-c", "%s", "/data/nodes.conf"})
		if err != nil || size == "" || size == "0" {
			t.Errorf("pod %s nodes.conf is not persisted and nonempty: size=%q err=%v", pod.Name, size, err)
		}
	}
	var pvcs corev1.PersistentVolumeClaimList
	if err := h.client.List(h.ctx, &pvcs, client.InNamespace(s.Namespace)); err != nil {
		t.Fatal(err)
	}
	bound := 0
	for _, pvc := range pvcs.Items {
		if pvc.Status.Phase == corev1.ClaimBound {
			bound++
		}
	}
	if bound != 6 {
		t.Fatalf("Bound PVC count=%d, want 6", bound)
	}
	h.seedAndVerify(s)
}

func TestCleanroom_RedisPodRecovery(t *testing.T) {
	h := newHarness(t)
	s := h.reference()
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	pod := h.pods(s)[0]
	uid := pod.UID
	claimName, err := mountedClaim(pod, "redis", "/data")
	if err != nil {
		t.Fatal(err)
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: s.Namespace, Name: claimName}, claim); err != nil {
		t.Fatalf("get mounted PVC %s/%s: %v", s.Namespace, claimName, err)
	}
	claimUID := claim.UID
	marker := fmt.Sprintf("cleanroom-pvc-%d", time.Now().UnixNano())
	markerPath := "/data/.cleanroom-persistence-marker"
	if _, err := h.exec(h.ctx, pod.Namespace, pod.Name, "redis", []string{
		"sh", "-c", `printf '%s' "$1" > "$2"`, "write-persistence-marker", marker, markerPath,
	}); err != nil {
		t.Fatalf("write persistence marker: %v", err)
	}
	if err := h.client.Delete(h.ctx, &pod); err != nil {
		t.Fatalf("delete Redis pod: %v", err)
	}
	var recreated corev1.Pod
	h.eventually("Redis pod recreated from its PVC", readyTimeout, func(ctx context.Context) error {
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: pod.Name}, &recreated); err != nil {
			return err
		}
		if recreated.UID == uid || !podReady(recreated) {
			return fmt.Errorf("pod UID=%s ready=%t", recreated.UID, podReady(recreated))
		}
		recreatedClaim, err := mountedClaim(recreated, "redis", "/data")
		if err != nil {
			return err
		}
		if recreatedClaim != claimName {
			return fmt.Errorf("recreated pod mounted PVC %q, want %q", recreatedClaim, claimName)
		}
		return nil
	})
	recreatedClaim := &corev1.PersistentVolumeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: s.Namespace, Name: claimName}, recreatedClaim); err != nil {
		t.Fatalf("get recreated pod PVC %s/%s: %v", s.Namespace, claimName, err)
	}
	if recreatedClaim.UID != claimUID {
		t.Fatalf("recreated pod PVC UID=%s, want original UID=%s", recreatedClaim.UID, claimUID)
	}
	gotMarker, err := h.exec(h.ctx, recreated.Namespace, recreated.Name, "redis", []string{"cat", markerPath})
	if err != nil {
		t.Fatalf("read persistence marker: %v", err)
	}
	if gotMarker != marker {
		t.Fatalf("persistence marker=%q, want %q", gotMarker, marker)
	}
	h.waitReady(s, 3, 1)
	h.verifyData(s)
}

func TestCleanroom_NamespaceIsolation(t *testing.T) {
	h := newHarness(t)
	first := h.newScenario("isolation-a")
	second := h.newScenario("isolation-b")
	h.createClusterFromReference(first)
	h.createClusterFromReference(second)
	h.waitReady(first, 3, 1)
	h.waitReady(second, 3, 1)

	serviceAccounts := make(map[string]string, 2)
	for _, s := range []scenario{first, second} {
		var deployments appsv1.DeploymentList
		if err := h.client.List(h.ctx, &deployments, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: operatorComponent}); err != nil {
			t.Fatal(err)
		}
		if len(deployments.Items) != 1 {
			t.Fatalf("%s does not have one namespace-scoped operator", s.Namespace)
		}
		operator := deployments.Items[0]
		serviceAccounts[s.Namespace] = operator.Spec.Template.Spec.ServiceAccountName
		if serviceAccounts[s.Namespace] == "" {
			t.Fatalf("operator in %s has no service account", s.Namespace)
		}
		assertNamespaceFieldRef(t, operator, "WATCH_NAMESPACE")
	}
	assertRedisClusterForbidden(t, h.restConfig, serviceAccounts[first.Namespace], first.Namespace, second)
	assertRedisClusterForbidden(t, h.restConfig, serviceAccounts[second.Namespace], second.Namespace, first)

	secondStatefulSet := &appsv1.StatefulSet{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: second.Namespace, Name: second.Name}, secondStatefulSet); err != nil {
		t.Fatalf("get second StatefulSet: %v", err)
	}
	secondChartVersion := secondStatefulSet.Labels["chart_version"]
	marker := fmt.Sprintf("isolation-%d", time.Now().UnixNano())
	h.patchCluster(first, func(rc *cachev1.RedisCluster) {
		if rc.Labels == nil {
			rc.Labels = map[string]string{}
		}
		if rc.Annotations == nil {
			rc.Annotations = map[string]string{}
		}
		rc.Labels["chart_version"] = marker
		rc.Annotations["cleanroom.redis-operator/reconcile-marker"] = marker
	})
	h.eventually("only the owning operator reconciles its cluster", 2*time.Minute, func(ctx context.Context) error {
		firstStatefulSet := &appsv1.StatefulSet{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: first.Namespace, Name: first.Name}, firstStatefulSet); err != nil {
			return err
		}
		if got := firstStatefulSet.Labels["chart_version"]; got != marker {
			return fmt.Errorf("first StatefulSet chart_version=%q, want %q", got, marker)
		}
		currentSecond := &appsv1.StatefulSet{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: second.Namespace, Name: second.Name}, currentSecond); err != nil {
			return err
		}
		if got := currentSecond.Labels["chart_version"]; got != secondChartVersion {
			return fmt.Errorf("second StatefulSet chart_version=%q, want unchanged %q", got, secondChartVersion)
		}
		return nil
	})
}

func mountedClaim(pod corev1.Pod, containerName, mountPath string) (string, error) {
	var volumeName string
	for _, container := range pod.Spec.Containers {
		if container.Name != containerName {
			continue
		}
		for _, mount := range container.VolumeMounts {
			if mount.MountPath == mountPath {
				volumeName = mount.Name
				break
			}
		}
	}
	if volumeName == "" {
		return "", fmt.Errorf("pod %s/%s container %q has no %s mount", pod.Namespace, pod.Name, containerName, mountPath)
	}
	for _, volume := range pod.Spec.Volumes {
		if volume.Name == volumeName && volume.PersistentVolumeClaim != nil {
			return volume.PersistentVolumeClaim.ClaimName, nil
		}
	}
	return "", fmt.Errorf("pod %s/%s volume %q is not a PVC", pod.Namespace, pod.Name, volumeName)
}

func assertNamespaceFieldRef(t *testing.T, deployment appsv1.Deployment, name string) {
	t.Helper()
	found := 0
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != "operator" {
			continue
		}
		for _, env := range container.Env {
			if env.Name != name {
				continue
			}
			found++
			if env.Value != "" || env.ValueFrom == nil || env.ValueFrom.FieldRef == nil || env.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
				t.Fatalf("operator Deployment %s/%s has invalid %s source", deployment.Namespace, deployment.Name, name)
			}
		}
	}
	if found != 1 {
		t.Fatalf("operator Deployment %s/%s has %d %s entries, want exactly one", deployment.Namespace, deployment.Name, found, name)
	}
}

func assertRedisClusterForbidden(t *testing.T, config *rest.Config, serviceAccount, serviceAccountNamespace string, target scenario) {
	t.Helper()
	impersonated := rest.CopyConfig(config)
	impersonated.Impersonate.UserName = fmt.Sprintf("system:serviceaccount:%s:%s", serviceAccountNamespace, serviceAccount)
	client, err := dynamic.NewForConfig(impersonated)
	if err != nil {
		t.Fatalf("create client impersonating %s: %v", impersonated.Impersonate.UserName, err)
	}
	redisClusters := schema.GroupVersionResource{Group: cachev1.GroupVersion.Group, Version: cachev1.GroupVersion.Version, Resource: "redisclusters"}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	_, err = client.Resource(redisClusters).Namespace(target.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
	if err == nil {
		t.Fatalf("%s can read RedisCluster %s/%s", impersonated.Impersonate.UserName, target.Namespace, target.Name)
	}
	if !apierrors.IsForbidden(err) {
		t.Fatalf("cross-namespace RedisCluster read as %s returned %v, want Forbidden", impersonated.Impersonate.UserName, err)
	}
}

func TestCleanroom_LeaderElectionFailover(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("leader-election", "operator.replicaCount=2", "operator.leaderElection.enabled=true")
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)

	leaseName := "006a5d62.datadoghq.com"
	var original string
	h.eventually("leader lease acquired", 3*time.Minute, func(ctx context.Context) error {
		lease := &coordinationv1.Lease{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: leaseName}, lease); err != nil {
			return err
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
			return fmt.Errorf("lease has no holder")
		}
		original = *lease.Spec.HolderIdentity
		return nil
	})
	var pods corev1.PodList
	if err := h.client.List(h.ctx, &pods, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: operatorComponent}); err != nil {
		t.Fatal(err)
	}
	deleted := false
	for _, pod := range pods.Items {
		if strings.HasPrefix(original, pod.Name) {
			if err := h.client.Delete(h.ctx, &pod); err != nil {
				t.Fatal(err)
			}
			deleted = true
			break
		}
	}
	if !deleted {
		t.Fatalf("lease holder %q did not identify an operator pod", original)
	}
	h.eventually("leadership transferred", 3*time.Minute, func(ctx context.Context) error {
		lease := &coordinationv1.Lease{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: leaseName}, lease); err != nil {
			return err
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" || *lease.Spec.HolderIdentity == original {
			return fmt.Errorf("lease holder has not changed from %q", original)
		}
		return nil
	})
	h.waitReady(s, 3, 1)
}

func TestCleanroom_HelmLifecycle(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("helm-lifecycle")
	if _, err := h.helm(h.ctx, "upgrade", s.Release, h.repoRoot, "--namespace", s.Namespace, "--reuse-values", "--wait", "--timeout", "5m"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.helm(h.ctx, "uninstall", s.Release, "--namespace", s.Namespace, "--wait", "--timeout", "3m"); err != nil {
		t.Fatal(err)
	}
	h.eventually("Helm release workloads removed", 2*time.Minute, func(ctx context.Context) error {
		var deployments appsv1.DeploymentList
		if err := h.client.List(ctx, &deployments, client.InNamespace(s.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": s.Release}); err != nil {
			return err
		}
		if len(deployments.Items) != 0 {
			return fmt.Errorf("%d release Deployments remain", len(deployments.Items))
		}
		return nil
	})
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "redisclusters.cache.datadoghq.com"}}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(crd), crd); err != nil {
		t.Fatalf("shared RedisCluster CRD did not survive uninstall: %v", err)
	}
}
