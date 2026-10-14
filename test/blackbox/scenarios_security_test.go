// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build blackbox_e2e

package blackbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func randomCredential(t *testing.T, prefix string) []byte {
	t.Helper()
	raw := make([]byte, 18)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return []byte(prefix + hex.EncodeToString(raw))
}

func (h *harness) createAuthFixture(prefix string, wrongOperatorPassword bool) scenario {
	h.t.Helper()
	s := h.newScenarioNamespace(prefix)
	s.Auth = true
	username := randomCredential(h.t, "cleanroom-")
	password := randomCredential(h.t, "p-")
	operatorPassword := password
	if wrongOperatorPassword {
		operatorPassword = randomCredential(h.t, "wrong-")
	}
	h.recordSecret(username, password, operatorPassword)
	acl := []byte(fmt.Sprintf("user default off\nuser %s on >%s ~* &* +@all\n", username, password))
	redisSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-acl", Namespace: s.Namespace},
		Data:       map[string][]byte{"users.acl": acl},
	}
	operatorSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "operator-redis-auth", Namespace: s.Namespace},
		Data:       map[string][]byte{"username": username, "password": operatorPassword},
	}
	for _, secret := range []*corev1.Secret{redisSecret, operatorSecret} {
		if err := h.client.Create(h.ctx, secret); err != nil {
			h.t.Fatalf("create authentication Secret %s: %v", secret.Name, err)
		}
	}
	h.installOperator(s.Namespace, s.Release,
		"operator.redisAuth.secretName="+operatorSecret.Name,
		"operator.redisAuth.usernameKey=username",
		"operator.redisAuth.passwordKey=password")

	h.createHeadlessService(s)
	rc := h.clusterFromReference(s)
	rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Volumes = append(rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Volumes,
		corev1.Volume{Name: "redis-acl", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: redisSecret.Name}}})
	container := &rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0]
	container.Args = append(container.Args, "--aclfile", "/run/redis-auth/users.acl")
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "redis-acl", MountPath: "/run/redis-auth", ReadOnly: true})
	container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)}}, PeriodSeconds: 5}
	container.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)}}, InitialDelaySeconds: 15, PeriodSeconds: 10}
	if err := h.client.Create(h.ctx, rc); err != nil {
		h.t.Fatalf("create authenticated RedisCluster: %v", err)
	}
	return s
}

func TestCleanroom_SecretAuthentication(t *testing.T) {
	h := newHarness(t)
	s := h.createAuthFixture("secret-auth", false)
	h.waitReady(s, 3, 1)
	h.seedAndVerify(s)
	if err := h.assertSecretsAbsent(s.Namespace); err != nil {
		t.Fatal(err)
	}
}

func TestCleanroom_AuthenticationFailsClosed(t *testing.T) {
	t.Run("wrong password", func(t *testing.T) {
		h := newHarness(t)
		s := h.createAuthFixture("wrong-auth", true)
		h.eventually("authenticated Redis pods running", readyTimeout, func(ctx context.Context) error {
			pods := h.pods(s)
			if len(pods) != 6 {
				return fmt.Errorf("Redis pods=%d, want 6", len(pods))
			}
			for _, pod := range pods {
				if !podReady(pod) {
					return fmt.Errorf("pod %s is not Ready", pod.Name)
				}
			}
			return nil
		})
		pods := h.pods(s)
		if _, err := h.redis(h.ctx, s, pods[0], "CLUSTER", "INFO"); err == nil {
			t.Fatal("wrong operator credentials unexpectedly authenticated")
		}
		time.Sleep(15 * time.Second)
		for _, pod := range h.pods(s) {
			if pod.Labels[redisReadyLabel] == "true" {
				t.Fatalf("pod %s was marked topology-ready with wrong toolbox credentials", pod.Name)
			}
		}
		if nodes := h.cluster(s).Status.Nodes; len(nodes) != 0 {
			t.Fatalf("RedisCluster reported %d topology nodes with wrong toolbox credentials", len(nodes))
		}
		if err := h.assertSecretsAbsent(s.Namespace); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		h := newHarness(t)
		s := h.newScenarioNamespace("missing-auth-key")
		username := randomCredential(t, "missing-")
		h.recordSecret(username)
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "incomplete-auth", Namespace: s.Namespace}, Data: map[string][]byte{"username": username}}
		if err := h.client.Create(h.ctx, secret); err != nil {
			t.Fatal(err)
		}
		repository, tag, err := splitImage(h.operatorImage)
		if err != nil {
			t.Fatal(err)
		}
		_, err = h.helm(h.ctx, "upgrade", "--install", s.Release, h.repoRoot, "--namespace", s.Namespace,
			"--set-string", "operator.image.repository="+repository,
			"--set-string", "operator.image.tag="+tag,
			"--set-string", "operator.redisAuth.secretName="+secret.Name)
		if err != nil {
			t.Fatal(err)
		}
		h.eventually("toolbox rejects incomplete Secret", 2*time.Minute, func(ctx context.Context) error {
			var pods corev1.PodList
			if err := h.client.List(ctx, &pods, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: toolboxComponent}); err != nil {
				return err
			}
			if len(pods.Items) != 1 {
				return fmt.Errorf("toolbox pods=%d", len(pods.Items))
			}
			if podReady(pods.Items[0]) {
				return fmt.Errorf("toolbox unexpectedly Ready with missing password key")
			}
			return nil
		})
		if err := h.assertSecretsAbsent(s.Namespace); err != nil {
			t.Fatal(err)
		}
	})
}

func (h *harness) assertSecretsAbsent(namespace string) error {
	var pods corev1.PodList
	if err := h.client.List(h.ctx, &pods, client.InNamespace(namespace)); err != nil {
		return err
	}
	var events corev1.EventList
	if err := h.client.List(h.ctx, &events, client.InNamespace(namespace)); err != nil {
		return err
	}
	data, err := json.Marshal(struct {
		Pods   []corev1.Pod
		Events []corev1.Event
	}{pods.Items, events.Items})
	if err != nil {
		return err
	}
	for _, pod := range pods.Items {
		for _, container := range pod.Spec.Containers {
			logs, err := h.clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container.Name, TailLines: ptr.To(int64(300))}).DoRaw(h.ctx)
			if err == nil {
				data = append(data, logs...)
			}
		}
	}
	for _, secret := range h.secrets {
		if bytes.Contains(data, secret) {
			return fmt.Errorf("generated credential appeared in pod specifications, events, or logs")
		}
	}
	return nil
}

func TestCleanroom_ObservabilityAndRBAC(t *testing.T) {
	h := newHarness(t)
	s := h.newScenario("observability")
	var operatorPods corev1.PodList
	if err := h.client.List(h.ctx, &operatorPods, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: operatorComponent}); err != nil || len(operatorPods.Items) != 1 {
		t.Fatalf("operator pod count=%d err=%v", len(operatorPods.Items), err)
	}
	operatorPod := operatorPods.Items[0]
	for _, path := range []string{"healthz", "readyz"} {
		body, err := h.proxyPod(s.Namespace, operatorPod.Name, 8081, path)
		if err != nil || strings.TrimSpace(body) != "ok" {
			t.Fatalf("/%s body=%q err=%v", path, body, err)
		}
	}
	h.createClusterFromReference(s)
	h.waitReady(s, 3, 1)
	metrics, err := h.proxyPod(s.Namespace, operatorPod.Name, 8080, "metrics")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"redis_cluster_operator_health", "redis_cluster_operator_events_total", "redis_cluster_operator_cluster_primaries"} {
		if !strings.Contains(metrics, name) {
			t.Errorf("metrics output is missing %s", name)
		}
	}

	var toolboxDeployments appsv1.DeploymentList
	if err := h.client.List(h.ctx, &toolboxDeployments, client.InNamespace(s.Namespace), client.MatchingLabels{componentLabel: toolboxComponent}); err != nil || len(toolboxDeployments.Items) != 1 {
		t.Fatalf("toolbox Deployment count=%d err=%v", len(toolboxDeployments.Items), err)
	}
	toolboxPodSpec := toolboxDeployments.Items[0].Spec.Template.Spec
	if toolboxPodSpec.AutomountServiceAccountToken == nil || *toolboxPodSpec.AutomountServiceAccountToken {
		t.Error("toolbox must explicitly disable service-account-token automount")
	}
	if toolboxPodSpec.ServiceAccountName == operatorPod.Spec.ServiceAccountName {
		t.Error("operator and toolbox use the same ServiceAccount")
	}
	var roles rbacv1.RoleList
	if err := h.client.List(h.ctx, &roles, client.InNamespace(s.Namespace), client.MatchingLabels{"app.kubernetes.io/instance": s.Release}); err != nil || len(roles.Items) != 1 {
		t.Fatalf("release Role count=%d err=%v", len(roles.Items), err)
	}
	var clusterBindings rbacv1.ClusterRoleBindingList
	if err := h.client.List(h.ctx, &clusterBindings, client.MatchingLabels{"app.kubernetes.io/instance": s.Release}); err != nil {
		t.Fatal(err)
	}
	if len(clusterBindings.Items) != 0 {
		t.Fatalf("release created %d ClusterRoleBindings", len(clusterBindings.Items))
	}
	var clusterRoles rbacv1.ClusterRoleList
	if err := h.client.List(h.ctx, &clusterRoles, client.MatchingLabels{"app.kubernetes.io/instance": s.Release}); err != nil {
		t.Fatal(err)
	}
	if len(clusterRoles.Items) != 0 {
		t.Fatalf("release created %d ClusterRoles", len(clusterRoles.Items))
	}
	container := operatorPod.Spec.Containers[0]
	if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation ||
		container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem {
		t.Error("operator container security context is not hardened")
	}
}
