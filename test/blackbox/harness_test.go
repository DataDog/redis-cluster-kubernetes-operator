// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build blackbox_e2e

package blackbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	referenceNamespace = "redis-cluster-example"
	referenceName      = "cache"
	redisClusterLabel  = "redis_cluster_name"
	redisReadyLabel    = "redis-cluster-ready"
	componentLabel     = "app.kubernetes.io/component"
	operatorComponent  = "operator"
	toolboxComponent   = "toolbox"
	runLabel           = "cleanroom.redis-operator/run"
	pollInterval       = 2 * time.Second
	readyTimeout       = 12 * time.Minute
	topologyTimeout    = 8 * time.Minute
)

var invalidDNSCharacters = regexp.MustCompile(`[^a-z0-9-]+`)

type harness struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	client        client.Client
	clientset     kubernetes.Interface
	restConfig    *rest.Config
	kubeconfig    string
	contextName   string
	provider      string
	runID         string
	namespace     string
	release       string
	operatorImage string
	redisImage    string
	repoRoot      string
	artifacts     string
	secrets       [][]byte
}

type scenario struct {
	Namespace string
	Name      string
	Release   string
	Auth      bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctrllog.SetLogger(logr.Discard())
	kubeconfig := strings.TrimSpace(os.Getenv("KUBECONFIG"))
	if kubeconfig == "" {
		t.Fatal("KUBECONFIG must point at the clean-room cluster; ambient Kubernetes configuration is refused")
	}
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	raw, err := rules.Load()
	if err != nil {
		t.Fatalf("load explicit KUBECONFIG: %v", err)
	}
	contextName := raw.CurrentContext
	if contextName == "" {
		t.Fatal("KUBECONFIG has no current-context")
	}
	if _, ok := raw.Contexts[contextName]; !ok {
		t.Fatalf("KUBECONFIG current-context %q does not exist", contextName)
	}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
	if err != nil {
		t.Fatalf("build Kubernetes client config: %v", err)
	}
	config.UserAgent = "redis-cluster-operator-cleanroom-tests"
	config.Timeout = 45 * time.Second

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"Kubernetes":    clientgoscheme.AddToScheme,
		"apiextensions": apiextensionsv1.AddToScheme,
		"RedisCluster":  cachev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("register %s scheme: %v", name, err)
		}
	}
	kubeClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create Kubernetes client: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create Kubernetes clientset: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		client:        kubeClient,
		clientset:     clientset,
		restConfig:    config,
		kubeconfig:    kubeconfig,
		contextName:   contextName,
		provider:      envOr("CLEANROOM_PROVIDER", "unknown"),
		runID:         requiredEnv(t, "CLEANROOM_RUN_ID"),
		namespace:     envOr("CLEANROOM_NAMESPACE", referenceNamespace),
		release:       envOr("CLEANROOM_RELEASE", "redis-cluster-kubernetes-operator"),
		operatorImage: requiredEnv(t, "CLEANROOM_OPERATOR_IMAGE"),
		redisImage:    requiredEnv(t, "CLEANROOM_REDIS_IMAGE"),
		repoRoot:      requiredEnv(t, "CLEANROOM_REPO_ROOT"),
		artifacts:     requiredEnv(t, "CLEANROOM_ARTIFACT_DIR"),
	}
	if state := strings.TrimSpace(os.Getenv("CLEANROOM_RESOURCE_STATE")); state == "" {
		t.Fatal("CLEANROOM_RESOURCE_STATE must identify the provisioner's state file")
	}
	if err := os.MkdirAll(h.artifacts, 0o750); err != nil {
		t.Fatalf("create artifact directory: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			h.collectFailureArtifacts()
		}
		cancel()
	})
	return h
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func (h *harness) eventually(label string, timeout time.Duration, check func(context.Context) error) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	var last error
	err := wait.PollUntilContextTimeout(ctx, pollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		last = check(ctx)
		return last == nil, nil
	})
	if err != nil {
		h.t.Fatalf("%s did not converge within %s: %v", label, timeout, last)
	}
}

func (h *harness) reference() scenario {
	return scenario{Namespace: h.namespace, Name: referenceName, Release: h.release}
}

func (h *harness) cluster(s scenario) *cachev1.RedisCluster {
	h.t.Helper()
	rc := &cachev1.RedisCluster{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, rc); err != nil {
		h.t.Fatalf("get RedisCluster %s/%s: %v", s.Namespace, s.Name, err)
	}
	return rc
}

func (h *harness) pods(s scenario) []corev1.Pod {
	h.t.Helper()
	var pods corev1.PodList
	if err := h.client.List(h.ctx, &pods, client.InNamespace(s.Namespace), client.MatchingLabels{redisClusterLabel: s.Name}); err != nil {
		h.t.Fatalf("list Redis pods for %s/%s: %v", s.Namespace, s.Name, err)
	}
	sort.Slice(pods.Items, func(i, j int) bool { return pods.Items[i].Name < pods.Items[j].Name })
	return pods.Items
}

func podReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (h *harness) waitReady(s scenario, primaries, replicas int32) {
	h.t.Helper()
	wantPods := int(primaries * (1 + replicas))
	h.eventually(fmt.Sprintf("%s/%s ready", s.Namespace, s.Name), readyTimeout, func(ctx context.Context) error {
		rc := &cachev1.RedisCluster{}
		if err := h.client.Get(ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, rc); err != nil {
			return err
		}
		if !rc.Status.StatefulSetReady || rc.Status.RolloutInProgress || rc.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseNone {
			return fmt.Errorf("status ready=%t rollout=%t blueGreen=%q", rc.Status.StatefulSetReady, rc.Status.RolloutInProgress, rc.Status.BlueGreen.Phase)
		}
		var pods corev1.PodList
		if err := h.client.List(ctx, &pods, client.InNamespace(s.Namespace), client.MatchingLabels{redisClusterLabel: s.Name}); err != nil {
			return err
		}
		ready := 0
		for _, pod := range pods.Items {
			if podReady(pod) {
				ready++
			}
		}
		if ready != wantPods {
			return fmt.Errorf("ready Redis pods=%d, want %d", ready, wantPods)
		}
		return h.checkTopology(ctx, s, int(primaries), int(primaries*replicas))
	})
}

func (h *harness) checkTopology(ctx context.Context, s scenario, wantPrimaries, wantReplicas int) error {
	pods := h.pods(s)
	for _, pod := range pods {
		if !podReady(pod) {
			continue
		}
		out, err := h.redis(ctx, s, pod, "CLUSTER", "NODES")
		if err != nil {
			return err
		}
		nodes := parseClusterNodes(out)
		primaries, replicas := topologyCounts(nodes)
		if primaries != wantPrimaries || replicas != wantReplicas {
			return fmt.Errorf("%s sees %d primaries/%d replicas, want %d/%d", pod.Name, primaries, replicas, wantPrimaries, wantReplicas)
		}
		coverage, err := slotCoverage(nodes)
		if err != nil {
			return fmt.Errorf("%s slot ownership: %w", pod.Name, err)
		}
		if coverage != totalRedisSlots {
			return fmt.Errorf("%s slot coverage=%d, want %d", pod.Name, coverage, totalRedisSlots)
		}
		info, err := h.redis(ctx, s, pod, "CLUSTER", "INFO")
		if err != nil {
			return err
		}
		if got := infoValue(info, "cluster_state"); got != "ok" {
			return fmt.Errorf("%s cluster_state=%q", pod.Name, got)
		}
	}
	return nil
}

func (h *harness) redis(ctx context.Context, s scenario, pod corev1.Pod, args ...string) (string, error) {
	if s.Auth {
		command := append([]string{"-h", pod.Status.PodIP, "-p", "6379", "--raw"}, args...)
		return redisResult(h.toolbox(ctx, s.Namespace, true, command...))
	}
	return h.redisInPod(ctx, pod, args...)
}

func (h *harness) redisInPod(ctx context.Context, pod corev1.Pod, args ...string) (string, error) {
	command := append([]string{"redis-cli", "-c", "-h", "127.0.0.1", "-p", "6379", "--raw"}, args...)
	return redisResult(h.exec(ctx, pod.Namespace, pod.Name, "redis", command))
}

func (h *harness) redisViaService(ctx context.Context, s scenario, args ...string) (string, error) {
	host := fmt.Sprintf("%s.%s.svc.cluster.local", s.Name, s.Namespace)
	command := append([]string{"-h", host, "-p", "6379", "--raw"}, args...)
	return redisResult(h.toolbox(ctx, s.Namespace, s.Auth, command...))
}

func (h *harness) serviceAvailabilityProbe(ctx context.Context, s scenario) (string, error) {
	pod, err := h.toolboxPod(ctx, s.Namespace)
	if err != nil {
		return "", err
	}
	host := fmt.Sprintf("%s.%s.svc.cluster.local", s.Name, s.Namespace)
	script := `while :; do
  result="$(redis-cli -c -h "$1" -p 6379 --raw GET cleanroom:string 2>/dev/null)"
  status=$?
  if [ "$status" -ne 0 ]; then
    echo exec-error
  elif [ "$result" = operator-public ]; then
    echo ok
  elif [ -z "$result" ]; then
    echo empty
  else
    echo redis-error
  fi
  sleep 0.5
done`
	return h.exec(ctx, s.Namespace, pod.Name, "managed-redis-cluster-operator-toolbox", []string{
		"sh", "-c", script, "cleanroom-service-probe", host,
	})
}

func redisResult(output string, err error) (string, error) {
	if err != nil {
		return output, err
	}
	trimmed := strings.TrimSpace(output)
	for _, prefix := range []string{"(error)", "ERR ", "WRONGPASS ", "NOAUTH ", "CROSSSLOT ", "CLUSTERDOWN ", "READONLY ", "LOADING ", "MISCONF ", "OOM ", "BUSY ", "MASTERDOWN ", "NOREPLICAS ", "TRYAGAIN "} {
		if strings.HasPrefix(trimmed, prefix) {
			return output, fmt.Errorf("Redis command failed: %s", trimmed)
		}
	}
	return output, nil
}

func (h *harness) toolbox(ctx context.Context, namespace string, authenticated bool, args ...string) (string, error) {
	pod, err := h.toolboxPod(ctx, namespace)
	if err != nil {
		return "", err
	}
	executable := "redis-cli"
	if authenticated {
		executable = "/usr/local/bin/redis-operator-redis-cli"
	}
	command := append([]string{executable, "-c"}, args...)
	return h.exec(ctx, namespace, pod.Name, "managed-redis-cluster-operator-toolbox", command)
}

func (h *harness) toolboxPod(ctx context.Context, namespace string) (corev1.Pod, error) {
	var pods corev1.PodList
	if err := h.client.List(ctx, &pods, client.InNamespace(namespace), client.MatchingLabels{componentLabel: toolboxComponent}); err != nil {
		return corev1.Pod{}, err
	}
	if len(pods.Items) != 1 || !podReady(pods.Items[0]) {
		return corev1.Pod{}, fmt.Errorf("toolbox in %s is not uniquely ready", namespace)
	}
	return pods.Items[0], nil
}

func (h *harness) exec(ctx context.Context, namespace, pod, container string, command []string) (string, error) {
	req := h.clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{Container: container, Command: command, Stdout: true, Stderr: true}, clientgoscheme.ParameterCodec)
	streamConfig := rest.CopyConfig(h.restConfig)
	// A normal Kubernetes request should be bounded, but an exec stream lives for
	// as long as its caller's context. In particular, the availability probe must
	// span an entire blue-green rollout, which can take several minutes.
	streamConfig.Timeout = 0
	executor, err := remotecommand.NewSPDYExecutor(streamConfig, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("create exec transport: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return stdout.String(), fmt.Errorf("exec %s/%s container %s: %w: %s", namespace, pod, container, err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (h *harness) seedAndVerify(s scenario) {
	h.t.Helper()
	pods := h.pods(s)
	if len(pods) == 0 {
		h.t.Fatal("cannot seed data without Redis pods")
	}
	commands := [][]string{
		{"DEL", "cleanroom:string"},
		{"DEL", "cleanroom:hash"},
		{"DEL", "cleanroom:list"},
		{"SET", "cleanroom:string", "operator-public"},
		{"HSET", "cleanroom:hash", "kind", "redis", "version", "8.6"},
		{"RPUSH", "cleanroom:list", "one", "two", "three"},
	}
	for _, command := range commands {
		if _, err := h.redis(h.ctx, s, pods[0], command...); err != nil {
			h.t.Fatalf("seed %s: %v", command[0], err)
		}
	}
	h.verifyData(s)
}

func (h *harness) verifyData(s scenario) {
	h.t.Helper()
	pods := h.pods(s)
	if len(pods) == 0 {
		h.t.Fatal("cannot verify data without Redis pods")
	}
	checks := []struct {
		command []string
		want    string
	}{
		{[]string{"GET", "cleanroom:string"}, "operator-public"},
		{[]string{"HGET", "cleanroom:hash", "version"}, "8.6"},
		{[]string{"LLEN", "cleanroom:list"}, "3"},
	}
	for _, check := range checks {
		got, err := h.redis(h.ctx, s, pods[0], check.command...)
		if err != nil {
			h.t.Fatalf("verify %s: %v", check.command[0], err)
		}
		if got != check.want {
			h.t.Fatalf("%s returned %q, want %q", check.command[0], got, check.want)
		}
	}
}

func (h *harness) patchCluster(s scenario, mutate func(*cachev1.RedisCluster)) {
	h.t.Helper()
	for attempt := 0; attempt < 8; attempt++ {
		rc := &cachev1.RedisCluster{}
		if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: s.Namespace, Name: s.Name}, rc); err != nil {
			h.t.Fatalf("get RedisCluster before update: %v", err)
		}
		mutate(rc)
		if err := h.client.Update(h.ctx, rc); err == nil {
			return
		} else if !apierrors.IsConflict(err) {
			h.t.Fatalf("update RedisCluster: %v", err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	h.t.Fatal("update RedisCluster kept conflicting")
}

func (h *harness) freshName(prefix string) string {
	return h.freshNameWithLimit(prefix, 63)
}

func (h *harness) freshNameWithLimit(prefix string, limit int) string {
	clean := invalidDNSCharacters.ReplaceAllString(strings.ToLower(prefix), "-")
	clean = strings.Trim(clean, "-")
	suffix := fmt.Sprintf("%s-%d", sanitizeDNS(h.runID), time.Now().UnixNano()%1_000_000)
	maxPrefix := limit - 1 - len(suffix)
	if maxPrefix < 1 {
		maxPrefix = 1
	}
	if len(clean) > maxPrefix {
		clean = strings.Trim(clean[:maxPrefix], "-")
	}
	return clean + "-" + suffix
}

func sanitizeDNS(value string) string {
	value = invalidDNSCharacters.ReplaceAllString(strings.ToLower(value), "-")
	value = strings.Trim(value, "-")
	if value == "" {
		return "run"
	}
	if len(value) > 20 {
		value = strings.Trim(value[:20], "-")
	}
	return value
}

func artifactSafeName(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%s-%x", sanitizeDNS(value), sum[:4])
}

func splitImage(image string) (string, string, error) {
	if strings.Contains(image, "@") {
		return "", "", errors.New("digest-only CLEANROOM_OPERATOR_IMAGE is not supported by the chart")
	}
	idx := strings.LastIndex(image, ":")
	if idx <= strings.LastIndex(image, "/") {
		return "", "", fmt.Errorf("operator image %q must include a tag", image)
	}
	return image[:idx], image[idx+1:], nil
}

func (h *harness) helm(ctx context.Context, args ...string) ([]byte, error) {
	base := []string{"--kubeconfig", h.kubeconfig, "--kube-context", h.contextName}
	command := exec.CommandContext(ctx, "helm", append(base, args...)...)
	command.Dir = h.repoRoot
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("helm %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func (h *harness) installOperator(namespace, release string, extraValues ...string) {
	h.t.Helper()
	repository, tag, err := splitImage(h.operatorImage)
	if err != nil {
		h.t.Fatal(err)
	}
	args := []string{"upgrade", "--install", release, h.repoRoot, "--namespace", namespace, "--wait", "--timeout", "5m",
		"--set-string", "operator.image.repository=" + repository,
		"--set-string", "operator.image.tag=" + tag,
		"--set-string", "operator.image.pullPolicy=IfNotPresent"}
	for _, value := range extraValues {
		args = append(args, "--set", value)
	}
	if _, err := h.helm(h.ctx, args...); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) newScenario(prefix string, installValues ...string) scenario {
	h.t.Helper()
	s := h.newScenarioNamespace(prefix)
	h.installOperator(s.Namespace, s.Release, installValues...)
	return s
}

func (h *harness) newScenarioNamespace(prefix string) scenario {
	h.t.Helper()
	namespace := h.freshName("redis-cluster-" + prefix)
	// The controller appends an 11-character hash to redis_cluster_name for
	// generated pod labels, so the CR name must leave room below the 63-byte
	// Kubernetes label-value limit.
	name := h.freshNameWithLimit("redis-cluster-"+prefix, 48)
	release := h.freshNameWithLimit("redis-operator-"+prefix, 53)
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace, Labels: map[string]string{runLabel: h.runID}}}
	if err := h.client.Create(h.ctx, ns); err != nil {
		h.t.Fatalf("create scenario namespace: %v", err)
	}
	s := scenario{Namespace: namespace, Name: name, Release: release}
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		runScenarioCleanup(h.t.Failed(), func() {
			h.collectNamespaceFailureArtifacts(ctx, s.Namespace)
		}, func() {
			_, _ = h.helm(ctx, "uninstall", s.Release, "--namespace", s.Namespace, "--wait", "--timeout", "2m")
			_ = h.client.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: s.Namespace}})
		})
	})
	return s
}

func runScenarioCleanup(failed bool, collect, teardown func()) {
	if failed {
		collect()
	}
	teardown()
}

func (h *harness) createClusterFromReference(s scenario) {
	h.t.Helper()
	h.createHeadlessService(s)
	base := h.clusterFromReference(s)
	if err := h.client.Create(h.ctx, base); err != nil {
		h.t.Fatalf("create scenario RedisCluster: %v", err)
	}
}

func (h *harness) createHeadlessService(s scenario) {
	h.t.Helper()
	reference := &corev1.Service{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: h.namespace, Name: referenceName}, reference); err != nil {
		h.t.Fatalf("get reference headless Service: %v", err)
	}
	ports := make([]corev1.ServicePort, len(reference.Spec.Ports))
	copy(ports, reference.Spec.Ports)
	for i := range ports {
		ports[i].NodePort = 0
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: reference.Spec.PublishNotReadyAddresses,
			Selector:                 map[string]string{redisClusterLabel: s.Name},
			Ports:                    ports,
		},
	}
	if err := h.client.Create(h.ctx, service); err != nil {
		h.t.Fatalf("create scenario headless Service: %v", err)
	}
}

func (h *harness) clusterFromReference(s scenario) *cachev1.RedisCluster {
	h.t.Helper()
	base := h.cluster(h.reference()).DeepCopy()
	base.TypeMeta = metav1.TypeMeta{APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster"}
	base.ObjectMeta = metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace, Labels: map[string]string{"chart_version": "0.1.0"}}
	base.Status = cachev1.RedisClusterStatus{}
	base.Spec.StatefulsetTemplate.ObjectMeta = metav1.ObjectMeta{}
	base.Spec.StatefulsetTemplate.Spec.ServiceName = s.Name
	labels := base.Spec.StatefulsetTemplate.Spec.Selector.MatchLabels
	labels[redisClusterLabel] = s.Name
	labels["chart_name"] = s.Name
	templateLabels := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		templateLabels[key] = value
	}
	for key, value := range base.Spec.StatefulsetTemplate.Spec.Template.Labels {
		if value == referenceName {
			value = s.Name
		}
		templateLabels[key] = value
	}
	templateLabels[redisClusterLabel] = s.Name
	base.Spec.StatefulsetTemplate.Spec.Template.Labels = templateLabels
	for containerIndex := range base.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers {
		container := &base.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[containerIndex]
		if container.Name != "redis" {
			continue
		}
		for envIndex := range container.Env {
			if container.Env[envIndex].Name == "REDIS_SERVICE_NAME" {
				container.Env[envIndex].Value = s.Name
			}
		}
	}
	return base
}

func (h *harness) proxyPod(namespace, pod string, port int, path string) (string, error) {
	request := h.clientset.CoreV1().RESTClient().Get().Namespace(namespace).Resource("pods").
		Name(fmt.Sprintf("%s:%d", pod, port)).SubResource("proxy").Suffix(path)
	result, err := request.DoRaw(h.ctx)
	return string(result), err
}

func (h *harness) recordSecret(values ...[]byte) {
	for _, value := range values {
		if len(value) > 0 {
			h.secrets = append(h.secrets, append([]byte(nil), value...))
		}
	}
}

func (h *harness) writeArtifact(name string, value any) {
	h.t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return
	}
	for _, secret := range h.secrets {
		if bytes.Contains(data, secret) {
			h.t.Errorf("refusing to write artifact %s because it contains generated credentials", name)
			return
		}
	}
	_ = os.WriteFile(filepath.Join(h.artifacts, sanitizeDNS(h.t.Name())+"-"+name+".json"), data, 0o600)
}

func (h *harness) collectFailureArtifacts() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	namespaces := map[string]struct{}{h.namespace: {}}
	var scenarioNamespaces corev1.NamespaceList
	if err := h.client.List(ctx, &scenarioNamespaces, client.MatchingLabels{runLabel: h.runID}); err == nil {
		for _, namespace := range scenarioNamespaces.Items {
			namespaces[namespace.Name] = struct{}{}
		}
	}
	h.collectFailureArtifactsForNamespaces(ctx, namespaces, "")
}

func (h *harness) collectNamespaceFailureArtifacts(ctx context.Context, namespace string) {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	namespaces := map[string]struct{}{namespace: {}}
	h.collectFailureArtifactsForNamespaces(ctx, namespaces, "scenario-"+artifactSafeName(namespace)+"-")
}

func (h *harness) collectFailureArtifactsForNamespaces(ctx context.Context, namespaces map[string]struct{}, artifactPrefix string) {
	objects := map[string]any{}
	for ns := range namespaces {
		collect := func(name string, list client.ObjectList) {
			if err := h.client.List(ctx, list, client.InNamespace(ns)); err == nil {
				objects[ns+"/"+name] = list
			}
		}
		collect("redisclusters", &cachev1.RedisClusterList{})
		collect("pods", &corev1.PodList{})
		collect("statefulsets", &appsv1.StatefulSetList{})
		collect("services", &corev1.ServiceList{})
		collect("persistentvolumeclaims", &corev1.PersistentVolumeClaimList{})
		collect("events", &corev1.EventList{})
		collect("leases", &coordinationv1.LeaseList{})
		collect("roles", &rbacv1.RoleList{})
	}
	h.writeArtifact(artifactPrefix+"objects", objects)

	logs := map[string]string{}
	for ns := range namespaces {
		var pods corev1.PodList
		if err := h.client.List(ctx, &pods, client.InNamespace(ns)); err != nil {
			continue
		}
		for _, pod := range pods.Items {
			for _, container := range pod.Spec.Containers {
				stream, err := h.clientset.CoreV1().Pods(ns).GetLogs(pod.Name, &corev1.PodLogOptions{Container: container.Name, TailLines: ptr.To(int64(500))}).Stream(ctx)
				if err != nil {
					continue
				}
				data, _ := io.ReadAll(io.LimitReader(stream, 2<<20))
				_ = stream.Close()
				logs[ns+"/"+pod.Name+"/"+container.Name] = string(data)
			}
		}
	}
	h.writeArtifact(artifactPrefix+"logs", logs)
}

func conditionReason(conditions []metav1.Condition, reason string) bool {
	for _, condition := range conditions {
		if condition.Reason == reason || strings.Contains(condition.Message, reason) {
			return true
		}
	}
	return false
}
