// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Package kind runs the operator and real Redis clusters on shared KIND.
package kind

import (
	"context"
	"sort"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/controller"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	sharedkind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/internal/kindharness"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// crdFiles lists the portable operator API installed by the KIND harness.
var crdFiles = []string{
	"crds/redis-cluster-v1.yaml",
}

// shared is the operator's one KIND cluster, reused across the test binary.
var shared = sharedkind.NewSharedCluster(crdFiles, cachev1.AddToScheme)

const (
	// toolboxPollInterval is the cadence for pod readiness waits.
	toolboxPollInterval = 2 * time.Second
	// bgPollInterval is the cadence for the blue-green phase wait loops.
	bgPollInterval = 2 * time.Second
	// specHashTimeout bounds the wait for the operator to stamp its baseline
	// LastAppliedSpecHash on a fresh cluster before a BG trigger is applied.
	specHashTimeout = 1 * time.Minute
	// bgTriggerEnv is the redis container env var TriggerBlueGreen bumps.
	bgTriggerEnv = "E2E_BG_TRIGGER"
)

// errBlueGreenFailed aborts a phase wait when the operator reports BG Failed.
var errBlueGreenFailed = errBG("blue-green deployment reached Failed phase")

type errBG string

func (e errBG) Error() string { return string(e) }

// Suite is one test's view of the operator and its Redis toolbox.
type Suite struct {
	*sharedkind.Fixture
	re *redisExec // shared toolbox exec for topology probes (also feeds StartLoadgen)
}

// NewSuite gives a test a fresh namespace, the headless Service + toolbox pod a
// real cluster needs, and an in-process manager running the real
// RedisClustersReconciler. Namespace defaults to the test name; pass
// sharedkind.WithNamespace to override it.
func NewSuite(t *testing.T, opts ...sharedkind.Opt) *Suite {
	t.Helper()
	requireRedisImage(t)
	cluster := shared.Ensure(t)
	b := newBootstrap(cluster.Cfg)
	fixtureOpts := []sharedkind.Opt{
		sharedkind.UseCluster(cluster),
		sharedkind.WithPollInterval(time.Second),
		sharedkind.WithFailureArtifactList("client-redisclusters.json", func() client.ObjectList {
			return &cachev1.RedisClusterList{}
		}),
	}
	f := sharedkind.New(t, b, append(fixtureOpts, opts...)...)
	b.namespace = f.Namespace // reconciler is namespace-scoped, like production

	s := &Suite{Fixture: f}
	// The toolbox pod is what the reconciler execs redis-cli into
	// (redis_cluster/toolbox.go GetToolbox), so it must exist before the manager
	// starts. The per-cluster headless Service is created by CreateCluster, where
	// its name can match the CR (pods get DNS <pod>.<svc>.<ns>).
	s.deployToolbox(t)
	re, err := newRedisExec(f.Cfg, f.Client, f.Namespace, "")
	require.NoError(t, err, "connect suite toolbox exec")
	s.re = re
	sharedkind.StartManager(t, f, b)
	return s
}

// requireRedisImage fails fast if no redis image was supplied (KIND e2e has no fallback).
func requireRedisImage(t *testing.T) {
	t.Helper()
	if defaultRedisImage == "" {
		t.Fatal("KIND_E2E_REDIS_IMAGE (or E2E_REDIS_IMAGE) must name a Redis image, for example redis:8.6.0")
	}
}

// CreateCluster deploys the CR's headless Service (named for the CR so pods get
// stable DNS), creates the CR, and waits for the operator to bring the cluster
// up (StatefulSet ready + no blue-green in flight).
func (s *Suite) CreateCluster(t *testing.T, rc *cachev1.RedisCluster, timeout time.Duration) {
	t.Helper()
	s.deployHeadlessService(t, rc.Name)
	require.NoError(t, s.Fixture.Client.Create(s.Ctx, rc))
	require.NoError(t, s.WaitReady(client.ObjectKey{Namespace: rc.Namespace, Name: rc.Name}, timeout),
		"operator did not bring RedisCluster %q to Ready within %s", rc.Name, timeout)
}

// RestartManager cancels the in-process manager and starts a fresh one against
// the same cluster + namespace — the operator-restart-during-BG hook. Reuses
// the same Bootstrap so reconciler wiring is identical to the first start.
func (s *Suite) RestartManager(t *testing.T) {
	t.Helper()
	sharedkind.StartManager(t, s.Fixture, s.Bootstrap)
}

// StartLoadgen returns a data-path handle bound to this suite's toolbox. SeedKeys
// may be 0 for a probe-only handle. Reuses the suite's shared toolbox exec.
func (s *Suite) StartLoadgen(t *testing.T, seedKeys int) Loadgen {
	t.Helper()
	return newToolboxLoadgen(s.re, seedKeys)
}

// GetCluster fetches the live RedisCluster CR.
func (s *Suite) GetCluster(t *testing.T, name string) *cachev1.RedisCluster {
	t.Helper()
	rc := &cachev1.RedisCluster{}
	require.NoError(t, s.Get(name, rc))
	return rc
}

// TriggerBlueGreen makes a rollout due: the operator triggers BG on a spec change
// (computeSpecHash hashes pod-template annotations), not on chart_version alone.
func (s *Suite) TriggerBlueGreen(t *testing.T, name string) {
	t.Helper()
	// On a fresh cluster the operator adopts the current spec as its baseline
	// LastAppliedSpecHash on the first post-Ready reconcile. Triggering before
	// that stamp folds the trigger annotation into the baseline, so the rollout
	// never starts — wait for the baseline first.
	s.waitSpecHashStamped(t, name)
	v := "e2e-bg-" + time.Now().Format("150405.000")
	rc := s.GetCluster(t, name)
	setChartVersion(rc, v)
	// An env var, not a pod annotation: the compatibility CRD prunes embedded
	// pod-template metadata, so an annotation never reaches the rollout hash.
	setContainerEnv(t, &rc.Spec.StatefulsetTemplate.Spec.Template.Spec, constants.RedisContainerName, bgTriggerEnv, v)
	require.NoError(t, s.Fixture.Client.Update(s.Ctx, rc))
	require.Equal(t, v, containerEnv(s.GetCluster(t, name).Spec.StatefulsetTemplate.Spec.Template.Spec, constants.RedisContainerName, bgTriggerEnv),
		"API server dropped the blue-green trigger")
}

// setContainerEnv sets an env var on the named container, replacing any existing value.
func setContainerEnv(t *testing.T, pod *corev1.PodSpec, container, name, value string) {
	t.Helper()
	for i := range pod.Containers {
		if pod.Containers[i].Name != container {
			continue
		}
		c := &pod.Containers[i]
		for j := range c.Env {
			if c.Env[j].Name == name {
				c.Env[j] = corev1.EnvVar{Name: name, Value: value}
				return
			}
		}
		c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: value})
		return
	}
	t.Fatalf("container %q not found", container)
}

// containerEnv returns the literal value of an env var on the named container.
func containerEnv(pod corev1.PodSpec, container, name string) string {
	for _, c := range pod.Containers {
		if c.Name != container {
			continue
		}
		for _, e := range c.Env {
			if e.Name == name {
				return e.Value
			}
		}
	}
	return ""
}

// waitSpecHashStamped blocks until the operator has recorded a baseline
// LastAppliedSpecHash, so a subsequent pod-template change reads as a real diff.
func (s *Suite) waitSpecHashStamped(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, specHashTimeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, bgPollInterval, specHashTimeout, true,
		func(ctx context.Context) (bool, error) {
			return s.GetCluster(t, name).Status.LastAppliedSpecHash != "", nil
		})
	require.NoError(t, err, "operator never stamped LastAppliedSpecHash within %s", specHashTimeout)
}

// WaitBlueGreenStarted blocks until the blue-green phase leaves None.
func (s *Suite) WaitBlueGreenStarted(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	s.pollPhase(t, name, timeout, "blue-green started",
		func(p cachev1.BlueGreenPhase) bool { return p != cachev1.BlueGreenPhaseNone })
}

// WaitBlueGreenComplete blocks until the blue-green phase returns to None after
// having been active. Fails fast if the phase reaches Failed.
func (s *Suite) WaitBlueGreenComplete(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, timeout)
	defer cancel()
	sawActive := false
	err := wait.PollUntilContextTimeout(ctx, bgPollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			phase := s.GetCluster(t, name).Status.BlueGreen.Phase
			if phase == cachev1.BlueGreenPhaseFailed {
				return false, errBlueGreenFailed
			}
			if phase != cachev1.BlueGreenPhaseNone {
				sawActive = true
				return false, nil
			}
			return sawActive, nil
		})
	require.NoError(t, err, "blue-green did not complete within %s", timeout)
}

// WaitClusterHealthy polls the loadgen until Redis reports cluster_state=ok with
// all slots assigned — the gate before trusting a data-integrity check.
func (s *Suite) WaitClusterHealthy(t *testing.T, lg Loadgen, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, timeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, bgPollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			ok, err := lg.ClusterHealthy(ctx)
			return ok, ignoreTransient(err)
		})
	require.NoError(t, err, "redis cluster never became healthy within %s", timeout)
}

// ignoreTransient swallows exec/redis errors during a poll (toolbox blips,
// CLUSTERDOWN mid-rollout) so the wait keeps retrying instead of aborting.
func ignoreTransient(error) error { return nil }

// pollPhase waits until the blue-green phase satisfies want.
func (s *Suite) pollPhase(t *testing.T, name string, timeout time.Duration, label string, want func(cachev1.BlueGreenPhase) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, timeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, bgPollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			return want(s.GetCluster(t, name).Status.BlueGreen.Phase), nil
		})
	require.NoError(t, err, "%s: phase condition not met within %s", label, timeout)
}

// PatchCR fetches the CR fresh, applies mutate, and Updates it — the basis for
// scale/topology/annotation patches under operator contention.
func (s *Suite) PatchCR(t *testing.T, name string, mutate func(*cachev1.RedisCluster)) error {
	t.Helper()
	rc := s.GetCluster(t, name)
	mutate(rc)
	return s.Fixture.Client.Update(s.Ctx, rc)
}

// RedisPods lists the cluster's Redis pod names (sorted) by the operator's label.
func (s *Suite) RedisPods(t *testing.T, name string) []string {
	t.Helper()
	pods, err := s.tryRedisPodList(name)
	require.NoError(t, err)
	out := make([]string, 0, len(pods))
	for i := range pods {
		out = append(out, pods[i].Name)
	}
	return out
}

func (s *Suite) tryReadyRedisPodIPs(name string) ([]string, error) {
	pods, err := s.tryRedisPodList(name)
	if err != nil {
		return nil, err
	}
	ips := make([]string, 0, len(pods))
	for i := range pods {
		if podRunningReady(pods[i]) && pods[i].Status.PodIP != "" {
			ips = append(ips, pods[i].Status.PodIP)
		}
	}
	return ips, nil
}

// PodIP returns one pod's IP by name ("" if not found / not assigned).
func (s *Suite) PodIP(t *testing.T, podName string) string {
	t.Helper()
	pod := &corev1.Pod{}
	if err := s.Get(podName, pod); err != nil {
		return ""
	}
	return pod.Status.PodIP
}

// PodUID returns one pod's UID by name ("" if not found). Unlike the pod IP, the
// UID always changes on recreate, so waits keyed on it survive KIND recycling the
// deleted pod's old IP.
func (s *Suite) PodUID(t *testing.T, podName string) types.UID {
	t.Helper()
	pod := &corev1.Pod{}
	if err := s.Get(podName, pod); err != nil {
		return ""
	}
	return pod.UID
}

// tryRedisPodList returns the cluster's Redis pods, sorted by name (non-fatal).
func (s *Suite) tryRedisPodList(name string) ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := s.Fixture.Client.List(s.Ctx, list, client.InNamespace(s.Namespace),
		client.MatchingLabels{constants.LabelRedisClusterName: name}); err != nil {
		return nil, err
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Name < list.Items[j].Name })
	return list.Items, nil
}

// RequireIdle asserts no blue-green is in flight and no stale alternate (-alt)
// StatefulSet is still present alongside the active one.
func (s *Suite) RequireIdle(t *testing.T, name string) {
	t.Helper()
	rc := s.GetCluster(t, name)
	require.Equal(t, cachev1.BlueGreenPhaseNone, rc.Status.BlueGreen.Phase, "blue-green still in flight")
	serving := rc.Status.ActiveClusterName
	if serving == "" {
		serving = name
	}
	altName := constants.AlternateSTSName(serving)
	err := s.Fixture.Client.Get(s.Ctx, types.NamespacedName{Namespace: s.Namespace, Name: altName}, &appsv1.StatefulSet{})
	require.True(t, apierrors.IsNotFound(err), "alternate StatefulSet %q should not exist when idle", altName)
}

// WaitBlueGreenPhase blocks until the CR satisfies want — a general phase
// predicate wait (e.g. WaitingSync with a full ShardMapping before force-cutover).
func (s *Suite) WaitBlueGreenPhase(t *testing.T, name string, timeout time.Duration, want func(*cachev1.RedisCluster) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, timeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, bgPollInterval, timeout, true,
		func(ctx context.Context) (bool, error) {
			return want(s.GetCluster(t, name)), nil
		})
	require.NoError(t, err, "blue-green phase predicate not met within %s", timeout)
}

// ExecOnPod runs an arbitrary argv in a pod container (CLUSTER RESET HARD on a
// target, aws-cli on the backup sidecar).
func (s *Suite) ExecOnPod(ctx context.Context, pod, container string, argv []string) (int, string, string, error) {
	return s.re.ExecOnPod(ctx, pod, container, argv)
}

// RedisCommandAtIP runs a redis-cli command against a specific node IP.
func (s *Suite) RedisCommandAtIP(t *testing.T, ip string, args ...string) string {
	t.Helper()
	out, err := s.re.CommandAtIP(s.Ctx, ip, args...)
	require.NoError(t, err, "redis-cli -h %s %v", ip, args)
	return out
}

func (s *Suite) tryWriteAndVerifyStrings(kvs []redisStringKV) error {
	return s.re.writeAndVerifyStrings(s.Ctx, kvs)
}

func (s *Suite) tryVerifyStrings(kvs []redisStringKV) error {
	return s.re.verifyStrings(s.Ctx, kvs)
}

// TryRedisCommandAtIP runs a redis-cli command against a node IP (non-fatal).
func (s *Suite) TryRedisCommandAtIP(ip string, args ...string) (string, error) {
	return s.re.CommandAtIP(s.Ctx, ip, args...)
}

// TryClusterNodes returns CLUSTER NODES text (non-fatal) for poll loops.
func (s *Suite) TryClusterNodes() (string, error) { return s.re.ClusterNodes(s.Ctx) }

// deployHeadlessService creates the headless Service the Redis pods resolve each
// other through; name + selector match the CR so pods get DNS <pod>.<name>.<ns>.
func (s *Suite) deployHeadlessService(t *testing.T, name string) {
	t.Helper()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: s.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 map[string]string{constants.LabelRedisClusterName: name},
			Ports: []corev1.ServicePort{
				{Name: "redis", Port: redisPort},
				{Name: "gossip", Port: gossipPort},
			},
		},
	}
	err := s.Fixture.Client.Create(s.Ctx, svc)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
}

// deployToolbox creates the pod the reconciler execs redis-cli into. It runs the
// redis image (provides redis-cli + bash) with the label selector GetToolbox
// uses. Secret-authenticated deployments are covered by the chart and toolbox
// unit tests; this shared KIND fixture intentionally exercises the no-auth default.
func (s *Suite) deployToolbox(t *testing.T) {
	t.Helper()
	container := corev1.Container{
		Name:    constants.ToolboxContainerName,
		Image:   defaultRedisImage,
		Command: []string{"sleep", "infinity"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toolboxPodName,
			Namespace: s.Namespace,
			Labels:    map[string]string{"app": constants.ToolboxContainerName, "service": constants.ToolboxContainerName},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers:    []corev1.Container{container},
		},
	}
	err := s.Fixture.Client.Create(s.Ctx, pod)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
}

// StopShared tears down the shared cluster; call it from TestMain after m.Run.
func StopShared() { shared.Stop() }

// bootstrap is the redis-cluster plug-in: scheme + the real reconciler wiring.
type bootstrap struct {
	cfg       *rest.Config
	namespace string
}

func newBootstrap(cfg *rest.Config) *bootstrap { return &bootstrap{cfg: cfg} }

var _ sharedkind.Bootstrap = (*bootstrap)(nil)

func (b *bootstrap) Scheme() *runtime.Scheme { return shared.Scheme() }

// Register wires the operator's RedisClustersReconciler onto mgr — the same
// construction as mains/main_manage.go, with a no-op metrics impl.
func (b *bootstrap) Register(mgr ctrl.Manager) error {
	cc := redis_cluster.ClusterClient{
		Client:             mgr.GetClient(),
		ClusterScheme:      mgr.GetScheme(),
		RestConfig:         b.cfg,
		Namespace:          b.namespace,
		Log:                ctrl.Log.WithName("controller"),
		Metrics:            metrics.CreateNoop(),
		CredentialProvider: redis_cluster.NoAuthentication{},
	}
	return (&controller.RedisClustersReconciler{ClusterClient: cc}).SetupWithManager(mgr)
}

// Ready reports the RedisCluster healthy: bootstrap done (STS ready) and no
// blue-green deployment in flight.
func (b *bootstrap) Ready(ctx context.Context, c client.Client, key client.ObjectKey) (bool, error) {
	rc := &cachev1.RedisCluster{}
	if err := c.Get(ctx, key, rc); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	idle := rc.Status.BlueGreen.Phase == cachev1.BlueGreenPhaseNone
	return rc.Status.StatefulSetReady && idle, nil
}
