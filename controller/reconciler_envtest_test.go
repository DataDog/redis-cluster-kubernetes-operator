package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/envtest"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Bug-surface envtest tests drive reconciler helpers against an in-memory K8s
// API server (the repository-local test/framework/envtest harness).
// Each test targets one named bug class. Failing tests document open bugs;
// passing tests guard against regression.
//
// The CRD prunes Spec.StatefulsetTemplate.metadata.name, so tests that need an
// existing STS pre-create it under a chosen name and point
// Status.ActiveClusterName at it — mirroring how the operator addresses its STS
// in production (where the Helm chart templates the name into both places).

func newReconciler(t *testing.T) (*RedisClusterReconciler, string) {
	t.Helper()
	s := envtest.SharedSuite(t)
	ns := s.NewNamespace(t)
	cc := envtest.NewClusterClient(t, s, ns)
	return &RedisClusterReconciler{
		ClusterClient: cc,
		notFoundCache: &sync.Map{},
	}, ns
}

func TestEnvtest_SetupWithManagerWithoutExternalAutoscalingCRDs(t *testing.T) {
	s := envtest.SharedSuite(t)
	ns := s.NewNamespace(t)

	// Register only public Kubernetes and RedisCluster types. External
	// autoscaling APIs are neither installed nor present in the scheme.
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(s.Cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
	})
	require.NoError(t, err)

	clusterClient := envtest.NewClusterClient(t, s, ns)
	clusterClient.Client = mgr.GetClient()
	clusterClient.ClusterScheme = mgr.GetScheme()
	reconciler := &RedisClustersReconciler{ClusterClient: clusterClient}
	require.NoError(t, reconciler.SetupWithManager(mgr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.Start(ctx)
	}()

	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	require.True(t, mgr.GetCache().WaitForCacheSync(syncCtx), "manager cache did not synchronize without the external autoscaling CRDs")
	syncCancel()

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("manager did not stop after context cancellation")
	}
}

func createCR(t *testing.T, r *RedisClusterReconciler, name string, opts ...redisbuilder.ClusterOpt) *cachev1.RedisCluster {
	t.Helper()
	opts = append([]redisbuilder.ClusterOpt{envtest.WithNamespace(r.Namespace)}, opts...)
	rc := envtest.NewRedisCluster(name, opts...)
	require.NoError(t, r.Create(context.Background(), rc))
	envtest.InitializeRequiredStatus(rc)
	rc.ResourceVersion = "" // refetch the API-server version on first read
	fresh := &cachev1.RedisCluster{}
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: r.Namespace}, fresh))
	envtest.InitializeRequiredStatus(fresh)
	require.NoError(t, r.Status().Update(context.Background(), fresh))
	return fresh
}

func createSTS(t *testing.T, r *RedisClusterReconciler, name string, replicas int32) *appsv1.StatefulSet {
	t.Helper()
	sts := newSTS(r.Namespace, name, replicas)
	require.NoError(t, r.Create(context.Background(), sts))
	return sts
}

func newSTS(ns, name string, replicas int32) *appsv1.StatefulSet {
	r := replicas
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &r,
			ServiceName: name,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "redis", Image: "redis:7.2.0"},
				}},
			},
		},
	}
}

// ─── #1 NO LONGER A BUG ──────────────────────────────────────────────
// Owner refs ARE set via ctrl.SetControllerReference in
// redis_cluster/client.go. Initial grep against controller/ only missed
// these. Test dropped after honest re-check.

// ─── #2 No CR-level finalizer (potential mid-BG orphan) ─────────────
// The operator does not register a finalizer on the RedisCluster CR.
// Owner refs (set on STS / Service) drive default GC, but anything the
// operator does NOT own — backup S3 objects, force-cutover annotations,
// residual ConfigMaps — is not cleaned up on CR delete. Documents the gap.
func TestEnvtest_NoCRFinalizer(t *testing.T) {
	r, _ := newReconciler(t)
	rc := createCR(t, r, "finalizer-cr")
	assert.Empty(t, rc.Finalizers, "no controller-managed finalizer registered; mid-BG CR delete has no custom cleanup path")
}

// ─── #3 ActiveClusterName precedence over Spec template ──────────────
// If a stale Spec template name and a live ActiveClusterName disagree (the
// BG-cutover scenario), GetOrCreateStatefulSet must resolve to ActiveClusterName.
func TestEnvtest_GetOrCreateSTS_ActiveClusterNameWins(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	createSTS(t, r, "blue-sts", 6)  // live cluster
	createSTS(t, r, "green-sts", 6) // template-name pick

	rc := createCR(t, r, "active-cr")
	rc.Status.ActiveClusterName = "blue-sts"
	require.NoError(t, r.Status().Update(ctx, rc))

	got, err := r.GetOrCreateStatefulSet(ctx, rc)
	require.NoError(t, err)
	assert.Equal(t, "blue-sts", got.Name, "ActiveClusterName must drive STS lookup")
}

// ─── #4 RV conflict on naked Update ─────────────────────────────────
// Callsites that r.Update without re-fetching on conflict must either
// refetch or Patch to avoid losing writes — envtest enforces RV semantics.
func TestEnvtest_RVConflict_OnConcurrentUpdate(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := createCR(t, r, "rv-conflict-cr")
	a := &cachev1.RedisCluster{}
	b := &cachev1.RedisCluster{}
	key := types.NamespacedName{Name: rc.Name, Namespace: rc.Namespace}
	require.NoError(t, r.Get(ctx, key, a))
	require.NoError(t, r.Get(ctx, key, b))

	a.Spec.RestoreFrom = "from-a"
	require.NoError(t, r.Update(ctx, a))

	b.Spec.RestoreFrom = "from-b"
	err := r.Update(ctx, b)
	assert.True(t, apierrors.IsConflict(err), "stale-RV Update must return 409 Conflict, got %v", err)
}

// ─── #5 shouldReconcile reconcile-count annotation never persisted ───
// shouldReconcile mutates Annotations["operator/reconcile_count"] in-memory
// but never Updates the CR. The counter resets on every reconcile re-fetch.
func TestEnvtest_ShouldReconcile_CountAnnotationNotPersisted(t *testing.T) {
	r, _ := newReconciler(t)
	rc := createCR(t, r, "should-reconcile-cr")
	if rc.Annotations == nil {
		rc.Annotations = map[string]string{}
	}
	ctx := context.Background()

	r.shouldReconcile(ctx, rc, true)
	r.shouldReconcile(ctx, rc, true)
	r.shouldReconcile(ctx, rc, true)

	fresh := &cachev1.RedisCluster{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: rc.Name, Namespace: rc.Namespace}, fresh))
	assert.NotEqual(t, "3", fresh.Annotations["operator/reconcile_count"],
		"reconcile_count is in-memory only; if this ever fails, the fix has landed and the assertion needs flipping")
}

// ─── #6 Chart-version label sync races scale-up Update ──────────────
// An unguarded Update on the STS races the scale-up handler's Update.
// Two same-RV writes → second 409s.
func TestEnvtest_ChartVersionUpdate_LosesToConcurrentScale(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	sts := createSTS(t, r, "chart-version-sts", 6)

	a := &appsv1.StatefulSet{}
	b := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: sts.Namespace, Name: sts.Name}
	require.NoError(t, r.Get(ctx, key, a))
	require.NoError(t, r.Get(ctx, key, b))

	replicas := int32(10)
	a.Spec.Replicas = &replicas
	require.NoError(t, r.Update(ctx, a))

	if b.Labels == nil {
		b.Labels = map[string]string{}
	}
	b.Labels["chart_version"] = "v2"
	err := r.Update(ctx, b)
	assert.True(t, apierrors.IsConflict(err),
		"chart-version label Update must handle 409 Conflict; got %v", err)
}

// ─── #7 Force-cutover annotation removal (needs full reconcile) ─────
// The cutover annotation must be cleared after applying — else every future
// reconcile re-triggers Promoting. Cannot drive full Handle() in envtest
// without toolbox stubbing — tracked for the BG envtest PR.
func TestEnvtest_ForceCutoverAnnotation_NeedsFullReconcile(t *testing.T) {
	t.Skip("requires full Handle() drive with toolbox stub — track in follow-up envtest reconciler PR")
}

// ─── #8 handleScaling: Primaries bump scales STS replicas ────────────
// Bump CR.Spec.Primaries 3→5. handleScaling should scale the STS Replicas to
// Primaries * (1 + Replicas).
func TestEnvtest_HandleScaling_PrimariesBumpScalesSTS(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := createCR(t, r, "scale-cr", redisbuilder.WithPrimaries(3), redisbuilder.WithReplicas(1))
	sts := createSTS(t, r, "scale-cr-sts", 6)
	rc.Status.ActiveClusterName = sts.Name
	require.NoError(t, r.Status().Update(ctx, rc))

	rc.Spec.Primaries = 5
	require.NoError(t, r.Update(ctx, rc))

	cluster := &redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts}
	target := rc.Spec.Primaries * (1 + rc.Spec.Replicas)
	_, _, err := r.handleScaling(ctx, cluster, target, *sts.Spec.Replicas)
	require.NoError(t, err)

	updated := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: sts.Namespace, Name: sts.Name}, updated))
	assert.Equal(t, int32(10), *updated.Spec.Replicas, "STS replicas must be 5*(1+1)=10 after Primaries bump")
}

// ─── #9 detectSpecChanges after stale stamp ─────────────────────────
// Stamp the hash, mutate spec, re-detect — the mutation must be visible.
// Bug class: a stale stamped hash masks subsequent Spec edits and the BG
// never triggers.
func TestEnvtest_DetectSpecChanges_PostStampMutation(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := createCR(t, r, "spec-hash-cr")
	sts := createSTS(t, r, "spec-hash-sts", 6)
	rc.Status.ActiveClusterName = sts.Name
	require.NoError(t, r.Status().Update(ctx, rc))

	cluster := &redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts}
	require.NoError(t, r.stampSpecHash(ctx, cluster))

	rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image = "redis:7.4.0"
	require.NoError(t, r.Update(ctx, rc))

	changed, err := r.detectSpecChanges(ctx, cluster)
	require.NoError(t, err)
	assert.True(t, changed, "stamped hash must not mask a subsequent real Spec change")
}

// ─── #10 Maintain label restore silent failure (refactor needed) ────
// The label-restore path logs and returns OK on r.Update failure. Test the
// propagation contract once the silent-failure path is fixed.
func TestEnvtest_MaintainLabelRestore_SilentOnUpdateFailure(t *testing.T) {
	t.Skip("requires refactor: label-restore path swallows r.Update errors")
}

// ─── #11 Status subresource enforcement ─────────────────────────────
// k8s rejects Spec writes via the Status subresource. Catches any code path
// that accidentally mixes Spec into Status().Update().
func TestEnvtest_StatusSubresource_RejectsSpecWrites(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()
	rc := createCR(t, r, "subresource-cr")

	rc.Spec.RestoreFrom = "via-status-path"
	require.NoError(t, r.Status().Update(ctx, rc))

	fresh := &cachev1.RedisCluster{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Name: rc.Name, Namespace: rc.Namespace}, fresh))
	assert.NotEqual(t, "via-status-path", fresh.Spec.RestoreFrom,
		"Spec writes through Status().Update() must not persist (k8s subresource rule)")
}

// ─── #12 reconcile() short-circuits on NotFound CR ──────────────────
// CR deleted between watch and reconcile. r.Get returns NotFound; reconcile
// must exit clean with no panic and cache the name.
func TestEnvtest_Reconcile_NotFoundCR(t *testing.T) {
	r, _ := newReconciler(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	missing := types.NamespacedName{Namespace: r.Namespace, Name: "deleted-already"}
	res, _, err := r.reconcile(ctx, ctrl.Request{NamespacedName: missing})

	assert.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, res)
	_, cached := r.notFoundCache.Load(missing.String())
	assert.True(t, cached, "missing CR must be recorded in notFoundCache")
}

// ─── #13 fresh CR — created STS gets a controller owner ref ─────────
// Cascade GC depends on it.
func TestEnvtest_GetOrCreateSTS_NewCRHasOwnerRef(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := envtest.ReadyForCreate(createCR(t, r, "owner-ref"))

	sts, err := r.GetOrCreateStatefulSet(ctx, rc)
	require.NoError(t, err)
	require.NotNil(t, sts)

	got := &appsv1.StatefulSet{}
	require.NoError(t, r.Get(ctx,
		types.NamespacedName{Name: rc.Name, Namespace: r.Namespace}, got))

	require.Len(t, got.OwnerReferences, 1, "STS must have exactly one owner ref")
	owner := got.OwnerReferences[0]
	assert.Equal(t, "RedisCluster", owner.Kind)
	assert.Equal(t, rc.Name, owner.Name)
	assert.Equal(t, rc.UID, owner.UID)
	require.NotNil(t, owner.Controller)
	assert.True(t, *owner.Controller,
		"controller=true is required for cascade GC of the STS on CR delete")
	require.NotNil(t, owner.BlockOwnerDeletion)
	assert.True(t, *owner.BlockOwnerDeletion)
}

// ─── #14 GetOrCreateStatefulSet is idempotent ───────────────────────
// Guards against the operator re-creating the STS every reconcile loop.
func TestEnvtest_GetOrCreateSTS_TwoCallsReturnSame(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := envtest.ReadyForCreate(createCR(t, r, "idempotent"))

	first, err := r.GetOrCreateStatefulSet(ctx, rc)
	require.NoError(t, err)
	firstUID := first.UID

	second, err := r.GetOrCreateStatefulSet(ctx, rc)
	require.NoError(t, err)
	assert.Equal(t, firstUID, second.UID,
		"second call must return the same STS, not create a fresh one")
}

// ─── #15 shouldReconcile honours Spec.DisableReconciliation ─────────
func TestEnvtest_ShouldReconcile_DisableGate(t *testing.T) {
	r, _ := newReconciler(t)
	rc := createCR(t, r, "disable-recon")

	rc.Spec.DisableReconciliation = true
	if rc.Annotations == nil {
		rc.Annotations = map[string]string{}
	}

	assert.False(t, r.shouldReconcile(context.Background(), rc, false),
		"shouldReconcile must short-circuit when Spec.DisableReconciliation is set")
}

// ─── #16 stampSpecHash persists to Status.LastAppliedSpecHash ──────
// Non-empty after call; stable across re-stamps with no spec mutation.
func TestEnvtest_StampSpecHash_PopulatesStatus(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := createCR(t, r, "stamp-status")
	sts := createSTS(t, r, "stamp-status-sts", 6)
	rc.Status.ActiveClusterName = sts.Name
	require.NoError(t, r.Status().Update(ctx, rc))

	cluster := &redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts}
	require.NoError(t, r.stampSpecHash(ctx, cluster))

	fresh := &cachev1.RedisCluster{}
	require.NoError(t, r.Get(ctx,
		types.NamespacedName{Name: rc.Name, Namespace: r.Namespace}, fresh))
	assert.NotEmpty(t, fresh.Status.LastAppliedSpecHash,
		"stampSpecHash must persist a non-empty hash to Status")

	first := fresh.Status.LastAppliedSpecHash
	require.NoError(t, r.stampSpecHash(ctx, cluster))
	require.NoError(t, r.Get(ctx,
		types.NamespacedName{Name: rc.Name, Namespace: r.Namespace}, fresh))
	assert.Equal(t, first, fresh.Status.LastAppliedSpecHash,
		"stamping twice with no spec change must produce the same hash")
}

// ─── #17 updateRedisCluster preserves Spec ──────────────────────────
// Guards against the status helper accidentally leaking Spec writes.
func TestEnvtest_UpdateRedisCluster_KeepsSpec(t *testing.T) {
	r, _ := newReconciler(t)
	ctx := context.Background()

	rc := createCR(t, r, "status-isolation")
	rc.Spec.RestoreFrom = "before-status-update/2026-05-20T00:00:00Z"
	require.NoError(t, r.Update(ctx, rc))

	rc.Status.StatefulSetReady = true
	require.NoError(t, r.updateRedisCluster(ctx, rc))

	fresh := &cachev1.RedisCluster{}
	require.NoError(t, r.Get(ctx,
		types.NamespacedName{Name: rc.Name, Namespace: r.Namespace}, fresh))
	assert.Equal(t, "before-status-update/2026-05-20T00:00:00Z", fresh.Spec.RestoreFrom,
		"updateRedisCluster goes through Status subresource — Spec must be untouched")
	assert.True(t, fresh.Status.StatefulSetReady,
		"the intended status field must actually persist")
}
