// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Pure-function unit tests for reconcile_utils — bound the contracts.

func TestShouldReconcileInitializesAnnotations(t *testing.T) {
	rc := &cachev1.RedisCluster{}
	r := &RedisClusterReconciler{}

	assert.True(t, r.shouldReconcile(context.Background(), rc, true))
	assert.Equal(t, "1", rc.Annotations["operator/reconcile_count"])
}

func TestResetReconcileCountInitializesAnnotations(t *testing.T) {
	rc := &cachev1.RedisCluster{}

	resetReconcileCount(rc)

	assert.Equal(t, "0", rc.Annotations["operator/reconcile_count"])
}

// ── getReconcileTimeout ──

func TestGetReconcileTimeout_RespectsExplicitOverride(t *testing.T) {
	got := getReconcileTimeout(1000, 42)
	assert.Equal(t, 42*time.Second, got, "override must win regardless of podCount")
}

func TestGetReconcileTimeout_ZeroOverrideFallsBackToScaling(t *testing.T) {
	got := getReconcileTimeout(0, 0)
	assert.Equal(t, constants.ReconcileTimeoutBase, got, "podCount=0 yields the base timeout")
}

func TestGetReconcileTimeout_ScalesByPodCount(t *testing.T) {
	got := getReconcileTimeout(10, 0)
	want := constants.ReconcileTimeoutBase + 10*constants.ReconcileTimeoutPerPod
	assert.Equal(t, want, got)
}

func TestGetReconcileTimeout_CappedAtMax(t *testing.T) {
	got := getReconcileTimeout(10_000, 0)
	assert.Equal(t, constants.ReconcileTimeoutMax, got)
}

func TestRestorePrunedStatefulSetMetadata(t *testing.T) {
	rc := &cachev1.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-test", Namespace: "default"},
		Spec: cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "redis"},
			}},
		}},
	}

	restorePrunedStatefulSetMetadata(rc)

	assert.Equal(t, rc.Name, rc.Spec.StatefulsetTemplate.Name)
	assert.Equal(t, rc.Namespace, rc.Spec.StatefulsetTemplate.Namespace)
	assert.Equal(t, "redis", rc.Spec.StatefulsetTemplate.Spec.Template.Labels["app"])
}

// ── computeSpecHash ──

func newCluster(primaries, replicas int32, image string) *redis_cluster.Cluster {
	rc := &cachev1.RedisCluster{
		Spec: cachev1.RedisClusterSpec{
			Primaries: primaries,
			Replicas:  replicas,
			StatefulsetTemplate: appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "redis", Image: image},
							},
						},
					},
				},
			},
		},
	}
	return &redis_cluster.Cluster{RedisCluster: rc}
}

func TestComputeSpecHash_StableForIdenticalSpec(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(3, 1, "redis:7.2.0")
	assert.Equal(t, computeSpecHash(a), computeSpecHash(b))
}

func TestComputeSpecHash_TracksPrimariesChange(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(5, 1, "redis:7.2.0")
	assert.NotEqual(t, computeSpecHash(a), computeSpecHash(b),
		"primaries are part of the spec hash (matches prod); a change must invalidate it")
}

func TestComputeSpecHash_DiffersOnImageChange(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(3, 1, "redis:7.4.0")
	assert.NotEqual(t, computeSpecHash(a), computeSpecHash(b),
		"image change must invalidate the hash so BG triggers")
}

func TestComputeRolloutSpecHash_IgnoresTopology(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(5, 2, "redis:7.2.0")
	assert.Equal(t, computeRolloutSpecHash(a), computeRolloutSpecHash(b))
}

func TestComputeRolloutSpecHash_IgnoresLiveBackupControls(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(3, 1, "redis:7.2.0")
	b.RedisCluster.Spec.BackupEnabled = true
	b.RedisCluster.Spec.BackupFrequency = 60
	b.RedisCluster.Spec.BackupRetention = 5

	assert.Equal(t, computeRolloutSpecHash(a), computeRolloutSpecHash(b),
		"backup controls are consumed directly from the CR and do not require pod replacement")
	assert.NotEqual(t, computeSpecHash(a), computeSpecHash(b),
		"the compatibility checkpoint must still track backup controls")
}

func TestComputeRolloutSpecHash_DiffersOnImageChange(t *testing.T) {
	a := newCluster(3, 1, "redis:7.2.0")
	b := newCluster(3, 1, "redis:7.4.0")
	assert.NotEqual(t, computeRolloutSpecHash(a), computeRolloutSpecHash(b))
}

func TestComputeSpecHash_InsensitiveToStatusMutation(t *testing.T) {
	c := newCluster(3, 1, "redis:7.2.0")
	before := computeSpecHash(c)

	c.RedisCluster.Status.LastBackupPath = "ignored-bucket/202610010000"
	c.RedisCluster.Status.StatefulSetReady = true
	c.RedisCluster.Status.ActiveClusterName = "blue-sts"

	assert.Equal(t, before, computeSpecHash(c),
		"status mutations must not change the hash — would trigger BG on every status write")
}

func TestStampSpecHash_RetriesConflict(t *testing.T) {
	scheme := newTestScheme(t)

	rc := newCluster(3, 1, "redis:7.2.0").RedisCluster
	rc.Name = "redis-cluster-test"
	rc.Namespace = "default"
	rc.Status.ActiveClusterName = "redis-cluster-test"
	serverRC := rc.DeepCopy()
	serverRC.Status.StatefulSetReady = true

	var updateCalls int
	statusErrors := []error{
		apierrors.NewConflict(
			cachev1.GroupVersion.WithResource("redisclusters").GroupResource(),
			rc.Name,
			errors.New("injected conflict"),
		),
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(serverRC).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		WithInterceptorFuncs(interceptStatusUpdates(statusErrors, &updateCalls)).
		Build()
	r := newTestReconciler(t, k8sClient)

	require.NoError(t, r.stampSpecHash(
		context.Background(),
		&redis_cluster.Cluster{RedisCluster: rc},
	))

	fresh := &cachev1.RedisCluster{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(rc), fresh))
	assert.Equal(t, computeSpecHash(&redis_cluster.Cluster{RedisCluster: rc}), fresh.Status.LastAppliedSpecHash)
	assert.Equal(t, computeRolloutSpecHash(&redis_cluster.Cluster{RedisCluster: rc}), fresh.Status.LastAppliedRolloutSpecHash)
	assert.True(t, fresh.Status.StatefulSetReady, "retry must preserve unrelated server-side status")
	assert.Equal(t, 2, updateCalls, "a conflict must trigger a fresh status update")
}

func TestDetectSpecChanges_MigratesRolloutHashFromActiveStatefulSet(t *testing.T) {
	for _, tc := range []struct {
		name         string
		desiredImage string
		configure    func(*cachev1.RedisCluster)
		liveDefaults bool
		wantChanged  bool
	}{
		{name: "unchanged", desiredImage: "redis:7.2.0", wantChanged: false},
		{name: "unchanged with API defaults", desiredImage: "redis:7.2.0", liveDefaults: true, wantChanged: false},
		{name: "pending image change", desiredImage: "redis:7.4.0", wantChanged: true},
		{
			name:         "pending live backup control change",
			desiredImage: "redis:7.2.0",
			configure: func(rc *cachev1.RedisCluster) {
				rc.Spec.BackupEnabled = true
				rc.Spec.BackupFrequency = 60
				rc.Spec.BackupRetention = 5
			},
			wantChanged: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			applied := newCluster(3, 1, "redis:7.2.0")
			rc := applied.RedisCluster.DeepCopy()
			rc.Name = "redis-cluster-test"
			rc.Namespace = "default"
			rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image = tc.desiredImage
			if tc.configure != nil {
				tc.configure(rc)
			}
			rc.Status.LastAppliedSpecHash = computeSpecHash(applied)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
				Spec: appsv1.StatefulSetSpec{
					Template: *applied.RedisCluster.Spec.StatefulsetTemplate.Spec.Template.DeepCopy(),
				},
			}
			if tc.liveDefaults {
				gracePeriod := int64(30)
				sts.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
				sts.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
				sts.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
				sts.Spec.Template.Spec.TerminationGracePeriodSeconds = &gracePeriod
				sts.Spec.Template.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
				sts.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(rc, sts).
				WithStatusSubresource(&cachev1.RedisCluster{}).
				Build()
			r := newTestReconciler(t, k8sClient)
			cluster := &redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts}

			changed, err := r.detectSpecChanges(context.Background(), cluster)

			require.NoError(t, err)
			assert.Equal(t, tc.wantChanged, changed)
			assert.Equal(t, computeRolloutSpecHash(applied), rc.Status.LastAppliedRolloutSpecHash,
				"migration must record the active StatefulSet, not desired state")
		})
	}
}

func TestHandleScaling_DoesNotAcknowledgePendingRolloutChange(t *testing.T) {
	for _, tc := range []struct {
		name         string
		desiredImage string
		wantChanged  bool
	}{
		{name: "topology only", desiredImage: "redis:7.2.0", wantChanged: false},
		{name: "topology and image", desiredImage: "redis:7.4.0", wantChanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := newTestScheme(t)
			applied := newCluster(1, 0, "redis:7.2.0")
			rc := applied.RedisCluster.DeepCopy()
			rc.Name = "redis-cluster-test"
			rc.Namespace = "default"
			rc.Status.LastAppliedSpecHash = computeSpecHash(applied)
			rc.Status.LastAppliedRolloutSpecHash = computeRolloutSpecHash(applied)
			rc.Spec.Primaries = 2
			rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image = tc.desiredImage
			replicas := int32(1)
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
				Spec: appsv1.StatefulSetSpec{
					Replicas: &replicas,
					Template: *applied.RedisCluster.Spec.StatefulsetTemplate.Spec.Template.DeepCopy(),
				},
			}

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(rc, sts).
				WithStatusSubresource(&cachev1.RedisCluster{}).
				Build()
			r := newTestReconciler(t, k8sClient)
			cluster := &redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts}

			_, _, err := r.handleScaling(context.Background(), cluster, 2, 1)
			require.NoError(t, err)
			changed, err := r.detectSpecChanges(context.Background(), cluster)
			require.NoError(t, err)
			assert.Equal(t, tc.wantChanged, changed)
			assert.Equal(t, computeRolloutSpecHash(applied), rc.Status.LastAppliedRolloutSpecHash)

			storedSTS := &appsv1.StatefulSet{}
			require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(sts), storedSTS))
			require.NotNil(t, storedSTS.Spec.Replicas)
			assert.Equal(t, int32(2), *storedSTS.Spec.Replicas)
		})
	}
}

func TestDetectSpecChanges_PropagatesInitialHashUpdateError(t *testing.T) {
	scheme := newTestScheme(t)
	rc := newCluster(3, 1, "redis:7.2.0").RedisCluster
	rc.Name = "redis-cluster-test"
	rc.Namespace = "default"
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      rc.Name,
		Namespace: rc.Namespace,
	}}

	var updateCalls int
	statusErr := errors.New("status update failed")
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rc, sts).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		WithInterceptorFuncs(interceptStatusUpdates([]error{statusErr}, &updateCalls)).
		Build()
	r := newTestReconciler(t, k8sClient)

	changed, err := r.detectSpecChanges(context.Background(), &redis_cluster.Cluster{
		RedisCluster: rc,
		StatefulSet:  sts,
	})

	assert.False(t, changed)
	require.ErrorIs(t, err, statusErr)
	assert.Equal(t, 1, updateCalls)
}

func TestHandleScaling_PropagatesSpecHashUpdateError(t *testing.T) {
	scheme := newTestScheme(t)

	rc := newCluster(2, 0, "redis:7.2.0").RedisCluster
	rc.Name = "redis-cluster-test"
	rc.Namespace = "default"
	replicas := int32(1)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-test", Namespace: "default"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{},
			}},
		},
	}

	var updateCalls int
	statusErr := errors.New("status update failed")
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(rc, sts).
		WithStatusSubresource(&cachev1.RedisCluster{}).
		WithInterceptorFuncs(interceptStatusUpdates([]error{statusErr}, &updateCalls)).
		Build()
	r := newTestReconciler(t, k8sClient)

	_, gotHealth, err := r.handleScaling(
		context.Background(),
		&redis_cluster.Cluster{RedisCluster: rc, StatefulSet: sts},
		2,
		1,
	)

	require.ErrorIs(t, err, statusErr)
	assert.Equal(t, health.Error, gotHealth)

	storedSTS := &appsv1.StatefulSet{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(sts), storedSTS))
	require.NotNil(t, storedSTS.Spec.Replicas)
	assert.Equal(t, int32(1), *storedSTS.Spec.Replicas, "hash failure must leave topology unchanged")
}

func interceptStatusUpdates(errs []error, calls *int) interceptor.Funcs {
	return interceptor.Funcs{
		SubResourceUpdate: func(
			ctx context.Context,
			k8sClient client.Client,
			subResourceName string,
			obj client.Object,
			opts ...client.SubResourceUpdateOption,
		) error {
			*calls++
			if len(errs) > 0 {
				err := errs[0]
				errs = errs[1:]
				if err != nil {
					return err
				}
			}
			return k8sClient.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	}
}

// ── truncateChecksum ──

func TestTruncateChecksum_EmptyReturnsSentinel(t *testing.T) {
	assert.Equal(t, "empty", truncateChecksum(""))
}

func TestTruncateChecksum_PrefixForLongInput(t *testing.T) {
	long := "abcdef1234567890ffffffff"
	assert.Equal(t, "abcdef12", truncateChecksum(long))
	assert.Len(t, truncateChecksum(long), 8)
}

func TestTruncateChecksum_ShortInputReturnedAsIs(t *testing.T) {
	assert.Equal(t, "abc", truncateChecksum("abc"))
}

func TestTruncateChecksum_BoundaryAtEightChars(t *testing.T) {
	eight := "abcdef12"
	assert.Equal(t, eight, truncateChecksum(eight), "exactly 8 chars returned whole")
}
