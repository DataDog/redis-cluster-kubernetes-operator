// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMapPodToOwningRedisCluster(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	redisCluster := &cachev1.RedisCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache",
			Namespace: "default",
			UID:       types.UID("cluster-uid"),
		},
	}
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-a",
			Namespace:       "default",
			UID:             types.UID("statefulset-uid"),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(redisCluster, cachev1.GroupVersion.WithKind("RedisCluster"))},
		},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:            "cache-a-0",
		Namespace:       "default",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(statefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
	}}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(statefulSet).Build()
	reconciler := &RedisClustersReconciler{ClusterClient: redis_cluster.ClusterClient{
		Client:        k8sClient,
		ClusterScheme: scheme,
		Log:           logr.Discard(),
		Metrics:       metrics.CreateNoop(),
	}}

	requests := reconciler.mapPodToRedisCluster(context.Background(), pod)
	require.Len(t, requests, 1)
	assert.Equal(t, types.NamespacedName{Namespace: "default", Name: "cache"}, requests[0].NamespacedName)
}

func TestMapPodToRedisClusterRejectsStaleStatefulSetOwner(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name:      "cache-a",
		Namespace: "default",
		UID:       types.UID("new-statefulset-uid"),
	}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "cache-a-0",
		Namespace: "default",
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(
			&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "cache-a", UID: types.UID("old-statefulset-uid")}},
			appsv1.SchemeGroupVersion.WithKind("StatefulSet"),
		)},
	}}

	reconciler := &RedisClustersReconciler{ClusterClient: redis_cluster.ClusterClient{
		Client:  fake.NewClientBuilder().WithScheme(scheme).WithObjects(statefulSet).Build(),
		Log:     logr.Discard(),
		Metrics: metrics.CreateNoop(),
	}}
	assert.Empty(t, reconciler.mapPodToRedisCluster(context.Background(), pod))
}
