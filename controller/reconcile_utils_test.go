package controller

import (
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestComputeSpecHashTracksTopology(t *testing.T) {
	cluster := testHashCluster()
	baseline := computeSpecHash(cluster)

	cluster.RedisCluster.Spec.Primaries = 4
	cluster.RedisCluster.Spec.Replicas = 1
	*cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.Replicas = 8

	// Primaries and replicas are part of the compatibility hash. Excluding them
	// would make an existing status hash trigger a spurious blue-green rollout.
	require.NotEqual(t, baseline, computeSpecHash(cluster))
}

func TestComputeSpecHashIgnoresCRMetadata(t *testing.T) {
	cluster := testHashCluster()
	baseline := computeSpecHash(cluster)

	cluster.RedisCluster.Labels = map[string]string{
		"chart_version": "metadata-only",
	}
	cluster.RedisCluster.Annotations = map[string]string{
		"operator/reconcile_count":             "1",
		"e2e.example.com/metadata-only":        "true",
		"kubectl.kubernetes.io/restartedAt":    "2026-05-25T00:00:00Z",
		"deployment.kubernetes.io/revision":    "2",
		"operator.example.com/ignore-change":   "true",
	}

	require.Equal(t, baseline, computeSpecHash(cluster))
}

func TestComputeSpecHashIgnoresStatefulSetObjectMetadata(t *testing.T) {
	cluster := testHashCluster()
	baseline := computeSpecHash(cluster)

	cluster.RedisCluster.Spec.StatefulsetTemplate.Labels = map[string]string{
		"chart_version": "metadata-only",
	}
	cluster.RedisCluster.Spec.StatefulsetTemplate.Annotations = map[string]string{
		"e2e.example.com/metadata-only": "true",
	}

	require.Equal(t, baseline, computeSpecHash(cluster))
}

func TestComputeSpecHashTracksPodTemplateMetadata(t *testing.T) {
	cluster := testHashCluster()
	baseline := computeSpecHash(cluster)

	cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.Template.Labels["e2e-bg-version"] = "v2"

	require.NotEqual(t, baseline, computeSpecHash(cluster))
}

func TestComputeSpecHashTracksRedisImage(t *testing.T) {
	cluster := testHashCluster()
	baseline := computeSpecHash(cluster)

	cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image = "redis:8.4.0"

	require.NotEqual(t, baseline, computeSpecHash(cluster))
}

func testHashCluster() *redis_cluster.Cluster {
	replicas := int32(9)
	return &redis_cluster.Cluster{
		RedisCluster: &cachev1.RedisCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-cluster-test",
				Namespace: "default",
			},
			Spec: cachev1.RedisClusterSpec{
				Primaries: 3,
				Replicas:  2,
				StatefulsetTemplate: appsv1.StatefulSet{
					Spec: appsv1.StatefulSetSpec{
						Replicas: &replicas,
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{"app": "redis"},
							},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{
									Name:  "redis",
									Image: "redis:7.2.0",
								}},
							},
						},
					},
				},
			},
		},
	}
}
