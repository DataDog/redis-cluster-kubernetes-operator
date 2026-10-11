package redis_cluster

import (
	"context"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
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

func TestGetFilterLabels(t *testing.T) {
	ast := assert.New(t)
	filters, err := getPodListLabels(map[string]string{
		"app":                "rco",
		"chart_name":         "redis-cluster-sample",
		"redis_cluster_name": "redis-cluster-sample-test",
	})
	ast.Equal(filters["app"], "rco")
	ast.Equal(filters["chart_name"], "redis-cluster-sample")
	ast.Equal(filters["redis_cluster_name"], "redis-cluster-sample-test")
	ast.Nil(err)

	filters2, err2 := getPodListLabels(map[string]string{
		"app":                "rco",
		"chart_name":         "redis-cluster-sample",
		"redis_cluster_name": "redis-cluster-sample-test",
		"foo":                "bar",
		"extra":              "key/values",
		"will_be":            "filtered_out",
	})
	ast.Equal(filters2["app"], "rco")
	ast.Equal(filters2["chart_name"], "redis-cluster-sample")
	ast.Equal(filters2["redis_cluster_name"], "redis-cluster-sample-test")
	ast.Nil(err2)

	filters3, err3 := getPodListLabels(map[string]string{
		"app":                "rco",
		"redis_cluster_name": "redis-cluster-sample-test",
	})
	ast.Equal("rco", filters3["app"])
	ast.Equal("redis-cluster-sample-test", filters3["redis_cluster_name"])
	ast.NotContains(filters3, "chart_name")
	ast.Nil(err3)

	_, err4 := getPodListLabels(map[string]string{
		"apple":               "rco",
		"charter_name":        "redis-cluster-sample",
		"redis_cluster_names": "redis-cluster-sample-test",
		"foo":                 "bar",
		"extra":               "key/values",
		"will_be":             "filtered_out",
	})
	ast.NotNil(err4)
}

func TestGetRedisClusterPodsUsesPersistedSelectorWhenTemplateMetadataIsPruned(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	redisCluster := &cachev1.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-cluster-test", Namespace: "default"},
		Spec: cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{
			Spec: appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app":                "redis",
				"redis_cluster_name": "redis-cluster-test",
				"release":            "redis-cluster",
			}}},
		}},
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      "redis-cluster-test-0",
		Namespace: "default",
		Labels: map[string]string{
			"app":                "redis",
			"chart_name":         "redis-cluster",
			"redis_cluster_name": "redis-cluster-test",
			"release":            "redis-cluster",
		},
	}}
	client := &ClusterClient{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build(),
		Log:    logr.Discard(),
	}

	pods, err := client.GetRedisClusterPods(context.Background(), redisCluster)
	require.NoError(t, err)
	require.Len(t, pods.Items, 1)
	assert.Equal(t, pod.Name, pods.Items[0].Name)
}

func TestGetPodIndexFromName(t *testing.T) {
	ast := assert.New(t)
	idx1, err1 := parseIndex("redis-cluster-sample-all-green-0")
	ast.Equal(idx1, 0)
	ast.Nil(err1)

	idx2, err2 := parseIndex("redis-cluster-sample-all-green-1231231")
	ast.Equal(idx2, 1231231)
	ast.Nil(err2)

	_, err3 := parseIndex("foobar")
	ast.NotNil(err3)
}

func TestGetStatefulSetForRedisClusterUsesEffectiveTargetWithoutMutatingTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	templateReplicas := int32(6)
	redisCluster := &cachev1.RedisCluster{
		TypeMeta: metav1.TypeMeta{APIVersion: cachev1.GroupVersion.String(), Kind: "RedisCluster"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-cluster-test",
			Namespace: "default",
			UID:       types.UID("redis-cluster-uid"),
			Labels:    map[string]string{"chart_version": "v1"},
		},
		Spec: cachev1.RedisClusterSpec{
			Primaries: 3,
			Replicas:  1,
			StatefulsetTemplate: appsv1.StatefulSet{
				Spec: appsv1.StatefulSetSpec{
					Replicas: &templateReplicas,
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
					Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
						Name:         "redis",
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}}}},
					VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{}},
				},
			},
		},
		Status: cachev1.RedisClusterStatus{
			Autoscaling: cachev1.RedisClusterAutoscalingStatus{EffectivePrimaries: 5},
		},
	}

	client := &ClusterClient{ClusterScheme: scheme}
	statefulSet, err := client.getStatefulSetForRedisCluster(redisCluster)
	require.NoError(t, err)
	require.NotNil(t, statefulSet.Spec.Replicas)
	assert.Equal(t, redisCluster.Name, statefulSet.Name)
	assert.Equal(t, redisCluster.Namespace, statefulSet.Namespace)
	assert.Equal(t, int32(10), *statefulSet.Spec.Replicas)
	require.NotNil(t, redisCluster.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, int32(6), *redisCluster.Spec.StatefulsetTemplate.Spec.Replicas,
		"the Helm-owned StatefulSet template must remain unchanged")
	assert.Equal(t, "v1", statefulSet.Labels["chart_version"])
	assert.Equal(t, "redis", statefulSet.Spec.Template.Labels["app"])
	require.Len(t, statefulSet.Spec.VolumeClaimTemplates, 1)
	assert.Equal(t, "data", statefulSet.Spec.VolumeClaimTemplates[0].Name)
	assert.Nil(t, redisCluster.Spec.StatefulsetTemplate.Labels,
		"restoring pruned labels must not mutate the CR template")
	assert.Empty(t, redisCluster.Spec.StatefulsetTemplate.Spec.VolumeClaimTemplates[0].Name,
		"restoring a pruned claim name must not mutate the CR template")
	require.Len(t, statefulSet.OwnerReferences, 1)
	assert.Equal(t, redisCluster.UID, statefulSet.OwnerReferences[0].UID)
}

func TestGetStatefulSetForRedisClusterRejectsAmbiguousPrunedVolumeClaimName(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, cachev1.AddToScheme(scheme))

	redisCluster := &cachev1.RedisCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: "default"},
		Spec: cachev1.RedisClusterSpec{
			Primaries: 1,
			StatefulsetTemplate: appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: "redis",
					VolumeMounts: []corev1.VolumeMount{
						{Name: "data", MountPath: "/data"},
						{Name: "config", MountPath: "/config"},
					},
				}}}},
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{}},
			}},
		},
	}

	client := &ClusterClient{ClusterScheme: scheme}
	_, err := client.getStatefulSetForRedisCluster(redisCluster)
	require.EqualError(t, err, "cannot unambiguously restore pruned volume claim template name: found 2 candidate pod volumes")
}
