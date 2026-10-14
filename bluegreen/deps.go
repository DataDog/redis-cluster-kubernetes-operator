// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"context"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Deps abstracts the reconciler operations that blue-green deployment needs.
// RedisClusterReconciler satisfies this interface via bluegreen_bridge.go.
type Deps interface {
	client.Reader
	client.Writer

	// Status persistence
	UpdateStatus(ctx context.Context, rc *cachev1.RedisCluster) error
	UpdateStatusWithSpecHash(ctx context.Context, cluster *redis_cluster.Cluster) error
	UpdateSpec(ctx context.Context, rc *cachev1.RedisCluster) error

	// StatefulSet / Pod queries
	GetStatefulSetByName(ctx context.Context, ns, name string) (*appsv1.StatefulSet, error)
	GetGreenStatefulSet(ctx context.Context, rc *cachev1.RedisCluster, name string) (*appsv1.StatefulSet, error)
	GetGreenPods(ctx context.Context, rc *cachev1.RedisCluster) ([]corev1.Pod, error)
	GetBluePods(ctx context.Context, rc *cachev1.RedisCluster) (*corev1.PodList, error)
	GetRedisClusterPods(ctx context.Context, rc *cachev1.RedisCluster) (*corev1.PodList, error)

	// Resource mutations
	CreateGreenStatefulSet(ctx context.Context, rc *cachev1.RedisCluster) (*appsv1.StatefulSet, string, error)
	CreateGreenHeadlessService(ctx context.Context, rc *cachev1.RedisCluster, name string, port int) error
	UpdateServiceSelector(ctx context.Context, ns, name string, sel map[string]string) error
	DeleteStatefulSet(ctx context.Context, ns, name string) error
	DeleteGreenResources(ctx context.Context, rc *cachev1.RedisCluster, name string) error

	// Backup
	RunSafetyBackup(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error)

	// Cluster helpers forwarded from controller package
	FindHealthyRefNode(cluster *redis_cluster.Cluster, exclude *redis_cluster.ClusterNode) *redis_cluster.ClusterNode
	ForgetNode(ctx context.Context, cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode)
}
