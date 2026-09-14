package controller

import (
	"context"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/bluegreen"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	ctrl "sigs.k8s.io/controller-runtime"
)

var _ bluegreen.Deps = (*RedisClusterReconciler)(nil)

// ────────────────────────────────────────────────────────────────────────────
// bluegreen.Deps implementation
//
// These exported methods satisfy the bluegreen.Deps interface so that
// RedisClusterReconciler can be passed directly to bluegreen.NewHandler.
// ────────────────────────────────────────────────────────────────────────────

func (r *RedisClusterReconciler) UpdateStatus(ctx context.Context, rc *cachev1.RedisCluster) error {
	return r.updateRedisCluster(ctx, rc)
}

func (r *RedisClusterReconciler) UpdateSpec(ctx context.Context, rc *cachev1.RedisCluster) error {
	return r.updateRedisClusterSpec(ctx, rc)
}

func (r *RedisClusterReconciler) UpdateStatusWithSpecHash(ctx context.Context, cluster *redis_cluster.Cluster) error {
	cluster.RedisCluster.Status.LastAppliedSpecHash = computeSpecHash(cluster)
	cluster.RedisCluster.Status.LastAppliedRolloutSpecHash = computeRolloutSpecHash(cluster)
	return r.updateRedisCluster(ctx, cluster.RedisCluster)
}

func (r *RedisClusterReconciler) RunBackup(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {
	return r.backup(ctx, cluster)
}

func (r *RedisClusterReconciler) FindHealthyRefNode(cluster *redis_cluster.Cluster, exclude *redis_cluster.ClusterNode) *redis_cluster.ClusterNode {
	return getHealthyRefNode(cluster, exclude)
}

func (r *RedisClusterReconciler) ForgetNode(ctx context.Context, cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode) {
	forgetNode(ctx, cluster, node, r.Log, r.Metrics)
}

// ────────────────────────────────────────────────────────────────────────────
// Delegation methods — called from reconcile.go
// ────────────────────────────────────────────────────────────────────────────

func (r *RedisClusterReconciler) newBGHandler() *bluegreen.Handler {
	return bluegreen.NewHandler(r, r.Log, r.Metrics)
}

func (r *RedisClusterReconciler) handleBlueGreenDeployment(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	return r.newBGHandler().Handle(ctx, cluster)
}

func (r *RedisClusterReconciler) startBlueGreenDeployment(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	return r.newBGHandler().Start(ctx, cluster)
}
