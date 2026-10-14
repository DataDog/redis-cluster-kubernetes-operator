// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package cli

import (
	"context"
	"fmt"
	"strings"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"k8s.io/apimachinery/pkg/types"
)

// Run is the actual function that we want to execute
func Run(ctx context.Context, cc redis_cluster.ClusterClient, namespace string, name string) error {
	redisCluster := &cachev1.RedisCluster{}
	err := cc.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, redisCluster)
	if err != nil {
		cc.Log.Error(err, "Failed to get the Redis cluster")
		return err
	}
	cc.Log.Info("Got the Redis cluster")
	println(redisCluster)

	statefulSet, err := cc.GetOrCreateStatefulSet(ctx, redisCluster)
	if err != nil {
		cc.Log.Error(err, "Failed to get the StatefulSet")
		return err
	}
	cc.Log.Info("Got the StatefulSet")
	println(statefulSet)

	cluster, err := cc.GetCluster(ctx, redisCluster, statefulSet)
	if err != nil {
		cc.Log.Error(err, "Failed to get the cluster")
		return err
	}

	cc.Log.Info("Got the cluster", "nodeCount", len(cluster.AllNodes))

	var builder strings.Builder
	builder.WriteString("Cluster\n")
	builder.WriteString(fmt.Sprintf("Redis %s\n", cluster.RedisVersion))
	builder.WriteString("Nodes\n")
	for _, node := range cluster.AllNodes {
		builder.WriteString(fmt.Sprintf("%d: %s %s v%s %s", node.PodIndex, node.PodName, node.ID, node.RedisVersion, node.Role))
		if node.RoleIs(redis_cluster.ReplicaNode) {
			builder.WriteString(fmt.Sprintf(" of %s", node.PrimaryID))
		}
		if node.RoleIs(redis_cluster.OtherNode) {
			builder.WriteString(fmt.Sprintf(" ID:%s IP:%s", node.ID, node.IP))
		}
		builder.WriteString("\n")
		builder.WriteString(fmt.Sprintf("  %s\n", node.InstanceName))
	}
	cc.Log.Info(builder.String())

	return nil
}
