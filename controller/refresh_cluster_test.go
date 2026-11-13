// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRefreshClusterAbortsWhenChartVersionChanged(t *testing.T) {
	ctx := context.Background()
	scheme := newAutoscalingScheme(t)

	stored := validAutoscalingCluster()
	stored.Labels = map[string]string{"chart_version": "v2"}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(stored).Build()
	reconciler := newAutoscalingReconciler(scheme, k8sClient)

	// The maintenance loop still holds the snapshot taken before the redeploy.
	snapshot := validAutoscalingCluster()
	snapshot.Labels = map[string]string{"chart_version": "v1"}

	refreshed, err := reconciler.refreshCluster(ctx, &redis_cluster.Cluster{RedisCluster: snapshot})
	require.Error(t, err)
	require.ErrorContains(t, err, `chart version changed from "v1" to "v2"`)
	require.Nil(t, refreshed)
}
