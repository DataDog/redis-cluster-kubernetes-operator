// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"fmt"
	"testing"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestE2E_Bootstrap verifies a healthy default cluster and live replication.
func TestE2E_Bootstrap(t *testing.T) {
	s := newCluster(t)
	rc := s.GetCluster(t, clusterName)
	want := int(redisbuilder.PodCount(rc.Spec.Primaries, rc.Spec.Replicas))
	assert.Len(t, s.RedisPods(t, clusterName), want, "want %d pods", want)

	assert.NoError(t, s.CheckReadyStatus(clusterName, clusterReadyTimeout), "ready status")
	assert.NoError(t, s.CheckSlotCoverage(clusterName, topologySettleTimeout), "all slots owned")
	assert.NoError(
		t,
		s.CheckReplicasPointToPrimaries(clusterName, topologySettleTimeout),
		"replicas point to primaries",
	)
	assert.NoError(t, s.CheckReplicationOffsetsAdvance(
		"steady-offset", replicationWriteBatch, dataReadRetry, replicationPollTimeout,
	), "replication offsets advance")

	lg := s.StartLoadgen(t, seedKeyCount)
	s.WaitClusterHealthy(t, lg, clusterReadyTimeout)
	require.NoError(t, lg.SeedData(s.Ctx))
	assert.NoError(t, rckind.CheckSeedDataIntact(t.Context(), lg, seedKeyCount), "exact string data")
}

// TestE2E_CRDeleteCascadesToSTS verifies the operator's owner reference.
func TestE2E_CRDeleteCascadesToSTS(t *testing.T) {
	s := rckind.NewSuite(t)
	rc := rckind.NewRedisCluster(clusterName, s.Namespace, redisbuilder.WithReplicas(0))
	require.NoError(t, s.Client.Create(s.Ctx, rc))
	stsKey := types.NamespacedName{Namespace: s.Namespace, Name: clusterName}
	s.EventuallyErrorT(t, "StatefulSet has RedisCluster owner", clusterReadyTimeout, func() error {
		sts := &appsv1.StatefulSet{}
		if err := s.Client.Get(s.Ctx, stsKey, sts); err != nil {
			return err
		}
		if !metav1.IsControlledBy(sts, rc) {
			return fmt.Errorf("StatefulSet is not controlled by RedisCluster %s", rc.UID)
		}
		return nil
	})
	require.NoError(t, s.Client.Delete(s.Ctx, rc))

	s.EventuallyT(t, "STS GC'd after CR delete", gcAfterDeleteTimeout, func() bool {
		return apierrors.IsNotFound(s.Client.Get(s.Ctx, stsKey, &appsv1.StatefulSet{}))
	})
}
