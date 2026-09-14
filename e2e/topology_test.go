//go:build kind_e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestE2E_PodIPChurn_StaleEntryDropped verifies address convergence for a persisted node ID.
func TestE2E_PodIPChurn_StaleEntryDropped(t *testing.T) {
	s := newCluster(t)

	target := s.PickReplicaPod(t, clusterName, topologySettleTimeout)
	oldIP := s.PodIP(t, target)
	require.NotEmpty(t, oldIP)
	oldUID := s.PodUID(t, target)
	require.NotEmpty(t, oldUID)
	targetID := s.RedisNodeIDForPod(t, target)

	alias := createPodAddressAliasService(t, s, target)
	aliasIP := alias.Spec.ClusterIP
	require.Equal(t, "OK", strings.TrimSpace(s.RedisCommandAtIP(t, oldIP,
		"CONFIG", "SET", "cluster-announce-ip", aliasIP)))
	waitForPeerNodeAddress(t, s, "temporary address learned by every peer",
		targetID, oldIP, aliasIP, alias.Spec.Ports[0].Port)

	// Keep the alias routable until the node restarts with the same ID.
	s.DeleteRedisPod(t, target)
	s.WaitPodRecreated(t, target, oldUID, failoverTimeout)

	var newIP, newID string
	s.EventuallyErrorT(t, "recreated target accepts Redis commands", failoverTimeout, func() error {
		newIP = s.PodIP(t, target)
		if newIP == "" {
			return fmt.Errorf("pod %s has no IP yet", target)
		}
		out, err := s.TryRedisCommandAtIP(newIP, "CLUSTER", "MYID")
		if err != nil {
			return err
		}
		newID = strings.TrimSpace(out)
		if newID == "" {
			return fmt.Errorf("pod %s returned an empty node ID", target)
		}
		return nil
	})
	require.Equal(t, targetID, newID, "recreated pod did not retain its Redis node ID")
	require.NotEqual(t, aliasIP, newIP, "recreated pod reused the deleted Service alias")

	// Replace the address persisted in nodes.conf with the recreated pod IP.
	require.Equal(t, "OK", strings.TrimSpace(s.RedisCommandAtIP(t, newIP,
		"CONFIG", "SET", "cluster-announce-ip", newIP)))
	require.NoError(t, s.Client.Delete(s.Ctx, alias))
	waitForPeerNodeAddress(t, s, "recreated Pod address learned by every peer",
		targetID, newIP, newIP, alias.Spec.Ports[0].Port)

	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	require.NoError(t, s.WaitReady(clusterObjectKey(s), failoverTimeout))

	s.EventuallyErrorT(t, "stale address dropped + pod IP gossiped on every node", failoverTimeout, func() error {
		views, err := s.TryClusterViews(clusterName)
		if err != nil {
			return err
		}
		targetViewerReady := false
		for _, view := range views {
			if view.ViewerIP == newIP {
				targetViewerReady = true
			}
			seenAt := ""
			for _, n := range view.Nodes {
				if n.IP == aliasIP {
					return fmt.Errorf("node %s still lists stale alias %s (id %s)", view.ViewerIP, aliasIP, n.ID)
				}
				if oldIP != newIP && n.IP == oldIP {
					return fmt.Errorf("node %s still lists old pod IP %s (id %s)", view.ViewerIP, oldIP, n.ID)
				}
				if n.ID == targetID {
					seenAt = n.IP
				}
			}
			if seenAt != newIP {
				return fmt.Errorf("node %s lists target %s at %q, want %s", view.ViewerIP, targetID, seenAt, newIP)
			}
		}
		if !targetViewerReady {
			return fmt.Errorf("recreated target %s (%s) is not Ready", target, newIP)
		}
		return nil
	})
	requireCRNodeAddressEventually(t, s, targetID, newIP)
	s.RequireTopology(t, clusterName, redisbuilder.DefaultPrimaries, redisbuilder.DefaultReplicas, failoverTimeout)
	s.RequireReplicasPointToPrimaries(t, clusterName, topologySettleTimeout)
	s.RequireReplicationOffsetsAdvance(t, "replica-rejoin", replicationWriteBatch, dataReadRetry, replicationPollTimeout)
}

func createPodAddressAliasService(t *testing.T, s *rckind.Suite, pod string) *corev1.Service {
	t.Helper()
	headless := &corev1.Service{}
	require.NoError(t, s.Client.Get(s.Ctx, client.ObjectKey{Namespace: s.Namespace, Name: clusterName}, headless))
	require.NotEmpty(t, headless.Spec.Ports, "headless Redis Service has no ports")
	alias := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: pod + "-address-alias", Namespace: s.Namespace},
		Spec: corev1.ServiceSpec{
			PublishNotReadyAddresses: true,
			Selector:                 map[string]string{appsv1.StatefulSetPodNameLabel: pod},
			Ports:                    append([]corev1.ServicePort(nil), headless.Spec.Ports...),
		},
	}
	require.NoError(t, s.Client.Create(s.Ctx, alias))
	require.NotEmpty(t, alias.Spec.ClusterIP)
	require.NotEqual(t, corev1.ClusterIPNone, alias.Spec.ClusterIP)
	return alias
}

func waitForPeerNodeAddress(t *testing.T, s *rckind.Suite, what, targetID, targetIP, wantIP string, port int32) {
	t.Helper()
	s.EventuallyErrorT(t, what, failoverTimeout, func() error {
		views, err := s.TryClusterViews(clusterName)
		if err != nil {
			return err
		}
		peers := 0
		for _, view := range views {
			if view.ViewerIP == targetIP {
				continue
			}
			peers++
			seenAt := ""
			if node, ok := rckind.FindNodeByID(view.Nodes, targetID); ok {
				seenAt = node.IP
			}
			if seenAt != wantIP {
				// Reissue idempotent MEET to refresh a stale cluster-bus link.
				if _, err := s.TryRedisCommandAtIP(view.ViewerIP, "CLUSTER", "MEET", wantIP, fmt.Sprint(port)); err != nil {
					return fmt.Errorf("peer %s meet address %s: %w", view.ViewerIP, wantIP, err)
				}
				return fmt.Errorf("peer %s lists target %s at %q, want address %s", view.ViewerIP, targetID, seenAt, wantIP)
			}
		}
		if peers == 0 {
			return fmt.Errorf("no ready Redis peers observed target %s", targetID)
		}
		return nil
	})
}

func requireCRNodeAddressEventually(t *testing.T, s *rckind.Suite, targetID, wantIP string) {
	t.Helper()
	s.EventuallyErrorT(t, "RedisCluster status records recreated node address", failoverTimeout, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Client.Get(s.Ctx, client.ObjectKey{Namespace: s.Namespace, Name: clusterName}, rc); err != nil {
			return err
		}
		for _, node := range rc.Status.Nodes {
			if node.ID != targetID {
				continue
			}
			if node.IP != wantIP {
				return fmt.Errorf("status lists target %s at %s, want %s", targetID, node.IP, wantIP)
			}
			return nil
		}
		return fmt.Errorf("status does not list target node %s", targetID)
	})
}
