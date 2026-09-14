package kind

import (
	"fmt"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TryClusterView reads one Redis view for lightweight checks.
func (s *Suite) TryClusterView() ([]ClusterNode, string, error) {
	raw, err := s.TryClusterNodes()
	if err != nil {
		return nil, "", err
	}
	nodes := ParseClusterNodes(raw)
	if len(nodes) == 0 {
		return nil, raw, fmt.Errorf("CLUSTER NODES returned no nodes:\n%s", raw)
	}
	return nodes, raw, nil
}

// RedisNodeIDForPod returns the Redis node ID owned by a pod.
func (s *Suite) RedisNodeIDForPod(t *testing.T, pod string) string {
	t.Helper()
	ip := s.PodIP(t, pod)
	id := strings.TrimSpace(s.RedisCommandAtIP(t, ip, "CLUSTER", "MYID"))
	require.NotEmpty(t, id, "pod %s returned an empty Redis node ID", pod)
	return id
}

// RequireSlotCoverage waits until every Redis view reports all slots owned.
func (s *Suite) RequireSlotCoverage(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	require.NoError(t, s.CheckSlotCoverage(name, timeout))
}

// CheckSlotCoverage waits until every Redis view reports all slots owned.
func (s *Suite) CheckSlotCoverage(name string, timeout time.Duration) error {
	return s.EventuallyError("slot coverage", timeout, func() error {
		views, err := s.TryClusterViews(name)
		if err != nil {
			return err
		}
		for _, view := range views {
			if total := sumPrimarySlotCoverage(view.Nodes); total != constants.TotalSlots {
				return fmt.Errorf("Redis view %s slot coverage: want %d, got %d\nCLUSTER NODES:\n%s",
					view.ViewerIP, constants.TotalSlots, total, view.Raw)
			}
		}
		return nil
	})
}

// RequireReplicasPointToPrimaries waits for every live replica link.
func (s *Suite) RequireReplicasPointToPrimaries(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	require.NoError(t, s.CheckReplicasPointToPrimaries(name, timeout))
}

// CheckReplicasPointToPrimaries waits for every live replica link.
func (s *Suite) CheckReplicasPointToPrimaries(name string, timeout time.Duration) error {
	return s.EventuallyError("replica topology", timeout, func() error {
		readyIPs, err := s.tryReadyRedisPodIPs(name)
		if err != nil {
			return err
		}
		if len(readyIPs) == 0 {
			return fmt.Errorf("no ready Redis pods")
		}
		raw, err := s.TryRedisCommandAtIP(readyIPs[0], "CLUSTER", "NODES")
		if err != nil {
			return err
		}
		nodes := parseClusterNodesFrom(raw, readyIPs[0])
		liveIPs := make(map[string]bool, len(readyIPs))
		for _, ip := range readyIPs {
			liveIPs[ip] = true
		}
		primaryIPByID := make(map[string]string)
		for _, node := range nodes {
			if node.IsMaster && !node.Failed && liveIPs[node.IP] {
				primaryIPByID[node.ID] = node.IP
			}
		}
		checked := 0
		for _, node := range nodes {
			if node.IsMaster || node.Failed || !liveIPs[node.IP] {
				continue
			}
			expected := primaryIPByID[node.MasterID]
			if expected == "" {
				return fmt.Errorf("live replica %s references unknown live primary %s\nCLUSTER NODES:\n%s",
					node.IP, node.MasterID, raw)
			}
			info, err := s.TryRedisCommandAtIP(node.IP, "INFO", "replication")
			if err != nil {
				return fmt.Errorf("INFO replication at replica %s: %w", node.IP, err)
			}
			if got := infoField(info, "master_host"); got != expected {
				return fmt.Errorf("replica %s master_host: want %s, got %s", node.IP, expected, got)
			}
			checked++
		}
		if checked == 0 {
			return fmt.Errorf("no live replicas found\nCLUSTER NODES:\n%s", raw)
		}
		return nil
	})
}

// RequireTopology waits until every Redis view reports the expected live topology.
func (s *Suite) RequireTopology(t *testing.T, name string, primaries, replicas int32, timeout time.Duration) {
	t.Helper()
	wantPrimaries := int(primaries)
	wantReplicas := int(redisbuilder.PodCount(primaries, replicas) - primaries)
	wantPods := wantPrimaries + wantReplicas
	s.EventuallyErrorT(t, fmt.Sprintf("topology reaches %dp/%dr", primaries, replicas), timeout, func() error {
		views, err := s.TryClusterViews(name)
		if err != nil {
			return err
		}
		if len(views) != wantPods {
			return fmt.Errorf("ready Redis pods: want %d, got %d", wantPods, len(views))
		}
		readyNodeIDs := make(map[string]bool, len(views))
		for _, view := range views {
			var selfID string
			for _, node := range view.Nodes {
				if node.Myself {
					selfID = node.ID
					break
				}
			}
			if selfID == "" {
				return fmt.Errorf("Redis view %s has no self node", view.ViewerIP)
			}
			readyNodeIDs[selfID] = true
		}
		for _, view := range views {
			gotPrimaries, gotReplicas := 0, 0
			for _, node := range view.Nodes {
				if node.Failed || !readyNodeIDs[node.ID] {
					continue
				}
				if node.IsMaster {
					gotPrimaries++
				} else {
					gotReplicas++
				}
			}
			if gotPrimaries != wantPrimaries || gotReplicas != wantReplicas {
				return fmt.Errorf("Redis view %s: want %dp/%dr, got %dp/%dr\nCLUSTER NODES:\n%s",
					view.ViewerIP, wantPrimaries, wantReplicas, gotPrimaries, gotReplicas, view.Raw)
			}
		}
		return nil
	})
}

// RequireReadyStatus waits until the RedisCluster reports a steady state.
func (s *Suite) RequireReadyStatus(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	require.NoError(t, s.CheckReadyStatus(name, timeout))
}

// CheckReadyStatus waits until the RedisCluster reports a steady state.
func (s *Suite) CheckReadyStatus(name string, timeout time.Duration) error {
	return s.EventuallyError("RedisCluster Ready status", timeout, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Get(name, rc); err != nil {
			return err
		}
		if !rc.Status.StatefulSetReady {
			return fmt.Errorf("status.statefulset_ready=false")
		}
		expected := int(redisbuilder.PodCount(rc.Spec.Primaries, rc.Spec.Replicas))
		if len(rc.Status.Nodes) != expected {
			return fmt.Errorf("status.nodes: want %d, got %d", expected, len(rc.Status.Nodes))
		}
		if rc.Status.RolloutInProgress {
			return fmt.Errorf("status.rollout_in_progress=true")
		}
		if rc.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseNone {
			return fmt.Errorf("blue-green phase=%q", rc.Status.BlueGreen.Phase)
		}
		return nil
	})
}

// RequireSingleActiveStatefulSet asserts one ready StatefulSet with the active name.
func (s *Suite) RequireSingleActiveStatefulSet(t *testing.T, name string) {
	t.Helper()
	require.NoError(t, s.CheckSingleActiveStatefulSet(name))
}

// CheckSingleActiveStatefulSet checks for one ready StatefulSet with the active name.
func (s *Suite) CheckSingleActiveStatefulSet(name string) error {
	var list appsv1.StatefulSetList
	if err := s.Client.List(s.Ctx, &list, client.InNamespace(s.Namespace),
		client.MatchingLabels{constants.LabelRedisClusterName: name}); err != nil {
		return fmt.Errorf("list StatefulSets: %w", err)
	}
	if len(list.Items) != 1 {
		return fmt.Errorf("StatefulSet count: want 1, got %d", len(list.Items))
	}
	pods, err := s.tryRedisPodList(name)
	if err != nil {
		return fmt.Errorf("list Redis pods: %w", err)
	}
	if got, want := list.Items[0].Status.ReadyReplicas, int32(len(pods)); got != want {
		return fmt.Errorf("ready replicas: want %d, got %d", want, got)
	}
	rc := &cachev1.RedisCluster{}
	if err := s.Get(name, rc); err != nil {
		return fmt.Errorf("get RedisCluster: %w", err)
	}
	active := rc.Status.ActiveClusterName
	if active == "" {
		active = name
	}
	if got := list.Items[0].Name; got != active {
		return fmt.Errorf("active StatefulSet: want %q, got %q", active, got)
	}
	return nil
}

// RequireServiceSelectsActive checks that the cluster Service targets active pods.
func (s *Suite) RequireServiceSelectsActive(t *testing.T, name string) {
	t.Helper()
	require.NoError(t, s.CheckServiceSelectsActive(name))
}

// CheckServiceSelectsActive checks that the cluster Service targets active pods.
func (s *Suite) CheckServiceSelectsActive(name string) error {
	service := &corev1.Service{}
	if err := s.Get(name, service); err != nil {
		return fmt.Errorf("get cluster Service: %w", err)
	}
	got := service.Spec.Selector[constants.BlueGreenRoleLabel]
	if got != constants.BlueGreenRoleActive {
		return fmt.Errorf(
			"Service selector %q: want %q, got %q",
			constants.BlueGreenRoleLabel, constants.BlueGreenRoleActive, got,
		)
	}
	return nil
}

// WriteStringData writes deterministic keys and verifies every value.
func (s *Suite) WriteStringData(t *testing.T, prefix string, count int, timeout time.Duration) {
	t.Helper()
	kvs := sequentialStringKVs(prefix, count)
	s.EventuallyErrorT(t, fmt.Sprintf("durable keys %q", prefix), timeout, func() error {
		return s.tryWriteAndVerifyStrings(kvs)
	})
}

// RequireStringData waits until every deterministic value is readable.
func (s *Suite) RequireStringData(t *testing.T, prefix string, count int, timeout time.Duration) {
	t.Helper()
	kvs := sequentialStringKVs(prefix, count)
	s.EventuallyErrorT(t, fmt.Sprintf("keys %q readable", prefix), timeout, func() error {
		return s.tryVerifyStrings(kvs)
	})
}

// WaitPodRecreated waits for a new pod UID that is Running and Ready.
func (s *Suite) WaitPodRecreated(t *testing.T, name string, oldUID types.UID, timeout time.Duration) {
	t.Helper()
	s.EventuallyErrorT(t, "pod "+name+" recreated", timeout, func() error {
		pod := &corev1.Pod{}
		if err := s.Get(name, pod); err != nil {
			return err
		}
		if pod.UID == oldUID {
			return fmt.Errorf("pod %s still has UID %s", name, oldUID)
		}
		if pod.Status.PodIP == "" {
			return fmt.Errorf("recreated pod %s has no IP", name)
		}
		if !podRunningReady(*pod) {
			return fmt.Errorf("recreated pod %s is not Ready", name)
		}
		return nil
	})
}

// PickPrimaryPod returns a live primary pod.
func (s *Suite) PickPrimaryPod(t *testing.T, name string, timeout time.Duration) string {
	t.Helper()
	return s.pickPodByRole(t, name, true, timeout)
}

// PickReplicaPod returns a live replica pod.
func (s *Suite) PickReplicaPod(t *testing.T, name string, timeout time.Duration) string {
	t.Helper()
	return s.pickPodByRole(t, name, false, timeout)
}

func (s *Suite) pickPodByRole(t *testing.T, name string, primary bool, timeout time.Duration) string {
	t.Helper()
	role := "replica"
	if primary {
		role = "primary"
	}
	var found string
	s.EventuallyErrorT(t, "find a "+role+" pod", timeout, func() error {
		nodes, _, err := s.TryClusterView()
		if err != nil {
			return err
		}
		pods, err := s.tryRedisPodList(name)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			if node.IsMaster != primary || node.Failed {
				continue
			}
			for _, pod := range pods {
				if pod.Status.PodIP == node.IP {
					found = pod.Name
					return nil
				}
			}
		}
		return fmt.Errorf("no live %s pod", role)
	})
	return found
}

// DeleteRedisPod gracefully deletes a Redis pod.
func (s *Suite) DeleteRedisPod(t *testing.T, name string) {
	t.Helper()
	require.NoError(t, s.Client.Delete(s.Ctx, podRef(s.Namespace, name)))
}

// ForceDeleteRedisPod deletes a Redis pod without a grace period.
func (s *Suite) ForceDeleteRedisPod(t *testing.T, name string) {
	t.Helper()
	grace := int64(0)
	require.NoError(t, s.Client.Delete(s.Ctx, podRef(s.Namespace, name),
		&client.DeleteOptions{GracePeriodSeconds: &grace}))
}

// PatchPrimaries updates the primary count and StatefulSet size.
func (s *Suite) PatchPrimaries(t *testing.T, name string, primaries int32) {
	t.Helper()
	require.NoError(t, s.PatchCR(t, name, func(rc *cachev1.RedisCluster) {
		redisbuilder.SetPrimaries(rc, primaries)
	}))
}

// PatchTopology updates primary, replica, and StatefulSet counts.
func (s *Suite) PatchTopology(t *testing.T, name string, primaries, replicas int32) {
	t.Helper()
	require.NoError(t, s.PatchCR(t, name, func(rc *cachev1.RedisCluster) {
		redisbuilder.SetTopology(rc, primaries, replicas)
	}))
}

// TriggerReconcile changes a harmless timeout field to bump the CR generation.
func (s *Suite) TriggerReconcile(t *testing.T, name string, base time.Duration) {
	t.Helper()
	require.NoError(t, s.PatchCR(t, name, func(rc *cachev1.RedisCluster) {
		seconds := int32(base.Seconds())
		if rc.Spec.ReconcileTimeoutSeconds == seconds {
			seconds++
		}
		rc.Spec.ReconcileTimeoutSeconds = seconds
	}))
}

// DeleteClusterAndWait deletes a RedisCluster and waits for its Redis pods to disappear.
func (s *Suite) DeleteClusterAndWait(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	require.NoError(t, s.Client.Delete(s.Ctx, s.GetCluster(t, name)))
	s.EventuallyErrorT(t, "cluster "+name+" torn down", timeout, func() error {
		var pods corev1.PodList
		if err := s.Client.List(s.Ctx, &pods, client.InNamespace(s.Namespace),
			client.MatchingLabels{constants.LabelRedisClusterName: name}); err != nil {
			return err
		}
		if len(pods.Items) != 0 {
			return fmt.Errorf("%d Redis pods remain", len(pods.Items))
		}
		return nil
	})
}

// RequireReplicationOffsetsAdvance verifies that every replica receives new writes.
func (s *Suite) RequireReplicationOffsetsAdvance(
	t *testing.T,
	prefix string,
	batch int,
	writeTimeout time.Duration,
	offsetTimeout time.Duration,
) {
	t.Helper()
	require.NoError(t, s.CheckReplicationOffsetsAdvance(prefix, batch, writeTimeout, offsetTimeout))
}

// CheckReplicationOffsetsAdvance verifies that every replica receives new writes.
func (s *Suite) CheckReplicationOffsetsAdvance(
	prefix string,
	batch int,
	writeTimeout time.Duration,
	offsetTimeout time.Duration,
) error {
	nodes, _, err := s.TryClusterView()
	if err != nil {
		return err
	}
	replicas := make([]ClusterNode, 0, len(nodes))
	for _, node := range nodes {
		if !node.IsMaster {
			replicas = append(replicas, node)
		}
	}
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas in cluster")
	}

	if err := s.EventuallyError("write replication probe A", writeTimeout, func() error {
		return s.tryWriteAndVerifyStrings(sequentialStringKVs(prefix+"-a", batch))
	}); err != nil {
		return err
	}
	before, err := s.replicationOffsets(replicas)
	if err != nil {
		return err
	}
	if err := s.EventuallyError("write replication probe B", writeTimeout, func() error {
		return s.tryWriteAndVerifyStrings(sequentialStringKVs(prefix+"-b", batch))
	}); err != nil {
		return err
	}

	return s.EventuallyError("slave_repl_offset advances", offsetTimeout, func() error {
		for _, replica := range replicas {
			info, err := s.TryRedisCommandAtIP(replica.IP, "INFO", "replication")
			if err != nil {
				return err
			}
			if infoFieldInt(info, "slave_repl_offset") <= before[replica.IP] {
				return fmt.Errorf("replica %s offset has not advanced past %d", replica.IP, before[replica.IP])
			}
		}
		return nil
	})
}

func (s *Suite) replicationOffsets(replicas []ClusterNode) (map[string]int, error) {
	offsets := make(map[string]int, len(replicas))
	for _, replica := range replicas {
		info, err := s.TryRedisCommandAtIP(replica.IP, "INFO", "replication")
		if err != nil {
			return nil, fmt.Errorf("INFO replication at replica %s: %w", replica.IP, err)
		}
		offsets[replica.IP] = infoFieldInt(info, "slave_repl_offset")
	}
	return offsets, nil
}

func podRef(namespace, name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}
