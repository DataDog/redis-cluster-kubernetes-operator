//go:build kind_e2e

package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	bgBlankPrefix         = "bg-blank"
	watchdogHoldMarker    = "(paged, holding for operator)"
	watchdogActionTimeout = 3 * time.Minute
	watchdogReconcileWait = 2*constants.DefaultSleepTime + 15*time.Second
)

// TestE2E_BlankReplicaDuringPromoting replays the blank mapped-replica failure.
// The operator must preserve blue and hold instead of counting it as promoted.
func TestE2E_BlankReplicaDuringPromoting(t *testing.T) {
	requireSlow(t, "blank a mapped replica during blue-green promotion")
	s := newCluster(t, redisbuilder.WithReplicas(1))
	s.WriteStringData(t, bgBlankPrefix, dataIntegrityKeyCount, dataReadRetry)
	blueName := activeStatefulSetName(t, s)

	s.TriggerBlueGreen(t, clusterName)
	victim := waitMappedReplicaInPromoting(t, s)
	oldUID := blankRedisPod(t, s, victim.GreenPodName)
	requireBlankReplacement(t, s, victim.GreenPodName, oldUID)

	invariant := func() error {
		return statefulSetRunning(s, blueName)
	}
	heldMessage := waitForExpiredWatchdogHold(t, s, cachev1.BlueGreenPhasePromoting,
		constants.BlueGreenPromoteStallTimeout, invariant)
	requireWatchdogReconciled(t, s, cachev1.BlueGreenPhasePromoting, heldMessage, invariant)
	s.RequireStringData(t, bgBlankPrefix, dataIntegrityKeyCount, dataReadRetry)
}

// TestE2E_SwitchDNSRefusesBlankPrimary deletes a mapped green primary during
// the DNS drain. The ownership guard must leave its replacement inactive.
func TestE2E_SwitchDNSRefusesBlankPrimary(t *testing.T) {
	requireSlow(t, "blank a mapped primary during the DNS drain")
	s := newCluster(t, redisbuilder.WithReplicas(1))
	s.WriteStringData(t, bgBlankPrefix, dataIntegrityKeyCount, dataReadRetry)
	blueName := activeStatefulSetName(t, s)

	s.TriggerBlueGreen(t, clusterName)
	s.WaitBlueGreenPhase(t, clusterName, bgConvergeTimeout, func(rc *cachev1.RedisCluster) bool {
		return rc.Status.BlueGreen.Phase == cachev1.BlueGreenPhaseSwitchingDNS &&
			rc.Status.BlueGreen.DrainStartedAt != 0 && len(rc.Status.BlueGreen.ShardMapping) > 0
	})
	victim := s.GetCluster(t, clusterName).Status.BlueGreen.ShardMapping[0]
	raw, err := s.TryRedisCommandAtIP(s.PodIP(t, victim.GreenPodName), "CLUSTER", "NODES")
	require.NoError(t, err)
	nodes := rckind.ParseClusterNodes(raw)
	require.GreaterOrEqual(t, len(nodes), len(s.GetCluster(t, clusterName).Status.BlueGreen.ShardMapping))
	require.NoError(t, requireSlotsOwnedBy(nodes, victim.GreenNodeID, victim.SlotsRanges))

	oldUID, stopRoleWatch := watchReplacementNeverActive(t, s, victim.GreenPodName)
	require.Equal(t, oldUID, blankRedisPod(t, s, victim.GreenPodName))
	requireBlankReplacement(t, s, victim.GreenPodName, oldUID)
	require.NotEqual(t, victim.GreenNodeID, s.RedisNodeIDForPod(t, victim.GreenPodName))
	require.NoError(t, requireRecreatedPodInactive(s, victim.GreenPodName, oldUID))
	expireSwitchDNSDrain(t, s)
	waitSwitchDNSBlocked(t, s, victim.GreenPodName, oldUID)

	invariant := func() error {
		if err := requireRecreatedPodInactive(s, victim.GreenPodName, oldUID); err != nil {
			return err
		}
		return statefulSetRunning(s, blueName)
	}
	heldMessage := waitForExpiredWatchdogHold(t, s, cachev1.BlueGreenPhaseSwitchingDNS,
		constants.BlueGreenSwitchDNSStallTimeout, invariant)
	requireWatchdogReconciled(t, s, cachev1.BlueGreenPhaseSwitchingDNS, heldMessage, invariant)
	require.NoError(t, stopRoleWatch())
	s.RequireStringData(t, bgBlankPrefix, dataIntegrityKeyCount, dataReadRetry)
}

// TestE2E_OnlyCopiesGoneHolds removes both copies of one shard. The watchdog
// must hold across reconciles and leave the blue StatefulSet running.
func TestE2E_OnlyCopiesGoneHolds(t *testing.T) {
	requireSlow(t, "remove both copies of a shard during blue-green promotion")
	s := newCluster(t, redisbuilder.WithReplicas(0))
	s.WriteStringData(t, bgBlankPrefix, dataIntegrityKeyCount, dataReadRetry)
	blueName := activeStatefulSetName(t, s)

	s.TriggerBlueGreen(t, clusterName)
	victim := waitMappedReplicaInPromoting(t, s)
	bluePod := podNameAtIP(t, s, victim.BluePrimaryIP)
	greenUID := blankRedisPod(t, s, victim.GreenPodName)
	blueUID := blankRedisPod(t, s, bluePod)
	requireBlankReplacement(t, s, victim.GreenPodName, greenUID)
	requireBlankReplacement(t, s, bluePod, blueUID)
	waitForSlotGap(t, s, victim, victim.GreenPodName, bluePod)

	invariant := func() error {
		return statefulSetRunning(s, blueName)
	}
	heldMessage := waitForExpiredWatchdogHold(t, s, cachev1.BlueGreenPhasePromoting,
		constants.BlueGreenPromoteStallTimeout, invariant)
	require.Contains(t, heldMessage, "uncovered slots, no backup")
	requireWatchdogReconciled(t, s, cachev1.BlueGreenPhasePromoting, heldMessage, invariant)
}

func waitMappedReplicaInPromoting(t *testing.T, s *rckind.Suite) cachev1.ShardMappingEntry {
	t.Helper()
	var selected cachev1.ShardMappingEntry
	s.EventuallyErrorT(t, "mapped replica available during Promoting", bgConvergeTimeout, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Get(clusterName, rc); err != nil {
			return err
		}
		bg := rc.Status.BlueGreen
		if bg.Phase != cachev1.BlueGreenPhasePromoting {
			return fmt.Errorf("phase=%s", bg.Phase)
		}
		for i := len(bg.ShardMapping) - 1; i >= 0; i-- {
			mapping := bg.ShardMapping[i]
			info, err := s.TryRedisCommandAtIP(mapping.GreenPodIP, "INFO", "replication")
			if err == nil && strings.Contains(info, constants.RedisRoleSlave) &&
				strings.Contains(info, constants.RedisLinkStatusUp) {
				selected = mapping
				return nil
			}
		}
		return fmt.Errorf("no synchronized mapped replica remains")
	})
	return selected
}

func blankRedisPod(t *testing.T, s *rckind.Suite, podName string) types.UID {
	t.Helper()
	oldUID := s.PodUID(t, podName)
	require.NotEmpty(t, oldUID)
	code, stdout, stderr, err := s.ExecOnPod(s.Ctx, podName, constants.RedisContainerName,
		[]string{"sh", "-c", "rm -rf /data/*"})
	require.NoError(t, err)
	require.Zero(t, code, "wipe %s: stdout=%q stderr=%q", podName, stdout, stderr)
	s.ForceDeleteRedisPod(t, podName)
	return oldUID
}

func requireBlankReplacement(t *testing.T, s *rckind.Suite, podName string, oldUID types.UID) {
	t.Helper()
	s.WaitPodRecreated(t, podName, oldUID, failoverTimeout)
	s.EventuallyErrorT(t, "pod "+podName+" is a blank Redis master", failoverTimeout, func() error {
		raw, err := s.TryRedisCommandAtIP(s.PodIP(t, podName), "CLUSTER", "NODES")
		if err != nil {
			return err
		}
		for _, node := range rckind.ParseClusterNodes(raw) {
			if node.Myself && node.IsMaster && node.OwnedSlots() == 0 {
				return nil
			}
		}
		return fmt.Errorf("replacement is not a zero-slot master:\n%s", raw)
	})
}

func waitSwitchDNSBlocked(t *testing.T, s *rckind.Suite, podName string, oldUID types.UID) {
	t.Helper()
	s.EventuallyErrorT(t, "operator blocks DNS switch on the blank primary", 2*time.Minute, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Get(clusterName, rc); err != nil {
			return err
		}
		bg := rc.Status.BlueGreen
		if bg.Phase != cachev1.BlueGreenPhaseSwitchingDNS {
			return fmt.Errorf("phase=%s", bg.Phase)
		}
		if err := requireRecreatedPodInactive(s, podName, oldUID); err != nil {
			return err
		}
		if !strings.Contains(bg.Message, "do not own their slots") {
			return fmt.Errorf("DNS switch is not blocked yet: %q", bg.Message)
		}
		return nil
	})
}

func expireSwitchDNSDrain(t *testing.T, s *rckind.Suite) {
	t.Helper()
	s.EventuallyErrorT(t, "backdate SwitchingDNS drain", time.Minute, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Get(clusterName, rc); err != nil {
			return err
		}
		if rc.Status.BlueGreen.Phase != cachev1.BlueGreenPhaseSwitchingDNS {
			return fmt.Errorf("phase changed to %s", rc.Status.BlueGreen.Phase)
		}
		now := time.Now()
		rc.Status.BlueGreen.DrainStartedAt = now.Add(-constants.BlueGreenClientRefreshDelay - time.Minute).Unix()
		rc.Status.BlueGreen.PhaseEntryTime = now.Unix()
		rc.Status.BlueGreen.LastProgressTime = now.Unix()
		return s.Client.Status().Update(s.Ctx, rc)
	})
}

func waitForExpiredWatchdogHold(
	t *testing.T,
	s *rckind.Suite,
	phase cachev1.BlueGreenPhase,
	bound time.Duration,
	invariant func() error,
) string {
	t.Helper()
	expiredAt := time.Now().Add(-bound - time.Minute).Unix()
	var heldMessage string
	s.EventuallyErrorT(t, "watchdog holds the blue-green phase", watchdogActionTimeout, func() error {
		rc := &cachev1.RedisCluster{}
		if err := s.Get(clusterName, rc); err != nil {
			return err
		}
		bg := &rc.Status.BlueGreen
		if bg.Phase != phase {
			return fmt.Errorf("watchdog changed phase from %s to %s", phase, bg.Phase)
		}
		if invariant != nil {
			if err := invariant(); err != nil {
				return err
			}
		}
		if strings.Contains(bg.Message, watchdogHoldMarker) {
			heldMessage = bg.Message
			return nil
		}

		bg.PhaseEntryTime = expiredAt
		bg.LastProgressTime = expiredAt
		if phase == cachev1.BlueGreenPhasePromoting {
			bg.ConsistencyGateAttempts = int32(constants.BlueGreenConsistencyGateAttempts)
			bg.LastSyncedCount = int32(len(bg.ShardMapping))
		}
		if err := s.Client.Status().Update(s.Ctx, rc); err != nil {
			return err
		}
		return fmt.Errorf("watchdog has not held yet: %q", bg.Message)
	})
	return heldMessage
}

func requireWatchdogReconciled(
	t *testing.T,
	s *rckind.Suite,
	phase cachev1.BlueGreenPhase,
	heldMessage string,
	invariant func() error,
) {
	t.Helper()
	s.EventuallyErrorT(t, "watchdog hold survives another reconcile", watchdogReconcileWait, func() error {
		message, err := watchdogHoldState(s, phase, invariant)
		if err != nil {
			return err
		}
		if message == heldMessage {
			return fmt.Errorf("watchdog message has not been refreshed: %q", heldMessage)
		}
		return nil
	})
}

func watchdogHoldState(s *rckind.Suite, phase cachev1.BlueGreenPhase, invariant func() error) (string, error) {
	rc := &cachev1.RedisCluster{}
	if err := s.Get(clusterName, rc); err != nil {
		return "", err
	}
	bg := rc.Status.BlueGreen
	if bg.Phase != phase {
		return "", fmt.Errorf("watchdog changed phase from %s to %s", phase, bg.Phase)
	}
	if invariant != nil {
		if err := invariant(); err != nil {
			return "", err
		}
	}
	if !strings.Contains(bg.Message, watchdogHoldMarker) {
		return "", fmt.Errorf("watchdog has not held yet: %q", bg.Message)
	}
	return bg.Message, nil
}

func requireRecreatedPodInactive(s *rckind.Suite, name string, oldUID types.UID) error {
	pod := &corev1.Pod{}
	if err := s.Get(name, pod); err != nil {
		return err
	}
	if pod.UID == oldUID {
		return fmt.Errorf("pod %s still has UID %s", name, oldUID)
	}
	return requirePodInactiveObject(pod)
}

func watchReplacementNeverActive(t *testing.T, s *rckind.Suite, name string) (types.UID, func() error) {
	t.Helper()
	pod := &corev1.Pod{}
	require.NoError(t, s.Get(name, pod))
	oldUID := pod.UID

	watchClient, err := client.NewWithWatch(s.Cfg, client.Options{Scheme: s.Client.Scheme()})
	require.NoError(t, err)
	watchCtx, cancel := context.WithCancel(s.Ctx)
	watcher, err := watchClient.Watch(watchCtx, &corev1.PodList{}, &client.ListOptions{
		Namespace: s.Namespace,
		Raw: &metav1.ListOptions{
			FieldSelector:   "metadata.name=" + name,
			ResourceVersion: pod.ResourceVersion,
		},
	})
	require.NoError(t, err)

	done := make(chan struct{})
	replacementSeen := false
	var violation error
	go func() {
		defer close(done)
		for event := range watcher.ResultChan() {
			updated, ok := event.Object.(*corev1.Pod)
			if !ok {
				if watchCtx.Err() != nil {
					return
				}
				violation = fmt.Errorf("unexpected pod watch object %T", event.Object)
				cancel()
				return
			}
			if updated.UID == oldUID {
				continue
			}
			replacementSeen = true
			if constants.IsRoleActive(updated.Labels) {
				violation = fmt.Errorf("replacement pod %s was transiently role=active", name)
				cancel()
				return
			}
		}
		if watchCtx.Err() == nil {
			violation = fmt.Errorf("pod watch for %s closed unexpectedly", name)
		}
	}()

	return oldUID, func() error {
		cancel()
		watcher.Stop()
		<-done
		if violation != nil {
			return violation
		}
		if !replacementSeen {
			return fmt.Errorf("pod watch did not observe a replacement for %s", name)
		}
		return nil
	}
}

func requirePodInactiveObject(pod *corev1.Pod) error {
	if constants.IsRoleActive(pod.Labels) {
		return fmt.Errorf("pod %s has role=active", pod.Name)
	}
	return nil
}

func podNameAtIP(t *testing.T, s *rckind.Suite, wantIP string) string {
	t.Helper()
	for _, pod := range s.RedisPods(t, clusterName) {
		if s.PodIP(t, pod) == wantIP {
			return pod
		}
	}
	t.Fatalf("no Redis pod has IP %s", wantIP)
	return ""
}

func waitForSlotGap(
	t *testing.T,
	s *rckind.Suite,
	victim cachev1.ShardMappingEntry,
	excluded ...string,
) {
	t.Helper()
	skip := make(map[string]bool, len(excluded))
	for _, name := range excluded {
		skip[name] = true
	}
	s.EventuallyErrorT(t, "surviving Redis view reports a slot gap", failoverTimeout, func() error {
		views, err := s.TryClusterViews(clusterName)
		if err != nil {
			return err
		}
		survivorIDs := make(map[string]bool)
		for _, view := range views {
			if skip[view.ViewerPod] {
				continue
			}
			for _, node := range view.Nodes {
				if node.Myself {
					survivorIDs[node.ID] = true
					break
				}
			}
		}
		if len(survivorIDs) == 0 {
			return fmt.Errorf("no surviving Redis identities")
		}
		for _, view := range views {
			if skip[view.ViewerPod] {
				continue
			}
			seen := make(map[string]bool, len(view.Nodes))
			for _, node := range view.Nodes {
				seen[node.ID] = true
			}
			for id := range survivorIDs {
				if !seen[id] {
					return fmt.Errorf("Redis view %s does not include survivor %s", view.ViewerPod, id)
				}
			}
			if err := requireSlotsUnowned(view.Nodes, victim.SlotsRanges); err != nil {
				return fmt.Errorf("Redis view %s: %w", view.ViewerPod, err)
			}
		}
		return nil
	})
}

func requireSlotsUnowned(nodes []rckind.ClusterNode, ranges string) error {
	slots, err := slotsInRanges(ranges)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if !node.IsMaster || node.Failed {
			continue
		}
		for _, slot := range slots {
			if node.OwnsSlot(slot) {
				return fmt.Errorf("node %s still owns slot %d", node.ID, slot)
			}
		}
	}
	return nil
}

func requireSlotsOwnedBy(nodes []rckind.ClusterNode, nodeID, ranges string) error {
	node, ok := rckind.FindNodeByID(nodes, nodeID)
	if !ok || !node.IsMaster || node.Failed {
		return fmt.Errorf("node %s is not a healthy primary", nodeID)
	}
	slots, err := slotsInRanges(ranges)
	if err != nil {
		return err
	}
	for _, slot := range slots {
		if !node.OwnsSlot(slot) {
			return fmt.Errorf("node %s does not own slot %d", nodeID, slot)
		}
	}
	return nil
}

func slotsInRanges(ranges string) ([]int, error) {
	var slots []int
	for _, part := range strings.Split(ranges, ",") {
		bounds := strings.SplitN(part, "-", 2)
		first, err := strconv.Atoi(bounds[0])
		if err != nil {
			return nil, fmt.Errorf("invalid slot range %q", part)
		}
		last := first
		if len(bounds) == 2 {
			last, err = strconv.Atoi(bounds[1])
			if err != nil || last < first {
				return nil, fmt.Errorf("invalid slot range %q", part)
			}
		}
		for slot := first; slot <= last; slot++ {
			slots = append(slots, slot)
		}
	}
	if len(slots) == 0 {
		return nil, fmt.Errorf("empty slot range")
	}
	return slots, nil
}

func statefulSetRunning(s *rckind.Suite, name string) error {
	sts := &appsv1.StatefulSet{}
	if err := s.Get(name, sts); err != nil {
		return err
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas == 0 {
		return fmt.Errorf("StatefulSet %s is scaled down", name)
	}
	return nil
}
