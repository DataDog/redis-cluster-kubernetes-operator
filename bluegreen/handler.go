package bluegreen

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// reconcileCache holds per-reconcile cached K8s results to avoid duplicate API calls
// within a single Handle() invocation. Reset at the start of every reconcile.
type reconcileCache struct {
	greenPods    []corev1.Pod
	greenPodsOK  bool
	greenPodsErr error
	bluePods     *corev1.PodList
	bluePodsOK   bool
	bluePodsErr  error
}

// Handler manages the blue-green deployment lifecycle.
type Handler struct {
	deps    Deps
	log     logr.Logger
	metrics metrics.Metrics
	cache   reconcileCache
}

// NewHandler creates a new blue-green deployment handler.
func NewHandler(deps Deps, log logr.Logger, m metrics.Metrics) *Handler {
	return &Handler{deps: deps, log: log.WithName("bluegreen"), metrics: m}
}

func (h *Handler) getGreenPods(ctx context.Context, rc *cachev1.RedisCluster) ([]corev1.Pod, error) {
	if h.cache.greenPodsOK {
		return h.cache.greenPods, h.cache.greenPodsErr
	}
	pods, err := h.deps.GetGreenPods(ctx, rc)
	h.cache.greenPods = pods
	h.cache.greenPodsErr = err
	h.cache.greenPodsOK = true
	return pods, err
}

func (h *Handler) getBluePods(ctx context.Context, rc *cachev1.RedisCluster) (*corev1.PodList, error) {
	if h.cache.bluePodsOK {
		return h.cache.bluePods, h.cache.bluePodsErr
	}
	pods, err := h.deps.GetBluePods(ctx, rc)
	h.cache.bluePods = pods
	h.cache.bluePodsErr = err
	h.cache.bluePodsOK = true
	return pods, err
}

func GreenSTSName(rc *cachev1.RedisCluster) string {
	active := rc.Status.ActiveClusterName
	if active == "" {
		active = rc.Spec.StatefulsetTemplate.Name
	}
	return constants.AlternateSTSName(active)
}

func BlueSTSName(rc *cachev1.RedisCluster) string {
	active := rc.Status.ActiveClusterName
	if active == "" {
		return rc.Spec.StatefulsetTemplate.Name
	}
	return active
}

// PhaseValue maps a BG phase to a numeric gauge value for dashboards.
// 0 = idle, higher = further along. Enables time-series graphs of deployment progress.
var PhaseValue = map[cachev1.BlueGreenPhase]float64{
	cachev1.BlueGreenPhaseNone:            0,
	cachev1.BlueGreenPhaseCreatingGreen:   1,
	cachev1.BlueGreenPhaseWaitingPods:     2,
	cachev1.BlueGreenPhaseReplicating:     3,
	cachev1.BlueGreenPhaseWaitingSync:     4,
	cachev1.BlueGreenPhasePromoting:       5,
	cachev1.BlueGreenPhaseSwitchingDNS:    6,
	cachev1.BlueGreenPhaseFormingCluster:  7,
	cachev1.BlueGreenPhaseScalingDownBlue: 8,
	cachev1.BlueGreenPhaseTearingDown:     9,
	cachev1.BlueGreenPhaseFailed:          -1,
}

// EmitPhaseGauge emits the current BG phase as a single numeric gauge.
// The value steps through 0 (None) → 1 (CreatingGreen) → … → 9 (TearingDown), -1 (Failed).
// The phase name is included as a tag for filtering but only one gauge series exists per cluster.
func (h *Handler) EmitPhaseGauge(phase cachev1.BlueGreenPhase) {
	val, ok := PhaseValue[phase]
	if !ok {
		val = -2 // unknown
	}
	_ = h.metrics.Gauge(metrics.GaugeBlueGreenPhase, val)
}

func (h *Handler) emitFailedDuration(bgStatus *cachev1.BlueGreenStatus) {
	if bgStatus.StartTime <= 0 {
		return
	}
	totalDuration := time.Since(time.Unix(bgStatus.StartTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenDuration, totalDuration, "result:failed")
}

// ────────────────────────────────────────────────────────────────────────────
// Phase dispatch
// ────────────────────────────────────────────────────────────────────────────

// Handle dispatches to the appropriate blue/green phase handler.
func (h *Handler) Handle(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	h.cache = reconcileCache{}

	phase := cluster.RedisCluster.Status.BlueGreen.Phase

	h.log.Info("Blue/green deployment in progress", "phase", phase)

	// ── Overall timeout: pre-promotion phases only ──
	// Post-promotion phases (Promoting, SwitchingDNS, ScalingDownBlue) are exempt
	// because green already owns data — rolling back would cause data loss.
	bgStatus := &cluster.RedisCluster.Status.BlueGreen
	if bgStatus.StartTime > 0 &&
		phase != cachev1.BlueGreenPhaseWaitingSync &&
		phase != cachev1.BlueGreenPhaseFailed &&
		phase != cachev1.BlueGreenPhaseTearingDown &&
		phase != cachev1.BlueGreenPhasePromoting &&
		phase != cachev1.BlueGreenPhaseSwitchingDNS &&
		phase != cachev1.BlueGreenPhaseFormingCluster &&
		phase != cachev1.BlueGreenPhaseScalingDownBlue {

		elapsed := time.Since(time.Unix(bgStatus.StartTime, 0))
		if elapsed > constants.BlueGreenOverallTimeout {
			h.log.Error(nil, "Blue/green deployment exceeded overall timeout, transitioning to Failed",
				"elapsed", elapsed.Truncate(time.Second), "timeout", constants.BlueGreenOverallTimeout, "phase", phase)
			_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration,
				elapsed.Milliseconds(), fmt.Sprintf("phase:%s", phase), "result:timeout")
			h.emitFailedDuration(bgStatus)
			setPhase(bgStatus, cachev1.BlueGreenPhaseFailed, fmt.Sprintf("Deployment timed out after %s (in phase %s)", elapsed.Truncate(time.Second), phase))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			_ = h.metrics.Count(counters.BlueGreenFailed)
			return ctrl.Result{Requeue: true}, health.Error, nil
		}
	}

	if result, done := h.checkForceReset(ctx, cluster); done {
		return result, health.Warn, nil
	}

	h.EmitPhaseGauge(phase)
	if phase != cachev1.BlueGreenPhaseCreatingGreen &&
		phase != cachev1.BlueGreenPhaseFailed &&
		phase != cachev1.BlueGreenPhaseTearingDown {

		gName := GreenSTSName(cluster.RedisCluster)
		greenSTS, err := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, gName)
		if err != nil {
			h.log.Error(err, "Failed to query green StatefulSet, transitioning to Failed",
				"greenStatefulSet", gName, "phase", phase)
			h.emitFailedDuration(bgStatus)
			setPhase(bgStatus, cachev1.BlueGreenPhaseFailed, fmt.Sprintf("Failed to get green StatefulSet %s during %s: %v", gName, phase, err))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			_ = h.metrics.Count(counters.BlueGreenFailed)
			return ctrl.Result{Requeue: true}, health.Error, nil
		}
		if greenSTS == nil {
			h.log.Info("Green StatefulSet not found (deleted externally?), transitioning to Failed",
				"greenStatefulSet", gName, "phase", phase)
			h.emitFailedDuration(bgStatus)
			setPhase(bgStatus, cachev1.BlueGreenPhaseFailed, fmt.Sprintf("Green StatefulSet %s was deleted during %s phase", gName, phase))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			_ = h.metrics.Count(counters.BlueGreenFailed)
			return ctrl.Result{Requeue: true}, health.Error, nil
		}
	}

	// ── Refresh shard mapping from real cluster state ──
	if len(bgStatus.ShardMapping) > 0 && phase != cachev1.BlueGreenPhaseFailed {
		if err := h.refreshShardMappingFromCluster(ctx, bgStatus, cluster); err != nil {
			h.log.Info("Failed to refresh shard mapping, continuing with cached data", "error", err)
		} else {
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		}
	}

	// ── Manual force-cutover: annotation "bluegreen.force-cutover: true" ──
	if phase != cachev1.BlueGreenPhaseFailed &&
		phase != cachev1.BlueGreenPhaseTearingDown {

		annotations := cluster.RedisCluster.GetAnnotations()
		if annotations != nil && annotations[constants.BlueGreenForceCutover] == "true" {
			h.log.Info("MANUAL FORCE CUTOVER triggered via annotation",
				"phase", phase, "annotation", constants.BlueGreenForceCutover)
			_ = h.metrics.CountBadSituation("bluegreen_force_cutover_triggered")

			if bgStatus.ResumeForwardAttempts < int32(constants.BlueGreenTakeoverAttemptThreshold) {
				bgStatus.ResumeForwardAttempts = int32(constants.BlueGreenTakeoverAttemptThreshold)
			}
			setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, "Manual force-cutover triggered via annotation — promoting green via TAKEOVER")
			bgStatus.StartTime = time.Now().Unix()
			bgStatus.LastSyncedCount = 0
			bgStatus.LastProgressTime = 0
			if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
				return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to persist force-cutover status")
			}

			delete(annotations, constants.BlueGreenForceCutover)
			cluster.RedisCluster.SetAnnotations(annotations)
			if err := h.deps.UpdateSpec(ctx, cluster.RedisCluster); err != nil {
				h.log.Error(err, "Failed to clear force-cutover annotation (will retry next reconcile)")
			}

			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
	}

	// ── Blue-alive check during Replicating/WaitingSync ──
	if phase == cachev1.BlueGreenPhaseReplicating ||
		phase == cachev1.BlueGreenPhaseWaitingSync {

		if handled, bResult, bHealth, bErr := h.CheckBlueAlive(ctx, cluster); handled {
			return bResult, bHealth, bErr
		}
	}

	// ── Cluster consistency check (bounded) ──
	if phase == cachev1.BlueGreenPhasePromoting {
		if ok, reason := h.bgClusterConsistencyCheck(ctx, bgStatus, cluster.Toolbox); !ok {
			if bgStatus.ConsistencyGateAttempts < int32(constants.BlueGreenConsistencyGateAttempts) {
				h.log.Info("Cluster consistency check failed — pausing promotion",
					"reason", reason, "consistencyGateAttempts", bgStatus.ConsistencyGateAttempts)
				bgStatus.ConsistencyGateAttempts++
				bgStatus.Message = fmt.Sprintf("Paused %s: %s", phase, reason)
				_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
				return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
			}
			// Let bgPromote skip blank primaries instead of wedging on consistency.
			h.log.Info("Consistency gate exhausted — proceeding to promotion (bounded escape)",
				"reason", reason, "consistencyGateAttempts", bgStatus.ConsistencyGateAttempts)
			_ = h.metrics.CountBadSituation("bluegreen_consistency_gate_bypassed")
		} else if bgStatus.ConsistencyGateAttempts != 0 {
			bgStatus.ConsistencyGateAttempts = 0
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		}
	}

	// ── Progress watchdog ──
	if wr, wh, handled, werr := h.checkProgressWatchdog(ctx, cluster); handled {
		return wr, wh, werr
	}

	var result ctrl.Result
	var hp health.ClusterHealth
	var err error

	switch phase {
	case cachev1.BlueGreenPhaseCreatingGreen:
		result, hp, err = h.bgCreateGreen(ctx, cluster)
	case cachev1.BlueGreenPhaseWaitingPods:
		result, hp, err = h.bgWaitForPods(ctx, cluster)
	case cachev1.BlueGreenPhaseReplicating:
		result, hp, err = h.bgSetupReplication(ctx, cluster)
	case cachev1.BlueGreenPhaseWaitingSync:
		result, hp, err = h.bgWaitForSync(ctx, cluster)
	case cachev1.BlueGreenPhasePromoting:
		result, hp, err = h.bgPromote(ctx, cluster)
	case cachev1.BlueGreenPhaseScalingDownBlue:
		result, hp, err = h.bgScaleDownBlue(ctx, cluster)
	case cachev1.BlueGreenPhaseFormingCluster:
		result, hp, err = h.bgFormGreenCluster(ctx, cluster)
	case cachev1.BlueGreenPhaseSwitchingDNS:
		result, hp, err = h.bgSwitchDNS(ctx, cluster)
	case cachev1.BlueGreenPhaseTearingDown:
		result, hp, err = h.bgTeardown(ctx, cluster)
	case cachev1.BlueGreenPhaseFailed:
		result, hp, err = h.bgCleanupFailed(ctx, cluster)
	default:
		err = fmt.Errorf("unknown blue/green phase: %s", phase)
		hp = health.Error
	}

	if err != nil {
		bgStatus.ConsecutiveErrors++
		h.log.Error(err, "Blue/green deployment error", "phase", phase, "consecutiveErrors", bgStatus.ConsecutiveErrors)
		_ = h.metrics.CountBadSituation("bluegreen_error")

		if bgStatus.ConsecutiveErrors >= constants.BlueGreenMaxConsecutivePhaseErrors {
			h.log.Error(err, "Too many consecutive errors in phase, aborting BG deployment",
				"phase", phase, "consecutiveErrors", bgStatus.ConsecutiveErrors)
			if phase != cachev1.BlueGreenPhaseFailed {
				h.emitFailedDuration(bgStatus)
			}
			setPhase(bgStatus, cachev1.BlueGreenPhaseFailed, fmt.Sprintf("Aborted: %d consecutive errors in phase %s: %v", bgStatus.ConsecutiveErrors, phase, err))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		}
	} else {
		bgStatus.ConsecutiveErrors = 0
	}

	return result, hp, err
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 0: Start
// ────────────────────────────────────────────────────────────────────────────

// Start initiates a new blue/green deployment.
func (h *Handler) Start(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	h.log.Info("Starting blue/green deployment")

	blueName := cluster.RedisCluster.Status.ActiveClusterName
	if blueName == "" {
		blueName = cluster.RedisCluster.Spec.StatefulsetTemplate.Name
		h.log.Info("ActiveClusterName not set, using spec STS name", "blueName", blueName)
		cluster.RedisCluster.Status.ActiveClusterName = blueName
	}

	greenName := constants.AlternateSTSName(blueName)

	h.log.Info("Blue/green names resolved", "blue", blueName, "green", greenName)

	if err := h.prepareBlueServiceRouting(ctx, cluster); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to isolate serving blue pods before rollout")
	}

	// Disable replica migration for the BG duration — prevents gossip races from
	// moving replicas to wrong primaries during promotion.
	allIPs := make([]string, 0, len(cluster.AllNodes))
	for _, n := range cluster.AllNodes {
		if n.IP != "" {
			allIPs = append(allIPs, n.IP)
		}
	}
	h.setReplicaMigration(ctx, cluster.Toolbox, allIPs, false)

	cluster.RedisCluster.Status.BlueGreen = cachev1.BlueGreenStatus{
		Phase:     cachev1.BlueGreenPhaseCreatingGreen,
		StartTime: time.Now().Unix(),
		Message:   "Starting blue/green deployment",
	}

	if err := h.deps.UpdateStatusWithSpecHash(ctx, cluster); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to save blue/green start status and spec hash")
	}

	_ = h.metrics.Count(counters.BlueGreenStarted)
	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

func (h *Handler) prepareBlueServiceRouting(ctx context.Context, cluster *redis_cluster.Cluster) error {
	bluePods, err := h.getBluePods(ctx, cluster.RedisCluster)
	if err != nil {
		return utils.WrapErr(err, "failed to list serving blue pods")
	}
	if len(bluePods.Items) == 0 {
		return fmt.Errorf("no serving blue pods found")
	}
	for i := range bluePods.Items {
		pod := &bluePods.Items[i]
		if constants.IsRoleActive(pod.Labels) {
			continue
		}
		markPodServable(pod)
		if err := h.deps.Update(ctx, pod); err != nil {
			return utils.WrapErr(err, "failed to mark blue pod active: "+pod.Name)
		}
	}

	serviceName := cluster.RedisCluster.Spec.StatefulsetTemplate.Spec.ServiceName
	if serviceName == "" {
		return fmt.Errorf("statefulsetTemplate.spec.serviceName is empty")
	}
	service := &corev1.Service{}
	if err := h.deps.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: cluster.RedisCluster.Namespace}, service); err != nil {
		return utils.WrapErr(err, "failed to get serving headless service")
	}
	if len(service.Spec.Selector) == 0 {
		return fmt.Errorf("serving headless service %s/%s has no selector", service.Namespace, service.Name)
	}
	if service.Spec.Selector[constants.BlueGreenRoleLabel] == constants.BlueGreenRoleActive {
		return nil
	}
	selector := make(map[string]string, len(service.Spec.Selector)+1)
	for key, value := range service.Spec.Selector {
		selector[key] = value
	}
	selector[constants.BlueGreenRoleLabel] = constants.BlueGreenRoleActive
	if err := h.deps.UpdateServiceSelector(ctx, service.Namespace, service.Name, selector); err != nil {
		return utils.WrapErr(err, "failed to restrict serving headless service to active pods")
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 1: Create green resources
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgCreateGreen(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	if n := h.deleteOrphanedPods(ctx, cluster); n > 0 {
		h.log.Info("Cleaned orphaned pods, requeueing", "deleted", n)
		return ctrl.Result{RequeueAfter: constants.ShortDelay}, health.Warn, nil
	}

	if err := h.deps.CreateGreenHeadlessService(ctx, cluster.RedisCluster, GreenSTSName(cluster.RedisCluster), cluster.RedisPort); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to create green headless service")
	}

	_, _, err := h.deps.CreateGreenStatefulSet(ctx, cluster.RedisCluster)
	if err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to create green StatefulSet")
	}

	h.log.Info("Green StatefulSet created, waiting for pods")
	setPhase(bgStatus, cachev1.BlueGreenPhaseWaitingPods, "Waiting for green pods to be ready")
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		return ctrl.Result{}, health.Error, err
	}

	return ctrl.Result{RequeueAfter: constants.BlueGreenPodReadyDelay}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 2: Wait for green pods
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgWaitForPods(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	greenSTS, err := h.deps.GetGreenStatefulSet(ctx, cluster.RedisCluster.Namespace, GreenSTSName(cluster.RedisCluster))
	if err != nil {
		if apierrors.IsNotFound(err) {
			if reset, resetErr := h.bgResetIfGreenSTSGone(ctx, cluster); reset {
				return ctrl.Result{Requeue: true}, health.Warn, resetErr
			}
		}
		return ctrl.Result{}, health.Error, err
	}

	if greenSTS.Spec.Replicas != nil && *greenSTS.Spec.Replicas == 0 {
		if reset, resetErr := h.bgResetIfGreenSTSGone(ctx, cluster); reset {
			return ctrl.Result{Requeue: true}, health.Warn, resetErr
		}
	}

	if greenSTS.Spec.Replicas == nil {
		return ctrl.Result{RequeueAfter: constants.BlueGreenPodReadyDelay}, health.Warn, nil
	}
	expectedPods := *greenSTS.Spec.Replicas
	availablePods := greenSTS.Status.AvailableReplicas

	h.log.Info("Checking green pod readiness", "expected", expectedPods, "available", availablePods)

	if availablePods < expectedPods {
		h.checkGreenSTSQuotaEvents(ctx, cluster.RedisCluster, bgStatus)
		return ctrl.Result{RequeueAfter: constants.BlueGreenPodReadyDelay}, health.Warn, nil
	}

	greenPods, err := h.getGreenPods(ctx, cluster.RedisCluster)
	if err != nil {
		return ctrl.Result{}, health.Error, err
	}

	readyCount := 0
	for _, pod := range greenPods {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
			readyCount++
		}
	}

	if readyCount < int(expectedPods) {
		h.log.Info("Not all green pods have IPs yet", "ready", readyCount, "expected", expectedPods)
		return ctrl.Result{RequeueAfter: constants.BlueGreenPodReadyDelay}, health.Warn, nil
	}

	h.log.Info("All green pods are ready", "count", readyCount)
	setPhase(bgStatus, cachev1.BlueGreenPhaseReplicating, "Setting up replication from blue primaries to green pods")
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		return ctrl.Result{}, health.Error, err
	}

	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 3: Setup replication
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgSetupReplication(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	if len(bgStatus.ShardMapping) > 0 {
		h.log.Info("Shard mapping already exists, skipping to WaitingSync (re-entry)")
		if bgStatus.SyncStartTime == 0 {
			bgStatus.SyncStartTime = time.Now().Unix()
		}
		setPhase(bgStatus, cachev1.BlueGreenPhaseWaitingSync, fmt.Sprintf("Replicating %d shards from blue to green (resumed)", len(bgStatus.ShardMapping)))
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, err
		}
		return ctrl.Result{RequeueAfter: constants.BlueGreenSyncCheckDelay}, health.Warn, nil
	}

	bluePrimaries := make([]*redis_cluster.ClusterNode, len(cluster.Nodes[redis_cluster.PrimaryNode]))
	copy(bluePrimaries, cluster.Nodes[redis_cluster.PrimaryNode])
	sort.Slice(bluePrimaries, func(i, j int) bool {
		return bluePrimaries[i].PodIndex < bluePrimaries[j].PodIndex
	})

	greenPods, err := h.getGreenPods(ctx, cluster.RedisCluster)
	if err != nil {
		return ctrl.Result{}, health.Error, err
	}
	sort.Slice(greenPods, func(i, j int) bool {
		return GetPodIndex(greenPods[i].Name) < GetPodIndex(greenPods[j].Name)
	})

	if len(greenPods) < len(bluePrimaries) {
		return ctrl.Result{}, health.Error, fmt.Errorf("not enough green pods (%d) for blue primaries (%d)",
			len(greenPods), len(bluePrimaries))
	}

	shardMapping := make([]cachev1.ShardMappingEntry, 0, len(bluePrimaries))

	blueClusterNodes := ""
	if len(bluePrimaries) > 0 {
		_, stdout, _, err := cluster.Toolbox.ClusterNodes(ctx, bluePrimaries[0].IP)
		if err == nil {
			blueClusterNodes = stdout
		} else {
			h.log.Info("Could not get CLUSTER NODES from blue primary, will add all green pods",
				"bluePrimary", bluePrimaries[0].IP, "error", err)
		}
	}

	h.labelGreenPodsForPeerDiscovery(ctx, greenPods)

	for i, bluePrimary := range bluePrimaries {
		greenPod := greenPods[i]
		slotsStr := FormatSlotsRanges(bluePrimary.SlotsRanges)

		h.log.Info("Mapping shard for replication",
			"bluePrimary", bluePrimary.PodName,
			"bluePrimaryIP", bluePrimary.IP,
			"bluePrimaryID", bluePrimary.ID,
			"greenPod", greenPod.Name,
			"greenPodIP", greenPod.Status.PodIP,
			"slots", slotsStr)

		greenAlreadyInCluster := blueClusterNodes != "" && strings.Contains(blueClusterNodes, greenPod.Status.PodIP)

		var greenNodeID string
		if !greenAlreadyInCluster {
			refNode := h.deps.FindHealthyRefNode(cluster, nil)
			if refNode == nil {
				return ctrl.Result{}, health.Error, fmt.Errorf("no healthy reference node found for adding green pod %s as replica", greenPod.Name)
			}
			greenNode := &redis_cluster.ClusterNode{
				IP:       greenPod.Status.PodIP,
				PodName:  greenPod.Name,
				PodIndex: i,
			}

			h.log.Info("Adding green pod as replica of blue primary",
				"greenPod", greenPod.Name, "bluePrimary", bluePrimary.PodName)
			err := cluster.Toolbox.AddReplica(refNode, greenNode, cluster.RedisPort, bluePrimary.ID)
			if err != nil {
				return ctrl.Result{}, health.Error, utils.WrapErr(err,
					fmt.Sprintf("failed to add green pod %s as replica of blue primary %s", greenPod.Name, bluePrimary.PodName))
			}
			h.setReplicaMigration(ctx, cluster.Toolbox, []string{greenPod.Status.PodIP}, false)

			greenNodeID, err = cluster.Toolbox.GetNodeIDByIP(greenPod.Status.PodIP)
			if err != nil {
				h.log.Info("Could not get green node ID yet, will be populated in sync phase",
					"greenPod", greenPod.Name)
			}
		} else {
			greenNodeID, _ = cluster.Toolbox.GetNodeIDByIP(greenPod.Status.PodIP)
			h.log.Info("Green pod already in blue cluster (re-entry), skipping add",
				"greenPod", greenPod.Name, "greenNodeID", greenNodeID)
		}

		shardMapping = append(shardMapping, cachev1.ShardMappingEntry{
			BluePrimaryID: bluePrimary.ID,
			BluePrimaryIP: bluePrimary.IP,
			GreenPodName:  greenPod.Name,
			GreenPodIP:    greenPod.Status.PodIP,
			GreenNodeID:   greenNodeID,
			SlotsRanges:   slotsStr,
		})
	}

	bgStatus.ShardMapping = shardMapping
	bgStatus.SyncStartTime = time.Now().Unix()
	setPhase(bgStatus, cachev1.BlueGreenPhaseWaitingSync, fmt.Sprintf("Replicating %d shards from blue to green", len(shardMapping)))

	replicatingDuration := time.Since(time.Unix(bgStatus.PhaseEntryTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, replicatingDuration, "phase:replicating")

	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		return ctrl.Result{}, health.Error, err
	}
	h.log.Info("Replication setup complete, waiting for sync", "shards", len(shardMapping),
		"replicating_duration_ms", replicatingDuration)
	return ctrl.Result{RequeueAfter: constants.BlueGreenSyncCheckDelay}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 4: Wait for replication sync
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgWaitForSync(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	if len(bgStatus.ShardMapping) == 0 {
		return ctrl.Result{}, health.Error, fmt.Errorf("no shard mapping found in status")
	}

	didBackup, backupErr := h.deps.RunBackup(ctx, cluster)
	if backupErr != nil {
		h.log.Error(backupErr, "Failed to run backup during BG sync (non-fatal)")
	}
	if didBackup {
		cluster.RedisCluster.Status.LastBackupTime = time.Now().Unix()
	}

	for _, mapping := range bgStatus.ShardMapping {
		if !cluster.Toolbox.PingIP(mapping.GreenPodIP) {
			return ctrl.Result{}, health.Error,
				fmt.Errorf("green pod %s (%s) is not responding — may have crashed", mapping.GreenPodName, mapping.GreenPodIP)
		}
	}

	deadBlueCount := 0
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.BluePrimaryIP == "" || !cluster.Toolbox.PingIP(mapping.BluePrimaryIP) {
			deadBlueCount++
			h.log.Info("Blue primary unreachable during sync",
				"bluePrimaryIP", mapping.BluePrimaryIP,
				"greenPod", mapping.GreenPodName,
				"slots", mapping.SlotsRanges)
		}
	}
	if deadBlueCount > 0 {
		h.log.Info("Partial blue failure detected — promoting green early to preserve data",
			"deadBlueCount", deadBlueCount, "totalShards", len(bgStatus.ShardMapping))
		_ = h.metrics.CountBadSituation("bluegreen_partial_blue_failure")

		if bgStatus.ResumeForwardAttempts < int32(constants.BlueGreenTakeoverAttemptThreshold) {
			bgStatus.ResumeForwardAttempts = int32(constants.BlueGreenTakeoverAttemptThreshold)
		}
		setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, fmt.Sprintf("Early promote: %d/%d blue primaries unreachable, promoting green via TAKEOVER",
			deadBlueCount, len(bgStatus.ShardMapping)))
		bgStatus.StartTime = time.Now().Unix()
		bgStatus.LastSyncedCount = 0
		bgStatus.LastProgressTime = 0
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	allSynced := true
	syncedCount := 0
	totalShards := len(bgStatus.ShardMapping)

	for i, mapping := range bgStatus.ShardMapping {
		if mapping.GreenNodeID == "" {
			nodeID, err := cluster.Toolbox.GetNodeIDByIP(mapping.GreenPodIP)
			if err == nil && nodeID != "" {
				bgStatus.ShardMapping[i].GreenNodeID = nodeID
			}
		}

		synced, err := h.isReplicaSynced(cluster.Toolbox, mapping.GreenPodIP, mapping.BluePrimaryIP)
		if err != nil {
			h.log.Error(err, "Failed to check replication status",
				"greenPod", mapping.GreenPodName, "bluePrimary", mapping.BluePrimaryIP)
			allSynced = false
			continue
		}
		if !synced {
			replicaInfo, infoErr := cluster.Toolbox.GetReplicationInfo(mapping.GreenPodIP)
			if infoErr == nil && strings.Contains(replicaInfo, constants.RedisRoleMaster) {
				blueInfo, blueErr := cluster.Toolbox.GetReplicationInfo(mapping.BluePrimaryIP)
				blueLostMaster := mapping.BluePrimaryIP == "" ||
					!cluster.Toolbox.PingIP(mapping.BluePrimaryIP) ||
					(blueErr == nil && strings.Contains(blueInfo, constants.RedisRoleSlave))
				if blueLostMaster {
					h.log.Info("Green pod auto-promoted to master after blue primary lost ownership — counting as synced",
						"greenPod", mapping.GreenPodName,
						"greenNodeID", mapping.GreenNodeID,
						"bluePrimaryIP", mapping.BluePrimaryIP,
						"slots", mapping.SlotsRanges)
					syncedCount++
					continue
				}
				h.log.Info("Green pod lost replica role after crash — re-joining cluster and replicating",
					"greenPod", mapping.GreenPodName,
					"greenPodIP", mapping.GreenPodIP,
					"bluePrimaryIP", mapping.BluePrimaryIP,
					"bluePrimaryID", mapping.BluePrimaryID)
				cluster.Toolbox.MeetAddress(ctx, mapping.GreenPodIP, mapping.BluePrimaryIP, cluster.RedisPort)
				if replErr := cluster.Toolbox.ReplicateOnIP(mapping.GreenPodIP, mapping.BluePrimaryID); replErr != nil {
					h.log.Error(replErr, "Failed to re-replicate green pod",
						"greenPod", mapping.GreenPodName,
						"bluePrimaryID", mapping.BluePrimaryID)
				}
			}
			h.log.Info("Shard not yet synced",
				"greenPod", mapping.GreenPodName,
				"bluePrimary", mapping.BluePrimaryIP,
				"slots", mapping.SlotsRanges)
			allSynced = false
		} else {
			syncedCount++
		}
	}

	_ = h.metrics.Gauge(metrics.GaugeBlueGreenShardsSynced, float64(syncedCount),
		fmt.Sprintf("total:%d", totalShards))

	now := time.Now().Unix()
	if int32(syncedCount) > bgStatus.LastSyncedCount {
		bgStatus.LastSyncedCount = int32(syncedCount)
		bgStatus.LastProgressTime = now
	}
	if bgStatus.LastProgressTime == 0 {
		bgStatus.LastProgressTime = now
	}
	stallDuration := time.Since(time.Unix(bgStatus.LastProgressTime, 0))
	if stallDuration > constants.BlueGreenSyncStallTimeout {
		return ctrl.Result{}, health.Error,
			fmt.Errorf("blue/green sync stalled: no shard made progress for %s (stuck at %d/%d)",
				stallDuration.Truncate(time.Second), syncedCount, totalShards)
	}

	elapsed := time.Since(time.Unix(bgStatus.SyncStartTime, 0))

	if !allSynced {
		bgStatus.Message = fmt.Sprintf("Waiting for replication sync (%d/%d shards, elapsed: %s)",
			syncedCount, totalShards, elapsed.Truncate(time.Second))
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.BlueGreenSyncCheckDelay}, health.Warn, nil
	}

	h.log.Info("All shards synced, triggering pre-promotion backup")
	didSafetyBackup, safetyErr := h.deps.RunBackup(ctx, cluster)
	if safetyErr != nil {
		h.log.Error(safetyErr, "Pre-promotion safety backup failed (non-fatal, proceeding)")
	}
	if didSafetyBackup {
		cluster.RedisCluster.Status.LastBackupTime = time.Now().Unix()
	}

	syncDuration := time.Since(time.Unix(bgStatus.SyncStartTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, syncDuration, "phase:waiting_sync")

	setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, "Promoting green replicas to primaries via CLUSTER FAILOVER (zero downtime)")
	bgStatus.LastSyncedCount = 0
	bgStatus.LastProgressTime = 0
	bgStatus.SyncStartTime = time.Now().Unix()
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		return ctrl.Result{}, health.Error, err
	}

	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 5: Promote green replicas to primaries (CLUSTER FAILOVER)
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgPromote(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen
	viewCtx, cancelView := context.WithTimeout(ctx, blueGreenClusterViewTimeout)
	defer cancelView()

	// Count promotion by slot ownership, not role:master.
	byID, ipToID, viewErr := h.clusterNodesView(viewCtx, bgStatus, cluster.Toolbox)

	var toPromote []cachev1.ShardMappingEntry
	alreadyPromotedIDs := make(map[string]bool)
	for _, mapping := range bgStatus.ShardMapping {
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		info, err := cluster.Toolbox.GetReplicationInfoContext(probeCtx, mapping.GreenPodIP)
		cancelProbe()
		if err != nil {
			if viewErr == nil && isHealthyPrimary(byID, ipToID, mapping.GreenNodeID, mapping.GreenPodIP, mapping.SlotsRanges) {
				if mapping.GreenNodeID != "" {
					alreadyPromotedIDs[mapping.GreenNodeID] = true
				}
			} else {
				h.log.Info("Deferring promotion: green pod state is unavailable",
					"pod", mapping.GreenPodName, "err", err)
			}
			continue
		}
		if isPromoted(byID, ipToID, viewErr, mapping, info) {
			if mapping.GreenNodeID != "" {
				alreadyPromotedIDs[mapping.GreenNodeID] = true
			}
		} else if isSlave, masterHost, masterSlots := isSlaveOfGreenPod(info, bgStatus, mapping.SlotsRanges); isSlave {
			h.log.Info("Green pod is slave of green pod owning same slots — skipping",
				"pod", mapping.GreenPodName, "slots", mapping.SlotsRanges,
				"masterHost", masterHost, "masterSlots", masterSlots)
		} else if viewErr == nil && strings.Contains(info, constants.RedisRoleMaster) &&
			!isHealthyPrimary(byID, ipToID, mapping.GreenNodeID, mapping.GreenPodIP, mapping.SlotsRanges) {
			// FAILOVER cannot fix a blank master; maintenance must rejoin it.
			h.log.Info("Green pod is a blank/degraded master — deferring to maintenance (pending rejoin)",
				"pod", mapping.GreenPodName, "slots", mapping.SlotsRanges)
			_ = h.metrics.CountBadSituation("bluegreen_pending_rejoin", "pod:"+mapping.GreenPodName)
		} else {
			if masterSlots != "" {
				// Replica migration moved this replica to a green master with different slots.
				h.log.Info("Green pod is slave of green pod but owns different slots — must promote",
					"pod", mapping.GreenPodName, "slots", mapping.SlotsRanges,
					"masterHost", masterHost, "masterSlots", masterSlots)
				_ = h.metrics.CountBadSituation("bluegreen_replica_migration_detected")
			}
			toPromote = append(toPromote, mapping)
		}
	}

	if len(toPromote) == 0 {
		return h.finishPromotion(ctx, viewCtx, cluster, bgStatus)
	}

	promotedCount := int32(len(alreadyPromotedIDs))
	if bgStatus.LastProgressTime == 0 {
		bgStatus.LastProgressTime = time.Now().Unix()
		bgStatus.LastSyncedCount = promotedCount
	} else if promotedCount > bgStatus.LastSyncedCount {
		bgStatus.LastSyncedCount = promotedCount
		bgStatus.LastProgressTime = time.Now().Unix()
	} else if bgStatus.LastProgressTime > 0 {
		stall := time.Since(time.Unix(bgStatus.LastProgressTime, 0))
		if stall > constants.BlueGreenPromoteStallTimeout {
			if bgStatus.ResumeForwardAttempts < int32(constants.BlueGreenTakeoverAttemptThreshold) {
				bgStatus.ResumeForwardAttempts++
				h.log.Info("Promotion stalled, escalating failover mode",
					"stall", stall.Truncate(time.Second),
					"resumeForwardAttempts", bgStatus.ResumeForwardAttempts,
					"promoted", promotedCount, "remaining", len(toPromote))
				_ = h.metrics.CountBadSituation("bluegreen_promote_stall_escalation")
			}
		}
	}

	if len(alreadyPromotedIDs) > 0 && !h.topologyConverged(viewCtx, cluster, alreadyPromotedIDs, bgStatus) {
		bgStatus.Message = "Waiting for topology convergence before next batch"
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.BlueGreenInterShardFailoverDelay}, health.Warn, nil
	}

	useForce := bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenForceAttemptThreshold)
	useTakeover := bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenTakeoverAttemptThreshold)
	failoverMode := "soft"
	if useTakeover {
		failoverMode = "TAKEOVER"
	} else if useForce {
		failoverMode = "FORCE"
	}

	batchSize := len(bgStatus.ShardMapping) * constants.BlueGreenMaxBatchKeyspacePercent / 100
	if batchSize < 1 {
		batchSize = 1
	}
	if batchSize > len(toPromote) {
		batchSize = len(toPromote)
	}
	batch := toPromote[:batchSize]

	h.log.Info("Failover batch starting",
		"batchSize", len(batch),
		"toPromote", len(toPromote),
		"alreadyPromoted", len(alreadyPromotedIDs),
		"mode", failoverMode)

	for _, m := range batch {
		pod := &corev1.Pod{}
		podKey := types.NamespacedName{Namespace: cluster.RedisCluster.Namespace, Name: m.GreenPodName}
		if err := h.deps.Get(ctx, podKey, pod); err != nil {
			h.log.Error(err, "Failed to get green pod for labeling", "pod", m.GreenPodName)
			continue
		}
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		constants.ActivateRedisCluster(pod.Labels)
		if err := h.deps.Update(ctx, pod); err != nil {
			h.log.Error(err, "Failed to label green pod before failover", "pod", m.GreenPodName)
		}
	}

	var wg sync.WaitGroup
	for _, mapping := range batch {
		wg.Add(1)
		go func(m cachev1.ShardMappingEntry) {
			defer wg.Done()
			failoverCtx, cancelFailover := context.WithTimeout(ctx, blueGreenRedisProbeTimeout)
			defer cancelFailover()
			greenNode := &redis_cluster.ClusterNode{
				IP:      m.GreenPodIP,
				ID:      m.GreenNodeID,
				PodName: m.GreenPodName,
			}
			_ = h.metrics.Count(counters.BlueGreenFailoverAttempted)
			if err := cluster.Toolbox.FailoverContext(failoverCtx, greenNode, useForce, useTakeover); err != nil {
				h.log.Error(err, "CLUSTER FAILOVER failed",
					"pod", m.GreenPodName, "mode", failoverMode)
			}
		}(mapping)
	}
	wg.Wait()

	for poll := 0; poll < constants.BlueGreenFailoverPollRetries; poll++ {
		time.Sleep(constants.BlueGreenFailoverPollDelay)
		pByID, pIPToID, pViewErr := h.clusterNodesView(viewCtx, bgStatus, cluster.Toolbox)
		batchDone := 0
		for _, m := range batch {
			probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
			info, err := cluster.Toolbox.GetReplicationInfoContext(probeCtx, m.GreenPodIP)
			cancelProbe()
			if err == nil && isPromoted(pByID, pIPToID, pViewErr, m, info) {
				batchDone++
			}
		}
		if batchDone == len(batch) {
			h.log.Info("Batch failovers confirmed", "confirmed", batchDone)
			_ = h.metrics.Count(counters.BlueGreenFailoverSucceeded)
			_ = h.metrics.Gauge(metrics.GaugeBlueGreenShardsSynced,
				float64(len(alreadyPromotedIDs)+batchDone),
				fmt.Sprintf("total:%d", len(bgStatus.ShardMapping)), "stage:failover_confirmed")
			break
		}
		if poll == constants.BlueGreenFailoverPollRetries-1 {
			h.log.Info("Batch failover timeout, proceeding",
				"confirmed", batchDone, "expected", len(batch))
		}
	}

	elapsed := time.Since(time.Unix(bgStatus.StartTime, 0))
	bgStatus.Message = fmt.Sprintf("Promoting via FAILOVER %s (%d/%d promoted, elapsed: %s)",
		failoverMode, len(alreadyPromotedIDs)+len(batch), len(bgStatus.ShardMapping), elapsed.Truncate(time.Second))
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Status update conflict during promotion progress, will retry immediately")
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}
	return ctrl.Result{RequeueAfter: constants.BlueGreenInterShardFailoverDelay}, health.Warn, nil
}

func (h *Handler) finishPromotion(ctx, viewCtx context.Context, cluster *redis_cluster.Cluster, bgStatus *cachev1.BlueGreenStatus) (ctrl.Result, health.ClusterHealth, error) {
	byID, ipToID, viewErr := h.clusterNodesView(viewCtx, bgStatus, cluster.Toolbox)

	allPromoted := 0
	for _, mapping := range bgStatus.ShardMapping {
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		info, err := cluster.Toolbox.GetReplicationInfoContext(probeCtx, mapping.GreenPodIP)
		cancelProbe()
		if err != nil {
			continue
		}
		if isPromoted(byID, ipToID, viewErr, mapping, info) {
			allPromoted++
		} else if isSlave, _, _ := isSlaveOfGreenPod(info, bgStatus, mapping.SlotsRanges); isSlave {
			allPromoted++
		}
	}
	if allPromoted < len(bgStatus.ShardMapping) {
		elapsed := time.Since(time.Unix(bgStatus.StartTime, 0))
		bgStatus.Message = fmt.Sprintf("Promoting green replicas (%d/%d confirmed, elapsed: %s)",
			allPromoted, len(bgStatus.ShardMapping), elapsed.Truncate(time.Second))
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.BlueGreenFailoverDelay}, health.Warn, nil
	}

	_ = h.metrics.Gauge(metrics.GaugeBlueGreenShardsSynced, float64(len(bgStatus.ShardMapping)),
		fmt.Sprintf("total:%d", len(bgStatus.ShardMapping)), "stage:promote")

	allPromotedIDs := make(map[string]bool, len(bgStatus.ShardMapping))
	for _, m := range bgStatus.ShardMapping {
		if m.GreenNodeID == "" {
			continue
		}
		probeCtx, cancelProbe := context.WithTimeout(viewCtx, blueGreenRedisProbeTimeout)
		info, _ := cluster.Toolbox.GetReplicationInfoContext(probeCtx, m.GreenPodIP)
		cancelProbe()
		if isPromoted(byID, ipToID, viewErr, m, info) {
			allPromotedIDs[m.GreenNodeID] = true
		}
	}
	if !h.topologyConverged(viewCtx, cluster, allPromotedIDs, bgStatus) {
		h.broadcastClusterMeet(viewCtx, cluster, bgStatus, bgStatus.ShardMapping)
		bgStatus.Message = "All promoted — waiting for topology convergence"
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.BlueGreenInterShardFailoverDelay}, health.Warn, nil
	}

	promoteDuration := time.Since(time.Unix(bgStatus.SyncStartTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, promoteDuration, "phase:promoting")
	h.log.Info("All green nodes promoted and topology converged, advancing to SwitchingDNS",
		"promote_duration_ms", promoteDuration)

	setPhase(bgStatus, cachev1.BlueGreenPhaseSwitchingDNS, "All promoted, switching DNS to green cluster")
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Status update conflict advancing to SwitchingDNS, will retry immediately")
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 6: Switch DNS
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgSwitchDNS(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	if bgStatus.DrainStartedAt == 0 {
		bgStatus.DrainStartedAt = time.Now().Unix()
		bgStatus.Message = "Post-failover drain: blue pods still serving for MOVED redirects"
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, err
		}
		h.log.Info("Drain period started, requeueing", "duration", constants.BlueGreenClientRefreshDelay)
		return ctrl.Result{RequeueAfter: constants.BlueGreenClientRefreshDelay}, health.Warn, nil
	}

	elapsed := time.Since(time.Unix(bgStatus.DrainStartedAt, 0))
	if remaining := constants.BlueGreenClientRefreshDelay - elapsed; remaining > 0 {
		h.log.Info("Drain period still active", "remaining", remaining.Truncate(time.Second))
		return ctrl.Result{RequeueAfter: remaining}, health.Warn, nil
	}

	greenPods, err := h.getGreenPods(ctx, cluster.RedisCluster)
	if err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to get green pods for DNS switch")
	}
	// Fail closed unless slot ownership can be verified.
	byID, ipToID, viewErr := h.clusterNodesView(ctx, bgStatus, cluster.Toolbox)
	if viewErr != nil {
		h.log.Info("Deferring DNS switch: cannot read cluster view to verify slot ownership", "err", viewErr)
		_ = h.metrics.CountBadSituation("bluegreen_switchdns_view_unavailable")
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
	}
	shardByPod := make(map[string]cachev1.ShardMappingEntry, len(bgStatus.ShardMapping))
	greenPrimaryNames := make(map[string]bool, len(bgStatus.ShardMapping))
	for _, m := range bgStatus.ShardMapping {
		greenPrimaryNames[m.GreenPodName] = true
		shardByPod[m.GreenPodName] = m
	}
	h.log.Info("Setting role=active on green PRIMARY pods only (replicas wait for FormingCluster MEET)",
		"totalGreen", len(greenPods), "primaries", len(greenPrimaryNames))
	blocked := 0
	identityCtx, cancelIdentity := context.WithTimeout(ctx, blueGreenClusterViewTimeout)
	defer cancelIdentity()
	for i := range greenPods {
		pod := &greenPods[i]
		if !greenPrimaryNames[pod.Name] {
			continue
		}
		m, mapped := shardByPod[pod.Name]
		identityMatches := false
		if mapped {
			probeCtx, cancelProbe := context.WithTimeout(identityCtx, blueGreenRedisProbeTimeout)
			currentID, identityErr := cluster.Toolbox.GetNodeIDByIPContext(probeCtx, pod.Status.PodIP)
			cancelProbe()
			identityMatches = identityErr == nil && currentID == m.GreenNodeID &&
				isHealthyPrimary(byID, ipToID, currentID, pod.Status.PodIP, m.SlotsRanges)
		}
		if !identityMatches {
			h.log.Info("Refusing role=active: green pod does not own its slots (deferring, not serving empty)",
				"pod", pod.Name, "slots", m.SlotsRanges)
			_ = h.metrics.CountBadSituation("bluegreen_activate_blocked_not_owner")
			// Remove any previously active pod that lost its slots.
			if constants.IsRoleActive(pod.Labels) {
				constants.DeactivateRole(pod.Labels)
				if err := h.deps.Update(ctx, pod); err != nil {
					return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to deactivate unsafe green pod "+pod.Name)
				}
				h.log.Info("Deactivated previously-active green pod that lost its slots", "pod", pod.Name)
			}
			blocked++
			continue
		}
		if constants.IsRoleActive(pod.Labels) {
			continue
		}
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		constants.ActivateRole(pod.Labels)
		if err := h.deps.Update(ctx, pod); err != nil {
			return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to set role=active on green pod "+pod.Name)
		}
	}

	if blocked > 0 {
		// Do not advance toward blue deletion while a primary is unsafe to serve.
		h.log.Info("Deferring FormingCluster: some green primaries not safe to activate", "blocked", blocked)
		bgStatus.Message = fmt.Sprintf("SwitchingDNS: %d green primary(ies) do not own their slots — deferring", blocked)
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
	}

	h.log.Info("Drain complete, keeping blue pods in service until FORGET (ScalingDownBlue)")

	switchDNSDuration := time.Since(time.Unix(bgStatus.PhaseEntryTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, switchDNSDuration, "phase:switch_dns")
	h.log.Info("DNS switched: only green pods serving", "switch_dns_duration_ms", switchDNSDuration)

	setPhase(bgStatus, cachev1.BlueGreenPhaseFormingCluster, "Green primaries serving, MEET replicas before scaling down blue")
	bgStatus.DrainStartedAt = 0
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Status update conflict advancing to FormingCluster, will retry immediately")
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 8: Scale down blue (FORGET + scale to 0)
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgScaleDownBlue(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	if len(bgStatus.ShardMapping) == 0 {
		blueSTS, err := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, BlueSTSName(cluster.RedisCluster))
		if err != nil || blueSTS == nil || (blueSTS.Spec.Replicas != nil && *blueSTS.Spec.Replicas == 0) {
			h.log.Info("No shard mapping but blue STS already scaled down, advancing to TearingDown")
			setPhase(bgStatus, cachev1.BlueGreenPhaseTearingDown, "Blue already scaled down, tearing down")
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
		return ctrl.Result{}, health.Error, fmt.Errorf("no shard mapping in status")
	}

	blueSTS, blueSTSErr := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, BlueSTSName(cluster.RedisCluster))
	blueAlreadyGone := blueSTSErr != nil || blueSTS == nil || (blueSTS.Spec.Replicas != nil && *blueSTS.Spec.Replicas == 0)

	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenPodIP == "" {
			continue
		}
		info, err := cluster.Toolbox.GetReplicationInfo(mapping.GreenPodIP)
		if err != nil {
			if blueAlreadyGone {
				h.log.Info("Cannot reach green pod but blue is already gone, continuing forward",
					"pod", mapping.GreenPodName, "ip", mapping.GreenPodIP)
				continue
			}
			h.log.Error(err, "Cannot reach green pod to verify role, returning to Promoting",
				"pod", mapping.GreenPodName, "ip", mapping.GreenPodIP)
			setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, fmt.Sprintf("Green pod %s unreachable, returning to Promoting to verify/retry", mapping.GreenPodName))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
		isSlave, _, _ := isSlaveOfGreenPod(info, bgStatus, mapping.SlotsRanges)
		if !strings.Contains(info, constants.RedisRoleMaster) && !isSlave {
			if blueAlreadyGone {
				h.log.Info("Green node not yet primary but blue is already gone, continuing forward",
					"pod", mapping.GreenPodName, "ip", mapping.GreenPodIP)
				continue
			}
			h.log.Info("Green node not yet primary, returning to Promoting phase",
				"pod", mapping.GreenPodName, "ip", mapping.GreenPodIP)
			setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, fmt.Sprintf("Green pod %s still a replica, returning to Promoting", mapping.GreenPodName))
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
	}

	earlyCheckPods, _ := h.getBluePods(ctx, cluster.RedisCluster)
	bluePodCount := 0
	if earlyCheckPods != nil {
		for _, p := range earlyCheckPods.Items {
			if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending {
				bluePodCount++
			}
		}
	}
	if bluePodCount == 0 {
		h.log.Info("Blue pods already gone, proceeding to TearingDown")
		scaleDownDuration := time.Since(time.Unix(bgStatus.PhaseEntryTime, 0)).Milliseconds()
		_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, scaleDownDuration, "phase:scale_down_blue")
		setPhase(bgStatus, cachev1.BlueGreenPhaseTearingDown, "Blue pods gone, tearing down")
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			h.log.Error(err, "Status update conflict advancing to TearingDown, will retry immediately")
			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	if len(bgStatus.BlueNodeIDsToForget) == 0 {
		greenIP, _, err := h.getHealthyGreenIP(bgStatus.ShardMapping, cluster.Toolbox)
		if err != nil {
			return ctrl.Result{}, health.Error, utils.WrapErr(err, "no healthy green node for CLUSTER NODES")
		}
		_, clusterNodesStr, _, err := cluster.Toolbox.ClusterNodes(ctx, greenIP)
		if err != nil {
			return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to get CLUSTER NODES from green primary")
		}

		greenIPSet := make(map[string]bool)
		for _, m := range bgStatus.ShardMapping {
			if m.GreenPodIP != "" {
				greenIPSet[m.GreenPodIP] = true
			}
		}
		greenPodsList, gpErr := h.getGreenPods(ctx, cluster.RedisCluster)
		if gpErr == nil {
			for _, gp := range greenPodsList {
				if gp.Status.PodIP != "" {
					greenIPSet[gp.Status.PodIP] = true
				}
			}
		}

		clusterIPToNodeID, _ := ParseClusterNodesOutput(clusterNodesStr)

		// Never FORGET a known green node ID — its "myself" entry can report an
		// empty IP (no cluster-announce-ip) and otherwise be misread as blue.
		greenNodeIDs := make(map[string]bool)
		for _, m := range bgStatus.ShardMapping {
			if m.GreenNodeID != "" {
				greenNodeIDs[m.GreenNodeID] = true
			}
		}

		var blueNodeIDs []string
		for ip, nodeID := range clusterIPToNodeID {
			if greenNodeIDs[nodeID] {
				h.log.Info("Skipping green node ID from FORGET list (IP not matched as green)",
					"nodeID", nodeID, "ip", ip)
				continue
			}
			if !greenIPSet[ip] {
				blueNodeIDs = append(blueNodeIDs, nodeID)
			}
		}

		for i, m := range bgStatus.ShardMapping {
			if resolvedID, ok := clusterIPToNodeID[m.GreenPodIP]; ok && resolvedID != "" {
				bgStatus.ShardMapping[i].GreenNodeID = resolvedID
			}
		}

		bgStatus.BlueNodeIDsToForget = blueNodeIDs
		if len(blueNodeIDs) == 0 {
			// Nothing to FORGET → blue isn't in green's view → slot count is already
			// green-only. Mark forgotten so the slot-coverage gate still runs.
			h.log.Info("WARNING: No blue nodes found to forget - cluster may already be clean or in unexpected state",
				"greenIPCount", len(greenIPSet))
			bgStatus.BlueNodesForgotten = true
		} else {
			h.log.Info("Identified blue nodes to forget",
				"blueCount", len(blueNodeIDs),
				"greenIPCount", len(greenIPSet))
		}

		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to save BlueNodeIDsToForget")
		}
	}

	bName := BlueSTSName(cluster.RedisCluster)
	blueSTS, err := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, bName)
	if err != nil {
		h.log.Error(err, "Failed to get blue StatefulSet")
	} else if blueSTS != nil && blueSTS.Spec.Replicas != nil && *blueSTS.Spec.Replicas > 0 {
		if len(bgStatus.BlueNodeIDsToForget) > 0 && !bgStatus.BlueNodesForgotten {
			allGreenIPs := make([]string, 0, len(bgStatus.ShardMapping))
			for _, m := range bgStatus.ShardMapping {
				allGreenIPs = append(allGreenIPs, m.GreenPodIP)
			}
			greenPodsList, gpErr := h.getGreenPods(ctx, cluster.RedisCluster)
			if gpErr == nil {
				greenIPSet := make(map[string]bool)
				for _, ip := range allGreenIPs {
					greenIPSet[ip] = true
				}
				for _, gp := range greenPodsList {
					if gp.Status.PodIP != "" && !greenIPSet[gp.Status.PodIP] {
						allGreenIPs = append(allGreenIPs, gp.Status.PodIP)
					}
				}
			}

			var validBlueIDs []string
			for _, blueID := range bgStatus.BlueNodeIDsToForget {
				if len(blueID) == 40 {
					validBlueIDs = append(validBlueIDs, blueID)
				} else {
					h.log.Info("Skipping invalid blue node ID", "blueID", blueID, "length", len(blueID))
				}
			}

			// 1) FORGET → removes blue from green CLUSTER SLOTS
			// 2) Immediately KILL+PAUSE (batched) → drops blue connections
			//    before blue can return stale CLUSTER SLOTS (prevents nil wipe)
			// 3) Scale to 0
			h.log.Info("FORGET all blue nodes from all green nodes",
				"blueNodes", len(validBlueIDs), "greenNodes", len(allGreenIPs))
			totalSuccess := 0
			var allFailedIPs []string
			for _, blueID := range validBlueIDs {
				success, failedIPs := cluster.Toolbox.ForgetNodeFromIPs(allGreenIPs, cluster.RedisPort, blueID)
				totalSuccess += success
				allFailedIPs = append(allFailedIPs, failedIPs...)
			}
			h.log.Info("FORGET complete",
				"totalSuccess", totalSuccess, "totalFailed", len(allFailedIPs))
			_ = h.metrics.Gauge(metrics.GaugeBlueGreenForgetResult, float64(totalSuccess), "result:success")
			_ = h.metrics.Gauge(metrics.GaugeBlueGreenForgetResult, float64(len(allFailedIPs)), "result:failed")

			// KILL+PAUSE drops blue connections before it reacts to gossip.
			// Convergence is verified by the gate below before scale-to-0.
			bluePods, bpErr := h.getBluePods(ctx, cluster.RedisCluster)
			if bpErr == nil && bluePods != nil {
				var blueIPs []string
				for _, bp := range bluePods.Items {
					if bp.Status.PodIP != "" {
						blueIPs = append(blueIPs, bp.Status.PodIP)
					}
				}
				if len(blueIPs) > 0 {
					h.log.Info("CLIENT KILL on blue pods (batched)", "count", len(blueIPs))
					for _, ip := range blueIPs {
						_, _, _, _ = cluster.Toolbox.ClientKillLocal(ctx, ip)
					}
					h.log.Info("CLIENT PAUSE on blue pods (batched)", "count", len(blueIPs))
					for _, ip := range blueIPs {
						_, _, _, _ = cluster.Toolbox.ClientPause(ctx, ip, constants.BlueGreenClientPauseTimeout)
					}
				}
			}

			bgStatus.BlueNodesForgotten = true
			// Stamp the convergence clock at FORGET completion (not phase entry):
			// DrainStartedAt is reused here as the convergence-start timestamp.
			bgStatus.DrainStartedAt = time.Now().Unix()
			if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
				return ctrl.Result{}, health.Error, err
			}
		}

		// Safety gate: green must cover all slots before blue is deleted.
		// Post-FORGET green's slot count excludes blue, so a gap means data loss.
		if bgStatus.BlueNodesForgotten {
			if greenSlots := h.countGreenOnlySlots(ctx, cluster.Toolbox, bgStatus); greenSlots < 0 {
				_ = h.metrics.CountBadSituation("bluegreen_pre_delete_slot_gap")
				return ctrl.Result{}, health.Error, fmt.Errorf(
					"aborting blue delete: unable to determine green-only slot coverage (no green node responded to CLUSTER INFO)")
			} else if greenSlots != int64(constants.TotalSlots) {
				_ = h.metrics.CountBadSituation("bluegreen_pre_delete_slot_gap")
				return ctrl.Result{}, health.Error, fmt.Errorf(
					"aborting blue delete: green-only slot coverage is %d/%d — blue deletion would lose data",
					greenSlots, constants.TotalSlots)
			}
			h.log.Info("Pre-delete safety check passed: green covers all slots", "slots", constants.TotalSlots)
		}

		// Convergence gate: block until every green primary has dropped the
		// forgotten blue IDs (else it routes to a dead node), or timeout.
		if bgStatus.BlueNodesForgotten && len(bgStatus.BlueNodeIDsToForget) > 0 {
			greenPrimaryIPs := make([]string, 0, len(bgStatus.ShardMapping))
			for _, m := range bgStatus.ShardMapping {
				if m.GreenPodIP != "" {
					greenPrimaryIPs = append(greenPrimaryIPs, m.GreenPodIP)
				}
			}
			// Start the convergence clock if unset (forgotten flag from older code).
			if bgStatus.DrainStartedAt == 0 {
				bgStatus.DrainStartedAt = time.Now().Unix()
				_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			}
			converged, stillPresentOn := h.forgetConverged(ctx, cluster.Toolbox, greenPrimaryIPs, bgStatus.BlueNodeIDsToForget)
			if !converged {
				elapsed := time.Since(time.Unix(bgStatus.DrainStartedAt, 0))
				if elapsed < constants.BlueGreenForgetConvergenceTimeout {
					h.log.Info("FORGET not yet converged on all green primaries — waiting before scaling down blue",
						"stillPresentOn", stillPresentOn, "elapsed", elapsed.Truncate(time.Second))
					_ = h.metrics.CountBadSituation("bluegreen_forget_not_converged")
					bgStatus.Message = fmt.Sprintf("Waiting for FORGET convergence (%d green nodes still see blue)", len(stillPresentOn))
					_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
					return ctrl.Result{RequeueAfter: constants.BlueGreenInterForgetDelay}, health.Warn, nil
				}
				h.log.Info("FORGET convergence timed out, proceeding with scale-down anyway",
					"stillPresentOn", stillPresentOn, "timeout", constants.BlueGreenForgetConvergenceTimeout)
				_ = h.metrics.CountBadSituation("bluegreen_forget_convergence_timeout")
			} else {
				h.log.Info("FORGET convergence confirmed — all green primaries dropped blue IDs")
			}
		}

		h.log.Info("Scaling blue StatefulSet to 0", "blueSTS", bName)
		if err := h.scaleStatefulSet(ctx, cluster.RedisCluster.Namespace, bName, 0); err != nil {
			return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to scale blue STS to 0")
		}
		bgStatus.Message = "Blue StatefulSet scaled to 0, waiting for pods to terminate"
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, err
		}
		return ctrl.Result{RequeueAfter: constants.ShortDelay}, health.Warn, nil
	}

	bluePods, err := h.getBluePods(ctx, cluster.RedisCluster)
	if err != nil {
		h.log.Error(err, "Failed to get blue pods, will retry")
		return ctrl.Result{RequeueAfter: constants.ShortDelay}, health.Warn, nil
	}
	if bluePods != nil && len(bluePods.Items) > 0 {
		runningCount := 0
		for _, p := range bluePods.Items {
			if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending {
				runningCount++
			}
		}
		if runningCount > 0 {
			h.deleteOrphanedPods(ctx, cluster)

			h.log.Info("Waiting for blue pods to terminate", "remaining", runningCount)
			bgStatus.Message = fmt.Sprintf("Waiting for %d blue pods to terminate", runningCount)
			if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
				return ctrl.Result{}, health.Error, err
			}
			return ctrl.Result{RequeueAfter: constants.ShortDelay}, health.Warn, nil
		}
	}

	scaleDownDuration := time.Since(time.Unix(bgStatus.PhaseEntryTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, scaleDownDuration, "phase:scale_down_blue")
	h.log.Info("Blue pods terminated, proceeding to TearingDown",
		"scale_down_duration_ms", scaleDownDuration)
	setPhase(bgStatus, cachev1.BlueGreenPhaseTearingDown, "Blue scaled down, tearing down")
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Status update conflict advancing to TearingDown, will retry immediately")
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 7: Form green cluster (MEET + REPLICATE replicas)
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgFormGreenCluster(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen
	if len(bgStatus.ShardMapping) == 0 {
		return ctrl.Result{}, health.Error, fmt.Errorf("no shard mapping in status")
	}

	h.log.Info("FormingCluster: starting",
		"shardMappingLen", len(bgStatus.ShardMapping),
		"blueNodeIDsToForget", len(bgStatus.BlueNodeIDsToForget))

	firstGreenIP, _, err := h.getHealthyGreenIP(bgStatus.ShardMapping, cluster.Toolbox)
	if err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "no healthy green node for FormingCluster")
	}

	greenPods, err := h.getGreenPods(ctx, cluster.RedisCluster)
	if err != nil {
		return ctrl.Result{}, health.Error, err
	}
	sort.Slice(greenPods, func(i, j int) bool {
		return GetPodIndex(greenPods[i].Name) < GetPodIndex(greenPods[j].Name)
	})

	numPrimaries := len(bgStatus.ShardMapping)
	replicasPerPrimary := int(cluster.RedisCluster.Spec.Replicas)

	greenPrimaryIDs := make([]string, numPrimaries)
	for i, m := range bgStatus.ShardMapping {
		greenPrimaryIDs[i] = m.GreenNodeID
	}
	for i, id := range greenPrimaryIDs {
		if id == "" {
			return ctrl.Result{}, health.Error, fmt.Errorf(
				"green primary %s has no node ID in shard mapping (index %d); cannot assign replicas",
				bgStatus.ShardMapping[i].GreenPodName, i)
		}
	}

	_, postForgetNodes, _, err := cluster.Toolbox.ClusterNodes(ctx, firstGreenIP)
	if err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to get CLUSTER NODES for replica MEET")
	}
	// Build node state from CLUSTER NODES for idempotency checks
	nodesInCluster := make(map[string]bool)
	nodeIPToRole := make(map[string]string)
	nodeIPToMaster := make(map[string]string)
	for _, line := range strings.Split(postForgetNodes, "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) >= 2 {
			ip := strings.Split(fields[1], ":")[0]
			nodesInCluster[ip] = true
			if len(fields) >= 4 {
				flags := fields[2]
				if strings.Contains(flags, "slave") {
					nodeIPToRole[ip] = "slave"
					nodeIPToMaster[ip] = fields[3]
				} else if strings.Contains(flags, "master") {
					nodeIPToRole[ip] = "master"
				}
			}
		}
	}

	if len(greenPods) > numPrimaries && replicasPerPrimary > 0 {
		h.log.Info("Adding green replica pods to cluster (batched by replica index)",
			"replicaPods", len(greenPods)-numPrimaries, "replicasPerPrimary", replicasPerPrimary)

		type replicaInfo struct {
			pod       *corev1.Pod
			primaryID string
		}

		var pendingReplicas []replicaInfo
		replicasAdded := 0
		for i := numPrimaries; i < len(greenPods); i++ {
			replicaPod := greenPods[i]
			if replicaPod.Status.Phase != corev1.PodRunning || replicaPod.Status.PodIP == "" {
				h.log.Info("Skipping non-running green replica pod", "pod", replicaPod.Name)
				continue
			}

			primaryIdx := replicasAdded / replicasPerPrimary
			if primaryIdx >= numPrimaries {
				primaryIdx = primaryIdx % numPrimaries
			}
			primaryID := greenPrimaryIDs[primaryIdx]

			if nodesInCluster[replicaPod.Status.PodIP] {
				if nodeIPToRole[replicaPod.Status.PodIP] == "slave" && nodeIPToMaster[replicaPod.Status.PodIP] == primaryID {
					replicasAdded++
					continue
				}
			}

			pendingReplicas = append(pendingReplicas, replicaInfo{pod: &greenPods[i], primaryID: primaryID})
			replicasAdded++
		}

		if len(pendingReplicas) > 0 {
			var batches [][]replicaInfo
			if len(pendingReplicas) < constants.BlueGreenSmartBatchThreshold {
				batches = [][]replicaInfo{pendingReplicas}
			} else {
				batches = make([][]replicaInfo, replicasPerPrimary)
				for i, r := range pendingReplicas {
					batchIdx := i % replicasPerPrimary
					batches[batchIdx] = append(batches[batchIdx], r)
				}
			}

			for batchNum, batch := range batches {
				if len(batch) == 0 {
					continue
				}
				h.log.Info("Processing replica batch",
					"batch", batchNum+1, "totalBatches", len(batches),
					"replicasInBatch", len(batch))

				for _, r := range batch {
					_, _, _, err := cluster.Toolbox.MeetAddress(ctx, firstGreenIP, r.pod.Status.PodIP, cluster.RedisPort)
					if err != nil {
						return ctrl.Result{}, health.Error, utils.WrapErr(err,
							fmt.Sprintf("failed to CLUSTER MEET replica %s", r.pod.Name))
					}
					_, _, _, pauseErr := cluster.Toolbox.ClientPause(ctx, r.pod.Status.PodIP, constants.BlueGreenClientPauseTimeout)
					if pauseErr != nil {
						h.log.Info("CLIENT PAUSE on replica failed (non-fatal)", "pod", r.pod.Name, "err", pauseErr)
					} else {
						h.log.Info("CLIENT PAUSE on freshly-MEETed replica", "pod", r.pod.Name)
					}
				}

				h.log.Info("Batch MEET complete, replicas paused, waiting for gossip",
					"batch", batchNum+1, "count", len(batch),
					"delay", constants.BlueGreenGossipSettleDelay.String())
				time.Sleep(constants.BlueGreenGossipSettleDelay)

				replicateFailures := 0
				for _, r := range batch {
					err := cluster.Toolbox.ReplicateOnIP(r.pod.Status.PodIP, r.primaryID)
					if err != nil {
						h.log.Info("CLUSTER REPLICATE failed (will retry next reconcile)",
							"pod", r.pod.Name, "primaryID", r.primaryID, "err", err)
						replicateFailures++
						continue
					}
					_, _, _, unpauseErr := cluster.Toolbox.ClientUnpause(ctx, r.pod.Status.PodIP)
					if unpauseErr != nil {
						h.log.Info("CLIENT UNPAUSE failed (non-fatal)", "pod", r.pod.Name, "err", unpauseErr)
					} else {
						h.log.Info("CLIENT UNPAUSE after REPLICATE", "pod", r.pod.Name)
					}
				}

				h.log.Info("Batch complete",
					"batch", batchNum+1, "met", len(batch),
					"replicated", len(batch)-replicateFailures)
			}
		}
	}

	h.log.Info("Verifying green cluster health")

	_, clusterInfo, _, err := cluster.Toolbox.ClusterInfo(ctx, firstGreenIP)
	allOK := err == nil
	if allOK {
		if !strings.Contains(clusterInfo, "cluster_state:ok") {
			h.log.Info("Green cluster state not OK yet")
			allOK = false
		}
		slotsAssigned := ParseInfoField(clusterInfo, "cluster_slots_assigned")
		if slotsAssigned != int64(constants.TotalSlots) {
			h.log.Info("Green cluster missing slots",
				"assigned", slotsAssigned, "expected", constants.TotalSlots)
			allOK = false
		}
	}

	if !allOK {
		bgStatus.Message = "Green cluster verification — waiting for convergence"
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return ctrl.Result{RequeueAfter: constants.ShortSleepTime}, health.Warn, nil
	}

	formingDuration := time.Since(time.Unix(bgStatus.PhaseEntryTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenPhaseDuration, formingDuration, "phase:forming_cluster")
	h.log.Info("Green cluster verified (all 16384 slots, cluster_state:ok), proceeding to scale down blue",
		"forming_cluster_duration_ms", formingDuration)

	setPhase(bgStatus, cachev1.BlueGreenPhaseScalingDownBlue, "Green cluster formed, scaling down blue")
	if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
		h.log.Error(err, "Status update conflict advancing to ScalingDownBlue, will retry immediately")
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase 9: Tear down blue
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgTeardown(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	gName := GreenSTSName(cluster.RedisCluster)

	greenSTS, err := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, gName)
	if err == nil && greenSTS != nil && !constants.IsRoleActive(greenSTS.Spec.Template.Labels) {
		constants.ActivateRole(greenSTS.Spec.Template.Labels)
		if err := h.deps.Update(ctx, greenSTS); err != nil {
			h.log.Error(err, "Failed to update green STS template to role=active (non-fatal)")
		} else {
			h.log.Info("Updated green STS template to role=active", "sts", gName)
		}
	}

	totalDuration := time.Since(time.Unix(bgStatus.StartTime, 0)).Milliseconds()
	_ = h.metrics.Timing(metrics.TimingBlueGreenDuration, totalDuration)
	_ = h.metrics.Count(counters.BlueGreenCompleted)

	h.log.Info("Blue/green deployment complete", "duration_ms", totalDuration, "greenSTS", gName)

	// Replica migration is re-enabled inside resetBGToNone (single exit point).
	if err := h.resetBGToNone(ctx, cluster, gName,
		fmt.Sprintf("Completed in %s", time.Duration(totalDuration)*time.Millisecond)); err != nil {
		return ctrl.Result{}, health.Error, err
	}

	return ctrl.Result{}, health.OK, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Failed phase: cleanup & rollback
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) bgCleanupFailed(ctx context.Context, cluster *redis_cluster.Cluster) (ctrl.Result, health.ClusterHealth, error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	h.refreshShardMappingIPs(ctx, bgStatus, cluster.RedisCluster)

	h.log.Info("Blue/green deployment failed, evaluating cleanup strategy",
		"greenSTS", GreenSTSName(cluster.RedisCluster),
		"message", bgStatus.Message)

	if reset, resetErr := h.bgResetIfGreenSTSGone(ctx, cluster); reset {
		return ctrl.Result{Requeue: true}, health.Warn, resetErr
	}

	if bgStatus.ResumeForwardAttempts < int32(constants.BlueGreenConsistencyGateAttempts) {
		if ok, reason := h.bgClusterConsistencyCheck(ctx, bgStatus, cluster.Toolbox); !ok {
			h.log.Info("Cluster inconsistent during cleanup — waiting for partition to heal",
				"reason", reason, "resumeAttempts", bgStatus.ResumeForwardAttempts)
			bgStatus.Message = fmt.Sprintf("Cleanup paused: %s", reason)
			_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
			return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn, nil
		}
	} else {
		h.log.Info("Skipping consistency check — already attempted resume-forward, proceeding with best-effort decision",
			"resumeAttempts", bgStatus.ResumeForwardAttempts)
	}

	allGreenPrimaries := true
	greenMasterCount := 0
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenPodIP == "" {
			allGreenPrimaries = false
			break
		}
		info, err := cluster.Toolbox.GetReplicationInfo(mapping.GreenPodIP)
		if err != nil || !strings.Contains(info, constants.RedisRoleMaster) {
			allGreenPrimaries = false
			break
		}
		greenMasterCount++
	}
	if allGreenPrimaries && greenMasterCount > 0 && greenMasterCount == len(bgStatus.ShardMapping) {
		h.log.Info("All green nodes are primaries — cluster is healthy, skipping to SwitchingDNS",
			"greenMasters", greenMasterCount)
		setPhase(bgStatus, cachev1.BlueGreenPhaseSwitchingDNS, "All green nodes are primaries, switching DNS before cleanup")
		bgStatus.ResumeForwardAttempts = 0
		bgStatus.StartTime = time.Now().Unix()
		if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
			return ctrl.Result{}, health.Error, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
	}

	unreachableCount := 0
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenPodIP == "" {
			unreachableCount++
			continue
		}
		info, err := cluster.Toolbox.GetReplicationInfo(mapping.GreenPodIP)
		if err != nil {
			unreachableCount++
			continue
		}
		if strings.Contains(info, constants.RedisRoleMaster) {
			_ = h.metrics.CountBadSituation("bluegreen_resume_forward_after_failure")

			bgStatus.ResumeForwardAttempts++

			if bgStatus.ResumeForwardAttempts > int32(constants.BlueGreenMaxResumeForwardAttempts) {
				h.log.Error(nil, "Exceeded max resume-forward attempts — manual intervention required",
					"attempts", bgStatus.ResumeForwardAttempts,
					"max", constants.BlueGreenMaxResumeForwardAttempts)
				bgStatus.Message = fmt.Sprintf("Exceeded max resume-forward attempts (%d/%d) — manual intervention required",
					bgStatus.ResumeForwardAttempts, constants.BlueGreenMaxResumeForwardAttempts)
				_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
				return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Error,
					fmt.Errorf("exceeded max resume-forward attempts (%d), manual intervention required", bgStatus.ResumeForwardAttempts)
			}

			failoverMode := "soft"
			if bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenTakeoverAttemptThreshold) {
				failoverMode = "TAKEOVER"
			} else if bgStatus.ResumeForwardAttempts >= int32(constants.BlueGreenForceAttemptThreshold) {
				failoverMode = "FORCE"
			}

			h.log.Info("Green node is a primary — resuming forward to Promoting phase (escalated)",
				"pod", mapping.GreenPodName, "ip", mapping.GreenPodIP,
				"attempt", bgStatus.ResumeForwardAttempts,
				"nextFailoverMode", failoverMode)

			setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, fmt.Sprintf("Resumed forward to Promoting (attempt %d, mode: %s): green owns data",
				bgStatus.ResumeForwardAttempts, failoverMode))
			bgStatus.StartTime = time.Now().Unix()
			bgStatus.LastSyncedCount = 0
			bgStatus.LastProgressTime = 0
			if err := h.deps.UpdateStatus(ctx, cluster.RedisCluster); err != nil {
				return ctrl.Result{}, health.Error, err
			}
			return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
		}
	}

	if unreachableCount > 0 {
		h.log.Error(nil, "Green pod ownership is unknown — refusing to rollback",
			"unreachable", unreachableCount)
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Error,
			fmt.Errorf("%d green pods have unknown ownership, manual intervention required", unreachableCount)
	}

	h.log.Info("No green primaries detected — safe to rollback")

	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenNodeID != "" {
			greenNode := &redis_cluster.ClusterNode{
				ID:      mapping.GreenNodeID,
				IP:      mapping.GreenPodIP,
				PodName: mapping.GreenPodName,
			}
			h.deps.ForgetNode(ctx, cluster, greenNode)
		}
	}

	gName := GreenSTSName(cluster.RedisCluster)
	h.log.Info("Operator deleting green resources during rollback (after FORGET)",
		"greenSTS", gName)
	if err := h.deps.DeleteGreenResources(ctx, cluster.RedisCluster.Namespace, gName); err != nil {
		h.log.Error(err, "Failed to clean up green resources (will retry)")
		return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Error,
			utils.WrapErr(err, "failed to delete green resources during cleanup")
	}

	_ = h.metrics.Count(counters.BlueGreenFailed)

	survivingSTS := BlueSTSName(cluster.RedisCluster)
	if err := h.resetBGToNone(ctx, cluster, survivingSTS,
		fmt.Sprintf("Deployment failed and rolled back: %s", bgStatus.Message)); err != nil {
		return ctrl.Result{}, health.Error, err
	}

	h.log.Info("Blue/green cleanup complete, reverting to normal operation")
	return ctrl.Result{RequeueAfter: time.Second}, health.Warn, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ────────────────────────────────────────────────────────────────────────────

func (h *Handler) checkGreenSTSQuotaEvents(ctx context.Context, rc *cachev1.RedisCluster, bgStatus *cachev1.BlueGreenStatus) {
	var events corev1.EventList
	if err := h.deps.List(ctx, &events, client.InNamespace(rc.Namespace)); err != nil {
		return
	}
	for _, ev := range events.Items {
		if ev.InvolvedObject.Name == GreenSTSName(rc) &&
			ev.Reason == "FailedCreate" && strings.Contains(ev.Message, "exceeded quota") {
			h.log.Error(nil, "Green pods blocked by quota",
				"event", ev.Message, "count", ev.Count)
			bgStatus.Message = fmt.Sprintf("Quota exceeded: %s", ev.Message)
			_ = h.metrics.CountBadSituation("bluegreen_pods_quota_blocked")
			return
		}
	}
}

func (h *Handler) deleteOrphanedPods(ctx context.Context, cluster *redis_cluster.Cluster) int {
	allPods, err := h.deps.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if err != nil {
		h.log.Error(err, "Failed to list pods for orphan check")
		return 0
	}

	deleted := 0
	for i := range allPods.Items {
		pod := &allPods.Items[i]
		if len(pod.OwnerReferences) > 0 {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
			continue
		}
		h.log.Info("Deleting orphaned pod", "pod", pod.Name, "ip", pod.Status.PodIP)
		if err := h.deps.Delete(ctx, pod); err != nil {
			h.log.Error(err, "Failed to delete orphaned pod", "pod", pod.Name)
			continue
		}
		deleted++
	}
	if deleted > 0 {
		_ = h.metrics.CountBadSituation("bluegreen_orphaned_pods_deleted")
	}
	return deleted
}

func (h *Handler) markPodsAsActive(ctx context.Context, pods []corev1.Pod) error {
	h.log.Info("Marking pods as active", "count", len(pods))
	for i := range pods {
		pod := &pods[i]
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		markPodServable(pod)
		if err := h.deps.Update(ctx, pod); err != nil {
			h.log.Error(err, "Failed to mark pod as active", "pod", pod.Name)
		}
	}
	return nil
}

func markPodServable(pod *corev1.Pod) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	constants.ActivateRole(pod.Labels)
	constants.ActivateRedisCluster(pod.Labels)
}

func (h *Handler) labelGreenPodsForPeerDiscovery(ctx context.Context, greenPods []corev1.Pod) {
	for i := range greenPods {
		if greenPods[i].Labels == nil {
			greenPods[i].Labels = make(map[string]string)
		}
		constants.ActivateRedisCluster(greenPods[i].Labels)
		if err := h.deps.Update(ctx, &greenPods[i]); err != nil {
			h.log.Error(err, "Failed to label green pod for peer discovery", "pod", greenPods[i].Name)
		} else {
			h.log.Info("Labeled green pod for peer discovery before CLUSTER MEET", "pod", greenPods[i].Name)
		}
	}
}

func (h *Handler) isReplicaSynced(toolbox *redis_cluster.Toolbox, replicaIP string, primaryIP string) (bool, error) {
	replicaInfo, err := toolbox.GetReplicationInfo(replicaIP)
	if err != nil {
		return false, utils.WrapErr(err, "failed to get replica replication info")
	}

	if !strings.Contains(replicaInfo, constants.RedisLinkStatusUp) {
		h.log.Info("Replica link not up yet", "replicaIP", replicaIP)
		return false, nil
	}

	if strings.Contains(replicaInfo, constants.RedisSyncInProgress) {
		h.log.Info("Replica full sync still in progress", "replicaIP", replicaIP)
		return false, nil
	}

	const maxLastIOSeconds int64 = 30
	lastIO := ParseInfoField(replicaInfo, constants.RedisLastIOSecondsField)
	if lastIO > maxLastIOSeconds {
		h.log.Info("Replica last IO too old — primary may be unreachable",
			"replicaIP", replicaIP, "lastIOSeconds", lastIO, "maxSeconds", maxLastIOSeconds)
		return false, nil
	}

	replicaOffset := ParseInfoField(replicaInfo, constants.RedisReplicaOffsetField)
	if replicaOffset < 0 {
		replicaOffset = ParseInfoField(replicaInfo, constants.RedisReplicaReadOffsetField)
	}
	primaryInfo, _ := toolbox.GetReplicationInfo(primaryIP)
	primaryOffset := ParseInfoField(primaryInfo, constants.RedisPrimaryOffsetField)

	var diffBytes int64
	if replicaOffset > 0 && primaryOffset > 0 {
		diffBytes = primaryOffset - replicaOffset
		if diffBytes < 0 {
			diffBytes = -diffBytes
		}
	}

	h.log.Info("Replica is synced and ready for failover",
		"replicaIP", replicaIP,
		"lastIOSeconds", lastIO,
		"replicaOffset", replicaOffset,
		"primaryOffset", primaryOffset,
		"lagBytes", diffBytes)
	return true, nil
}

func (h *Handler) scaleStatefulSet(ctx context.Context, namespace, name string, replicas int32) error {
	sts := &appsv1.StatefulSet{}
	if err := h.deps.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return utils.WrapErr(err, "failed to get StatefulSet for scaling")
	}
	sts.Spec.Replicas = &replicas
	if err := h.deps.Update(ctx, sts); err != nil {
		return utils.WrapErr(err, fmt.Sprintf("failed to scale StatefulSet %s to %d", name, replicas))
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Blue-pods-gone detection
// ────────────────────────────────────────────────────────────────────────────

// CheckBlueAlive checks if blue pods are still available.
// Returns (handled bool) — if true, caller should return the result/health/err.
func (h *Handler) CheckBlueAlive(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
) (handled bool, result ctrl.Result, hp health.ClusterHealth, err error) {
	bgStatus := &cluster.RedisCluster.Status.BlueGreen

	bName := BlueSTSName(cluster.RedisCluster)
	if bName == "" || len(bgStatus.ShardMapping) == 0 {
		return false, ctrl.Result{}, health.OK, nil
	}

	blueSTS, stsErr := h.deps.GetStatefulSetByName(ctx, cluster.RedisCluster.Namespace, bName)
	blueStsGone := stsErr != nil || blueSTS == nil || (blueSTS.Spec.Replicas != nil && *blueSTS.Spec.Replicas == 0)

	if !blueStsGone {
		blueReachable := 0
		for _, mapping := range bgStatus.ShardMapping {
			if mapping.BluePrimaryIP != "" && cluster.Toolbox.PingIP(mapping.BluePrimaryIP) {
				blueReachable++
			}
		}
		if blueReachable > 0 {
			return false, ctrl.Result{}, health.OK, nil
		}
		h.log.Info("Blue STS exists but no blue primaries are reachable", "blueReachable", blueReachable)
	}

	h.log.Info("Blue pods are gone — checking if green has replicated data",
		"blueSTS", bName, "blueSTSGone", blueStsGone)
	_ = h.metrics.CountBadSituation("bluegreen_blue_pods_gone")

	greenHasData := false
	for _, mapping := range bgStatus.ShardMapping {
		if mapping.GreenPodIP == "" {
			continue
		}
		info, infoErr := cluster.Toolbox.GetReplicationInfo(mapping.GreenPodIP)
		if infoErr != nil {
			continue
		}
		if strings.Contains(info, constants.RedisRoleMaster) ||
			strings.Contains(info, constants.RedisLinkStatusUp) ||
			strings.Contains(info, constants.RedisSyncInProgress) {
			greenHasData = true
			break
		}
	}

	if !greenHasData {
		h.log.Error(nil, "Blue pods gone AND green has no replicated data — cannot recover")
		h.emitFailedDuration(bgStatus)
		setPhase(bgStatus, cachev1.BlueGreenPhaseFailed, "Blue pods gone before replication completed, no data to preserve")
		_ = h.deps.UpdateStatus(ctx, cluster.RedisCluster)
		return true, ctrl.Result{Requeue: true}, health.Error, nil
	}

	h.log.Info("EMERGENCY: Blue gone but green has data — promoting green via TAKEOVER to preserve data")

	if bgStatus.ResumeForwardAttempts < int32(constants.BlueGreenTakeoverAttemptThreshold) {
		bgStatus.ResumeForwardAttempts = int32(constants.BlueGreenTakeoverAttemptThreshold)
	}
	setPhase(bgStatus, cachev1.BlueGreenPhasePromoting, "Emergency promote: blue pods gone, promoting green via TAKEOVER to preserve data")
	bgStatus.StartTime = time.Now().Unix()
	bgStatus.LastSyncedCount = 0
	bgStatus.LastProgressTime = 0
	if updateErr := h.deps.UpdateStatus(ctx, cluster.RedisCluster); updateErr != nil {
		return true, ctrl.Result{}, health.Error, updateErr
	}

	return true, ctrl.Result{Requeue: true}, health.Warn, nil
}
