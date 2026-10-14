// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	goerrors "errors"
	"fmt"
	"maps"
	"runtime/debug"
	"sync"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/bluegreen"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// RedisClustersReconciler reconciles Redis clusters object.
type RedisClustersReconciler struct {
	redis_cluster.ClusterClient
	AutoscalingProvider      AutoscalingProvider
	LifecycleAdapter         LifecycleAdapter
	PrimaryEndpointResolver  PrimaryEndpointResolver
	BackupBackendFactory     BackupBackendFactory
	notFoundCache            sync.Map // key: NamespacedName string, value: time.Time
	primaryEndpointReadiness primaryEndpointReadinessTracker
}

// RedisClusterReconciler reconciles one Redis cluster object.
type RedisClusterReconciler struct {
	redis_cluster.ClusterClient
	AutoscalingProvider      AutoscalingProvider
	LifecycleAdapter         LifecycleAdapter
	PrimaryEndpointResolver  PrimaryEndpointResolver
	BackupBackendFactory     BackupBackendFactory
	notFoundCache            *sync.Map
	primaryEndpointReadiness *primaryEndpointReadinessTracker
	primaryEndpointProber    primaryEndpointProber
	primaryEndpointNow       func() time.Time
}

// restorePrunedStatefulSetMetadata reconstructs workload identity fields that
// Kubernetes removes from the opaque metadata objects in the compatibility CRD.
func restorePrunedStatefulSetMetadata(redisCluster *cachev1.RedisCluster) {
	template := &redisCluster.Spec.StatefulsetTemplate
	if template.Name == "" {
		template.Name = redisCluster.Name
	}
	template.Namespace = redisCluster.Namespace
	if template.Spec.Template.Labels == nil {
		template.Spec.Template.Labels = make(map[string]string)
	}
	if template.Spec.Selector != nil {
		maps.Copy(template.Spec.Template.Labels, template.Spec.Selector.MatchLabels)
	}
}

// getReconcileTimeout returns the reconcile context timeout (override or auto-scaled).
func getReconcileTimeout(podCount int32, overrideSeconds int32) time.Duration {
	if overrideSeconds > 0 {
		return time.Duration(overrideSeconds) * time.Second
	}
	timeout := constants.ReconcileTimeoutBase + time.Duration(podCount)*constants.ReconcileTimeoutPerPod
	if timeout > constants.ReconcileTimeoutMax {
		return constants.ReconcileTimeoutMax
	}
	return timeout
}

// Reconcile reconciles the state of the Redis cluster.
func (r *RedisClustersReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	// Skip resources recently confirmed as not found to prevent tight reconcile loops
	// from pod/StatefulSet events targeting non-existent RedisCluster CRs.
	cacheKey := req.NamespacedName.String()
	if ts, ok := r.notFoundCache.Load(cacheKey); ok {
		cachedAt, valid := ts.(time.Time)
		if valid {
			elapsed := time.Since(cachedAt)
			if elapsed < constants.NotFoundCacheTTL {
				return ctrl.Result{RequeueAfter: constants.NotFoundCacheTTL - elapsed}, nil
			}
		}
		r.notFoundCache.Delete(cacheKey)
	}

	// the reconcile function does not differentiate between create, update or deletion events.
	// it simply reads the state of the cluster at the time it is called.
	// all it provides is NamespacedName, for example 'sample/redis-cluster-sample'

	// the StatefulSet ensures that the expected number of pods are running, it is created by the Operator
	// its version needs to match the chart version (else, it needs to be updated)
	// it is 'ready' when it has the expected number of pods, and they all have an IP (are scheduled)
	// (else wait... the Operator is in charge of marking it ready/not)
	// and remains ready forever (even if some pods are missing)

	// the RedisCluster describes the desired Redis cluster
	// it is 'ready' when the cluster has been created, and remains ready forever
	// (even if the cluster breaks)

	cc := r.ClusterClient.ForCluster(req.NamespacedName)

	// Recover from panics to prevent operator crashes.
	// A nil pointer or unexpected state in any reconciliation path should not
	// take down the entire operator process for all clusters.
	defer func() {
		if rec := recover(); rec != nil {
			stackTrace := string(debug.Stack())
			cc.Log.Error(nil, "PANIC in reconcile - recovered",
				"panic", rec,
				"cluster", req.Name,
				"stack", stackTrace)
			_ = cc.Metrics.CountBadSituation("panic_recovered")
			err = fmt.Errorf("panic recovered: %v", rec)
			result = ctrl.Result{RequeueAfter: constants.NormalRequeue}
		}
	}()

	cr := RedisClusterReconciler{
		ClusterClient:            cc,
		AutoscalingProvider:      r.AutoscalingProvider,
		LifecycleAdapter:         r.LifecycleAdapter,
		PrimaryEndpointResolver:  r.PrimaryEndpointResolver,
		BackupBackendFactory:     r.BackupBackendFactory,
		notFoundCache:            &r.notFoundCache,
		primaryEndpointReadiness: &r.primaryEndpointReadiness,
	}

	cc.Log.Info("Begin reconcile")
	_ = cc.Metrics.Count(counters.Reconcile)
	result, h, err := cr.reconcile(ctx, req)
	_ = cc.Metrics.ReportHealth(h)
	cc.Log.Info("End reconcile", "clusterHealth", h)
	return result, err
}

// reconcile reconciles the state of the Redis cluster.
func (r *RedisClusterReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, health.ClusterHealth, error) {

	// Get the RedisCluster resource, abort if not found
	redisCluster := &cachev1.RedisCluster{}
	err := r.Get(ctx, req.NamespacedName, redisCluster)
	if err != nil {
		if errors.IsNotFound(err) {
			r.Log.Info("RedisCluster resource not found, ignoring (object must be deleted)")
			r.notFoundCache.Store(req.NamespacedName.String(), time.Now())
			return ctrl.Result{}, health.Unknown, nil
		}
		r.Log.Error(err, "Failed to get the RedisCluster resource")
		_ = r.Metrics.CountBadSituation("could_not_get_cluster")
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to get redisCluster resource")
	}
	// ObjectMeta embedded in the preserved StatefulSet template schema is
	// intentionally opaque and may be pruned by modern API servers. Resource
	// identity is a controller concern, so restore it from the owning CR before
	// any StatefulSet or blue-green operation uses the template.
	restorePrunedStatefulSetMetadata(redisCluster)
	if err := redisCluster.ValidateTopology(); err != nil {
		r.Log.Error(err, "Invalid RedisCluster topology")
		_ = r.Metrics.CountBadSituation("invalid_topology")
		return ctrl.Result{}, health.Error, err
	}

	// Report
	statefulSetIsReady := redisCluster.Status.StatefulSetReady
	r.Log.Info("Got the RedisCluster resource", "statefulSetIsReady", statefulSetIsReady)

	// Check for reconciliation
	if !r.shouldReconcile(ctx, redisCluster, true) {
		return ctrl.Result{}, health.Unknown, nil
	}

	// Bound provider acquisition and every subsequent reconcile operation with
	// the same cluster-sized deadline.
	podCount := redisCluster.EffectivePodCount()
	timeout := getReconcileTimeout(podCount, redisCluster.Spec.ReconcileTimeoutSeconds)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r.Log.Info("Reconcile timeout set", "podCount", podCount, "timeout", timeout.String(),
		"configuredOverride", redisCluster.Spec.ReconcileTimeoutSeconds)

	autoscalingState, err := r.prepareAutoscaling(ctx, redisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to prepare autoscaling state")
		return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error, err
	}

	// Get the StatefulSet resource, create it if not found (new cluster)
	statefulSet, err := r.GetOrCreateStatefulSet(ctx, redisCluster)
	if err != nil {
		r.Log.Error(err, "Failed to get the StatefulSet")
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to get the StatefulSet")
	}

	// If the StatefulSet is not ready yet, try to ready it
	if !statefulSetIsReady {
		statefulSetIsReady, err = r.TryReadyStatefulSet(ctx, redisCluster, statefulSet)
		if err != nil {
			_ = r.Metrics.CountBadSituation("statefulset_not_ready")
			r.Log.Error(err, "Failed to check StatefulSet readiness")
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error, utils.WrapErr(err, "failed to try to ready the StatefulSet")
		}
		if !statefulSetIsReady {
			_ = r.Metrics.CountBadSituation("statefulset_not_ready")
			r.Log.Info("StatefulSet not ready yet, will retry")
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Pending, nil
		}
		// Save the status update
		redisCluster.Status.StatefulSetReady = true
		err = r.updateRedisCluster(ctx, redisCluster)
		if err != nil {
			return ctrl.Result{}, health.Unknown, err
		}
		r.Log.Info("The StatefulSet is now ready")
	}

	// At that point the StatefulSet is ready though maybe not on the correct version,
	// some of its pods may be missing or not ready, and the Redis cluster may not
	// exist at all.

	// Intercept blue-green deployment before normal reconcile.
	// Early phases (Creating → WaitingSync): query blue pods only because green
	// pods may not exist yet or be unreachable.
	// Late phases (Promoting → TearingDown/Failed): blue is scaled to 0, so
	// query all remaining pods to find green.
	bgPhase := redisCluster.Status.BlueGreen.Phase
	if bgPhase != "" && bgPhase != cachev1.BlueGreenPhaseNone {
		var bgCluster *redis_cluster.Cluster
		var err error

		switch bgPhase {
		// Green-only phases: blue pods are dying or gone, querying them would hang.
		case cachev1.BlueGreenPhaseScalingDownBlue,
			cachev1.BlueGreenPhaseTearingDown:
			r.Log.Info("BG late phase, using green-only cluster", "phase", bgPhase)
			greenName := bluegreen.GreenSTSName(redisCluster)
			greenSTS, stsErr := r.GetStatefulSetByName(ctx, redisCluster.Namespace, greenName)
			if stsErr != nil {
				err = fmt.Errorf("failed to get green STS %q: %w", greenName, stsErr)
			} else if greenSTS == nil {
				err = fmt.Errorf("green STS %q not found", greenName)
			} else {
				bgCluster, err = r.GetGreenOnlyCluster(ctx, redisCluster, greenSTS)
			}

		// Mixed phases: both blue and green are alive and needed.
		case cachev1.BlueGreenPhasePromoting,
			cachev1.BlueGreenPhaseFormingCluster,
			cachev1.BlueGreenPhaseSwitchingDNS,
			cachev1.BlueGreenPhaseFailed:
			r.Log.Info("BG late phase, using all available pods", "phase", bgPhase)
			bgCluster, err = r.GetCluster(ctx, redisCluster, statefulSet)

		// Early phases: blue is source of truth, green may not exist or be offline.
		default:
			r.Log.Info("BG early phase, using blue-only cluster", "phase", bgPhase)
			bgCluster, err = r.GetBlueOnlyCluster(ctx, redisCluster, statefulSet)
		}

		if err != nil {
			r.Log.Error(err, "Failed to get cluster during BG deployment", "phase", bgPhase)
			return ctrl.Result{RequeueAfter: constants.DefaultSleepTime}, health.Warn,
				utils.WrapErr(err, "failed to get cluster for BG deployment")
		}
		return r.handleBlueGreenDeployment(ctx, bgCluster)
	}
	_ = r.Metrics.Gauge(metrics.GaugeBlueGreenPhase, 0)

	// Get the cluster (normal path — no BG deployment active)
	cluster, err := r.GetCluster(ctx, redisCluster, statefulSet)
	if err != nil {
		r.Log.Error(err, "Failed to get the cluster's Redis nodes")
		return ctrl.Result{}, health.Warn, utils.WrapErr(err, "failed to get the cluster's Redis nodes")
	}
	r.Log.Info("Got cluster", "nodes", cluster.AllNodes)
	if autoscalingState.manualResetPending {
		r.Log.Info("Restoring Redis replica migration before completing manual autoscaling reset")
		replicaMigrationGuard := newAutoscalingReplicaMigrationGuard(r, r.Log.WithName("autoscaling-reset"))
		if err := replicaMigrationGuard.restore(ctx, cluster); err != nil {
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error,
				utils.WrapErr(err, "failed to restore replica migration before autoscaling reset")
		}
		if err := r.completeManualAutoscalingReset(ctx, redisCluster); err != nil {
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error, err
		}
		return ctrl.Result{RequeueAfter: constants.QuickRequeue}, health.Pending, nil
	}

	// At that point the StatefulSet is ready, though maybe not on the latest version, and some of its
	// pods may be missing or not ready, and the Redis cluster may not exist at all

	// Count pods
	expectedPodCount := *statefulSet.Spec.Replicas
	availablePodCount := statefulSet.Status.AvailableReplicas
	podsAreReady := expectedPodCount == availablePodCount

	// If the Redis cluster has not been formed yet, try to form it
	if !r.isClusterAlreadyFormed(cluster) {

		// If the StatefulSet does not have the right amount of available pods, retry later
		if !podsAreReady {
			r.Log.Error(goerrors.New("statefulSet is ready, Redis cluster is not, but cannot create now (pods are not ready), wait"), "error")
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error, nil
		}

		h, err := r.formCluster(ctx, cluster)
		if h != health.OK {
			return ctrl.Result{}, h, err
		}
		return ctrl.Result{RequeueAfter: constants.QuickRequeue}, h, nil
	}

	// ── Scale-up detection: must run BEFORE maintainCluster ──
	// Otherwise maintain sees fewer primaries than expected and fails with
	// "no outsider node available" before we ever scale the STS.
	// Scale-down stays after maintain because downShard assumes a healthy cluster.
	targetPodCount := cluster.RedisCluster.EffectivePodCount()
	currentPodCount := *cluster.StatefulSet.Spec.Replicas
	if targetPodCount > currentPodCount {
		return r.handleScaling(ctx, cluster, targetPodCount, currentPodCount)
	}

	// Drain handlers run before isClusterHealthy: a mid-failover cluster with
	// slots momentarily in transition needs handleTerminatingPods/handleNLA
	// to unstick it, not a "missing slots" bail-out.
	terminatingStart := time.Now()
	podList, podListErr := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if podListErr != nil {
		r.Log.Error(podListErr, "Failed to list pods for termination handling, continuing reconcile")
	} else if err := r.handleTerminatingPodsFromList(ctx, cluster, podList); err != nil {
		r.Log.Error(err, "Failed to handle terminating pods, continuing reconcile")
	}
	_ = r.Metrics.Timing(metrics.TimingReconcileStep, time.Since(terminatingStart).Milliseconds(), metrics.StepTagHandleTerminating)
	if optionalProviderIsNil(r.LifecycleAdapter) {
		if podList == nil {
			podList, podListErr = r.GetRedisClusterPods(ctx, cluster.RedisCluster)
			if podListErr != nil {
				return ctrl.Result{}, health.Error, fmt.Errorf("failed to list pods for drain candidate correction: %w", podListErr)
			}
		}
		corrected, err := r.handleDrainCandidatePrimary(ctx, cluster, podList)
		if err != nil {
			r.Log.Error(err, "Failed to correct drain candidate primary")
			return ctrl.Result{}, health.Error, err
		}
		if corrected {
			return ctrl.Result{RequeueAfter: constants.QuickRequeue}, health.Pending, nil
		}
	}

	nlaStart := time.Now()
	stable, nlaErr := r.handleLifecycle(ctx, cluster)
	_ = r.Metrics.Timing(metrics.TimingReconcileStep, time.Since(nlaStart).Milliseconds(), metrics.StepTagNLA)
	if nlaErr != nil {
		r.Log.Error(nlaErr, "Failed to handle Network Lifecycle Automation notifications")
	}

	// At that point the Redis cluster exists - check if it's healthy enough to maintain
	if !r.isClusterHealthy(cluster) {
		slotsAssigned := r.getClusterSlotsAssigned(cluster)
		if slotsAssigned != constants.TotalSlots {
			r.Log.Info("Cluster has missing slots, attempting automatic recovery in maintainCluster",
				"slotsAssigned", slotsAssigned, "expected", constants.TotalSlots)
			_ = r.Metrics.CountBadSituation("missing_slots_auto_recovery")
		} else {
			// Cluster unhealthy but has all slots - maintainCluster can try to fix
			r.Log.Info("Cluster is unhealthy but has all slots, attempting to fix", "slotsAssigned", slotsAssigned)
		}
	}

	r.Log.Info("Maintain the cluster")
	maintainStart := time.Now()
	cluster, err = r.maintainCluster(ctx, cluster)
	_ = r.Metrics.Timing(metrics.TimingReconcileStep, time.Since(maintainStart).Milliseconds(), metrics.StepTagMaintainCluster)
	if err != nil {
		if goerrors.Is(err, errConnectivityRepairPending) {
			r.Log.Info("Requeueing while Redis connectivity repair converges",
				"requeueAfter", constants.QuickRequeue.String())
			return ctrl.Result{RequeueAfter: constants.QuickRequeue}, health.Pending, nil
		}
		if goerrors.Is(err, errWaitingForRedisEndpointReady) {
			r.Log.Info("Requeueing while a Redis member waits for client endpoint readiness",
				"requeueAfter", constants.QuickRequeue.String())
			return ctrl.Result{RequeueAfter: constants.QuickRequeue}, health.Pending, nil
		}
		if goerrors.Is(err, errWaitingForAutoscalingCapacity) {
			r.Log.Info("Requeueing while autoscaling waits for safe replica-repair capacity",
				"requeueAfter", constants.QuickRequeue.String())
			return ctrl.Result{RequeueAfter: constants.QuickRequeue}, health.Pending, nil
		}
		r.Log.Error(err, "Failed to maintain the cluster")
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to maintain the cluster")
	}
	if cluster == nil {
		// failed without an error = cannot maintain, no point to retry
		r.Log.Info("Cannot maintain the cluster")
		return ctrl.Result{}, health.Error, nil
	}
	r.Log.Info("Maintained the cluster")

	autoscalingHandled, autoscalingRequeue, autoscalingErr := r.reconcileAutoscalingAfterMaintenance(ctx, cluster, autoscalingState, stable, time.Now())
	if autoscalingErr != nil {
		return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.Error, utils.WrapErr(autoscalingErr, "failed to reconcile autoscaling decision")
	}
	if autoscalingHandled {
		return ctrl.Result{RequeueAfter: autoscalingRequeue}, health.Pending, nil
	}

	// Handle cluster backup
	backupStart := time.Now()
	didBackup, backupErr := r.backup(ctx, cluster)
	_ = r.Metrics.Timing(metrics.TimingReconcileStep, time.Since(backupStart).Milliseconds(), metrics.StepTagBackup)
	if backupErr != nil {
		r.Log.Error(backupErr, "Backup failed, continuing reconcile")
		_ = r.Metrics.CountBadSituation("backup_failed")
	}
	if didBackup {
		// Save the status change
		cluster.RedisCluster.Status.LastBackupTime = time.Now().Unix()
		err = r.updateRedisCluster(ctx, cluster.RedisCluster)
		if err != nil {
			return ctrl.Result{}, health.Warn, err
		}
	}

	// ── Scale-down detection: runs after maintain so cluster is healthy ──
	if targetPodCount < currentPodCount {
		return r.handleScaling(ctx, cluster, targetPodCount, currentPodCount)
	}

	// Update the StatefulSet if needed, handle up- and down-scale
	// - the RedisCluster chart version is updated when the cluster is deployed
	// - the StatefulSet chart version is updated when the operator updates the StatefulSet (below)
	clusterChartVersion, statefulSetChartVersion, err := getChartVersions(redisCluster, statefulSet, r.Log)
	if err != nil {
		return ctrl.Result{}, health.Warn, utils.WrapErr(err, "failed to get the chart versions")
	}

	// Check for Redis config/resource changes that require a blue/green deployment.
	// Only actual Redis configuration changes (redis.conf, memory, CPU) should trigger BG.
	// Chart version mismatches alone (e.g. operator-only redeploy) do NOT trigger BG.
	chartVersionChanged := clusterChartVersion != statefulSetChartVersion
	specChanged, err := r.detectSpecChanges(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to detect spec changes")
	}

	if chartVersionChanged && !specChanged {
		r.Log.Info("Chart version changed but no config/resource changes detected, syncing STS label only",
			"redisClusterVersion", clusterChartVersion,
			"statefulSetVersion", statefulSetChartVersion)
		if statefulSet.Labels == nil {
			statefulSet.Labels = make(map[string]string)
		}
		statefulSet.Labels["chart_version"] = clusterChartVersion
		if err := r.Update(ctx, statefulSet); err != nil {
			return ctrl.Result{}, health.Warn, utils.WrapErr(err, "failed to sync chart_version label on STS")
		}
	}

	if specChanged {
		if !r.isClusterAlreadyFormed(cluster) {
			r.Log.Info("Spec changed but cluster has no slot assignments — fresh/empty cluster, stamping hash and skipping blue/green",
				"chartVersionChanged", chartVersionChanged,
				"redisClusterVersion", clusterChartVersion,
				"statefulSetVersion", statefulSetChartVersion)
			if err := r.stampSpecHash(ctx, cluster); err != nil {
				return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to stamp spec hash")
			}
			return ctrl.Result{Requeue: true}, health.OK, nil
		}
		r.Log.Info("Redis config/resource changes detected, starting blue/green deployment",
			"chartVersionChanged", chartVersionChanged,
			"specChanged", specChanged,
			"redisClusterVersion", clusterChartVersion,
			"statefulSetVersion", statefulSetChartVersion)
		return r.startBlueGreenDeployment(ctx, cluster)
	}

	if err := r.stampSpecHash(ctx, cluster); err != nil {
		return ctrl.Result{}, health.Error, utils.WrapErr(err, "failed to stamp spec hash")
	}

	if stable {
		var updated bool
		updated, err = r.handleRollout(ctx, cluster)
		if err != nil {
			return ctrl.Result{}, health.Warn, err
		}
		if updated {
			return ctrl.Result{}, health.Warn, nil
		}
	}

	// Reset the reconciliation count
	resetReconcileCount(cluster.RedisCluster)

	// Finally update the cluster resource with the cluster nodes structure, for info
	// Use dedicated timeout — main ctx may be nearly expired after toolbox commands
	cluster.RedisCluster.Status.Nodes = redis_cluster.ClusterNodesToRedisNodes(cluster.AllNodes)
	statusCtx, statusCancel := context.WithTimeout(context.Background(), constants.DefaultTimeout)
	defer statusCancel()
	err = r.updateRedisCluster(statusCtx, cluster.RedisCluster)
	if err != nil {
		r.Log.Info("Failed to update status (will retry next reconcile)", "error", err.Error())
	}

	// Requeue unfinished NLA work, including failed attempts.
	if !stable {
		return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.OK, nil
	}

	// Schedule next reconcile for backup if enabled
	if cluster.RedisCluster.Spec.BackupEnabled && cluster.RedisCluster.Spec.BackupFrequency > 0 {
		nextBackupIn := cluster.RedisCluster.Spec.BackupFrequency - (time.Now().Unix() - cluster.RedisCluster.Status.LastBackupTime)
		if nextBackupIn < int64(constants.NormalRequeue.Seconds()) {
			return ctrl.Result{RequeueAfter: constants.NormalRequeue}, health.OK, nil
		}
		return ctrl.Result{RequeueAfter: time.Duration(nextBackupIn) * time.Second}, health.OK, nil
	}

	return ctrl.Result{}, health.OK, nil
}

// AutoscalingWatchProvider optionally attaches provider-owned resource watches
// without exposing those resource types to the portable controller.
type AutoscalingWatchProvider interface {
	AddWatches(ctrl.Manager, *builder.Builder) error
}

func (r *RedisClustersReconciler) addAutoscalingProviderWatches(mgr ctrl.Manager, controllerBuilder *builder.Builder) (*builder.Builder, error) {
	watchProvider, ok := r.AutoscalingProvider.(AutoscalingWatchProvider)
	if !ok {
		return controllerBuilder, nil
	}
	if err := watchProvider.AddWatches(mgr, controllerBuilder); err != nil {
		return nil, fmt.Errorf("configure autoscaling provider watches: %w", err)
	}
	return controllerBuilder, nil
}

func (r *RedisClustersReconciler) mapPodToRedisCluster(ctx context.Context, object client.Object) []ctrl.Request {
	statefulSetOwner := metav1.GetControllerOf(object)
	if statefulSetOwner == nil || statefulSetOwner.APIVersion != appsv1.SchemeGroupVersion.String() || statefulSetOwner.Kind != "StatefulSet" {
		return nil
	}

	statefulSet := &appsv1.StatefulSet{}
	key := types.NamespacedName{Namespace: object.GetNamespace(), Name: statefulSetOwner.Name}
	if err := r.Get(ctx, key, statefulSet); err != nil {
		if !errors.IsNotFound(err) {
			r.Log.Error(err, "Failed to resolve Pod owner", "pod", object.GetName(), "statefulSet", key)
		}
		return nil
	}
	if statefulSetOwner.UID != "" && statefulSet.UID != statefulSetOwner.UID {
		return nil
	}

	redisClusterOwner := metav1.GetControllerOf(statefulSet)
	if redisClusterOwner == nil || redisClusterOwner.APIVersion != cachev1.GroupVersion.String() || redisClusterOwner.Kind != "RedisCluster" {
		return nil
	}
	return []ctrl.Request{{NamespacedName: types.NamespacedName{
		Namespace: object.GetNamespace(),
		Name:      redisClusterOwner.Name,
	}}}
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClustersReconciler) SetupWithManager(mgr ctrl.Manager) error {
	controllerBuilder := ctrl.NewControllerManagedBy(mgr).

		// see
		// - https://pkg.go.dev/sigs.k8s.io/controller-runtime
		// - https://yash-kukreja-98.medium.com/develop-on-kubernetes-series-demystifying-the-for-vs-owns-vs-watches-controller-builders-in-c11ab32a046e
		//
		// the Manager runs Controllers
		// the Controller uses events to trigger reconcile requests (via event sources, with predicates, to handlers)
		// the Reconciler implements the reconciliation logic

		// anytime the metadata.generation changes on a rediscluster resource, reconcile
		// metadata.generation changes when writing to the spec (not the metadata nor the status)
		// note that this includes changing spec.disableReconciliation
		For(&cachev1.RedisCluster{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{}))).

		// the statefulset resource is owned by the rediscluster resource
		// whenever an owned statefulset changes, reconcile
		Owns(&appsv1.StatefulSet{})

	var err error
	controllerBuilder, err = r.addAutoscalingProviderWatches(mgr, controllerBuilder)
	if err != nil {
		return err
	}

	return controllerBuilder.
		// watch(source, handler)
		// - source = pod, trigger when a pod event triggers
		// - handler = what will be triggered
		// Whenever a Pod changes, resolve its StatefulSet controller and enqueue
		// the RedisCluster that controls that StatefulSet.
		//
		// Intentionally unfiltered: DeletionTimestamp transitions must trigger
		// reconcile so handleTerminatingPods can react before grace expires.
		// Adding a metadata-filter predicate here would break that contract.
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodToRedisCluster),
		).

		// Cap exponential backoff to prevent long stalls after transient errors
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](
				constants.RateLimiterBaseDelay,
				constants.RateLimiterMaxDelay,
			),
		}).

		// to the redis cluster reconciler
		Complete(r)
}
