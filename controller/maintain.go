// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/health"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics/counters"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	errNoPrimaryNodesForUnassignedSlots = errors.New("no primary nodes available for unassigned slots")
	errFailoverSuperseded               = errors.New("failover superseded by synchronized primary replacement")
	errConnectivityRepairPending        = errors.New("connectivity repair is converging")
)

type cleanFailoverAbortCheck func(context.Context, string) (bool, error)

func (r *RedisClusterReconciler) maintainCluster(ctx context.Context, cluster *redis_cluster.Cluster) (*redis_cluster.Cluster, error) {

	// FIXME: maintainCluster should...
	//  - trigger more bad-situations + operations metrics
	//  - report health asap
	//  - return / report final health

	log := r.Log.WithName("maintain")

	bgPhase := cluster.RedisCluster.Status.BlueGreen.Phase
	if bgPhase != "" && bgPhase != cachev1.BlueGreenPhaseNone {
		log.Info("BG deployment active, skipping maintain", "phase", bgPhase)
		return cluster, nil
	}

	if bgDone := cluster.RedisCluster.Status.BlueGreen.CompletedAt; bgDone > 0 {
		elapsed := time.Since(time.Unix(bgDone, 0))
		if elapsed < constants.BlueGreenPostCompletionGracePeriod {
			log.Info("BG recently completed, skipping aggressive maintain to let topology stabilize",
				"completedSecondsAgo", int(elapsed.Seconds()),
				"gracePeriod", constants.BlueGreenPostCompletionGracePeriod.String())
			return cluster, nil
		}
	}

	log.Info("Starting cluster maintenance",
		"clusterName", cluster.RedisCluster.Name,
		"nodeSummary", redis_cluster.GetNodeSummary(cluster.Nodes),
		"totalNodes", len(cluster.AllNodes))

	// Report cluster node count metrics
	_ = r.Metrics.Gauge(metrics.GaugeClusterKnownNodes, float64(len(cluster.AllNodes)))
	_ = r.Metrics.Gauge(metrics.GaugeClusterPrimaries, float64(len(cluster.Nodes[redis_cluster.PrimaryNode])))
	_ = r.Metrics.Gauge(metrics.GaugeClusterReplicas, float64(len(cluster.Nodes[redis_cluster.ReplicaNode])))
	_ = r.Metrics.Gauge(metrics.GaugeClusterOtherNodes, float64(len(cluster.Nodes[redis_cluster.OtherNode])))
	_ = r.Metrics.Gauge(metrics.GaugeClusterExpectedPrimaries, float64(cluster.RedisCluster.EffectivePrimaries()))
	_ = r.Metrics.Gauge(metrics.GaugeClusterExpectedReplicas, float64(cluster.RedisCluster.EffectivePrimaries()*cluster.RedisCluster.Spec.Replicas))

	takeover := false
	mustRebalance := false
	iterations := 0
	var err error
	automaticScaleUp := autoscalingIsScaling(&cluster.RedisCluster.Status.Autoscaling)
	replicaMigrationGuard := newAutoscalingReplicaMigrationGuard(r, log)
	if !automaticScaleUp {
		if err := replicaMigrationGuard.restore(ctx, cluster); err != nil {
			return cluster, err
		}
	}

	// Once the new primary has joined, it is temporarily orphaned while DNS
	// readiness and rebalance gates run. Keep existing replicas from migrating
	// to it on their own; the operator must remain the sole authority over
	// replica placement until the full primary group is assembled.
	if automaticScaleUp && len(cluster.Nodes[redis_cluster.PrimaryNode]) >= int(cluster.RedisCluster.EffectivePrimaries()) {
		if err := replicaMigrationGuard.disable(ctx, cluster); err != nil {
			return cluster, err
		}
	}

	// Loop as long as we are performing Redis operations that do not have StatefulSet or pod
	// impact, and would not requeue events, but just manipulate the Redis cluster composition.
	// The function returns as soon as an action (e.g. adding a primary or a replica) fails,
	// so the loop cannot be infinite: either all actions succeed and the loop exits successfully,
	// or an action fails and the function returns.

	for {

		if iterations > cluster.Timeouts.MaxMaintainIterations {
			return cluster, fmt.Errorf("exceeding maximum number of iterations (%d)", cluster.Timeouts.MaxMaintainIterations)
		}
		iterations++

		cluster, err = r.detectAndFixPartitionedNodes(ctx, cluster, log)
		if err != nil {
			return cluster, err
		}

		// Probe and repair nodes in transient "other" state.
		// These have a pod but couldn't be classified as primary/replica from gossip.
		// Instead of aborting, probe each one directly — if it's healthy and knows
		// its role, CLUSTER MEET it back from a healthy node to restore gossip.
		if len(cluster.Nodes[redis_cluster.OtherNode]) > 0 {
			_ = r.Metrics.CountBadSituation("other_nodes_found")
			repaired := 0
			// A terminating or draining pod was FORGOTTEN on purpose; probing it can
			// hang and a MEET would undo the FORGET.
			podList, podErr := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
			if podErr != nil {
				log.Error(podErr, "Failed to list pods for other-node repair, probing every other node")
			}
			for _, node := range cluster.Nodes[redis_cluster.OtherNode] {
				debugInfo := node.GetRoleDebugInfo()
				log.Info("Found 'other' node (transient state) - role detection debug",
					"podName", debugInfo["podName"],
					"podIndex", debugInfo["podIndex"],
					"nodeID", debugInfo["nodeID"],
					"ip", debugInfo["ip"],
					"flags", debugInfo["flags"],
					"relevantFlags", debugInfo["relevantFlags"],
					"hasOnlyOneRelevantFlag", debugInfo["hasOnlyOneRelevantFlag"],
					"isPrimary", debugInfo["isPrimary"],
					"isReplica", debugInfo["isReplica"],
					"knownNodes", debugInfo["knownNodes"],
					"clusterSize", debugInfo["clusterSize"],
					"viewsCount", debugInfo["viewsCount"])

				if node.IP == "" {
					log.Info("Other node has no IP, skipping probe", "podName", node.PodName)
					continue
				}
				if podList != nil && podIsDrainingOrTerminating(podList, node.IP) {
					log.Info("Other node pod is draining or terminating, skipping probe",
						"podName", node.PodName, "ip", node.IP)
					continue
				}

				alive := cluster.Toolbox.PingIP(node.IP)
				if !alive {
					log.Info("Other node not responding to PING — continuing maintenance without it (Redis will mark FAIL and trigger failover)",
						"podName", node.PodName, "ip", node.IP)
					continue
				}

				info, err := cluster.Toolbox.GetReplicationInfo(node.IP)
				if err != nil {
					log.Info("Other node alive but INFO replication failed — continuing maintenance without it",
						"podName", node.PodName, "ip", node.IP, "error", err)
					continue
				}

				nodeRole := "unknown"
				if strings.Contains(info, constants.RedisRoleMaster) {
					nodeRole = "master"
				} else if strings.Contains(info, constants.RedisRoleSlave) {
					nodeRole = "slave"
				}

				log.Info("Other node is alive and reports a role — attempting CLUSTER MEET to re-integrate",
					"podName", node.PodName, "ip", node.IP, "selfReportedRole", nodeRole)

				var refNode *redis_cluster.ClusterNode
				if len(cluster.Nodes[redis_cluster.PrimaryNode]) > 0 {
					refNode = cluster.Nodes[redis_cluster.PrimaryNode][0]
				} else if len(cluster.Nodes[redis_cluster.ReplicaNode]) > 0 {
					refNode = cluster.Nodes[redis_cluster.ReplicaNode][0]
				}

				if refNode != nil {
					if err := cluster.Toolbox.Meet(refNode.IP, node.IP, cluster.RedisPort); err != nil {
						log.Error(err, "Failed to MEET other node", "from", refNode.IP, "to", node.IP)
					} else {
						log.Info("CLUSTER MEET sent for other node",
							"from", refNode.PodName, "to", node.PodName)
						repaired++
					}
				}
			}

			if repaired > 0 {
				log.Info("Re-integrated other node(s) via CLUSTER MEET, re-reading cluster next reconcile",
					"repaired", repaired)
				return cluster, fmt.Errorf("found %d 'other' node(s), repaired %d — re-reading cluster", len(cluster.Nodes[redis_cluster.OtherNode]), repaired)
			}
			log.Info("Other node(s) could not be repaired — continuing maintenance on healthy nodes",
				"otherNodes", len(cluster.Nodes[redis_cluster.OtherNode]))
		}

		// Failover replicas that replicate a primary that is not OK
		// That should help ensuring that all slots are assigned to OK primaries
		var failedOver bool
		cluster, failedOver, err = r.failoverReplicas(ctx, cluster, takeover, log)
		if err != nil {
			return cluster, err
		}
		if failedOver {
			// next time, if any, be more aggressive
			takeover = true
			continue
		}

		// A restored RDB can contain keys while its fresh Redis node owns only those
		// individual hash slots (or no slots at all). Reconstruct the missing ranges
		// from the backup manifest before assignAllSlots gets a chance to spread them
		// across every temporary primary. The latter creates excess, data-bearing
		// primaries that cannot be reset or turned into replicas.
		restoredSlotsUpdated, restoredSlotsErr := r.reconcileRestoredSlotOwnership(ctx, cluster, log)
		if restoredSlotsErr != nil {
			return cluster, restoredSlotsErr
		}
		if restoredSlotsUpdated {
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			continue
		}

		// Backup-enabled clusters must never let assignAllSlots (below) empty-assign lost
		// slots — that zeroes recoverable data. Recover from backup or requeue instead;
		// empty-assign is reserved for backup-disabled (cache-only) clusters.
		if cluster.RedisCluster.Spec.BackupEnabled {
			unassignedRanges, uerr := cluster.GetUnassignedSlotsRanges()
			if uerr != nil {
				return cluster, utils.WrapErr(uerr, "failed to check unassigned slots before restore")
			}
			if len(unassignedRanges) > 0 {
				// recoverLostSlots validates latest and then previous before any
				// mutation. If neither exists or can be verified, it fails closed.
				return r.recoverLostSlots(ctx, cluster, log)
			}
		}

		// Verify that all slots are assigned to OK primaries
		// This is a pre-requisite before we can do anything with the cluster, really
		ok, updated, err := assignAllSlots(ctx, cluster, cluster.Nodes[redis_cluster.PrimaryNode], log)
		if err != nil {
			if errors.Is(err, errNoPrimaryNodesForUnassignedSlots) {
				_ = r.Metrics.CountBadSituation("missing_slots_no_primary")
			}
			return cluster, err
		}
		if updated {
			// refresh
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			continue
		}

		if !ok {
			// assignAllSlots only ever returns ok=false together with a non-nil error
			// (handled above), so this branch is currently unreachable. Keep it as an
			// active requeue rather than a silent (nil,nil) stop — which reconcile()
			// treats as "cannot maintain, no point to retry" — in case that contract
			// ever changes.
			return cluster, fmt.Errorf("slots still unassigned after assignment attempt; requeueing")
		}

		openSlots := cluster.OpenSlots()
		if len(openSlots) > 0 {
			log.Info("Detected open Redis slots, running cluster fix",
				"openSlotCount", len(openSlots),
				"openSlots", redis_cluster.OpenSlotSummary(openSlots))
			if err := cluster.Toolbox.FixCluster(cluster); err != nil {
				log.Error(err, "Failed to fix open Redis slots")
				return cluster, utils.WrapErr(err, "failed to fix open slots")
			}
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			continue
		}

		// All slots are now assigned, we can manage the cluster
		// It may be missing primaries or replicas, compared to specs

		// Remove the zombie nodes
		cluster, err = r.forgetZombies(ctx, cluster, log)
		if err != nil {
			return cluster, err
		}

		// === SCALING: Build up the cluster ===
		expectedPrimaryCount := int(cluster.RedisCluster.EffectivePrimaries())
		primaryCount := len(cluster.Nodes[redis_cluster.PrimaryNode])
		primariesWithSlots := countPrimariesWithSlots(cluster)

		// Steps 1-2: Apply autoscaling-specific endpoint and NLA safety gates
		// before the generic primary and replica maintenance sequence.
		var repaired bool
		cluster, repaired, err = r.prepareAutoscalingScaleUpMaintenance(ctx, cluster, log)
		if err != nil {
			return cluster, err
		}
		if repaired {
			continue
		}

		// Step 3: Remove redundant replicas (primaries have too many)
		// Must run before addPrimaries: during scale-up with PVC reuse,
		// new pods auto-join as replicas. Removing them creates outsiders
		// that addPrimaries can then promote.
		// Skip if scale-down is pending — downShard handles the full teardown.
		targetPodCount := int(cluster.RedisCluster.EffectivePodCount())
		currentPodCount := cluster.HealthyNodeCount()
		scaleDownPending := targetPodCount < currentPodCount
		if !scaleDownPending && r.removeRedundantReplicas(ctx, cluster, log) {
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			continue
		}

		// Step 4: Convert empty masters to replicas (excess masters with 0 slots)
		// This happens after NLA: a FORGET'd node restarts as an empty master.
		// Convert one node per iteration and refresh immediately. Continuing with
		// the stale pre-conversion topology makes addReplicas report
		// "no outsider node available" even though the conversion succeeded.
		convertedEmptyPrimary := false
		if primaryCount > expectedPrimaryCount {
			for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
				if len(primary.SlotsRanges) == 0 {
					// Find a master that needs more replicas
					targetMaster := r.findMasterNeedingReplica(cluster, expectedPrimaryCount)
					if targetMaster != nil {
						log.Info("Converting empty master to replica",
							"emptyMaster", primary.PodName,
							"targetMaster", targetMaster.PodName)
						if err := cluster.Toolbox.Replicates(primary, targetMaster); err != nil {
							log.Error(err, "Failed to convert empty master to replica", "pod", primary.PodName)
						} else {
							convertedEmptyPrimary = true
							break
						}
					}
				}
			}
		}
		if convertedEmptyPrimary {
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			continue
		}

		// Step 5: Add missing primaries
		if primaryCount < expectedPrimaryCount {
			// Redis' default replica migration can immediately move a replica
			// from an existing two-replica primary to the newly orphaned
			// primary. If the donor's other replica is an acknowledged NLA
			// candidate, that silently removes its last non-draining copy.
			// Disable migration before admitting the empty primary; addReplicas
			// below will place the new group's replicas deterministically.
			if automaticScaleUp {
				if err := replicaMigrationGuard.disable(ctx, cluster); err != nil {
					return cluster, err
				}
			}
			cluster, err = r.addPrimaries(ctx, cluster, primaryCount, expectedPrimaryCount, log)
			if err != nil {
				return cluster, err
			}
			continue // Refresh and continue
		}

		// Step 6: Rebalance if primaries need slots (must happen before addReplicas)
		if primariesWithSlots < expectedPrimaryCount && primariesWithSlots < primaryCount {
			mustRebalance = true
		}

		// Step 7: Rebalance NOW if needed (before addReplicas which skips empty primaries)
		if mustRebalance {
			if err := r.ensureEmptyPrimaryEndpointsReadyForRebalance(ctx, cluster, log); err != nil {
				return cluster, err
			}
			log.Info("Rebalancing cluster",
				"primariesWithSlots", primariesWithSlots,
				"expectedPrimaryCount", expectedPrimaryCount)
			if err := cluster.Toolbox.Rebalance(cluster, true); err != nil {
				log.Error(err, "Failed to rebalance cluster")
				return cluster, utils.WrapErr(err, "failed to rebalance cluster")
			}
			cluster, err = r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			mustRebalance = false
			continue
		}

		// Step 8: Add missing replicas
		expectedReplicaCount := int(cluster.RedisCluster.EffectivePrimaries() * cluster.RedisCluster.Spec.Replicas)
		replicaCount := len(cluster.Nodes[redis_cluster.ReplicaNode])
		if replicaCount < expectedReplicaCount {
			cluster, err = r.addReplicas(ctx, cluster, replicaCount, expectedReplicaCount, log)
			if err != nil {
				return cluster, err
			}
			continue
		}

		// === CLEANUP: Remove excess nodes ===

		// Step 9: Reset excess empty primaries (more primaries than expected)
		if primaryCount > expectedPrimaryCount {
			if r.resetExcessEmptyPrimaries(ctx, cluster, log) {
				cluster, err = r.refreshCluster(ctx, cluster)
				if err != nil {
					return cluster, err
				}
				continue
			}
		}

		// === FINALIZE ===
		_, err = r.readyClusterPods(ctx, cluster)
		if err != nil {
			log.Error(err, "Failed to update redis-ready labels")
			return cluster, utils.WrapErr(err, "failed to update redis-ready labels")
		}

		if err := replicaMigrationGuard.restore(ctx, cluster); err != nil {
			return cluster, err
		}

		return cluster, nil
	}
}

func assignAllSlots(ctx context.Context, cluster *redis_cluster.Cluster, primaryNodes []*redis_cluster.ClusterNode, mlog logr.Logger) (bool, bool, error) {

	// returns: ok, updated
	//   ok: whether the slots are all assigned
	//   updated: whether slots assignments were updated

	// verify that all slots are assigned
	unassignedSlotsRanges, err := cluster.GetUnassignedSlotsRanges()
	if err != nil {
		mlog.Error(err, "Failed to ensure all slots are assigned")
		return false, false, utils.WrapErr(err, "failed to ensure all slots are assigned")
	}
	if len(unassignedSlotsRanges) == 0 {
		return true, false, nil
	}
	if len(primaryNodes) == 0 {
		err := fmt.Errorf("%w (unassignedRanges=%d)", errNoPrimaryNodesForUnassignedSlots, len(unassignedSlotsRanges))
		mlog.Error(err, "Cluster has unassigned slots but no primaries are available for assignment")
		return false, false, utils.WrapErr(err, "failed to ensure all slots are assigned")
	}

	// detected unassigned slots - this is the first thing we need to fix, nothing else would work anyway
	// these slots have been lost and we need to assign them back to some primary nodes - but... in case
	// backup is enabled, users may want to restore?

	err = fmt.Errorf("unassigned slots")

	mlog.Error(err, "Cluster is not usable, not backed up, try to fix")

	// add to round-robin primaries, do not try to be too clever, we will rebalance later on
	primaryIndex := 0
	for _, slots := range unassignedSlotsRanges {
		if slots.First == slots.Last {
			err := cluster.Toolbox.AddSlot(primaryNodes[primaryIndex], slots.First)
			if err != nil {
				mlog.Error(err, "Failed to assign slots")
				return false, true, utils.WrapErr(err, "failed to assign slot")
			}
		} else {
			err := cluster.Toolbox.AddSlots(primaryNodes[primaryIndex], slots.First, slots.Last)
			if err != nil {
				mlog.Error(err, "Failed to assign slots")
				return false, true, utils.WrapErr(err, "failed to assign slots")
			}
		}
		primaryIndex = (primaryIndex + 1) % len(primaryNodes)
	}

	return true, true, nil
}

func (r *RedisClusterReconciler) prepareLostSlotRestore(ctx context.Context, cluster *redis_cluster.Cluster, backend BackupProvider, path string) (map[int]string, *BackupManifest, error) {
	coverage, manifest, err := r.backupSlotCoverage(cluster, path)
	if err != nil {
		return nil, nil, err
	}
	if err := r.validateBackupArtifacts(ctx, cluster, backend, path, manifest); err != nil {
		return nil, nil, err
	}
	return coverage, manifest, nil
}

// recoverLostSlots reconstructs every missing part of each manifest shard. A
// data-bearing primary reclaims only its unassigned ranges; an empty primary is
// restored before those ranges are reclaimed.
func (r *RedisClusterReconciler) recoverLostSlots(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) (*redis_cluster.Cluster, error) {
	latestPath := r.latestBackupPath(cluster)
	previousPath := r.previousBackupPath(cluster)
	backend, backendErr := r.backupBackend(cluster)
	if backendErr != nil {
		return cluster, backendErr
	}
	restorePath := latestPath
	coverage, manifest, latestErr := r.prepareLostSlotRestore(ctx, cluster, backend, latestPath)
	if latestErr != nil {
		log.Error(latestErr, "Latest backup is invalid, validating previous before mutation")
		var previousErr error
		coverage, manifest, previousErr = r.prepareLostSlotRestore(ctx, cluster, backend, previousPath)
		if previousErr != nil {
			return cluster, utils.WrapErr(errors.Join(latestErr, previousErr), "no fully validated backup generation is available for lost-slot recovery")
		}
		restorePath = previousPath
		_ = r.Metrics.CountBadSituation("restore_fallback_to_previous")
	}

	plans, planErr := planRestoredSlotOwnership(cluster, coverage)
	if planErr != nil {
		return cluster, utils.WrapErr(planErr, "refusing ambiguous lost-slot recovery")
	}
	var needRestore []*redis_cluster.ClusterNode
	var restoreTargets []restoreArtifactTarget
	var reownPlans []restoredShardPlan
	for _, plan := range plans {
		if len(plan.missing) == 0 {
			continue
		}
		dbsize, dbsizeErr := cluster.Toolbox.DBSize(plan.target.IP)
		if dbsizeErr != nil {
			return cluster, utils.WrapErr(dbsizeErr, fmt.Sprintf("failed to inspect recovery target %s", plan.target.PodName))
		}
		if dbsize == 0 {
			needRestore = append(needRestore, plan.target)
			restoreTargets = append(restoreTargets, restoreArtifactTarget{
				sourceOrdinal: plan.sourcePodIndex,
				target:        plan.target,
			})
			continue
		}
		reownPlans = append(reownPlans, plan)
	}

	var staged map[stagedArtifactKey]stagedBackupArtifact
	if len(restoreTargets) > 0 {
		var stageErr error
		staged, stageErr = r.stageMappedRDBRestoreArtifacts(ctx, cluster, backend, restorePath, manifest, restoreTargets)
		if stageErr != nil {
			return cluster, utils.WrapErr(stageErr, "failed to stage mapped lost-slot restore before topology mutation")
		}
		defer r.cleanupStagedArtifacts(cluster, staged)
	}

	reowned := 0
	for _, plan := range reownPlans {
		for _, slots := range plan.missing {
			if err := cluster.Toolbox.AddSlots(plan.target, slots.First, slots.Last); err != nil {
				return cluster, utils.WrapErr(err, fmt.Sprintf("failed to re-own slots %d-%d on pod %s", slots.First, slots.Last, plan.target.PodName))
			}
		}
		reowned++
	}
	if reowned > 0 {
		_ = r.Metrics.CountBadSituation("slot_reown_from_manifest")
		log.Info("Re-owned slots for data-bearing primaries from backup manifest", "count", reowned)
	}

	if len(needRestore) > 0 {
		log.Info("Restoring affected nodes from backup", "restorePath", restorePath, "count", len(needRestore))
		restoreErr := r.restoreStagedNodes(ctx, cluster, restorePath, needRestore, staged)
		if restoreErr != nil {
			// Have a backup but couldn't restore: requeue, never empty-assign.
			_ = r.Metrics.CountBadSituation("restore_slot_loss_recovery_failed")
			return cluster, utils.WrapErr(restoreErr, "restore of lost slots failed; requeueing")
		}
		refreshed, refreshErr := r.refreshCluster(ctx, cluster)
		if refreshErr != nil {
			return cluster, utils.WrapErr(refreshErr, "failed to refresh restored lost-slot targets")
		}
		postRestorePlans, postRestorePlanErr := planRestoredSlotOwnership(refreshed, coverage)
		if postRestorePlanErr != nil {
			return refreshed, utils.WrapErr(postRestorePlanErr, "refusing ambiguous post-restore slot recovery")
		}
		for _, plan := range postRestorePlans {
			for _, slots := range plan.missing {
				if err := refreshed.Toolbox.AddSlots(plan.target, slots.First, slots.Last); err != nil {
					return refreshed, utils.WrapErr(err, fmt.Sprintf("failed to complete restored slots %d-%d on pod %s", slots.First, slots.Last, plan.target.PodName))
				}
			}
		}
		_ = r.Metrics.CountBadSituation("restore_slot_loss_recovery")
		return refreshed, nil
	}

	// Pure re-own: refresh so a backup later this reconcile records the new coverage.
	if reowned > 0 {
		if refreshed, rerr := r.refreshCluster(ctx, cluster); rerr == nil {
			cluster = refreshed
		}
	}
	return cluster, nil
}

type restoredShardPlan struct {
	sourcePodIndex int
	target         *redis_cluster.ClusterNode
	missing        []redis_cluster.SlotsRange
}

// reconcileRestoredSlotOwnership reconstructs only the unassigned parts of a
// freshly restored cluster's slot map. It returns true when it changed Redis
// topology and the caller must refresh the cluster.
//
// A shard target is selected from an already-owned slot first. This preserves
// the data-bearing node Redis selected after loading the RDB (the production
// restore can have one such canary slot per shard). If a shard has no owned
// slots, its source primary pod index from the manifest is the deterministic
// fallback. A trusted manifest is never applied when ownership is ambiguous.
func (r *RedisClusterReconciler) reconcileRestoredSlotOwnership(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) (bool, error) {
	if !cluster.RedisCluster.Status.BackupRestored ||
		!cluster.RedisCluster.Status.RestoreTopologyPending {
		return false, nil
	}

	unassigned, err := cluster.GetUnassignedSlotsRanges()
	if err != nil {
		return false, utils.WrapErr(err, "failed to inspect restored slot ownership")
	}
	// Compare against the retained topology, not spec.primaries: autoscaling or
	// a retained larger target can make the running cluster (and therefore the
	// backup manifest) wider than the spec.
	expectedPrimaries := int(cluster.RedisCluster.EffectivePrimaries())
	expectedReplicas := expectedPrimaries * int(cluster.RedisCluster.Spec.Replicas)
	topologyNeedsRepair := len(unassigned) > 0 ||
		(len(cluster.Nodes[redis_cluster.PrimaryNode]) > expectedPrimaries &&
			len(cluster.Nodes[redis_cluster.ReplicaNode]) < expectedReplicas)
	if !topologyNeedsRepair {
		cluster.RedisCluster.Status.RestoreTopologyPending = false
		log.Info("Restore topology converged; disabling manifest-guided reconstruction")
		return false, nil
	}

	restorePath := restorePathForCluster(cluster)
	if restorePath == "" && cluster.RedisCluster.Spec.BackupEnabled {
		var selectErr error
		restorePath, selectErr = r.selectAutomaticRestorePath(ctx, cluster)
		if selectErr != nil {
			return false, utils.WrapErr(selectErr, "failed to recover selected restore generation")
		}
		if restorePath != "" {
			cluster.RedisCluster.Status.RestorePath = restorePath
			if err := r.updateRedisCluster(ctx, cluster.RedisCluster); err != nil {
				return false, utils.WrapErr(err, "failed to persist selected restore generation")
			}
		}
	}
	if restorePath == "" {
		return false, nil
	}

	manifest, manifestErr := r.readBackupManifest(cluster, restorePath)
	if manifestErr != nil {
		return false, utils.WrapErr(manifestErr, "refusing topology mutation without a valid restore manifest")
	}
	coverage, coverageErr := manifestSlotCoverage(manifest)
	if coverageErr != nil {
		return false, utils.WrapErr(coverageErr, "refusing topology mutation from invalid restore manifest")
	}
	if manifest.Primaries != expectedPrimaries || len(coverage) != expectedPrimaries {
		return false, fmt.Errorf("restore manifest topology has %d primaries, expected %d", manifest.Primaries, expectedPrimaries)
	}

	plans, planErr := planRestoredSlotOwnership(cluster, coverage)
	if planErr != nil {
		_ = r.Metrics.CountBadSituation("restore_slot_ownership_ambiguous")
		return false, utils.WrapErr(planErr, "refusing ambiguous restored slot assignment")
	}

	updated := false
	for _, plan := range plans {
		for _, slots := range plan.missing {
			if err := cluster.Toolbox.AddSlots(plan.target, slots.First, slots.Last); err != nil {
				return updated, utils.WrapErr(err,
					fmt.Sprintf("failed to restore slots %d-%d to pod %s",
						slots.First, slots.Last, plan.target.PodName))
			}
			updated = true
		}
	}
	if updated {
		_ = r.Metrics.CountBadSituation("restore_slot_ownership_from_manifest")
		log.Info("Reconstructed missing restored slots from backup manifest",
			"restorePath", restorePath,
			"shards", len(plans))
		return true, nil
	}

	// Every backup node loads an RDB, including source replicas. Once exactly
	// one target owns each complete manifest shard, the other restored primaries
	// contain only duplicate data. Redis refuses CLUSTER REPLICATE on a master
	// that still has keys, so empty those slotless duplicates before Step 2
	// converts them to replicas. Never flush a node with slots or a selected
	// shard target.
	replicaCandidates, candidateErr := restoredReplicaCandidates(cluster, plans)
	if candidateErr != nil {
		return false, utils.WrapErr(candidateErr, "refusing unsafe restored replica preparation")
	}
	flushed := false
	for _, candidate := range replicaCandidates {
		dbsize, dbsizeErr := cluster.Toolbox.DBSize(candidate.IP)
		if dbsizeErr != nil {
			return flushed, utils.WrapErr(dbsizeErr,
				fmt.Sprintf("failed to inspect restored replica candidate %s", candidate.PodName))
		}
		if dbsize == 0 {
			continue
		}

		rc, stdout, stderr, flushErr := cluster.Toolbox.FlushAll(candidate)
		if err := redisCliError(rc, stdout, stderr, flushErr); err != nil {
			return flushed, utils.WrapErr(err,
				fmt.Sprintf("failed to empty duplicate restored primary %s", candidate.PodName))
		}
		flushed = true
		_ = r.Metrics.CountBadSituation("restore_duplicate_primary_flushed", "pod_name:"+candidate.PodName)
		log.Info("Emptied duplicate restored primary before replica conversion",
			"podName", candidate.PodName,
			"discardedDuplicateKeys", dbsize)
	}
	return flushed, nil
}

func planRestoredSlotOwnership(cluster *redis_cluster.Cluster, coverage map[int]string) ([]restoredShardPlan, error) {
	if !slotCoverageIsComplete(coverage) {
		return nil, fmt.Errorf("manifest slot coverage is incomplete")
	}

	ownerBySlot := make([]*redis_cluster.ClusterNode, constants.TotalSlots)
	for _, node := range cluster.Nodes[redis_cluster.PrimaryNode] {
		for _, slots := range node.SlotsRanges {
			if slots.First < 0 || slots.Last >= constants.TotalSlots || slots.First > slots.Last {
				return nil, fmt.Errorf("primary %s reports invalid slot range %d-%d", node.PodName, slots.First, slots.Last)
			}
			for slot := slots.First; slot <= slots.Last; slot++ {
				if ownerBySlot[slot] != nil && ownerBySlot[slot] != node {
					return nil, fmt.Errorf("slot %d has multiple primary owners", slot)
				}
				ownerBySlot[slot] = node
			}
		}
	}

	sourcePodIndices := make([]int, 0, len(coverage))
	for podIndex := range coverage {
		sourcePodIndices = append(sourcePodIndices, podIndex)
	}
	slices.Sort(sourcePodIndices)

	selectedTargets := make(map[*redis_cluster.ClusterNode]int, len(sourcePodIndices))
	plans := make([]restoredShardPlan, 0, len(sourcePodIndices))
	for _, sourcePodIndex := range sourcePodIndices {
		ranges, err := parseManifestSlotRanges(coverage[sourcePodIndex])
		if err != nil {
			return nil, fmt.Errorf("source pod %d: %w", sourcePodIndex, err)
		}

		candidates := make(map[*redis_cluster.ClusterNode]struct{})
		for _, slots := range ranges {
			for slot := slots.First; slot <= slots.Last; slot++ {
				if owner := ownerBySlot[slot]; owner != nil {
					candidates[owner] = struct{}{}
				}
			}
		}

		var target *redis_cluster.ClusterNode
		switch len(candidates) {
		case 0:
			for _, node := range cluster.Nodes[redis_cluster.PrimaryNode] {
				if node.PodIndex == sourcePodIndex {
					target = node
					break
				}
			}
			if target == nil {
				return nil, fmt.Errorf("source primary pod index %d is not currently a primary", sourcePodIndex)
			}
		case 1:
			for candidate := range candidates {
				target = candidate
			}
		default:
			return nil, fmt.Errorf("manifest shard from source pod %d has slots on %d primaries",
				sourcePodIndex, len(candidates))
		}

		if otherSource, exists := selectedTargets[target]; exists {
			return nil, fmt.Errorf("primary %s matches manifest shards from source pods %d and %d",
				target.PodName, otherSource, sourcePodIndex)
		}
		selectedTargets[target] = sourcePodIndex

		missing := make([]redis_cluster.SlotsRange, 0)
		for _, slots := range ranges {
			start := -1
			for slot := slots.First; slot <= slots.Last; slot++ {
				if ownerBySlot[slot] == nil {
					if start < 0 {
						start = slot
					}
					continue
				}
				if start >= 0 {
					missing = append(missing, redis_cluster.SlotsRange{First: start, Last: slot - 1})
					start = -1
				}
			}
			if start >= 0 {
				missing = append(missing, redis_cluster.SlotsRange{First: start, Last: slots.Last})
			}
		}

		plans = append(plans, restoredShardPlan{
			sourcePodIndex: sourcePodIndex,
			target:         target,
			missing:        missing,
		})
	}
	return plans, nil
}

func restoredReplicaCandidates(cluster *redis_cluster.Cluster, plans []restoredShardPlan) ([]*redis_cluster.ClusterNode, error) {
	selectedTargets := make(map[*redis_cluster.ClusterNode]struct{}, len(plans))
	for _, plan := range plans {
		selectedTargets[plan.target] = struct{}{}
	}

	candidates := make([]*redis_cluster.ClusterNode, 0)
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if _, selected := selectedTargets[primary]; selected {
			continue
		}
		if len(primary.SlotsRanges) > 0 {
			return nil, fmt.Errorf("non-target restored primary %s still owns slots", primary.PodName)
		}
		candidates = append(candidates, primary)
	}
	return candidates, nil
}

func parseManifestSlotRanges(value string) ([]redis_cluster.SlotsRange, error) {
	parts := strings.Split(value, ",")
	ranges := make([]redis_cluster.SlotsRange, 0, len(parts))
	for _, part := range parts {
		first, last, ok := parseSlotRange(part)
		if !ok || first < 0 || last >= constants.TotalSlots {
			return nil, fmt.Errorf("invalid slot range %q", part)
		}
		ranges = append(ranges, redis_cluster.SlotsRange{First: first, Last: last})
	}
	return ranges, nil
}

// failoverReplicas issues a FAILOVER on every replica whose primary is unhealthy
// (once per primary), refreshing the cluster afterwards. Non-draining replicas
// are processed first; drain candidates only as a fallback.
func (r *RedisClusterReconciler) failoverReplicas(ctx context.Context, cluster *redis_cluster.Cluster, takeover bool, log logr.Logger) (*redis_cluster.Cluster, bool, error) {
	failedOverIDs := make([]string, 0)
	orderedReplicas := orderReplicasNonDrainingFirst(cluster.Nodes[redis_cluster.ReplicaNode])
	for _, replicaNode := range orderedReplicas {
		primaryNode, found := cluster.LookupNodeByID(replicaNode.PrimaryID)
		if (!found || !primaryNode.RoleIs(redis_cluster.PrimaryNode)) && !utils.ContainsString(failedOverIDs, replicaNode.PrimaryID) {

			if replicaNode.IsDrainCandidate() {
				log.Info("failoverReplicas: promoting a drain-marked replica because no healthy replica is available",
					"primaryID", replicaNode.PrimaryID,
					"replicaID", replicaNode.ID,
					"replicaPod", replicaNode.PodName)
				_ = r.Metrics.CountBadSituation("failover_promoting_draining_replica", "pod_name:"+replicaNode.PodName)
			}

			// primary is gone, failover
			log.Info("Failover missing node", "primaryID", replicaNode.PrimaryID, "replicaID", replicaNode.ID, "takeover", takeover)
			err := cluster.Toolbox.Failover(replicaNode, !takeover, takeover)
			if err != nil {
				log.Error(err, "Failed to failover a node", "primaryID", replicaNode.PrimaryID, "replicaID", replicaNode.ID)
				return cluster, false, utils.WrapErr(err, "failed to failover a node")
			}
			failedOverIDs = append(failedOverIDs, replicaNode.PrimaryID)

			// wait until replica has become primary
			var ok bool
			cluster, ok = r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
				node, found := c.LookupNodeByID(replicaNode.ID)
				return found && node.Info.ClusterStateIsOK && node.Role == redis_cluster.PrimaryNode
			}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)
			if !ok {
				err := fmt.Errorf("node still not a primary after retries")
				log.Error(err, "Failed to failover a node")
				return cluster, false, utils.WrapErr(err, "failed to failover a node")
			}
		}
	}
	return cluster, len(failedOverIDs) > 0, nil
}

// orderReplicasNonDrainingFirst returns a stable ordering with non-draining
// replicas first, drain candidates last. Original slice is not modified.
func orderReplicasNonDrainingFirst(replicas []*redis_cluster.ClusterNode) []*redis_cluster.ClusterNode {
	ordered := make([]*redis_cluster.ClusterNode, 0, len(replicas))
	for _, r := range replicas {
		if !r.IsDrainCandidate() {
			ordered = append(ordered, r)
		}
	}
	for _, r := range replicas {
		if r.IsDrainCandidate() {
			ordered = append(ordered, r)
		}
	}
	return ordered
}

// forgetZombies instructs primaries and replicas to forget about zombie nodes, if any, and refreshes the cluster.
func (r *RedisClusterReconciler) forgetZombies(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) (*redis_cluster.Cluster, error) {

	// for each zombie node, ensure that each primary or replica node forgets about that zombie node
	zombieCount := len(cluster.Nodes[redis_cluster.ZombieNode])

	_ = r.Metrics.Gauge(metrics.GaugeZombieNodes, float64(zombieCount))

	if zombieCount == 0 {
		return cluster, nil
	}

	_ = r.Metrics.Count(counters.ZombieDetected)

	zombieIDs := redis_cluster.GetNodeIDs(cluster.Nodes[redis_cluster.ZombieNode])
	log.Info("Detected zombie nodes to forget",
		"zombieCount", zombieCount,
		"zombieIDs", zombieIDs,
		"totalClusterNodes", len(cluster.AllNodes),
		"healthyNodes", cluster.HealthyNodeCount())

	for i, zombieNode := range cluster.Nodes[redis_cluster.ZombieNode] {
		log.Info("Forgetting zombie node", "nodeId", zombieNode.ID, "flags", zombieNode.Flags,
			"progress", fmt.Sprintf("%d/%d", i+1, zombieCount))
		forgetNode(ctx, cluster, zombieNode, log, r.Metrics)
	}

	// Refresh and check if zombies are still visible
	refreshedCluster, err := r.refreshCluster(ctx, cluster)
	if err != nil {
		return refreshedCluster, err
	}

	remainingZombieCount := len(refreshedCluster.Nodes[redis_cluster.ZombieNode])
	_ = r.Metrics.Gauge(metrics.GaugeZombieNodes, float64(remainingZombieCount))

	// Log error if zombies persist after forget (gossip may have re-added them)
	if remainingZombieCount > 0 {
		remainingZombieIDs := redis_cluster.GetNodeIDs(refreshedCluster.Nodes[redis_cluster.ZombieNode])

		// Find which healthy nodes still see each zombie - critical for debugging gossip issues
		nodesStillSeeingZombies := getNodesStillSeeingZombies(refreshedCluster)

		err := fmt.Errorf("zombie nodes still visible after CLUSTER FORGET")
		log.Error(err, "Zombies persist - gossip may have re-added them before all nodes received FORGET",
			"remainingCount", remainingZombieCount,
			"remainingIDs", remainingZombieIDs,
			"originalIDs", zombieIDs,
			"nodesStillSeeingZombies", nodesStillSeeingZombies)
		_ = r.Metrics.Count(counters.ZombiePersisted)
		_ = r.Metrics.CountBadSituation("zombie_persisted_after_forget")
	}

	return refreshedCluster, nil
}

// forgetNode instructs primaries and replicas to forget a node with retry on partial failure.
func forgetNode(ctx context.Context, cluster *redis_cluster.Cluster, zombie *redis_cluster.ClusterNode, log logr.Logger, m metrics.Metrics) {
	start := time.Now()
	defer func() { _ = m.Timing(metrics.TimingForgetDuration, time.Since(start).Milliseconds()) }()

	log.Info("Starting batch CLUSTER FORGET", "zombieNodeID", zombie.ID)

	successCount, failedIPs, err := cluster.Toolbox.ForgetNodeFromAll(cluster, zombie)
	if err != nil {
		log.Error(err, "Cannot execute batch CLUSTER FORGET", "zombieNodeID", zombie.ID)
		return
	}

	// Retry only failed IPs
	for attempt := 1; len(failedIPs) > 0 && attempt <= cluster.Timeouts.ForgetRetries; attempt++ {
		log.Info("Retrying CLUSTER FORGET for failed nodes",
			"zombieNodeID", zombie.ID,
			"attempt", attempt,
			"failedCount", len(failedIPs))
		time.Sleep(cluster.Timeouts.ForgetRetryDelay)

		retrySuccess, stillFailed := cluster.Toolbox.ForgetNodeFromIPs(failedIPs, cluster.RedisPort, zombie.ID)
		successCount += retrySuccess
		failedIPs = stillFailed
	}

	duration := time.Since(start).Milliseconds()

	log.Info("Batch CLUSTER FORGET completed",
		"zombieNodeID", zombie.ID,
		"successCount", successCount,
		"failCount", len(failedIPs),
		"duration_ms", duration)

	if len(failedIPs) > 0 {
		log.Info("WARNING: Some nodes still failed to forget zombie after retries",
			"zombieNodeID", zombie.ID,
			"failedIPs", failedIPs,
			"retries", cluster.Timeouts.ForgetRetries)
		_ = m.CountBadSituation("forget_partial_failure", "pod_name:"+zombie.PodName)
	}
}

// detectAndFixPartitionedNodes detects nodes that can't see each other and re-introduces
// them via CLUSTER MEET. Two steps:
//
//  1. Full partition: after a full pod restart, all IPs change and most nodes see < half peers.
//     Verified by checking if gossip IPs don't match current pod IPs (stale nodes.conf).
//     Since no single node has a correct view, we star-MEET all nodes through one reference.
//
//  2. Partial disconnect: individual nodes see fewer peers than the best-connected node,
//     or are invisible to the rest of the cluster (orphaned). This covers both split-topology
//     (operator restart mid-MEET left two internally-consistent subgroups) and orphaned nodes
//     (network blip left a node thinking it's connected while nobody sees it). All such nodes
//     are repaired by MEETing them with the best-connected reference node.
//
// This must run before slot assignment checks, which fail on a partitioned cluster.
func (r *RedisClusterReconciler) detectAndFixPartitionedNodes(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) (*redis_cluster.Cluster, error) {
	if len(cluster.AllNodes) < 2 {
		return cluster, nil
	}

	expectedNodes := int(cluster.RedisCluster.EffectivePodCount())

	// ── Step 1: Full partition ──
	// When most nodes see fewer than half the expected peers, the entire gossip
	// graph is broken (typically all IPs changed after a full pod restart).
	// No single node has a correct view, so we star-MEET all through AllNodes[0].
	partitionedCount := 0
	for _, node := range cluster.AllNodes {
		if node.Info.KnownNodes < expectedNodes/2 {
			partitionedCount++
		}
	}

	if partitionedCount >= len(cluster.AllNodes)/2 {
		currentIPs := make(map[string]bool, len(cluster.AllNodes))
		for _, node := range cluster.AllNodes {
			if node.IP != "" {
				currentIPs[node.IP] = true
			}
		}

		staleIPCount := 0
		for _, node := range cluster.AllNodes {
			if node.IP == "" {
				continue
			}
			_, stdout, _, err := cluster.Toolbox.ClusterNodes(ctx, node.IP)
			if err != nil {
				continue
			}
			for _, line := range strings.Split(stdout, "\n") {
				fields := strings.Fields(line)
				if len(fields) < 3 {
					continue
				}
				if strings.Contains(fields[2], "myself") {
					continue
				}
				peerAddr := strings.Split(fields[1], "@")[0]
				peerIP := strings.Split(peerAddr, ":")[0]
				if peerIP != "" && !currentIPs[peerIP] {
					staleIPCount++
				}
			}
			break
		}

		if staleIPCount == 0 {
			log.Info("Cluster appears partitioned but no stale IPs found — skipping auto-MEET",
				"partitionedNodes", partitionedCount)
			return cluster, nil
		}

		log.Info("Detected full partition with stale IPs — star-MEET all nodes",
			"partitionedNodes", partitionedCount,
			"totalNodes", len(cluster.AllNodes),
			"expectedNodes", expectedNodes,
			"staleIPCount", staleIPCount)
		_ = r.Metrics.CountBadSituation("cluster_partitioned")

		refNode := cluster.AllNodes[0]
		meetCount := 0
		for _, node := range cluster.AllNodes[1:] {
			if err := cluster.Toolbox.Meet(refNode.IP, node.IP, cluster.RedisPort); err != nil {
				log.Error(err, "Failed to MEET node during partition repair",
					"refIP", refNode.IP, "targetIP", node.IP, "targetPod", node.PodName)
				continue
			}
			meetCount++
		}

		log.Info("Full partition repair completed", "meetCount", meetCount)
		time.Sleep(constants.GossipDelay)

		refreshed, err := r.refreshCluster(ctx, cluster)
		if err != nil {
			return cluster, err
		}
		return refreshed, nil
	}

	// ── Step 2: Partial disconnect ──
	// After ruling out a full partition, detect nodes with incomplete connectivity.
	// A node needs repair if it is:
	//   - under-connected: sees fewer peers than the best-connected node (split topology)
	//   - orphaned: thinks it's in the cluster but invisible to all others (stale gossip)
	// Both are fixed identically: CLUSTER MEET with the best-connected reference.

	maxKnown := 0
	for _, node := range cluster.AllNodes {
		if node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) && len(node.Views) > 0 && node.Info.KnownNodes > maxKnown {
			maxKnown = node.Info.KnownNodes
		}
	}
	if maxKnown == 0 {
		for _, node := range cluster.AllNodes {
			if node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) && node.Info.KnownNodes > maxKnown {
				maxKnown = node.Info.KnownNodes
			}
		}
	}

	var orphanedNodes, splitNodes []*redis_cluster.ClusterNode
	for _, node := range cluster.AllNodes {
		if !node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) {
			continue
		}
		if node.Info.KnownNodes >= 1 && len(node.Views) == 0 {
			orphanedNodes = append(orphanedNodes, node)
		} else if node.Info.KnownNodes > 0 && node.Info.KnownNodes < maxKnown {
			splitNodes = append(splitNodes, node)
		}
	}

	if len(orphanedNodes) == 0 && len(splitNodes) == 0 {
		if maxKnown > 0 && maxKnown < cluster.HealthyNodeCount() {
			// Even split: every node sees the same subset, but nobody sees the full cluster.
			log.Info("Detected even split — all nodes see same peer count but less than actual cluster",
				"maxKnownNodes", maxKnown,
				"healthyNodeCount", cluster.HealthyNodeCount())
			_ = r.Metrics.CountBadSituation("split_topology_detected")
			refNode := cluster.AllNodes[0]
			meetCount := 0
			for _, node := range cluster.AllNodes[1:] {
				if err := cluster.Toolbox.Meet(refNode.IP, node.IP, cluster.RedisPort); err != nil {
					log.Error(err, "Failed to MEET node during even-split repair",
						"refIP", refNode.IP, "targetPod", node.PodName)
					continue
				}
				meetCount++
			}
			log.Info("Even split repair completed", "meetCount", meetCount)
			refreshed, err := r.refreshCluster(ctx, cluster)
			if err != nil {
				return cluster, err
			}
			return refreshed, nil
		}
		return cluster, nil
	}

	// Pick the best reference: highest KnownNodes + healthy state, with fallback.
	var refNode *redis_cluster.ClusterNode
	for _, node := range cluster.AllNodes {
		if !node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) {
			continue
		}
		if node.Info.KnownNodes == maxKnown && node.Info.ClusterStateIsOK {
			refNode = node
			break
		}
	}
	if refNode == nil {
		for _, node := range cluster.AllNodes {
			if node.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) && node.Info.KnownNodes == maxKnown {
				refNode = node
				break
			}
		}
	}
	if refNode == nil {
		log.Info("Connectivity issues detected but no reference node available",
			"orphanedCount", len(orphanedNodes),
			"splitCount", len(splitNodes))
		return cluster, nil
	}

	log.Info("Detected nodes with incomplete cluster view — repairing via CLUSTER MEET",
		"orphanedCount", len(orphanedNodes),
		"splitCount", len(splitNodes),
		"maxKnownNodes", maxKnown,
		"expectedNodes", expectedNodes,
		"refNodePod", refNode.PodName,
		"refNodeKnownNodes", refNode.Info.KnownNodes)

	attemptCount := 0
	var lastMeetErr error
	meetNode := func(node *redis_cluster.ClusterNode) bool {
		attemptCount++
		log.Info("CLUSTER MEET to repair connectivity",
			"podName", node.PodName, "ip", node.IP,
			"knownNodes", node.Info.KnownNodes, "views", len(node.Views),
			"refPod", refNode.PodName, "refIP", refNode.IP)
		if err := cluster.Toolbox.Meet(refNode.IP, node.IP, cluster.RedisPort); err != nil {
			lastMeetErr = err
			log.Error(err, "Failed to MEET node", "podName", node.PodName)
			return false
		}
		return true
	}

	// Skip orphan repair for pods that are draining or terminating.
	podList, podErr := r.GetRedisClusterPods(ctx, cluster.RedisCluster)
	if podErr != nil {
		log.Error(podErr, "Failed to get pod list for drain check, proceeding with repair")
	}

	meetCount := 0
	for _, node := range orphanedNodes {
		if podList != nil && podIsDrainingOrTerminating(podList, node.IP) {
			log.Error(nil, "Skipping orphan repair — pod is draining or terminating",
				"podName", node.PodName, "ip", node.IP,
				"drainCandidate", isPodDrainCandidate(podList, node.IP),
				"terminating", isPodTerminating(podList, node.IP))
			continue
		}
		if meetNode(node) {
			meetCount++
		}
	}
	for _, node := range splitNodes {
		if meetNode(node) {
			meetCount++
		}
	}

	if len(orphanedNodes) > 0 {
		_ = r.Metrics.CountBadSituation("orphaned_nodes_detected")
	}
	if len(splitNodes) > 0 {
		_ = r.Metrics.CountBadSituation("split_topology_detected")
	}

	log.Info("Connectivity repair completed", "meetCount", meetCount, "attempted", len(orphanedNodes)+len(splitNodes))

	refreshed, err := r.refreshCluster(ctx, cluster)
	if err != nil {
		return cluster, err
	}
	if attemptCount > 0 && meetCount == 0 {
		return refreshed, fmt.Errorf("connectivity repair failed for all %d attempted node(s): %w", attemptCount, lastMeetErr)
	}
	return refreshed, fmt.Errorf("%w: repaired %d of %d attempted node(s)", errConnectivityRepairPending, meetCount, attemptCount)
}

// addPrimaries adds missing primaries from outsider nodes, and refreshes the cluster.
func (r *RedisClusterReconciler) addPrimaries(ctx context.Context, cluster *redis_cluster.Cluster, count int, expectedCount int, log logr.Logger) (*redis_cluster.Cluster, error) {

	// while count < expectedCount, try to pick an outsider node and add it as a primary and wait until it is effectively a primary

	log.Info("Missing primary nodes",
		"expectedCount", expectedCount,
		"count", count,
		"availableOutsiders", len(cluster.Nodes[redis_cluster.OutsiderNode]))

	for count < expectedCount {
		outsiderNode := nonDrainingPrimaryOutsider(cluster)
		if outsiderNode == nil {
			err := fmt.Errorf("no non-draining outsider node available")
			log.Error(err, "Failed to add a primary",
				"currentCount", count,
				"expectedCount", expectedCount)
			return cluster, utils.WrapErr(err, "failed to add a primary")
		}
		log.Info("Add outsider node as primary",
			"newNodeIP", outsiderNode.IP,
			"newNodePodName", outsiderNode.PodName,
			"newNodeID", outsiderNode.ID)
		err := cluster.Toolbox.AddPrimary(getHealthyRefNode(cluster, outsiderNode), outsiderNode, cluster.RedisPort)
		if err != nil {
			log.Error(err, "Failed to add a primary", "newNodeIP", outsiderNode.IP)
			return cluster, utils.WrapErr(err, "failed to add a primary")
		}
		var ok bool
		cluster, ok = r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
			node, found := c.LookupNodeByID(outsiderNode.ID)
			return found && node.Info.ClusterStateIsOK && node.RoleIs(redis_cluster.PrimaryNode)
		}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)
		if !ok {
			err := fmt.Errorf("node still not a primary after retries")
			log.Error(err, "Failed to add a primary")
			return cluster, utils.WrapErr(err, "failed to add a primary")
		}
		count++
	}
	return cluster, nil
}

// addReplicas adds missing replicas from outsider nodes, and refreshes the cluster.
func (r *RedisClusterReconciler) addReplicas(ctx context.Context, cluster *redis_cluster.Cluster, count int, expectedCount int, mlog logr.Logger) (*redis_cluster.Cluster, error) {

	// while count < expectedCount, try to pick an outsider node and add it as a replica and wait until it is effectively a replica

	mlog.Info("Missing replica nodes",
		"expectedCount", expectedCount,
		"count", count,
		"availableOutsiders", len(cluster.Nodes[redis_cluster.OutsiderNode]))

	for i, outsider := range cluster.Nodes[redis_cluster.OutsiderNode] {
		debugInfo := outsider.GetRoleDebugInfo()
		mlog.Info("Available outsider node",
			"index", i,
			"podName", debugInfo["podName"],
			"ip", debugInfo["ip"],
			"flags", debugInfo["flags"],
			"knownNodes", debugInfo["knownNodes"],
			"clusterSize", debugInfo["clusterSize"])
	}

	for count < expectedCount {
		if len(cluster.Nodes[redis_cluster.OutsiderNode]) == 0 {
			err := fmt.Errorf("no outsider node available")
			mlog.Error(err, "Failed to add a replica",
				"currentCount", count,
				"expectedCount", expectedCount)
			return cluster, utils.WrapErr(err, "failed to add a replica")
		}

		// Find the primary with the fewest replicas to avoid the reset loop
		// (adding to a primary that already has enough replicas triggers reset)
		replicaMap := buildReplicaMap(cluster)
		targetPrimary := findPrimaryWithFewestReplicas(cluster, replicaMap, int(cluster.RedisCluster.Spec.Replicas))
		if targetPrimary == nil {
			err := fmt.Errorf("no primary needs more replicas")
			mlog.Error(err, "Failed to add a replica - all primaries have enough replicas")
			return cluster, utils.WrapErr(err, "failed to add a replica")
		}

		outsiderNode := preferredReplicaOutsider(cluster)
		if outsiderNode == nil {
			err := fmt.Errorf("no outsider node available for replica")
			mlog.Error(err, "Failed to add a replica",
				"currentCount", count,
				"expectedCount", expectedCount)
			return cluster, utils.WrapErr(err, "failed to add a replica")
		}
		mlog.Info("Add outsider node as replica",
			"newNodeIP", outsiderNode.IP,
			"newNodePodName", outsiderNode.PodName,
			"newNodeID", outsiderNode.ID,
			"targetPrimaryID", targetPrimary.ID,
			"targetPrimaryIP", targetPrimary.IP)
		err := cluster.Toolbox.AddReplica(targetPrimary, outsiderNode, cluster.RedisPort, targetPrimary.ID)
		if err != nil {
			mlog.Error(err, "Failed to add a replica", "newNodeIP", outsiderNode.IP)
			return cluster, utils.WrapErr(err, "failed to add a replica")
		}
		var ok bool
		cluster, ok = r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
			node, found := c.LookupNodeByID(outsiderNode.ID)
			return found && node.Info.ClusterStateIsOK && node.RoleIs(redis_cluster.ReplicaNode)
		}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)
		if !ok {
			err := fmt.Errorf("node still not a replica after retries")
			mlog.Error(err, "Failed to add a replica")
			return cluster, utils.WrapErr(err, "failed to add a replica")
		}
		count++
	}
	return cluster, nil
}

// buildReplicaMap builds a map of primaryID → replica count (O(n) instead of O(n²) lookups).
func buildReplicaMap(cluster *redis_cluster.Cluster) map[string]int {
	replicaMap := make(map[string]int)
	for _, replica := range cluster.Nodes[redis_cluster.ReplicaNode] {
		replicaMap[replica.PrimaryID]++
	}
	return replicaMap
}

// findPrimaryWithFewestReplicas returns the primary node that has the fewest replicas
// and still needs more (fewer than maxReplicas). Returns nil if all primaries have enough.
// Uses pre-built replicaMap for O(n) instead of O(n²) performance.
func findPrimaryWithFewestReplicas(cluster *redis_cluster.Cluster, replicaMap map[string]int, maxReplicas int) *redis_cluster.ClusterNode {
	var bestPrimary *redis_cluster.ClusterNode
	minReplicas := maxReplicas // Start with max, so we only pick primaries that need more

	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		// Skip empty masters (no slots) - they should be reset, not receive replicas
		if len(primary.SlotsRanges) == 0 {
			continue
		}
		replicaCount := replicaMap[primary.ID]
		// Only consider primaries that need more replicas
		if replicaCount < minReplicas {
			minReplicas = replicaCount
			bestPrimary = primary
		}
	}
	return bestPrimary
}

func getHealthyRefNode(cluster *redis_cluster.Cluster, excludeNode *redis_cluster.ClusterNode) *redis_cluster.ClusterNode {
	// Primary with slots (best)
	for _, n := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if len(n.SlotsRanges) > 0 && (excludeNode == nil || n.ID != excludeNode.ID) {
			return n
		}
	}
	// Replica with views (seen by others)
	for _, n := range cluster.Nodes[redis_cluster.ReplicaNode] {
		if len(n.Views) > 1 && (excludeNode == nil || n.ID != excludeNode.ID) {
			return n
		}
	}
	// Fallback
	return cluster.Node
}

func restorePathForCluster(cluster *redis_cluster.Cluster) string {
	if cluster.RedisCluster.Spec.RestoreFrom != "" {
		return cluster.RedisCluster.Spec.RestoreFrom
	}
	return cluster.RedisCluster.Status.RestorePath
}

func (r *RedisClusterReconciler) formCluster(ctx context.Context, cluster *redis_cluster.Cluster) (health.ClusterHealth, error) {

	// Create the Redis cluster either from scratch or from backups
	restorePath := restorePathForCluster(cluster)
	if restorePath == "" && cluster.RedisCluster.Spec.BackupEnabled {
		var selectErr error
		restorePath, selectErr = r.selectAutomaticRestorePath(ctx, cluster)
		if selectErr != nil {
			// Don't guess: forming an empty cluster when a backup may exist risks
			// discarding recoverable data. Requeue and retry the probe.
			return health.Error, utils.WrapErr(selectErr, "backup selection failed during cluster formation; requeueing")
		}
		if restorePath != "" {
			r.Log.Info("No explicit restoreFrom set, auto-restoring from backup", "restorePath", restorePath)
		}
	}

	if restorePath != "" {
		if cluster.RedisCluster.Status.RestorePath != restorePath {
			cluster.RedisCluster.Status.RestorePath = restorePath
			if err := r.updateRedisCluster(ctx, cluster.RedisCluster); err != nil {
				return health.Warn, utils.WrapErr(err, "failed to persist selected restore generation")
			}
		}

		// If backups have not been restored yet, restore
		if !cluster.RedisCluster.Status.BackupRestored {
			r.Log.Info("Restore the Redis nodes from backups", "restoreFrom", restorePath)
			if err := r.restore(ctx, cluster, restorePath); err != nil {
				r.Log.Error(err, "Failed to restore from backups")
				return health.Error, err
			}
			r.Log.Info("Restored the Redis nodes from backups", "restoreFrom", restorePath)
			_ = r.Metrics.Count(counters.ClusterRestored)

			// Save the status change
			cluster.RedisCluster.Status.BackupRestored = true
			err := r.updateRedisCluster(ctx, cluster.RedisCluster)
			if err != nil {
				return health.Warn, err
			}
		}

		// And then create the restored cluster
		r.Log.Info("Create Redis cluster from restored nodes")
		if err := cluster.CreateFromRestoredNodes(); err != nil {
			r.Log.Error(err, "failed to create the Redis cluster")
			return health.Error, err
		}
		_ = r.Metrics.Count(counters.ClusterMeet)

		// Only bypass the slot-based formation gate after every restored node has
		// accepted CLUSTER MEET. If meeting the nodes fails, leave this false so
		// the next reconciliation retries formation instead of entering maintain.
		if !cluster.RedisCluster.Status.RestoreTopologyPending {
			cluster.RedisCluster.Status.RestoreTopologyPending = true
			if err := r.updateRedisCluster(ctx, cluster.RedisCluster); err != nil {
				return health.Warn, err
			}
		}
	} else {
		r.Log.Info("Create Redis cluster from empty nodes")
		err := cluster.CreateFromEmptyNodes()
		if err != nil {
			if !shouldRetryCreateAfterHardReset(err) {
				r.Log.Error(err, "failed to create the Redis cluster")
				return health.Error, err
			}
			candidates := staleNodesForCreateHardReset(cluster)
			if len(candidates) == 0 {
				r.Log.Error(err, "failed to create the Redis cluster and no stale-reset candidates were found")
				return health.Error, err
			}
			r.Log.Info("Create Redis cluster failed with stale membership signature, resetting stale nodes and retrying",
				"candidateCount", len(candidates))
			resetSuccessCount := 0
			for _, n := range candidates {
				// Skip any node that still holds keys: CLUSTER RESET HARD wipes the
				// dataset, and Redis allows it on replicas and slotless primaries — which
				// may hold the only copy of the data. If DBSIZE is >0 or unreadable, don't reset.
				if dbsize, derr := cluster.Toolbox.DBSize(n.IP); derr != nil {
					r.Log.Error(derr, "Skipping stale-node hard reset: could not read DBSIZE (refusing to risk a data-bearing wipe)",
						"podName", n.PodName, "podIP", n.IP)
					_ = r.Metrics.CountBadSituation("hard_reset_skipped_dbsize_unknown", "pod_name:"+n.PodName)
					continue
				} else if dbsize > 0 {
					r.Log.Error(fmt.Errorf("node holds %d keys", dbsize), "Skipping stale-node hard reset: node holds data",
						"podName", n.PodName, "podIP", n.IP, "dbsize", dbsize)
					_ = r.Metrics.CountBadSituation("hard_reset_skipped_data_present", "pod_name:"+n.PodName)
					continue
				}
				r.Log.Info("Resetting stale node before retrying cluster create",
					"podName", n.PodName,
					"podIP", n.IP,
					"knownNodes", n.Info.KnownNodes,
					"slotsAssigned", n.Info.SlotsAssigned,
					"nodeID", n.ID)
				if resetErr := cluster.Toolbox.ResetNode(n, true); resetErr != nil {
					r.Log.Error(resetErr, "Failed to reset stale node before retrying cluster create", "podName", n.PodName)
					continue
				}
				resetSuccessCount++
			}
			if resetSuccessCount == 0 {
				return health.Error, err
			}
			err = cluster.CreateFromEmptyNodes()
			if err != nil {
				r.Log.Error(err, "failed to create the Redis cluster after stale-node reset retry")
				return health.Error, err
			}
		}
		_ = r.Metrics.Count(counters.ClusterCreated)
	}

	r.Log.Info("successfully created the Redis cluster")

	// Label all pods as ready so they become selectable by the service resource (DNS)
	if err := r.readyClusterAllPods(ctx, cluster); err != nil {
		// log but proceed anyway, will fix it later
		r.Log.Error(err, "failed to label pods as ready")
	}

	// Restore role=active on STS template so restarted pods also join the service
	sts := cluster.StatefulSet
	if !constants.IsRoleActive(sts.Spec.Template.Labels) {
		if sts.Spec.Template.Labels == nil {
			sts.Spec.Template.Labels = make(map[string]string)
		}
		constants.ActivateRole(sts.Spec.Template.Labels)
		if err := r.Update(ctx, sts); err != nil {
			r.Log.Error(err, "failed to restore role=active on STS template")
		}
	}

	return health.OK, nil
}

func staleNodesForCreateHardReset(cluster *redis_cluster.Cluster) []*redis_cluster.ClusterNode {
	candidates := make([]*redis_cluster.ClusterNode, 0)
	for _, n := range cluster.AllNodes {
		if n.IP == "" {
			continue
		}
		if n.Info.KnownNodes <= 1 {
			continue
		}
		if n.Info.SlotsAssigned > 0 {
			continue
		}
		if len(n.SlotsRanges) > 0 {
			continue
		}
		candidates = append(candidates, n)
	}
	return candidates
}

func shouldRetryCreateAfterHardReset(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	signatures := []string{
		"is not empty",
		"already knows",
		"already part of",
		"cluster configuration",
	}
	for _, signature := range signatures {
		if strings.Contains(msg, signature) {
			return true
		}
	}
	return false
}

func (r *RedisClusterReconciler) performFailover(ctx context.Context, cluster *redis_cluster.Cluster, replicaNode *redis_cluster.ClusterNode, primaryNode *redis_cluster.ClusterNode) error {
	return r.performFailoverWithAbort(ctx, cluster, replicaNode, primaryNode, nil)
}

func (r *RedisClusterReconciler) performFailoverWithAbort(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	replicaNode *redis_cluster.ClusterNode,
	primaryNode *redis_cluster.ClusterNode,
	abortCheck cleanFailoverAbortCheck,
) error {
	start := time.Now()

	r.Log.Info("Initiating failover",
		"primaryNodeID", primaryNode.ID,
		"primaryPod", primaryNode.PodName,
		"replicaNodeID", replicaNode.ID,
		"replicaPod", replicaNode.PodName)

	if err := r.waitForReplicaReadyForCleanFailover(ctx, cluster, replicaNode, primaryNode, abortCheck); err != nil {
		if errors.Is(err, errFailoverSuperseded) {
			r.Log.Info("Skipping failover because the replacement primary is synchronized",
				"primaryNodeID", primaryNode.ID,
				"replicaNodeID", replicaNode.ID)
			return err
		}
		r.Log.Error(err, "Replica is not ready for clean failover",
			"primaryNodeID", primaryNode.ID,
			"replicaNodeID", replicaNode.ID)
		return err
	}

	// Request a clean failover.
	err := cluster.Toolbox.Failover(replicaNode, false, false)
	if err != nil {
		if abortCheck != nil {
			if info, infoErr := cluster.Toolbox.GetReplicationInfo(replicaNode.IP); infoErr == nil {
				abort, abortErr := abortCheck(ctx, info)
				if abortErr != nil {
					return abortErr
				}
				if abort {
					r.Log.Info("Failover command was superseded by the synchronized replacement primary",
						"primaryNodeID", primaryNode.ID,
						"replicaNodeID", replicaNode.ID)
					return errFailoverSuperseded
				}
			}
		}
		r.Log.Error(err, "Failover command failed",
			"primaryNodeID", primaryNode.ID,
			"replicaNodeID", replicaNode.ID)
		return err
	}

	r.Log.Info("Waiting for current primary to become the replica", "primary_node", primaryNode.ID)
	var completionErr error
	var superseded bool
	_, success := r.retryRefreshClusterUntil(ctx, cluster, func(c *redis_cluster.Cluster) bool {
		if rolesHaveBeenSwapped(c, primaryNode, replicaNode) {
			return true
		}
		if abortCheck == nil {
			return false
		}
		info, infoErr := c.Toolbox.GetReplicationInfo(replicaNode.IP)
		if infoErr != nil {
			return false
		}
		superseded, completionErr = abortCheck(ctx, info)
		return superseded || completionErr != nil
	}, cluster.Timeouts.PollInterval, cluster.Timeouts.OperationTimeout)

	duration := time.Since(start).Milliseconds()
	if completionErr != nil {
		return completionErr
	}
	if superseded {
		r.Log.Info("Failover completion was superseded by the synchronized replacement primary",
			"primaryNodeID", primaryNode.ID,
			"replicaNodeID", replicaNode.ID,
			"duration_ms", duration)
		return errFailoverSuperseded
	}

	if !success {
		r.Log.Error(fmt.Errorf("failover timeout"), "Failed to observe failover completion",
			"primaryNodeID", primaryNode.ID,
			"replicaNodeID", replicaNode.ID,
			"duration_ms", duration)
		_ = r.Metrics.Timing(metrics.TimingFailoverDuration, duration, "status:timeout")
		return fmt.Errorf("failed to observe the failover after %v", cluster.Timeouts.OperationTimeout)
	}

	r.Log.Info("Failover complete",
		"newPrimaryNodeID", replicaNode.ID,
		"newPrimaryPod", replicaNode.PodName,
		"newReplicaNodeID", primaryNode.ID,
		"duration_ms", duration)
	_ = r.Metrics.Timing(metrics.TimingFailoverDuration, duration, "status:success")

	return nil
}

func (r *RedisClusterReconciler) waitForReplicaReadyForCleanFailover(
	ctx context.Context,
	cluster *redis_cluster.Cluster,
	replicaNode *redis_cluster.ClusterNode,
	primaryNode *redis_cluster.ClusterNode,
	abortCheck cleanFailoverAbortCheck,
) error {
	deadline := time.Now().Add(cluster.Timeouts.OperationTimeout)
	for {
		info, err := cluster.Toolbox.GetReplicationInfo(replicaNode.IP)
		if err == nil {
			if abortCheck != nil {
				abort, abortErr := abortCheck(ctx, info)
				if abortErr != nil {
					return abortErr
				}
				if abort {
					return errFailoverSuperseded
				}
			}
			if replicaReadyForCleanFailover(info, primaryNode.IP) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			if err != nil {
				return fmt.Errorf("replica %s was not ready for clean failover after %v: %w", replicaNode.ID, cluster.Timeouts.OperationTimeout, err)
			}
			return fmt.Errorf("replica %s was not synchronized with primary %s after %v", replicaNode.ID, primaryNode.ID, cluster.Timeouts.OperationTimeout)
		}

		r.Log.Info("Waiting for replica replication link before clean failover",
			"primaryNodeID", primaryNode.ID,
			"replicaNodeID", replicaNode.ID,
			"error", err)
		timer := time.NewTimer(cluster.Timeouts.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func replicaReadyForCleanFailover(info, primaryIP string) bool {
	masterHost, synchronized := synchronizedReplicaMaster(info)
	return synchronized && masterHost == primaryIP
}

func synchronizedReplicaMaster(info string) (string, bool) {
	fields := make(map[string]string)
	for _, line := range strings.Split(info, "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if found {
			fields[key] = value
		}
	}
	return fields["master_host"], fields["role"] == "slave" &&
		fields["master_host"] != "" &&
		fields["master_link_status"] == "up" &&
		fields["master_sync_in_progress"] == "0"
}

func (r *RedisClusterReconciler) handleRollout(ctx context.Context, cluster *redis_cluster.Cluster) (bool, error) {

	// rollout replica nodes first then primary nodes. if a node redis version != specs,
	// the node is removed from the cluster and the pod is terminated, and this method
	// returns immediately. the reconciliation mechanism will re-insert the re-created
	// pod back in the cluster in due time, and then come back to this method in order
	// to process more nodes, if required.
	//
	// this way... we roll out one node after another, since we do not enter this method
	// if the cluster is not full OK

	for _, node := range cluster.Nodes[redis_cluster.ReplicaNode] {
		rolledOut, err := r.rolloutNode(ctx, cluster, node)
		if rolledOut || err != nil {
			return true, err
		}
	}

	for _, node := range cluster.Nodes[redis_cluster.PrimaryNode] {
		rolledOut, err := r.rolloutNode(ctx, cluster, node)
		if rolledOut || err != nil {
			return true, err
		}
	}

	return false, nil
}

func (r *RedisClusterReconciler) rolloutNode(ctx context.Context, cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode) (bool, error) {
	if node.RedisVersion == cluster.RedisVersion {
		return false, nil
	}
	// take the node out of the cluster
	err := r.takeNodeOut(ctx, cluster, node, nil)
	if err != nil {
		return true, err
	}
	// get the corresponding pod
	pod := &corev1.Pod{}
	err = r.Client.Get(ctx, client.ObjectKey{
		Namespace: cluster.RedisCluster.Namespace,
		Name:      node.PodName,
	}, pod)
	if err != nil {
		return true, err
	}
	// terminate the pod
	err = r.Client.Delete(ctx, pod)
	if err != nil {
		return true, err
	}
	return true, nil
}

func (r *RedisClusterReconciler) takeNodeOut(ctx context.Context, cluster *redis_cluster.Cluster, node *redis_cluster.ClusterNode, preferReplica *redis_cluster.ClusterNode) error {

	// if the node on the pod is not a primary, we do not need to failover, just forget it and mark it done
	if !node.RoleIs(redis_cluster.PrimaryNode) {
		if node.RoleIs(redis_cluster.ReplicaNode) {
			r.Log.Info("Node is a replica, forgetting without failover", "pod_name", node.PodName, "pod_ip", node.IP, "nodeID", node.ID)
		} else {
			r.Log.Info("Node is neither primary nor replica, forgetting", "pod_name", node.PodName, "pod_ip", node.IP, "nodeID", node.ID)
		}
		forgetNode(ctx, cluster, node, r.Log, r.Metrics) // remove from cluster
		return nil
	}

	// node is a primary, failover
	r.Log.Info("Node is a primary, need to fail over", "pod_name", node.PodName, "pod_ip", node.IP)

	// Prefer a non-draining replica; fall back to any replica with a metric.
	replicaNode := preferReplica
	if replicaNode == nil {
		replicaNode = cluster.GetNonDrainingReplicaOf(node)
	}
	if replicaNode == nil {
		replicaNode = cluster.GetReplicaOf(node)
		if replicaNode != nil {
			r.Log.Info("takeNodeOut: only draining replicas available — promoting anyway",
				"primaryPod", node.PodName, "replicaPod", replicaNode.PodName)
			_ = r.Metrics.CountBadSituation("take_node_out_promoting_draining_replica", "pod_name:"+node.PodName)
		}
	}
	if replicaNode == nil {
		return fmt.Errorf("failed to find a replica of primary %s", node.ID)
	}

	err := r.performFailover(ctx, cluster, replicaNode, node)
	if err != nil {
		return err
	}

	forgetNode(ctx, cluster, node, r.Log, r.Metrics)
	return nil
}

func getNodesStillSeeingZombies(cluster *redis_cluster.Cluster) map[string][]string {
	result := make(map[string][]string)

	for _, zombieNode := range cluster.Nodes[redis_cluster.ZombieNode] {
		var seenBy []string
		// Check which healthy nodes have this zombie in their Views
		for _, healthyNode := range cluster.AllNodes {
			if healthyNode.RoleIs(redis_cluster.PrimaryNode, redis_cluster.ReplicaNode) {
				// Check if this healthy node sees the zombie in its view
				if _, sees := healthyNode.Views[zombieNode.PodIndex]; sees {
					seenBy = append(seenBy, healthyNode.PodName)
				}
				for podIndex := range zombieNode.Views {
					if healthyNode.PodIndex == podIndex {
						if !slices.Contains(seenBy, healthyNode.PodName) {
							seenBy = append(seenBy, healthyNode.PodName)
						}
					}
				}
			}
		}
		if len(seenBy) > 0 {
			result[zombieNode.ID] = seenBy
		}
	}

	return result
}

func (r *RedisClusterReconciler) isClusterAlreadyFormed(cluster *redis_cluster.Cluster) bool {
	return r.getClusterSlotsAssigned(cluster) > 0 ||
		(cluster.RedisCluster.Status.BackupRestored && cluster.RedisCluster.Status.RestoreTopologyPending)
}

func (r *RedisClusterReconciler) isClusterHealthy(cluster *redis_cluster.Cluster) bool {
	for _, node := range cluster.AllNodes {
		if node.Info.SlotsAssigned == 0 {
			continue
		}
		if !node.Info.ClusterStateIsOK {
			return false
		}
		if node.Info.SlotsAssigned != constants.TotalSlots {
			return false
		}
		if node.Info.SlotsFail > 0 {
			return false
		}
		return true
	}
	return false
}

func (r *RedisClusterReconciler) getClusterSlotsAssigned(cluster *redis_cluster.Cluster) int {
	for _, node := range cluster.AllNodes {
		if node.Info.SlotsAssigned > 0 {
			return node.Info.SlotsAssigned
		}
	}
	return 0
}

// countPrimariesWithSlots returns the number of primaries that have at least one slot assigned.
func countPrimariesWithSlots(cluster *redis_cluster.Cluster) int {
	count := 0
	for _, node := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if len(node.SlotsRanges) > 0 {
			count++
		}
	}
	return count
}

// findMasterNeedingReplica returns a master with fewer replicas than spec.Replicas.
func (r *RedisClusterReconciler) findMasterNeedingReplica(cluster *redis_cluster.Cluster, expectedPrimaryCount int) *redis_cluster.ClusterNode {
	targetReplicas := int(cluster.RedisCluster.Spec.Replicas)
	replicaCount := make(map[string]int)
	for _, replica := range cluster.Nodes[redis_cluster.ReplicaNode] {
		replicaCount[replica.PrimaryID]++
	}
	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if len(primary.SlotsRanges) > 0 && replicaCount[primary.ID] < targetReplicas {
			return primary
		}
	}
	return nil
}

// removeRedundantReplicas resets replicas that exceed the per-primary limit.
// Returns true if any replicas were reset. Iterated non-draining-first so
// draining replicas land on the "reset" side of the maxReplicas threshold.
func (r *RedisClusterReconciler) removeRedundantReplicas(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) bool {
	maxReplicas := int(cluster.RedisCluster.Spec.Replicas)
	removed := false
	alreadyReset := make(map[string]bool)
	orderedReplicas := orderReplicasNonDrainingFirst(cluster.Nodes[redis_cluster.ReplicaNode])

	for _, primary := range cluster.Nodes[redis_cluster.PrimaryNode] {
		replicaCount := 0
		for _, replica := range orderedReplicas {
			if replica.PrimaryID == primary.ID {
				if replicaCount < maxReplicas {
					replicaCount++
				} else if !alreadyReset[replica.ID] {
					if len(replica.Views) == 0 {
						log.Info("Skipping redundant replica reset: node not visible to cluster yet",
							"replicaPodName", replica.PodName, "nodeID", replica.ID)
						continue
					}
					alreadyReset[replica.ID] = true
					log.Info("Resetting redundant replica",
						"replicaPodName", replica.PodName,
						"primaryPodName", primary.PodName,
						"isDrainCandidate", replica.IsDrainCandidate())
					if err := cluster.Toolbox.ResetNode(replica, true); err != nil {
						log.Error(err, "Failed to reset redundant replica", "podName", replica.PodName)
						continue
					}
					removed = true
				}
			}
		}
	}
	return removed
}

// resetExcessEmptyPrimaries resets empty primaries when we have more than expected.
// Returns true if any primaries were reset.
func (r *RedisClusterReconciler) resetExcessEmptyPrimaries(ctx context.Context, cluster *redis_cluster.Cluster, log logr.Logger) bool {
	reset := false
	for _, node := range cluster.Nodes[redis_cluster.PrimaryNode] {
		if len(node.SlotsRanges) == 0 {
			_ = r.Metrics.CountBadSituation("empty_master_detected", "pod_name:"+node.PodName)
			log.Info("Resetting excess empty primary", "podName", node.PodName)
			if err := cluster.Toolbox.ResetNode(node, true); err != nil {
				log.Error(err, "Failed to reset empty primary", "podName", node.PodName)
				continue
			}
			reset = true
		}
	}
	return reset
}
