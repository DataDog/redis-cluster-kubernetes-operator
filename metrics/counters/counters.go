// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package counters

const (
	AppStarted         = "app.started"
	Reconcile          = "reconcile"
	ClusterCreated     = "cluster_created"
	ClusterMeet        = "cluster_meet"
	ClusterRestored    = "cluster_restored"
	ClusterBackedUp    = "cluster_backed_up"
	StatefulSetCreated = "statefulset_created"
	StatefulSetUpdated = "statefulset_updated"

	// Zombie and NLA related counters
	ZombieDetected  = "zombie_detected"
	ZombiePersisted = "zombie_persisted"
	NLAProcessed    = "nla_processed"
	NLAFailed       = "nla_failed"
	NLARetriggered  = "nla_retriggered"

	// Blue/green deployment counters
	BlueGreenStarted           = "bluegreen_started"
	BlueGreenCompleted         = "bluegreen_completed"
	BlueGreenFailed            = "bluegreen_failed"
	BlueGreenFailoverAttempted = "bluegreen_failover_attempted"
	BlueGreenFailoverSucceeded = "bluegreen_failover_succeeded"
)
