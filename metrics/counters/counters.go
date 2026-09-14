package counters

const (
	AppStarted         = "app.started"
	Reconcile          = "reconcile"
	ClusterCreated     = "cluster_created"
	ClusterMeet        = "cluster_meet"
	ClusterRestored    = "cluster_restored"
	ClusterBackedUp    = "cluster_backed_up"
	StatefulSetCreated = "statefuleset_created"
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
