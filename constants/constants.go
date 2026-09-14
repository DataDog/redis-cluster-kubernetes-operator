package constants

import "time"

const (
	ManagerPort = 9433
	TotalSlots  = 16384

	LeaderElectionID          = "006a5d62.datadoghq.com" // this was auto-generated
	ToolboxContainerName      = "managed-redis-cluster-operator-toolbox"
	RedisContainerName        = "redis"
	BackupContainerName       = "backup"
	RedisClusterReadyPodLabel = "redis-cluster-ready"

	// Cluster operation timeouts
	PollInterval     = 15 * time.Second // How often to check cluster state while waiting
	OperationTimeout = 5 * time.Minute  // Max time to wait for long operations (failover, add nodes, etc.)

	// NotFoundCacheTTL controls how long a not-found RedisCluster CR is cached
	// to prevent tight reconcile loops from pod/StatefulSet events.
	NotFoundCacheTTL = 60 * time.Second

	DefaultRetries = 10
	DefaultTimeout = 30 * time.Second

	// Reconcile context timeout (scales with cluster size)
	ReconcileTimeoutBase   = 2 * time.Minute  // Base timeout for small clusters
	ReconcileTimeoutPerPod = 2 * time.Second  // Additional time per pod
	ReconcileTimeoutMax    = 20 * time.Minute // Maximum timeout cap

	// Maintain loop control
	MaxMaintainIterations = 30 // Max fix operations per reconcile cycle

	// FORGET command retry
	ForgetRetries    = 5
	ForgetRetryDelay = 2 * time.Second
	GossipDelay      = 5 * time.Second

	// Controller reconcile intervals
	QuickRequeue     = 2 * time.Second  // Fast requeue (e.g., waiting for pod to be ready)
	NormalRequeue    = 30 * time.Second // Normal requeue (e.g., StatefulSet not ready)
	DefaultSleepTime = NormalRequeue    // Alias for backward compatibility
	ShortDelay       = 5 * time.Second  // Short requeue for BG phases awaiting quick changes
	ShortSleepTime   = 2 * time.Second  // Short sleep for in-loop waits

	// Redis cluster command responses
	OKForgetNode = "OK"
	OKAddNode    = "[OK] New node added correctly."

	// Rate limiter settings for controller backoff
	RateLimiterBaseDelay = 5 * time.Second  // initial retry delay after error
	RateLimiterMaxDelay  = 60 * time.Second // max retry delay (caps exponential backoff)

	// Redis INFO replication field keys and values
	RedisRoleMaster             = "role:master"
	RedisRoleSlave              = "role:slave"
	RedisLinkStatusUp           = "master_link_status:up"
	RedisSyncInProgress         = "master_sync_in_progress:1"
	RedisLastIOSecondsField     = "master_last_io_seconds_ago"
	RedisReplicaOffsetField     = "slave_repl_offset"
	RedisReplicaReadOffsetField = "slave_read_repl_offset"
	RedisPrimaryOffsetField     = "master_repl_offset"
)

// these have to be vars because we will need to '&' them
var (
	ReconcileFreq = 60 * time.Minute // min. freq. at which watched resources are reconciled
	LeaseDuration = 30 * time.Second
	RenewDeadline = 20 * time.Second
)
