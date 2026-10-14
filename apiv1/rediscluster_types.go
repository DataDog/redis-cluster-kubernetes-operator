package v1

import (
	appsv1 "k8s.io/api/apps/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// RedisClusterSpec defines the desired state of RedisCluster
// +kubebuilder:validation:XValidation:rule="self.primaries * (1 + self.replicas) <= 2147483647",message="primaries and replicas produce too many pods"
type RedisClusterSpec struct {
	// Primaries is the number of primary nodes
	// +kubebuilder:validation:Minimum=1
	Primaries int32 `json:"primaries"`
	// Replicas is the number of replicas each primary should have
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`
	// RestoreFrom is the path to a backup in cloud storage to seed this cluster, eg) redis-cluster-test/202012171413/
	RestoreFrom string `json:"restoreFrom"`
	// BackupEnabled enables or disables automatic backups (defaults to disabled)
	BackupEnabled bool `json:"backupEnabled"`
	// BackupFrequency specifies how often the cluster should be backed up in cloud storage (in seconds)
	BackupFrequency int64 `json:"backupFrequency"`
	// BackupRetention is retained for API compatibility.
	// Deprecated: backup retention is unsupported and this field is ignored.
	BackupRetention int32 `json:"backupRetention,omitempty"`
	// BackupBackend selects the snapshot storage provider. The reference binary
	// includes "s3", "gcs", and "azure"; embedders may register other names.
	// Defaults to the built-in AWS S3 implementation when unset.
	// +kubebuilder:validation:Pattern=`^[a-z][a-z0-9.-]{0,62}$`
	// +kubebuilder:default=s3
	BackupBackend string `json:"backupBackend,omitempty"`
	// StatefulsetTemplate is the template of the statefulset for the redis cluster
	StatefulsetTemplate appsv1.StatefulSet `json:"statefulsetTemplate"`
	// DisableReconciliation will prevent the operator from reconciling the cluster
	DisableReconciliation bool `json:"disableReconciliation,omitempty"`
	// ReconcileTimeoutSeconds overrides the dynamic reconcile timeout (0 = auto-scale with cluster size).
	ReconcileTimeoutSeconds int32 `json:"reconcileTimeoutSeconds,omitempty"`
	// Autoscaling configures recommendation-only or automatic primary scale-up.
	Autoscaling *RedisClusterAutoscalingSpec `json:"autoscaling,omitempty"`
}

// AutoscalingMode controls whether recommendations are observed or applied.
// +kubebuilder:validation:Enum=Disabled;Recommend;ScaleUp;ScaleUpAndDown
type AutoscalingMode string

const (
	AutoscalingModeDisabled       AutoscalingMode = "Disabled"
	AutoscalingModeRecommend      AutoscalingMode = "Recommend"
	AutoscalingModeScaleUp        AutoscalingMode = "ScaleUp"
	AutoscalingModeScaleUpAndDown AutoscalingMode = "ScaleUpAndDown"
)

// RedisClusterConditionType is a standard high-level summary of cluster state.
// +kubebuilder:validation:Enum=Ready;Progressing;Degraded
type RedisClusterConditionType string

const (
	RedisClusterConditionReady       RedisClusterConditionType = "Ready"
	RedisClusterConditionProgressing RedisClusterConditionType = "Progressing"
	RedisClusterConditionDegraded    RedisClusterConditionType = "Degraded"
)

// RedisClusterAutoscalingMetricPolicy configures one autoscaling recommendation signal.
type RedisClusterAutoscalingMetricPolicy struct {
	// HighWatermark is the utilization ratio above which scale-up may be recommended.
	HighWatermark resource.Quantity `json:"highWatermark"`
	// UpscaleDelaySeconds is how long the signal must remain above HighWatermark
	// before scale-up may be recommended.
	// +kubebuilder:validation:Minimum=0
	UpscaleDelaySeconds *int32 `json:"upscaleDelaySeconds,omitempty"`
}

// RedisClusterAutoscalingSpec configures primary-count recommendations and scaling.
type RedisClusterAutoscalingSpec struct {
	// Mode controls whether recommendations are reported or applied.
	Mode AutoscalingMode `json:"mode,omitempty"`
	// +kubebuilder:validation:Minimum=1
	MinPrimaries *int32 `json:"minPrimaries,omitempty"`
	// +kubebuilder:validation:Minimum=1
	MaxPrimaries *int32 `json:"maxPrimaries,omitempty"`
	// +kubebuilder:validation:Minimum=0
	UpscaleCooldownSeconds *int32                               `json:"upscaleCooldownSeconds,omitempty"`
	CPU                    *RedisClusterAutoscalingMetricPolicy `json:"cpu,omitempty"`
	Memory                 *RedisClusterAutoscalingMetricPolicy `json:"memory,omitempty"`
}

const (
	AutoscalingConditionReady                   = "AutoscalingReady"
	AutoscalingConditionRecommendationAvailable = "RecommendationAvailable"
	AutoscalingConditionScaling                 = "Scaling"
	AutoscalingConditionScalingBlocked          = "ScalingBlocked"
	AutoscalingConditionMetricsDegraded         = "MetricsDegraded"
)

// RedisClusterAutoscalingStatus contains the retained target and latest recommendation decision.
type RedisClusterAutoscalingStatus struct {
	EffectivePrimaries     int32        `json:"effectivePrimaries,omitempty"`
	CombinedRecommendation *int32       `json:"combinedRecommendation,omitempty"`
	CPURecommendation      *int32       `json:"cpuRecommendation,omitempty"`
	MemoryRecommendation   *int32       `json:"memoryRecommendation,omitempty"`
	ScaleStartedAt         *metav1.Time `json:"scaleStartedAt,omitempty"`
	ScaleCompletedAt       *metav1.Time `json:"scaleCompletedAt,omitempty"`
	LastDecisionReason     string       `json:"lastDecisionReason,omitempty"`
	// ReplicaMigrationDisabled records that the autoscaling guard may have
	// disabled Redis' autonomous replica migration and must restore it before
	// the scale operation is completed or cancelled.
	ReplicaMigrationDisabled bool `json:"replicaMigrationDisabled,omitempty"`
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// BlueGreenPhase represents the current phase of a blue/green deployment.
type BlueGreenPhase string

const (
	BlueGreenPhaseNone            BlueGreenPhase = ""
	BlueGreenPhaseCreatingGreen   BlueGreenPhase = "CreatingGreen"
	BlueGreenPhaseWaitingPods     BlueGreenPhase = "WaitingPods"
	BlueGreenPhaseReplicating     BlueGreenPhase = "Replicating"
	BlueGreenPhaseWaitingSync     BlueGreenPhase = "WaitingSync"
	BlueGreenPhasePromoting       BlueGreenPhase = "Promoting"       // CLUSTER FAILOVER: green replicas → primaries (zero downtime)
	BlueGreenPhaseScalingDownBlue BlueGreenPhase = "ScalingDownBlue" // Scale blue STS to 0, wait for pods to terminate
	BlueGreenPhaseFormingCluster  BlueGreenPhase = "FormingCluster"  // CLUSTER FORGET blue, add green replicas
	BlueGreenPhaseSwitchingDNS    BlueGreenPhase = "SwitchingDNS"
	BlueGreenPhaseTearingDown     BlueGreenPhase = "TearingDown"
	BlueGreenPhaseFailed          BlueGreenPhase = "Failed"
)

// BlueGreenStatus defines the observed state of a blue/green deployment.
type BlueGreenStatus struct {
	// Phase is the current phase of the blue/green deployment
	Phase BlueGreenPhase `json:"phase,omitempty"`
	// StartTime is when the deployment started (unix timestamp)
	StartTime int64 `json:"startTime,omitempty"`
	// SyncStartTime is when the WaitingSync phase started (unix timestamp)
	SyncStartTime int64 `json:"syncStartTime,omitempty"`
	// LastProgressTime is the last time replication made progress (synced count increased)
	LastProgressTime int64 `json:"lastProgressTime,omitempty"`
	// LastSyncedCount is the number of shards synced at LastProgressTime
	LastSyncedCount int32 `json:"lastSyncedCount,omitempty"`
	// Message provides human-readable status
	Message string `json:"message,omitempty"`
	// ShardMapping tracks the blue-primary to green-pod replication mapping
	ShardMapping []ShardMappingEntry `json:"shardMapping,omitempty"`
	// CompletedAt is when the BG deployment finished (unix timestamp).
	// Used by maintainCluster to skip aggressive restructuring for a grace period.
	CompletedAt int64 `json:"completedAt,omitempty"`
	// BlueNodeIDsToForget stores the blue node IDs identified during ScalingDownBlue phase
	// These are FORGOTten before blue pods are terminated to avoid client errors
	BlueNodeIDsToForget []string `json:"blueNodeIDsToForget,omitempty"`
	// BlueNodesForgotten tracks whether FORGET was already executed in ScalingDownBlue.
	// Prevents re-running the ~20s FORGET cycle on reconcile re-entry.
	BlueNodesForgotten bool `json:"blueNodesForgotten,omitempty"`
	// ResumeForwardAttempts counts how many times bgCleanupFailed resumed forward
	// to Promoting instead of rolling back (because green owns data).
	// Used to escalate failover mode: 0=soft, 1=FORCE, 2+=TAKEOVER.
	ResumeForwardAttempts int32 `json:"resumeForwardAttempts,omitempty"`
	// ConsistencyGateAttempts counts consecutive cluster-consistency failures
	// while Promoting. It only controls the consistency-gate bypass.
	ConsistencyGateAttempts int32 `json:"consistencyGateAttempts,omitempty"`
	// DrainStartedAt is when the SwitchingDNS drain period began (unix timestamp).
	// Used to implement non-blocking drain via RequeueAfter instead of time.Sleep.
	DrainStartedAt int64 `json:"drainStartedAt,omitempty"`
	// PhaseEntryTime is when the current phase started (unix timestamp).
	// Set on every phase transition, used for per-phase duration metrics.
	PhaseEntryTime int64 `json:"phaseEntryTime,omitempty"`
	// ConsecutiveErrors tracks how many consecutive reconcile errors occurred
	// in the current phase. Reset to 0 on phase transition. Used to abort early
	// instead of waiting for the full BG timeout.
	ConsecutiveErrors int32 `json:"consecutiveErrors,omitempty"`
}

// ShardMappingEntry maps a blue primary to a green pod for replication.
type ShardMappingEntry struct {
	// BluePrimaryID is the Redis node ID of the blue primary
	BluePrimaryID string `json:"bluePrimaryID"`
	// BluePrimaryIP is the IP of the blue primary
	BluePrimaryIP string `json:"bluePrimaryIP"`
	// GreenPodName is the name of the green pod replicating this shard
	GreenPodName string `json:"greenPodName"`
	// GreenPodIP is the IP of the green pod
	GreenPodIP string `json:"greenPodIP"`
	// GreenNodeID is the Redis node ID assigned to the green pod (after joining blue cluster)
	GreenNodeID string `json:"greenNodeID,omitempty"`
	// SlotsRanges is the slot ranges owned by the blue primary (e.g. "0-5460")
	SlotsRanges string `json:"slotsRanges"`
}

// RedisClusterNode represents a single redis node
type RedisClusterNode struct {
	Name  string   `json:"name"`
	ID    string   `json:"id"`
	IP    string   `json:"ip"`
	Port  string   `json:"port"`
	Flags []string `json:"flags"`
	// The node id of this node's master
	Master    string `json:"master"`
	LinkState string `json:"link_state"`
	Slots     string `json:"slots"`
	// The pod index (i.e. pods are named redis-cluster-foo-<index>)
	// The pod creation timestamp
	// The Redis version as detected from the container's image
	Index        int    `json:"index"`
	Creation     int64  `json:"creation_ts"`
	RedisVersion string `json:"redis_version"`
}

// RedisClusterStatus defines the observed state of RedisCluster
type RedisClusterStatus struct {
	// Conditions contains the standard high-level Ready, Progressing, and
	// Degraded summaries. Detailed subsystem state remains in the fields below.
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Nodes are the names of the redis cluster pods
	Nodes                 []RedisClusterNode `json:"nodes"`
	StatefulSetReady      bool               `json:"statefulset_ready"`
	LastBackupTime        int64              `json:"last_backup_time"`
	LastBackupPath        string             `json:"last_backup_path,omitempty"`
	LastBackupDurationMs  int64              `json:"last_backup_duration_ms,omitempty"`
	LastBackupFailedNodes []int              `json:"last_backup_failed_nodes,omitempty"`
	BackupRestored        bool               `json:"backup_restored"`
	// RestorePath records the backup generation selected for the current restore
	// so topology reconstruction uses the same generation across reconciles.
	RestorePath string `json:"restore_path,omitempty"`
	// RestoreTopologyPending limits manifest-guided slot reconstruction to the
	// cluster formation immediately following a restore.
	RestoreTopologyPending bool   `json:"restore_topology_pending,omitempty"`
	LastBackupNodeCount    int    `json:"last_backup_node_count,omitempty"`
	BadPrimariesReports    uint32 `json:"bad_primaries_reports"`
	// ActiveClusterName is the name of the StatefulSet currently serving traffic.
	// Set by the operator during the first BG phase and updated after promotion.
	// Used by GetOrCreateStatefulSet to find the correct STS regardless of spec name.
	ActiveClusterName string `json:"activeClusterName,omitempty"`
	// BlueGreen tracks the state of an ongoing blue/green deployment
	BlueGreen BlueGreenStatus `json:"blueGreen,omitempty"`
	// LastAppliedSpecHash is a hash of the CR spec fields that the operator last applied.
	// Used as the topology/config checkpoint shared with the standalone operator.
	LastAppliedSpecHash string `json:"lastAppliedSpecHash,omitempty"`
	// LastAppliedRolloutSpecHash tracks only fields that require a blue/green rollout.
	// Topology is excluded because scaling applies it independently.
	LastAppliedRolloutSpecHash string `json:"lastAppliedRolloutSpecHash,omitempty"`
	// RolloutInProgress indicates if a config rollout is currently happening
	RolloutInProgress        bool   `json:"rollout_in_progress,omitempty"`
	RolloutConfigChecksum    string `json:"rollout_config_checksum,omitempty"`
	RolloutResourcesChecksum string `json:"rollout_resources_checksum,omitempty"`
	RolloutPhase             string `json:"rollout_phase,omitempty"`
	RolloutProgress          string `json:"rollout_progress,omitempty"`
	RolloutStartTime         int64  `json:"rollout_start_time,omitempty"`
	// Autoscaling tracks the retained topology target and recommendation lifecycle.
	Autoscaling RedisClusterAutoscalingStatus `json:"autoscaling,omitempty"`
}

// SetCondition sets or updates one standard top-level condition. Kubernetes'
// status condition helper preserves LastTransitionTime while Status is
// unchanged and refreshes it only when Status transitions.
func (s *RedisClusterStatus) SetCondition(
	conditionType RedisClusterConditionType,
	status metav1.ConditionStatus,
	reason string,
	message string,
	observedGeneration int64,
) {
	apimeta.SetStatusCondition(&s.Conditions, metav1.Condition{
		Type:               string(conditionType),
		Status:             status,
		ObservedGeneration: observedGeneration,
		Reason:             reason,
		Message:            message,
	})
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// RedisCluster is the Schema for the redisclusters API
// +kubebuilder:subresource:status
// +kubebuilder:metadata:annotations="helm.sh/resource-policy=keep"
type RedisCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RedisClusterSpec   `json:"spec,omitempty"`
	Status RedisClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RedisClusterList contains a list of RedisCluster
type RedisClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RedisCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RedisCluster{}, &RedisClusterList{})
}
