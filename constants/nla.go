package constants

// NLA (Node Lifecycle Automation) constants. Shared by the controller package
// and the redis_cluster package (drain-aware ClusterNode fields).
const (
	// DrainCandidateConditionType and DrainCandidateConditionReason describe the
	// provider-neutral pod signal used by core topology safety.
	DrainCandidateConditionType   = "NodeLifecycle"
	DrainCandidateConditionReason = "NodeDrainCandidate"

	// NLAAnnotation: pre-activity key the chart registers and the platform
	// watches for the operator's "done" signal. Chart and constant must agree.
	NLAAnnotation = "node-lifecycle.datadoghq.com/pre-activity-redis-cluster-operator"

	// Values written to NLAAnnotation.
	NLAProcessing = "processing"
	NLAFailed     = "failed"
	NLADone       = "done"

	// Pod condition the NLA platform sets to mark a drain candidate.
	NLAConditionType   = DrainCandidateConditionType
	NLAConditionReason = DrainCandidateConditionReason

	// Set by the operator after CLUSTER FORGET to prevent duplicate FORGETs.
	NLAForgottenAnnotation = "managed-redis-cluster-operator/forgotten"

	// Test-only equivalent of the NodeDrainCandidate condition (used in staging).
	NLATestTriggerAnnotation = "managed-redis-cluster-operator/test-nla"
)
