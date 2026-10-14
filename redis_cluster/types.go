// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
	"github.com/DataDog/redis-cluster-kubernetes-operator/timeouts"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterClient extends the k8s client for Redis cluster management.
type ClusterClient struct {
	client.Client
	ClusterScheme *runtime.Scheme
	RestConfig    *rest.Config
	Namespace     string
	Log           logr.Logger
	Metrics       metrics.Metrics
	// CredentialProvider selects no authentication or the fixed mounted-Secret
	// helper. Credential values are read inside the toolbox pod.
	CredentialProvider CredentialProvider
}

// Cluster represents the Redis cluster.
type Cluster struct {

	// RedisCluster is the RedisCluster Kubernetes resource for this cluster.
	RedisCluster *cachev1.RedisCluster

	// StatefulSet is the StatefulSet Kubernetes resource for this cluster.
	StatefulSet *v1.StatefulSet

	// Toolbox is the Toolbox for this cluster.
	Toolbox *Toolbox

	// Timeouts holds timeout configuration scaled for this cluster size.
	Timeouts *timeouts.Config

	// RedisPort is the port Redis servers are listening to.
	RedisPort int

	// RedisVersion is the version defined by the StatefulSet
	RedisVersion string

	// AllNodes contains the cluster nodes.
	AllNodes []*ClusterNode

	// Nodes contains the cluster nodes, grouped by NodeRole.
	Nodes map[NodeRole][]*ClusterNode

	// Node is the "trusted" node if any.
	Node *ClusterNode
}

// ClusterNode represents a node in the cluster.
type ClusterNode struct {

	// Info about the Redis node as reported by CLUSTER INFO.
	Info NodeInfo

	// Views of this node from the pods in the cluster.
	Views map[int]*NodeView

	// Role played by this node.
	Role NodeRole

	// PodName is the name of the pod.
	// It can be the empty string if the node has no corresponding pod.
	PodName string

	// PodIndex is the StatefulSet index of the pod.
	// It is zero if the node has no corresponding pod.
	PodIndex int

	// PodCreation is the unix time at which the pod was created.
	// It is zero if the node has no corresponding pod.
	PodCreation int64

	// RedisVersion is the version of Redis running on the pod.
	// It can be the empty string if the version could not be detected.
	RedisVersion string

	// RedisReady indicates whether the pod is labeled as 'redis-ready' in order to appear in DNS results.
	RedisReady bool

	// ID is the Redis identifier of the node.
	ID string

	// IP is the pod IP of the cluster node.
	// It can be the empty string if the node has no corresponding pod.
	IP string

	// Hostname is the hostname announced by Redis in CLUSTER NODES.
	// It is empty when Redis uses IP endpoints only.
	Hostname string

	// Flags containsString the Redis flags as returned by CLUSTER NODES.
	// Possible values are
	// - myself         : the node you are contacting (filtered out)
	// - master         : node is a primary
	// - slave          : node is a replica
	// - fail?          : node in PFAIL state, not reachable from 'myself'' but still logically reachable
	// - fail           : node in FAIL state, not reachable from enough nodes to promote PFAIL to FAIL
	// - handshake      : untrusted node, handshaking
	// - noaddr         : no address known for this node
	// - nofailover     : replica will not try to Failover
	// - noflags        : no flags at all
	Flags []string

	// PrimaryID contains the Redis identifier of the primary node of a replica.
	// It is the empty string when the node is not a replica, or the master is unknown.
	PrimaryID string

	// PingSent is the unix time (ms) at which the currently active ping was sent, or zero if there are no pending pings.
	PingSent int64

	// PongReceived is the unix time (ms) at which the last pong was received.
	PongReceived int64

	// ConfigEpoch is the epoch (or version) of the node, or primary if the node is a replica.
	// Increases each time there is a Failover, used to arbitrate which nodes serves which slots.
	ConfigEpoch int

	// LinkState is the state of the link used for node-to-node cluster bus.
	// Possible values are 'connected' or 'disconnected'.
	LinkState string

	// InstanceName is the name of the k8s node supporting the pod
	InstanceName string

	// SlotsRanges containsString the ranges of slots owned by the node.
	SlotsRanges []SlotsRange

	// OpenSlots are slots in Redis' transient importing/migrating state.
	OpenSlots []OpenSlot

	// Drain status, populated from corev1.Pod at cluster construction.
	IsTerminating         bool // pod.DeletionTimestamp != nil
	HasDrainCandidateCond bool // NodeLifecycle/NodeDrainCandidate condition
}

// IsDrainCandidate: pod is unsuitable as a target for new responsibilities
// (terminating or carrying the generic NodeDrainCandidate condition).
func (n *ClusterNode) IsDrainCandidate() bool {
	return n.IsTerminating || n.HasDrainCandidateCond
}

// NodeInfo represents infos about a Redis node as reported by CLUSTER INFO.
type NodeInfo struct {

	// ClusterStateIsOK indicates whether the cluster state is OK.
	ClusterStateIsOK bool

	// SlotsAssigned counts the number of slots assigned to some node (should be 16384).
	SlotsAssigned int

	// SlotsOK counts the number of slots associated to a node not in FAIL nor PFAIL state.
	SlotsOK int

	// SlotsPFail counts the number of slots associated to a node in PFAIL state.
	SlotsPFail int

	// SlotsFail counts the number of slots associated to a node in FAIL state.
	SlotsFail int

	// KnownNodes counts the number of known hosts including in HANDSHAKE state.
	KnownNodes int

	// Size counts the number of primary nodes serving at least one slot.
	Size int
}

// NodeRole defines the possible roles for a ClusterNode.
type NodeRole int

// SlotsRange represents a range of slots owned by a ClusterNode.
type SlotsRange struct {

	// First is a value between 0 and 16383 specifying the first slot in the range.
	First int

	// First is a value between 0 and 16383, and greater-than-or-equal-to First, specifying the last slot in the range.
	Last int
}

// OpenSlotState is the transient Redis state for an open cluster slot.
type OpenSlotState string

const (
	OpenSlotImporting OpenSlotState = "importing"
	OpenSlotMigrating OpenSlotState = "migrating"
)

// OpenSlot represents a CLUSTER NODES open-slot marker.
type OpenSlot struct {
	Slot   int
	State  OpenSlotState
	PeerID string
}

// NodeView represents details of one node, as seen by itself or another node.
type NodeView struct {

	// ID is the Redis identifier of the node.
	ID string

	// Flags containsString the Redis flags as returned by CLUSTER NODES.
	// Possible values are:
	// - myself         the node you are contacting (filtered out)
	// - master         node is a primary
	// - slave          node is a replica
	// - fail?          node in PFAIL state, not reachable from 'myself'' but still logically reachable
	// - fail           node in FAIL state, not reachable from enough nodes to promote PFAIL to FAIL
	// - handshake      untrusted node, handshaking
	// - noaddr         no address known for this node
	// - nofailover     replica will not try to Failover
	// - noflags        no flags at all
	Flags []string

	// Master containsString the Redis identifier of the primary node of a replica.
	// It is the empty string when the node is not a replica, or the master is unknown.
	Master string

	// LinkState is the state of the link used for node-to-node cluster bus.
	// Possible values are 'connected' or 'disconnected'.
	LinkState string

	// SlotsRanges containsString the ranges of slots owned by the node.
	SlotsRanges []SlotsRange

	// OpenSlots are slots this view reports as importing/migrating.
	OpenSlots []OpenSlot
}

const (
	// OtherNode is the default value.
	OtherNode NodeRole = iota
	// PrimaryNode is a node that is a healthy primary node in the cluster.
	PrimaryNode
	// ReplicaNode is a node that is a healthy replica node in the cluster.
	ReplicaNode
	// FailingNode is a node that is part of the cluster but not a healthy primary nor replica.
	FailingNode
	// OutsiderNode is a node that has a matching pod but has not joined the cluster yet.
	OutsiderNode
	// ZombieNode is a node that is known by some other nodes but does not have a matching pod anymore.
	ZombieNode
)

func (role NodeRole) String() string {
	switch role {
	case OtherNode:
		return "OtherNode"
	case PrimaryNode:
		return "PrimaryNode"
	case ReplicaNode:
		return "ReplicaNode"
	case FailingNode:
		return "FailingNode"
	case OutsiderNode:
		return "OutsiderNode"
	case ZombieNode:
		return "ZombieNode"
	default:
		return "Unknown"
	}
}
