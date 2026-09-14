package redis_cluster

import (
	"fmt"
	"strconv"
	"strings"

	v1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	corev1 "k8s.io/api/core/v1"
)

// determineRole determines the role of a node.
func (n *ClusterNode) determineRole() NodeRole {

	if n.PodIndex < 0 {
		// node does not have a pod = zombie
		return ZombieNode
	}

	if n.IP == "" || n.ID == "" {
		// node has a pod but no IP or no ID = transient
		return OtherNode
	}

	// node has a pod, an IP and an ID
	isPrimary := utils.ContainsString(n.Flags, "master")
	isReplica := utils.ContainsString(n.Flags, "slave")

	relevantFlags := filterOutMyselfFlag(n.Flags)
	hasOnlyOneRelevantFlag := len(relevantFlags) == 1

	isOutsider := isPrimary && hasOnlyOneRelevantFlag &&
		n.Info.KnownNodes == 1 && // there is only one node
		n.Info.Size == 0 && // there is no cluster
		len(n.Views) == 0 // no other node sees that node

	if isOutsider {
		// node is not in the cluster
		return OutsiderNode
	}

	// assume that the node is in the cluster - though it can be PFAIL or FAIL
	if isPrimary || isReplica {
		if !hasOnlyOneRelevantFlag {
			return FailingNode
		} else if isPrimary {
			return PrimaryNode
		} else {
			return ReplicaNode
		}
	}

	// what is this?
	return OtherNode
}

// filterOutMyselfFlag removes the "myself" flag from the flags slice.
// The "myself" flag is always present on a node's own CLUSTER NODES output
// and shouldn't affect role determination.
func filterOutMyselfFlag(flags []string) []string {
	result := make([]string, 0, len(flags))
	for _, f := range flags {
		if f != "myself" {
			result = append(result, f)
		}
	}
	return result
}

// groupNodes initializes each node's NodeRole and groups nodes by NodeRole.
func groupNodes(nodes []*ClusterNode) map[NodeRole][]*ClusterNode {

	m := make(map[NodeRole][]*ClusterNode)

	for _, node := range nodes {
		node.Role = node.determineRole()
		m[node.Role] = append(m[node.Role], node)
	}

	return m
}

func GetNodeIDs(nodes []*ClusterNode) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func GetNodeSummary(nodes map[NodeRole][]*ClusterNode) string {
	return fmt.Sprintf("primaries=%d replicas=%d outsiders=%d zombies=%d failing=%d other=%d",
		len(nodes[PrimaryNode]),
		len(nodes[ReplicaNode]),
		len(nodes[OutsiderNode]),
		len(nodes[ZombieNode]),
		len(nodes[FailingNode]),
		len(nodes[OtherNode]))
}

func (n *ClusterNode) GetRoleDebugInfo() map[string]interface{} {
	relevantFlags := filterOutMyselfFlag(n.Flags)
	isPrimary := utils.ContainsString(n.Flags, "master")
	isReplica := utils.ContainsString(n.Flags, "slave")

	return map[string]interface{}{
		"podName":                n.PodName,
		"podIndex":               n.PodIndex,
		"nodeID":                 n.ID,
		"ip":                     n.IP,
		"hostname":               n.Hostname,
		"flags":                  n.Flags,
		"relevantFlags":          relevantFlags,
		"hasOnlyOneRelevantFlag": len(relevantFlags) == 1,
		"isPrimary":              isPrimary,
		"isReplica":              isReplica,
		"knownNodes":             n.Info.KnownNodes,
		"clusterSize":            n.Info.Size,
		"viewsCount":             len(n.Views),
		"determinedRole":         n.Role.String(),
	}
}

// RoleIs determines whether a node is playing one of the specified roles in the cluster.
func (n *ClusterNode) RoleIs(roles ...NodeRole) bool {
	for _, role := range roles {
		if n.Role == role {
			return true
		}
	}
	return false
}

func lookupClusterNodeByIP(nodes []*ClusterNode, ip string) (*ClusterNode, bool) {
	for _, node := range nodes {
		if node.IP == ip {
			return node, true
		}
	}
	return nil, false
}

func lookupClusterNodeByID(nodes []*ClusterNode, id string) (*ClusterNode, bool) {
	for _, node := range nodes {
		if node.ID == id {
			return node, true
		}
	}
	return nil, false
}

func lookupClusterNodeByIndex(nodes []*ClusterNode, index int) (*ClusterNode, bool) {
	for _, node := range nodes {
		if node.PodIndex == index {
			return node, true
		}
	}
	return nil, false
}

func (c *Cluster) LookupNodeByID(id string) (*ClusterNode, bool) {
	return lookupClusterNodeByID(c.AllNodes, id)
}

func (c *Cluster) LookupNodeByIP(id string) (*ClusterNode, bool) {
	return lookupClusterNodeByIP(c.AllNodes, id)
}

func (c *Cluster) lookupNodeByIndex(index int) (*ClusterNode, bool) {
	return lookupClusterNodeByIndex(c.AllNodes, index)
}

func (n *ClusterNode) IsGreen(targetPodCount int) bool {
	return n.PodIndex >= 0 && n.PodIndex < targetPodCount
}

func (n *ClusterNode) IsRed(targetPodCount int) bool {
	return n.PodIndex >= targetPodCount
}

func (c *Cluster) OpenSlots() []OpenSlot {
	openSlots := make([]OpenSlot, 0)
	for _, node := range c.AllNodes {
		openSlots = append(openSlots, node.OpenSlots...)
		for _, view := range node.Views {
			openSlots = append(openSlots, view.OpenSlots...)
		}
	}
	return openSlots
}

func OpenSlotSummary(openSlots []OpenSlot) string {
	if len(openSlots) == 0 {
		return ""
	}
	parts := make([]string, 0, len(openSlots))
	for _, openSlot := range openSlots {
		parts = append(parts, fmt.Sprintf("%d:%s:%s", openSlot.Slot, openSlot.State, openSlot.PeerID))
	}
	return strings.Join(parts, ",")
}

func ClusterNodesToRedisNodes(nodes []*ClusterNode) []v1.RedisClusterNode {
	result := make([]v1.RedisClusterNode, len(nodes))
	for i, node := range nodes {
		ranges := make([]string, 0)
		for _, r := range node.SlotsRanges {
			if r.First == r.Last {
				ranges = append(ranges, strconv.Itoa(r.First))
			} else {
				ranges = append(ranges, strconv.Itoa(r.First)+"-"+strconv.Itoa(r.Last))
			}
		}
		result[i] = v1.RedisClusterNode{
			Name:         node.PodName,
			ID:           node.ID,
			IP:           node.IP,
			Flags:        node.Flags,
			Master:       node.PrimaryID,
			LinkState:    node.LinkState,
			Slots:        strings.Join(ranges, ","),
			Index:        node.PodIndex,
			Creation:     node.PodCreation,
			RedisVersion: node.RedisVersion,
		}
	}
	return result
}

// podsToClusterNodes converts pods into ClusterNode instances.
func podsToClusterNodes(pods []corev1.Pod) ([]*ClusterNode, error) {
	nodes := make([]*ClusterNode, 0)
	for _, pod := range pods {
		// gather running pods with an IP address
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
			index, err := parseIndex(pod.Name)
			if err != nil {
				return nil, err
			}
			_, redisReadyLabelExists := pod.Labels[constants.RedisClusterReadyPodLabel]
			nodes = append(nodes, &ClusterNode{
				PodName:               pod.Name,
				PodIndex:              index,
				PodCreation:           pod.CreationTimestamp.Unix(),
				RedisVersion:          parseRedisVersion(pod.Spec.Containers[0].Image),
				RedisReady:            redisReadyLabelExists,
				Views:                 make(map[int]*NodeView),
				IP:                    pod.Status.PodIP,
				InstanceName:          pod.Spec.NodeName,
				IsTerminating:         pod.DeletionTimestamp != nil,
				HasDrainCandidateCond: PodHasDrainCandidateCondition(&pod),
			})
		}
	}
	return nodes, nil
}

// PodHasDrainCandidateCondition reports whether Kubernetes marked a pod as a
// drain candidate using the provider-neutral lifecycle condition.
func PodHasDrainCandidateCondition(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if string(cond.Type) == constants.DrainCandidateConditionType && cond.Status == corev1.ConditionTrue && cond.Reason == constants.DrainCandidateConditionReason {
			return true
		}
	}
	return false
}

func (c *Cluster) GetReplicaOf(primaryNode *ClusterNode) *ClusterNode {
	for _, n := range c.Nodes[ReplicaNode] {
		if n.PrimaryID == primaryNode.ID {
			return n
		}
	}
	return nil
}

// GetNonDrainingReplicaOf returns the first replica of primaryNode that is not
// a drain candidate. Prefer over GetReplicaOf for promotion targets.
func (c *Cluster) GetNonDrainingReplicaOf(primaryNode *ClusterNode) *ClusterNode {
	for _, n := range c.Nodes[ReplicaNode] {
		if n.PrimaryID == primaryNode.ID && !n.IsDrainCandidate() {
			return n
		}
	}
	return nil
}

func (c *Cluster) HealthyNodeCount() int {
	return len(c.Nodes[PrimaryNode]) + len(c.Nodes[ReplicaNode])
}

func (c *Cluster) GetReplicasOf(primaryNode *ClusterNode) []*ClusterNode {
	var replicas []*ClusterNode
	for _, n := range c.Nodes[ReplicaNode] {
		if n.PrimaryID == primaryNode.ID {
			replicas = append(replicas, n)
		}
	}
	return replicas
}
