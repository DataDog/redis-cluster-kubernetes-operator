// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/timeouts"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/apps/v1"
)

var (
	podNumberParser = regexp.MustCompile(".*-([0-9]+)")
)

// GetCluster gets the nodes in the cluster, and identifies one node as the trusted node.
func (cc *ClusterClient) GetCluster(ctx context.Context, redisCluster *cachev1.RedisCluster, statefulSet *v1.StatefulSet) (*Cluster, error) {

	log := cc.Log.WithName("cluster")

	// get the raw list of pods belonging to the StatefulSet
	podList, err := cc.GetRedisClusterPods(ctx, redisCluster)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get pods")
	}
	log.Info("Got pods")

	// get the Redis port
	redisPort, err := cc.getRedisPort(ctx, redisCluster)
	if err != nil {
		log.Error(err, "failed to get the Redis port")
		return nil, utils.WrapErr(err, "failed to get the Redis port")
	}

	toolbox, err := cc.GetToolbox(ctx, redisCluster.Namespace, redisPort)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get toolbox")
	}

	// convert into a list of nodes with name + IP + port.
	nodes, err := podsToClusterNodes(podList.Items)
	if err != nil {
		return nil, utils.WrapErr(err, "failed to get pods")
	}

	// sort by pod name / index
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].PodIndex < nodes[j].PodIndex
	})

	// complement the nodes with Redis INFO + NODES details
	nodes, err = mergeRedisNodes(toolbox, nodes)
	if err != nil {
		log.Error(err, "failed to populate redis details")
		return nil, utils.WrapErr(err, "failed to populate Redis details")
	}

	groupedNodes := groupNodes(nodes)
	var node *ClusterNode
	if len(groupedNodes[PrimaryNode]) > 0 {
		node = groupedNodes[PrimaryNode][0]
	} else if len(groupedNodes[ReplicaNode]) > 0 {
		node = groupedNodes[ReplicaNode][0]
	}

	// Calculate timeouts based on cluster size
	nodeCount := redisCluster.EffectivePodCount()
	timeoutConfig := timeouts.NewConfig(nodeCount)

	return &Cluster{
		RedisCluster: redisCluster,
		StatefulSet:  statefulSet,
		Toolbox:      toolbox,
		Timeouts:     timeoutConfig,
		RedisPort:    redisPort,
		RedisVersion: parseRedisVersionFromSTS(statefulSet),
		AllNodes:     nodes,
		Nodes:        groupedNodes,
		Node:         node,
	}, nil
}

func (c *Cluster) CreateFromEmptyNodes() error {
	return c.Toolbox.CreateCluster(c)
}

func (c *Cluster) CreateFromRestoredNodes() error {
	return c.Toolbox.MeetCluster(c)
}

const clusterViewPerNodeTimeout = 5 * time.Second

func clusterViewTimeout(nodeCount int) time.Duration {
	if nodeCount < 1 {
		nodeCount = 1
	}
	// Leave one per-node interval for Kubernetes exec setup and output parsing.
	return time.Duration(nodeCount+1) * clusterViewPerNodeTimeout
}

// mergeRedisNodes merges the Redis CLUSTER INFO+NODES output with the known pods.
func mergeRedisNodes(t *Toolbox, nodes []*ClusterNode) ([]*ClusterNode, error) {
	ctx, cancel := context.WithTimeout(t.ctx, clusterViewTimeout(len(nodes)))
	defer cancel()
	var response strings.Builder
	clusterViews := 0
	for _, node := range nodes {
		nodeCtx, cancelNode := context.WithTimeout(ctx, clusterViewPerNodeTimeout)
		rc, info, _, err := t.ClusterInfo(nodeCtx, node.IP)
		if err == nil && rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		}
		if err == nil && strings.HasPrefix(strings.TrimSpace(info), "ERR") {
			err = fmt.Errorf("Redis returned %s", strings.TrimSpace(info))
		}
		if err != nil {
			if ctx.Err() != nil {
				cancelNode()
				return nil, utils.WrapErr(ctx.Err(), "failed to get Redis info before cluster-view deadline")
			}
			t.Log.Info("Redis INFO unavailable; retaining pod in partial cluster view", "podName", node.PodName, "podIP", node.IP, "err", err)
		} else {
			response.WriteString(info)
			if !strings.HasSuffix(info, "\n") {
				response.WriteByte('\n')
			}
		}
		response.WriteString("PONG\n")

		rc, clusterNodes, _, err := t.runRedis(nodeCtx, node.IP, t.RedisPort, "CLUSTER", "NODES")
		if err == nil && rc != 0 {
			err = fmt.Errorf("command exit code was %d", rc)
		}
		if err == nil && strings.HasPrefix(strings.TrimSpace(clusterNodes), "ERR") {
			err = fmt.Errorf("Redis returned %s", strings.TrimSpace(clusterNodes))
		}
		if err == nil && strings.TrimSpace(clusterNodes) == "" {
			err = fmt.Errorf("Redis returned an empty CLUSTER NODES view")
		}
		if err != nil {
			if ctx.Err() != nil {
				cancelNode()
				return nil, utils.WrapErr(ctx.Err(), "failed to get Redis nodes before cluster-view deadline")
			}
			t.Log.Info("Redis CLUSTER NODES unavailable; retaining pod in partial cluster view", "podName", node.PodName, "podIP", node.IP, "err", err)
		} else {
			clusterViews++
			response.WriteString(clusterNodes)
			if !strings.HasSuffix(clusterNodes, "\n") {
				response.WriteByte('\n')
			}
		}
		response.WriteString("PONG\n")
		cancelNode()
	}
	if clusterViews == 0 {
		return nil, fmt.Errorf("no Redis process returned a usable CLUSTER NODES view")
	}
	t.Log.Info("Got Redis info+nodes")

	lines := strings.Split(response.String(), "\n")
	parse := 0 // start by parsing info
	index := 0
	node := nodes[index]
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "ERR") {
			t.Log.Error(fmt.Errorf("Redis ERR response"), "CLUSTER INFO/NODES returned error line",
				"errLine", line,
				"podName", node.PodName,
				"podIP", node.IP,
				"podIndex", node.PodIndex)
			return nil, utils.WrapErr(
				fmt.Errorf("node %s returned: %s", node.PodName, line),
				"ERR in CLUSTER INFO/NODES response",
			)
		}
		if parse == 0 {
			// parsing INFO output
			if line == "PONG" {
				parse = 1 // now parse NODES output for same node
			} else {
				parseInfoLine(node, line)
			}
		} else {
			// parsing NODES output
			if line == "PONG" {
				parse = 0 // now parse INFO output for next node, if any
				index++
				if index < len(nodes) {
					node = nodes[index]
				}
			} else {
				nodes = parseNodeLine(nodes, node, line, t.Log)
			}
		}
	}
	hydrateUnavailableNodesFromViews(nodes)

	return nodes, nil
}

func hydrateUnavailableNodesFromViews(nodes []*ClusterNode) {
	for _, node := range nodes {
		if node.ID != "" || len(node.Views) == 0 {
			continue
		}
		indexes := make([]int, 0, len(node.Views))
		for index := range node.Views {
			indexes = append(indexes, index)
		}
		sort.Ints(indexes)
		for _, index := range indexes {
			view := node.Views[index]
			if view == nil || view.ID == "" {
				continue
			}
			node.ID = view.ID
			node.Flags = append([]string(nil), view.Flags...)
			node.PrimaryID = view.Master
			node.LinkState = view.LinkState
			node.SlotsRanges = append([]SlotsRange(nil), view.SlotsRanges...)
			node.OpenSlots = append([]OpenSlot(nil), view.OpenSlots...)
			break
		}
	}
}

// parseInfoLine parses one CLUSTER INFO output line.
func parseInfoLine(node *ClusterNode, line string) {
	parts := strings.Split(line, ":")
	if len(parts) == 2 {
		field := parts[0]
		value := parts[1]
		switch field {
		case "cluster_state":
			node.Info.ClusterStateIsOK = value == "ok"
		case "cluster_slots_assigned":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.SlotsAssigned = i
		case "cluster_slots_ok":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.SlotsOK = i
		case "cluster_slots_pfail":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.SlotsPFail = i
		case "cluster_slots_fail":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.SlotsFail = i
		case "cluster_known_nodes":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.KnownNodes = i
		case "cluster_size":
			i, err := strconv.Atoi(value)
			if err != nil {
				return
			}
			node.Info.Size = i
		}
	}
}

// parseNodeLine parses one CLUSTER NODES output line.
func parseNodeLine(nodes []*ClusterNode, node *ClusterNode, line string, log logr.Logger) []*ClusterNode {
	details := strings.Fields(line)
	if len(details) < 8 {
		log.Info("Bad details format", "details", details)
		return nodes
	}

	lineMaster := details[3]
	if lineMaster == "-" {
		lineMaster = ""
	}
	lineID := details[0]
	lineLinkState := details[7]
	lineFlags, myself := parseFlags(details[2])

	lineIP, lineHostname, addressOK := ParseClusterNodeAddress(details[1])
	if !addressOK {
		log.Info("Bad node address", "address", details[1], "details", details)
		return nodes
	}

	lineSlotsRanges, lineOpenSlots := parseSlotTokens(details[8:], log)

	if myself {
		// my view of myself
		node.ID = lineID
		node.Flags = lineFlags
		node.PrimaryID = lineMaster
		node.LinkState = lineLinkState
		node.Hostname = lineHostname
		node.SlotsRanges = lineSlotsRanges
		node.OpenSlots = lineOpenSlots
	} else {
		// my view of another node - try to find that node, or create it
		var otherNode *ClusterNode
		var found bool
		if lineIP != "" {
			otherNode, found = lookupClusterNodeByIP(nodes, lineIP)
		}
		if !found {
			otherNode, found = lookupClusterNodeByID(nodes, lineID)
		}
		if !found {
			// This node exists in cluster metadata but has no corresponding pod.
			// This is a potential zombie node that may cause cluster issues.
			log.Info("Detected node in cluster without corresponding pod (potential zombie)",
				"nodeID", lineID,
				"nodeIP", lineIP,
				"seenByPodIndex", node.PodIndex,
				"seenByPod", node.PodName,
				"flags", lineFlags,
				"linkState", lineLinkState)
			otherNode = &ClusterNode{
				PodName:      "",
				PodIndex:     -1,
				IP:           "",
				ID:           lineID,
				RedisVersion: "",
				Views:        make(map[int]*NodeView),
				LinkState:    lineLinkState,
				Flags:        lineFlags,
			}
			nodes = append(nodes, otherNode)
		}
		if otherNode.Hostname == "" {
			otherNode.Hostname = lineHostname
		}
		otherNode.Views[node.PodIndex] = &NodeView{
			//IP: nodeIP,
			//Port: nodePort,
			ID:          lineID,
			Flags:       lineFlags,
			Master:      lineMaster,
			LinkState:   lineLinkState,
			SlotsRanges: lineSlotsRanges,
			OpenSlots:   lineOpenSlots,
		}
	}

	return nodes
}

// ParseClusterNodeAddress extracts the host and optional announced hostname from
// Redis' CLUSTER NODES address field: host:port@bus-port[,hostname]. It accepts
// bracketed and unbracketed IPv6 addresses in addition to IPv4 and DNS names.
func ParseClusterNodeAddress(value string) (host, hostname string, ok bool) {
	endpoint, hostname, _ := strings.Cut(value, ",")
	clientAddress, busPort, found := strings.Cut(endpoint, "@")
	if !found || busPort == "" {
		return "", "", false
	}
	if _, err := strconv.Atoi(busPort); err != nil {
		return "", "", false
	}

	host, port, err := net.SplitHostPort(clientAddress)
	if err != nil {
		// Redis can emit unbracketed IPv6. The final colon still separates the
		// client port, while every earlier colon belongs to the address.
		separator := strings.LastIndexByte(clientAddress, ':')
		if separator < 0 {
			return "", "", false
		}
		host = strings.Trim(clientAddress[:separator], "[]")
		port = clientAddress[separator+1:]
	}
	if _, err := strconv.Atoi(port); err != nil {
		return "", "", false
	}
	return host, hostname, true
}

func parseSlotTokens(tokens []string, log logr.Logger) ([]SlotsRange, []OpenSlot) {
	slotsRanges := make([]SlotsRange, 0)
	openSlots := make([]OpenSlot, 0)
	for _, token := range tokens {
		if strings.HasPrefix(token, "[") {
			openSlot, ok := parseOpenSlot(token, log)
			if ok {
				openSlots = append(openSlots, openSlot)
			}
			continue
		}
		p := strings.Split(token, "-")
		if len(p) == 2 {
			first, err := strconv.Atoi(p[0])
			if err != nil {
				log.Error(err, "error parsing slots range", "slot", token)
				continue
			}
			last, err := strconv.Atoi(p[1])
			if err != nil {
				log.Error(err, "error parsing slots range", "slot", token)
				continue
			}
			slotsRanges = append(slotsRanges, SlotsRange{First: first, Last: last})
		} else {
			first, err := strconv.Atoi(token)
			if err != nil {
				log.Error(err, "error parsing slots range", "slot", token)
				continue
			}
			slotsRanges = append(slotsRanges, SlotsRange{First: first, Last: first})
		}
	}
	return slotsRanges, openSlots
}

func parseOpenSlot(token string, log logr.Logger) (OpenSlot, bool) {
	inner := strings.TrimSuffix(strings.TrimPrefix(token, "["), "]")
	parts := strings.Split(inner, "-")
	if len(parts) != 3 {
		log.Info("Bad open slot marker", "slot", token)
		return OpenSlot{}, false
	}
	slot, err := strconv.Atoi(parts[0])
	if err != nil {
		log.Error(err, "error parsing open slot", "slot", token)
		return OpenSlot{}, false
	}
	var state OpenSlotState
	switch parts[1] {
	case "<":
		state = OpenSlotImporting
	case ">":
		state = OpenSlotMigrating
	default:
		log.Info("Bad open slot direction", "slot", token)
		return OpenSlot{}, false
	}
	return OpenSlot{Slot: slot, State: state, PeerID: parts[2]}, true
}

// parseFlags parses the flags csv into a sorted list.
func parseFlags(s string) ([]string, bool) {
	output := make([]string, 0)
	flags := strings.Split(s, ",")
	myself := false

	for _, flag := range flags {
		if flag != "myself" {
			output = append(output, flag)
		} else {
			myself = true
		}
	}

	// sort the flags by name
	sort.Strings(output)

	return output, myself
}

// parseIndex parse the ordinal index from a StatefulSet's pod's name.
func parseIndex(name string) (int, error) {
	m := podNumberParser.FindAllStringSubmatch(name, -1)
	if m == nil {
		return -1, fmt.Errorf("unable to parse cluster node name (%s)", name)
	}
	return strconv.Atoi(m[0][1])
}

// parseRedisVersion parses the Redis version from the container's image.
func parseRedisVersionFromSTS(sts *v1.StatefulSet) string {
	if len(sts.Spec.Template.Spec.Containers) == 0 {
		return "unknown"
	}
	return parseRedisVersion(sts.Spec.Template.Spec.Containers[0].Image)
}

func parseRedisVersion(image string) string {
	s := strings.Index(image, "redis:")
	if s < 0 {
		s = 0
	} else {
		s += len("redis:")
	}
	e := strings.Index(image, "@sha256")
	if e < 0 {
		e = len(image)
	}
	return image[s:e]
}

// GetUnassignedSlotsRanges get the ranges of unassigned slots.
func (c *Cluster) GetUnassignedSlotsRanges() ([]SlotsRange, error) {
	primaryNodes := c.Nodes[PrimaryNode]
	slots := make([]bool, constants.TotalSlots)
	for _, node := range primaryNodes {
		for _, slotRange := range node.SlotsRanges {
			for i := slotRange.First; i <= slotRange.Last; i++ {
				slots[i] = true
			}
		}
	}
	ranges := make([]SlotsRange, 0)
	var current *SlotsRange
	for i := 0; i < constants.TotalSlots; i++ {
		if slots[i] {
			if current != nil {
				current.Last = i - 1
				ranges = append(ranges, *current)
				current = nil
			}
		} else {
			if current == nil {
				current = &SlotsRange{First: i}
			}
		}
	}
	if current != nil {
		current.Last = constants.TotalSlots - 1
		ranges = append(ranges, *current)
	}

	return ranges, nil
}
