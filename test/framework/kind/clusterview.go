// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"fmt"
	"strconv"
	"strings"
)

// ClusterNode is one parsed row of `CLUSTER NODES` output.
type ClusterNode struct {
	ID       string
	IP       string   // announce IP (host part of the addr field)
	IsMaster bool     // "master" flag present
	Myself   bool     // "myself" flag present (the queried/self node)
	Failed   bool     // "fail"/"fail?" flag present
	MasterID string   // primary's node ID for a replica; "-" for a primary
	Slots    []string // raw slot tokens (ranges like "0-5460", singletons, "[..]" migrations)
}

// ClusterView is one Redis node's parsed view of the cluster.
type ClusterView struct {
	ViewerPod string
	ViewerIP  string
	Raw       string
	Nodes     []ClusterNode
}

// TryClusterViews reads CLUSTER NODES from every ready Redis pod.
func (s *Suite) TryClusterViews(name string) ([]ClusterView, error) {
	pods, err := s.tryRedisPodList(name)
	if err != nil {
		return nil, err
	}
	views := make([]ClusterView, 0, len(pods))
	for _, pod := range pods {
		if !podRunningReady(pod) || pod.Status.PodIP == "" {
			continue
		}
		raw, err := s.TryRedisCommandAtIP(pod.Status.PodIP, "CLUSTER", "NODES")
		if err != nil {
			return nil, fmt.Errorf("CLUSTER NODES from %s: %w", pod.Status.PodIP, err)
		}
		views = append(views, ClusterView{
			ViewerPod: pod.Name,
			ViewerIP:  pod.Status.PodIP,
			Raw:       raw,
			Nodes:     parseClusterNodesFrom(raw, pod.Status.PodIP),
		})
	}
	if len(views) == 0 {
		return nil, fmt.Errorf("no ready Redis pods")
	}
	return views, nil
}

// FindNodeByID returns a node from a parsed cluster view.
func FindNodeByID(nodes []ClusterNode, id string) (ClusterNode, bool) {
	for _, node := range nodes {
		if node.ID == id {
			return node, true
		}
	}
	return ClusterNode{}, false
}

// FindNodeByIP returns a node from a parsed cluster view.
func FindNodeByIP(nodes []ClusterNode, ip string) (ClusterNode, bool) {
	for _, node := range nodes {
		if node.IP == ip {
			return node, true
		}
	}
	return ClusterNode{}, false
}

// ParseClusterNodes parses `CLUSTER NODES` text into rows; malformed lines are
// skipped. Flag matching mirrors the peer-emitted "myself,master" form.
func ParseClusterNodes(out string) []ClusterNode {
	var nodes []ClusterNode
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		ip := fields[1]
		if i := strings.IndexByte(ip, ':'); i >= 0 {
			ip = ip[:i]
		}
		n := ClusterNode{
			ID:       fields[0],
			IP:       ip,
			IsMaster: strings.Contains(fields[2], "master"),
			Myself:   strings.Contains(fields[2], "myself"),
			Failed:   strings.Contains(fields[2], "fail"),
			MasterID: fields[3],
		}
		if len(fields) > 8 {
			n.Slots = fields[8:]
		}
		nodes = append(nodes, n)
	}
	return nodes
}

// parseClusterNodesFrom parses `CLUSTER NODES` taken from viewerIP, back-filling
// only the self ("myself") row's empty IP with viewerIP. KIND runs redis without
// --cluster-announce-ip, so the queried node's own row carries an empty addr host.
// Gating on the myself flag (not any empty IP) avoids mis-mapping a genuinely
// empty-IP peer row to the viewer.
func parseClusterNodesFrom(out, viewerIP string) []ClusterNode {
	nodes := ParseClusterNodes(out)
	for i := range nodes {
		if nodes[i].Myself && nodes[i].IP == "" {
			nodes[i].IP = viewerIP
		}
	}
	return nodes
}

// OwnedSlots counts the slots this node owns, skipping in-flight migration
// tokens ("[5462-<-NODEID]") which are slots in motion, not slots served.
func (n ClusterNode) OwnedSlots() int {
	total := 0
	for _, tok := range n.Slots {
		if lo, hi, ok := ownedSlotRange(tok); ok {
			total += hi - lo + 1
		}
	}
	return total
}

// OwnsSlot reports whether this node owns the slot in this cluster view.
func (n ClusterNode) OwnsSlot(slot int) bool {
	for _, tok := range n.Slots {
		lo, hi, ok := ownedSlotRange(tok)
		if ok && slot >= lo && slot <= hi {
			return true
		}
	}
	return false
}

func ownedSlotRange(token string) (int, int, bool) {
	if strings.HasPrefix(token, "[") {
		return 0, 0, false
	}
	if separator := strings.IndexByte(token, '-'); separator > 0 {
		lo, errLo := strconv.Atoi(token[:separator])
		hi, errHi := strconv.Atoi(token[separator+1:])
		return lo, hi, errLo == nil && errHi == nil
	}
	slot, err := strconv.Atoi(token)
	return slot, slot, err == nil
}

func sumPrimarySlotCoverage(nodes []ClusterNode) int {
	total := 0
	for _, n := range nodes {
		if n.IsMaster && !n.Failed {
			total += n.OwnedSlots()
		}
	}
	return total
}

func infoField(info, key string) string {
	prefix := key + ":"
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return line[len(prefix):]
		}
	}
	return ""
}

func infoFieldInt(info, key string) int {
	v := infoField(info, key)
	if v == "" {
		return -1
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}
