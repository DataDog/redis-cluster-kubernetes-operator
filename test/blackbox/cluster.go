// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package blackbox

import (
	"fmt"
	"strconv"
	"strings"
)

const totalRedisSlots = 16384

type redisNode struct {
	ID       string
	Address  string
	Primary  bool
	Myself   bool
	Failed   bool
	MasterID string
	Slots    []string
}

func parseClusterNodes(output string) []redisNode {
	var nodes []redisNode
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		address := strings.TrimPrefix(strings.SplitN(fields[1], "@", 2)[0], "[")
		if strings.Contains(address, "]:") {
			address = strings.SplitN(address, "]:", 2)[0]
		} else if i := strings.LastIndexByte(address, ':'); i >= 0 {
			address = address[:i]
		}
		flags := "," + fields[2] + ","
		node := redisNode{
			ID:       fields[0],
			Address:  address,
			Primary:  strings.Contains(flags, ",master,"),
			Myself:   strings.Contains(flags, ",myself,"),
			Failed:   strings.Contains(flags, ",fail,") || strings.Contains(flags, ",fail?,"),
			MasterID: fields[3],
		}
		if len(fields) > 8 {
			node.Slots = fields[8:]
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func slotCoverage(nodes []redisNode) (int, error) {
	seen := make([]bool, totalRedisSlots)
	for _, node := range nodes {
		if !node.Primary || node.Failed {
			continue
		}
		for _, token := range node.Slots {
			if strings.HasPrefix(token, "[") {
				continue
			}
			lo, hi, err := parseSlotRange(token)
			if err != nil {
				return 0, err
			}
			for slot := lo; slot <= hi; slot++ {
				if seen[slot] {
					return 0, fmt.Errorf("slot %d is owned more than once", slot)
				}
				seen[slot] = true
			}
		}
	}
	covered := 0
	for _, present := range seen {
		if present {
			covered++
		}
	}
	return covered, nil
}

func parseSlotRange(token string) (int, int, error) {
	parts := strings.SplitN(token, "-", 2)
	lo, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid slot %q: %w", token, err)
	}
	hi := lo
	if len(parts) == 2 {
		hi, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid slot range %q: %w", token, err)
		}
	}
	if lo < 0 || hi < lo || hi >= totalRedisSlots {
		return 0, 0, fmt.Errorf("slot range %q is outside 0-%d", token, totalRedisSlots-1)
	}
	return lo, hi, nil
}

func topologyCounts(nodes []redisNode) (primaries, replicas int) {
	for _, node := range nodes {
		if node.Failed {
			continue
		}
		if node.Primary {
			primaries++
		} else if node.MasterID != "-" {
			replicas++
		}
	}
	return primaries, replicas
}

func infoValue(output, key string) string {
	prefix := key + ":"
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}
