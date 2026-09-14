package property

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"pgregory.net/rapid"
)

// ClusterModel is a coordinated description of a Redis Cluster's state.
// One model renders consistent CLUSTER NODES, CLUSTER INFO, and INFO
// replication outputs so tests can drive parsers from realistic shapes
// without hardcoding any single example.
type ClusterModel struct {
	Pods []Pod // ordered by shard, primary first then replicas
	// NoAnnounceIP models a cluster without cluster-announce-ip set: a node's
	// own "myself" line reports a bare ":6379@16379" (empty IP). Parsing must
	// skip it rather than map an empty-string IP.
	NoAnnounceIP bool
}

// Pod is one Redis node in the model.
type Pod struct {
	NodeID      string
	IP          string // 100.x.x.x pod IP
	Name        string // redis-cluster-<rcname>-<ordinal>
	Flags       []string
	IsPrimary   bool
	PrimaryID   string // empty if primary
	Shard       int
	SlotsFirst  int    // primary only
	SlotsLast   int    // primary only
	Importing   string // optional "[slot-<-NODE]" appendage on this line
	Migrating   string // optional "[slot->-NODE]" appendage on this line
	ConfigEpoch int
}

// GenClusterModel returns a rapid generator producing realistic cluster
// topologies: 1-8 shards, 0-3 replicas per shard, random failure mix,
// optional importing/migrating slots mid-flight.
func GenClusterModel() *rapid.Generator[ClusterModel] {
	return rapid.Custom(func(t *rapid.T) ClusterModel {
		shards := rapid.IntRange(1, 8).Draw(t, "shards")
		replicas := rapid.IntRange(0, 3).Draw(t, "replicasPerShard")
		rcName := rapid.StringMatching(`[a-z]{3,8}`).Draw(t, "rcName")

		m := ClusterModel{NoAnnounceIP: rapid.Bool().Draw(t, "noAnnounceIP")}
		slotsPer := constants.TotalSlots / shards
		ordinal := 0
		ipCounter := 1

		for s := range shards {
			first := s * slotsPer
			last := first + slotsPer - 1
			if s == shards-1 {
				last = constants.TotalSlots - 1
			}
			// random failure mix
			failed := rapid.Bool().Draw(t, fmt.Sprintf("shard%dFailed", s))
			primaryFlags := []string{"master"}
			if failed {
				primaryFlags = append(primaryFlags, "fail")
			}
			primary := Pod{
				NodeID:      genNodeID(t, fmt.Sprintf("nid_p%d", s)),
				IP:          fmt.Sprintf("100.112.%d.%d", s, ipCounter),
				Name:        fmt.Sprintf("redis-cluster-%s-%d", rcName, ordinal),
				Flags:       primaryFlags,
				IsPrimary:   true,
				Shard:       s,
				SlotsFirst:  first,
				SlotsLast:   last,
				ConfigEpoch: s + 1,
			}
			// optional mid-flight migration on a primary
			if !failed && rapid.Bool().Draw(t, fmt.Sprintf("shard%dMigrating", s)) {
				primary.Importing = fmt.Sprintf("[%d-<-%s]", first, genNodeID(t, "importer"))
			}
			m.Pods = append(m.Pods, primary)
			ordinal++
			ipCounter++

			for r := range replicas {
				m.Pods = append(m.Pods, Pod{
					NodeID:      genNodeID(t, fmt.Sprintf("nid_s%d_%d", s, r)),
					IP:          fmt.Sprintf("100.112.%d.%d", s, ipCounter),
					Name:        fmt.Sprintf("redis-cluster-%s-%d", rcName, ordinal),
					Flags:       []string{"slave"},
					IsPrimary:   false,
					PrimaryID:   primary.NodeID,
					Shard:       s,
					ConfigEpoch: s + 1,
				})
				ordinal++
				ipCounter++
			}
		}
		return m
	})
}

// genNodeID produces a 40-char hex node ID (Redis format).
func genNodeID(t *rapid.T, label string) string {
	return rapid.StringMatching(`[0-9a-f]{40}`).Draw(t, label)
}

// HealthyPrimaries returns primaries without the "fail" flag (the parser
// excludes failed primaries from slot ownership).
func (m ClusterModel) HealthyPrimaries() []Pod {
	out := make([]Pod, 0, len(m.Pods))
	for _, p := range m.Pods {
		if p.IsPrimary && !containsString(p.Flags, "fail") {
			out = append(out, p)
		}
	}
	return out
}

// RenderClusterNodes emits CLUSTER NODES output from the perspective of
// `viewer`. The viewer's own line carries the "myself" flag (the bug
// surface that commit 45218e94 fixed).
func (m ClusterModel) RenderClusterNodes(viewer Pod) string {
	var b strings.Builder
	for _, p := range m.Pods {
		flags := p.Flags
		if p.NodeID == viewer.NodeID {
			flags = append([]string{"myself"}, p.Flags...)
		}
		// Redis emits flags sorted insertion-order; we shuffle into sorted
		// form so the parser doesn't accidentally rely on ordering.
		sort.Strings(flags)

		primary := "-"
		if p.PrimaryID != "" {
			primary = p.PrimaryID
		}
		// Without cluster-announce-ip, a node's own line reports an empty IP.
		ip := p.IP
		if m.NoAnnounceIP && p.NodeID == viewer.NodeID {
			ip = ""
		}
		// node_id ip:port@cport[,hostname] flags primary ping pong epoch link [slots...]
		line := fmt.Sprintf("%s %s:6379@16379 %s %s 0 1779231570000 %d connected",
			p.NodeID, ip, strings.Join(flags, ","), primary, p.ConfigEpoch)
		if p.IsPrimary && !containsString(p.Flags, "fail") {
			line += fmt.Sprintf(" %d-%d", p.SlotsFirst, p.SlotsLast)
		}
		if p.Importing != "" {
			line += " " + p.Importing
		}
		if p.Migrating != "" {
			line += " " + p.Migrating
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}

// RenderClusterInfo emits CLUSTER INFO output coordinated with the model:
// slot counts derived from healthy primaries, known_nodes from len(Pods),
// size from shard count. Includes 2 unknown fields (cluster_current_epoch,
// cluster_my_epoch) that real Redis always emits — exercises parser
// tolerance to unknown fields.
func (m ClusterModel) RenderClusterInfo() string {
	healthy := m.HealthyPrimaries()
	assigned := 0
	for _, p := range healthy {
		assigned += p.SlotsLast - p.SlotsFirst + 1
	}
	state := "ok"
	if assigned < constants.TotalSlots {
		state = "fail"
	}
	shardCount := 0
	for _, p := range m.Pods {
		if p.IsPrimary {
			shardCount++
		}
	}
	return fmt.Sprintf(
		"cluster_state:%s\n"+
			"cluster_slots_assigned:%d\n"+
			"cluster_slots_ok:%d\n"+
			"cluster_slots_pfail:0\n"+
			"cluster_slots_fail:0\n"+
			"cluster_known_nodes:%d\n"+
			"cluster_size:%d\n"+
			"cluster_current_epoch:%d\n"+
			"cluster_my_epoch:%d\n",
		state, assigned, assigned, len(m.Pods), shardCount, shardCount, shardCount,
	)
}

// RenderInfoReplication emits INFO replication output for `p`. Always
// includes both slave_repl_offset AND slave_read_repl_offset (with
// different values) when p is a replica — exercises the prefix-match
// hazard in ParseInfoField.
func (m ClusterModel) RenderInfoReplication(p Pod) string {
	if p.IsPrimary {
		return "role:master\n" +
			"connected_slaves:0\n" +
			"master_replid:0000000000000000000000000000000000000000\n"
	}
	var primaryIP string
	for _, q := range m.Pods {
		if q.NodeID == p.PrimaryID {
			primaryIP = q.IP
			break
		}
	}
	repl := int64(3087255338614)
	readRepl := repl + 100 // intentionally different
	return fmt.Sprintf(
		"role:slave\n"+
			"master_host:%s\n"+
			"master_port:6379\n"+
			"master_link_status:up\n"+
			"master_last_io_seconds_ago:0\n"+
			"master_sync_in_progress:0\n"+
			"slave_read_repl_offset:%d\n"+
			"slave_repl_offset:%d\n"+
			"connected_slaves:0\n",
		primaryIP, readRepl, repl,
	)
}

func containsString(s []string, v string) bool {
	return slices.Contains(s, v)
}
