// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"pgregory.net/rapid"
)

// "myself" must never change the classified role.
func TestProperty_DetermineRole_IgnoresMyself(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		flags := genFlagSet().Draw(rt, "flags")
		info := genNodeInfo().Draw(rt, "info")

		without := &ClusterNode{
			PodIndex: 0,
			IP:       "10.0.0.1",
			ID:       "abc",
			Flags:    flags,
			Info:     info,
		}
		// "myself" can appear at any position
		withMy := &ClusterNode{
			PodIndex: 0,
			IP:       "10.0.0.1",
			ID:       "abc",
			Flags:    append([]string{"myself"}, flags...),
			Info:     info,
		}

		if without.determineRole() != withMy.determineRole() {
			rt.Fatalf("role differs with myself flag: without=%v with=%v flags=%v",
				without.determineRole(), withMy.determineRole(), flags)
		}
	})
}

// Flag order must not affect role.
func TestProperty_DetermineRole_FlagOrderInvariant(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		flags := genFlagSet().Draw(rt, "flags")
		shuffled := rapid.Permutation(slices.Clone(flags)).Draw(rt, "perm")

		a := &ClusterNode{PodIndex: 0, IP: "10.0.0.1", ID: "abc", Flags: flags}
		b := &ClusterNode{PodIndex: 0, IP: "10.0.0.1", ID: "abc", Flags: shuffled}

		if a.determineRole() != b.determineRole() {
			rt.Fatalf("role depends on flag order: %v vs %v", flags, shuffled)
		}
	})
}

// parseInfoLine must round-trip all known fields and ignore unknown ones.
func TestProperty_ParseInfoLine_RoundTrips(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		want := genNodeInfo().Draw(rt, "info")
		extraUnknown := rapid.Bool().Draw(rt, "addUnknown")
		text := renderClusterInfo(want, extraUnknown)

		node := &ClusterNode{}
		for _, line := range strings.Split(text, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parseInfoLine(node, line)
		}

		if node.Info != want {
			rt.Fatalf("round-trip mismatch:\nwant %+v\ngot  %+v\ntext:\n%s",
				want, node.Info, text)
		}
	})
}

// genFlagSet produces a legal Redis flag combination. "myself" is excluded
// here — the ignores-myself property adds it explicitly.
func genFlagSet() *rapid.Generator[[]string] {
	role := rapid.SampledFrom([]string{"master", "slave"})
	return rapid.Custom(func(t *rapid.T) []string {
		out := []string{role.Draw(t, "role")}
		if rapid.Bool().Draw(t, "fail") {
			out = append(out, "fail")
		}
		if rapid.Bool().Draw(t, "pfail") {
			out = append(out, "fail?")
		}
		if rapid.Bool().Draw(t, "handshake") {
			out = append(out, "handshake")
		}
		if rapid.Bool().Draw(t, "noaddr") {
			out = append(out, "noaddr")
		}
		return out
	})
}

func genNodeInfo() *rapid.Generator[NodeInfo] {
	return rapid.Custom(func(t *rapid.T) NodeInfo {
		return NodeInfo{
			ClusterStateIsOK: rapid.Bool().Draw(t, "state"),
			SlotsAssigned:    rapid.IntRange(0, 16384).Draw(t, "assigned"),
			SlotsOK:          rapid.IntRange(0, 16384).Draw(t, "ok"),
			SlotsPFail:       rapid.IntRange(0, 16384).Draw(t, "pfail"),
			SlotsFail:        rapid.IntRange(0, 16384).Draw(t, "fail"),
			KnownNodes:       rapid.IntRange(0, 64).Draw(t, "known"),
			Size:             rapid.IntRange(0, 32).Draw(t, "size"),
		}
	})
}

func renderClusterInfo(i NodeInfo, addUnknown bool) string {
	state := "fail"
	if i.ClusterStateIsOK {
		state = "ok"
	}
	out := fmt.Sprintf(
		"cluster_state:%s\n"+
			"cluster_slots_assigned:%d\n"+
			"cluster_slots_ok:%d\n"+
			"cluster_slots_pfail:%d\n"+
			"cluster_slots_fail:%d\n"+
			"cluster_known_nodes:%d\n"+
			"cluster_size:%d\n",
		state, i.SlotsAssigned, i.SlotsOK, i.SlotsPFail, i.SlotsFail, i.KnownNodes, i.Size,
	)
	if addUnknown {
		out += "cluster_current_epoch:42\ncluster_my_epoch:17\n"
	}
	return out
}
