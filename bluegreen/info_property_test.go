package bluegreen

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/property"
	"pgregory.net/rapid"
)

// ParseInfoField must not confuse adjacent fields whose names share a
// prefix. The real fixture from staging proves both `slave_repl_offset`
// and `slave_read_repl_offset` are always present, with the same value
// in steady state — but the operator reads them via independent calls
// and a confusion would silently swap them.
//
// Property: rendering INFO replication via the coordinated ClusterModel
// always yields a different value for `slave_repl_offset` vs
// `slave_read_repl_offset`, and ParseInfoField returns the right one
// for each query.
func TestProperty_ParseInfoField_PrefixHazard(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := property.GenClusterModel().Draw(rt, "model")
		// pick a replica from the model; if none, regen
		idx := -1
		for i, p := range m.Pods {
			if !p.IsPrimary {
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		text := m.RenderInfoReplication(m.Pods[idx])

		repl := ParseInfoField(text, "slave_repl_offset")
		readRepl := ParseInfoField(text, "slave_read_repl_offset")

		if repl == -1 || readRepl == -1 {
			rt.Fatalf("either field returned -1; text:\n%s", text)
		}
		if repl == readRepl {
			rt.Fatalf("prefix collision: slave_repl_offset and slave_read_repl_offset both returned %d", repl)
		}
		// renderer makes readRepl exactly 100 greater than repl
		if readRepl-repl != 100 {
			rt.Fatalf("wrong-line match: repl=%d readRepl=%d (expected delta 100)", repl, readRepl)
		}
	})
}

// Absent field queries must not prefix-match another field's value:
// e.g. querying "master" must return -1, not the master_host:X line's
// numeric tail. Sample includes deliberate prefix-collision hazards.
func TestProperty_ParseInfoField_AbsentFieldDoesNotPrefixMatch(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := property.GenClusterModel().Draw(rt, "model")
		text := m.RenderInfoReplication(m.Pods[0])

		absent := rapid.SampledFrom([]string{
			"nonexistent_field",
			"slave_repl_offset_extra",
			"role_x",
			"master",
		}).Draw(rt, "absentField")

		if !strings.Contains(text, absent+":") {
			if got := ParseInfoField(text, absent); got != -1 {
				rt.Fatalf("ParseInfoField(%q) = %d; want -1; text:\n%s", absent, got, text)
			}
		}
	})
}

// isSlaveOfGreenPod must return true iff the parsed master_host appears
// in the ShardMapping's set of green pod IPs. Bug class: substring or
// prefix collision on master_host_* fields, or wrong field extracted.
//
// isSlaveOfGreenPod returns (matched, masterHost, slotsRanges), where matched
// also requires the mapped entry's SlotsRanges == expectedSlots.
// We leave each entry's SlotsRanges empty and pass expectedSlots="" so the
// boolean reduces to pure green-pod-IP membership — the property under test.
func TestProperty_IsSlaveOfGreenPod_MatchesGeneratedMaster(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		greenIPs := rapid.SliceOfN(
			rapid.StringMatching(`100\.112\.[0-9]{1,3}\.[0-9]{1,3}`),
			1, 5,
		).Draw(rt, "greenIPs")

		masterHost := rapid.OneOf(
			rapid.SampledFrom(greenIPs),                           // in set
			rapid.StringMatching(`10\.0\.[0-9]{1,3}\.[0-9]{1,3}`), // not in set
			rapid.Just(""), // blank (replica during failover)
		).Draw(rt, "masterHost")

		text := fmt.Sprintf(
			"role:slave\nmaster_host:%s\nmaster_port:6379\nmaster_link_status:up\n",
			masterHost,
		)

		bg := &cachev1.BlueGreenStatus{}
		for _, ip := range greenIPs {
			bg.ShardMapping = append(bg.ShardMapping, cachev1.ShardMappingEntry{GreenPodIP: ip})
		}

		got, _, _ := isSlaveOfGreenPod(text, bg, "")
		want := masterHost != "" && slices.Contains(greenIPs, masterHost)

		if got != want {
			rt.Fatalf("isSlaveOfGreenPod = %v; want %v\nmasterHost=%q greenIPs=%v",
				got, want, masterHost, greenIPs)
		}
	})
}
