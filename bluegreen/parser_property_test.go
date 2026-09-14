package bluegreen_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/bluegreen"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/property"
	"pgregory.net/rapid"
)

// Drives ParseClusterNodesOutput with arbitrary realistic CLUSTER NODES
// output generated from a coordinated ClusterModel. Asserts the
// structural invariants the operator depends on; failures shrink to a
// minimal counter-example courtesy of rapid.
//
// Bug classes caught:
//   - slot-owner IP missing from ipToNodeID (data-corruption family)
//   - failed primaries treated as slot owners (was a real promotion bug)
//   - importing/migrating `[slot-<-NODE]` syntax leaking into slot keys
//   - empty-IP "myself" line (no cluster-announce-ip) mapped under "" — the
//     empty-announcement hazard where the querying node is misclassified
func TestProperty_ParseClusterNodes_StructuralInvariants(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := property.GenClusterModel().Draw(rt, "model")
		viewer := m.Pods[rapid.IntRange(0, len(m.Pods)-1).Draw(rt, "viewerIdx")]
		text := m.RenderClusterNodes(viewer)

		ipToNodeID, slotRangeToPrimaryIP := bluegreen.ParseClusterNodesOutput(text)

		// When cluster-announce-ip is unset, the viewer's own line has an empty
		// IP and is legitimately absent from the parsed maps.
		viewerEmptyIP := func(p property.Pod) bool {
			return m.NoAnnounceIP && p.NodeID == viewer.NodeID
		}

		// An empty IP must never become a map key/value — that is the bug.
		if _, ok := ipToNodeID[""]; ok {
			rt.Fatalf("empty IP leaked into ipToNodeID (no cluster-announce-ip myself line)")
		}

		for _, p := range m.Pods {
			if viewerEmptyIP(p) {
				if _, ok := ipToNodeID[p.IP]; ok && p.IP == "" {
					rt.Fatalf("empty-IP viewer %s should not be in ipToNodeID", p.Name)
				}
				continue
			}
			got, ok := ipToNodeID[p.IP]
			if !ok {
				rt.Fatalf("ipToNodeID missing pod %s (IP %s)", p.Name, p.IP)
			}
			if got != p.NodeID {
				rt.Fatalf("ipToNodeID[%s] = %q; want %q (pod %s)", p.IP, got, p.NodeID, p.Name)
			}
		}

		for _, p := range m.HealthyPrimaries() {
			if viewerEmptyIP(p) {
				continue // its line was skipped; slots aren't attributable to an empty IP
			}
			key := fmt.Sprintf("%d-%d", p.SlotsFirst, p.SlotsLast)
			got, ok := slotRangeToPrimaryIP[key]
			if !ok {
				rt.Fatalf("slotRangeToPrimaryIP missing range %s for primary %s", key, p.Name)
			}
			if got != p.IP {
				rt.Fatalf("slotRangeToPrimaryIP[%s] = %s; want %s", key, got, p.IP)
			}
		}

		for _, p := range m.Pods {
			if !p.IsPrimary || !slices.Contains(p.Flags, "fail") {
				continue
			}
			for _, ip := range slotRangeToPrimaryIP {
				if ip == p.IP {
					rt.Fatalf("failed primary %s leaked into slotRangeToPrimaryIP", p.Name)
				}
			}
		}

		for k := range slotRangeToPrimaryIP {
			if strings.HasPrefix(k, "[") {
				rt.Fatalf("migration fragment %q leaked into slotRangeToPrimaryIP", k)
			}
		}

		for k, ip := range slotRangeToPrimaryIP {
			if _, ok := ipToNodeID[ip]; !ok {
				rt.Fatalf("slot range %s owned by %s which is absent from ipToNodeID", k, ip)
			}
		}
	})
}
