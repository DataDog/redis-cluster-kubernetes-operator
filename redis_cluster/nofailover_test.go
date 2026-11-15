// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// With cluster-replica-no-failover yes, Redis adds "nofailover" to every
// node's flags, masters included. A healthy node must keep its role.
func TestDetermineRoleIgnoresNofailover(t *testing.T) {
	cases := []struct {
		flags string
		role  NodeRole
	}{
		{"master,nofailover", PrimaryNode},
		{"myself,master,nofailover", PrimaryNode},
		{"slave,nofailover", ReplicaNode},
		{"myself,slave,nofailover", ReplicaNode},
		// Real failure flags still win.
		{"master,nofailover,fail", FailingNode},
		{"slave,nofailover,fail?", FailingNode},
	}
	for _, tc := range cases {
		t.Run(tc.flags, func(t *testing.T) {
			node := &ClusterNode{
				PodIndex: 0,
				IP:       "10.0.0.1",
				ID:       "abc",
				Flags:    strings.Split(tc.flags, ","),
				Info:     NodeInfo{KnownNodes: 6, Size: 3},
			}
			assert.Equal(t, tc.role, node.determineRole())
		})
	}
}

// "nofailover" must never change the classified role.
func TestProperty_DetermineRole_IgnoresNofailover(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		flags := genFlagSet().Draw(rt, "flags")
		info := genNodeInfo().Draw(rt, "info")

		without := &ClusterNode{PodIndex: 0, IP: "10.0.0.1", ID: "abc", Flags: flags, Info: info}
		with := &ClusterNode{PodIndex: 0, IP: "10.0.0.1", ID: "abc", Flags: append(append([]string{}, flags...), "nofailover"), Info: info}

		if without.determineRole() != with.determineRole() {
			rt.Fatalf("role differs with nofailover: without=%v with=%v flags=%v",
				without.determineRole(), with.determineRole(), flags)
		}
	})
}
