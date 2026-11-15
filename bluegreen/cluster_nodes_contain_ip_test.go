// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Replicating used to decide "green pod already in the blue cluster" with a
// substring test on the raw CLUSTER NODES text. Green pod 10.244.1.5 then
// matched a blue node at 10.244.1.53, AddReplica was skipped, and the rollout
// stalled in WaitingSync until the watchdog fired.
func TestClusterNodesContainIPMatchesWholeAddress(t *testing.T) {
	const blueView = "" +
		"1111111111111111111111111111111111111111 10.244.1.53:6379@16379 myself,master - 0 0 1 connected 0-5460\n" +
		"2222222222222222222222222222222222222222 10.244.2.7:6379@16379,cache-1.cache master - 0 0 2 connected 5461-10922\n" +
		"3333333333333333333333333333333333333333 10.244.3.9:6379@16379 master - 0 0 3 connected 10923-16383\n"

	cases := []struct {
		name     string
		nodes    string
		viewerIP string
		ip       string
		want     bool
	}{
		{"prefix of another node's IP", blueView, "10.244.1.53", "10.244.1.5", false},
		{"exact match", blueView, "10.244.1.53", "10.244.3.9", true},
		{"address with hostname suffix", blueView, "10.244.1.53", "10.244.2.7", true},
		{"partial IP", blueView, "10.244.1.53", "10.244.2", false},
		{"myself line without announced IP uses viewer", "4444444444444444444444444444444444444444 :6379@16379 myself,master - 0 0 1 connected\n", "10.244.4.4", "10.244.4.4", true},
		{"empty snapshot", "", "10.244.1.53", "10.244.1.5", false},
		{"empty pod IP", blueView, "10.244.1.53", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, clusterNodesContainIP(tc.nodes, tc.viewerIP, tc.ip))
		})
	}
}
