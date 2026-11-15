// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package bluegreen_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/redis-cluster-kubernetes-operator/bluegreen"
)

func TestParseClusterNodesOutput(t *testing.T) {
	const (
		idA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		idB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		idC = "cccccccccccccccccccccccccccccccccccccccc"
	)
	lines := func(l ...string) string { return strings.Join(l, "\n") + "\n" }

	cases := []struct {
		name      string
		nodes     string
		wantIPs   map[string]string
		wantSlots map[string]string
	}{
		{
			name: "IPv4 ranges, hostname suffix, migration markers and empty myself IP",
			nodes: lines(
				idA+" :6379@16379 myself,master - 0 0 1 connected 0-5460",
				idB+" 10.0.0.2:6379@16379,cache-1 master - 0 0 2 connected 5461-10922 [5461->-"+idC+"]",
				idC+" 10.0.0.3:6379@16379 slave "+idB+" 0 0 2 connected",
			),
			wantIPs:   map[string]string{"10.0.0.2": idB, "10.0.0.3": idC},
			wantSlots: map[string]string{"5461-10922": "10.0.0.2"},
		},
		{
			// Every unbracketed IPv6 address used to collapse to "2001".
			name: "IPv6 addresses",
			nodes: lines(
				idA+" 2001:db8::10:6379@16379 myself,master - 0 0 1 connected 0-8191",
				idB+" 2001:db8::11:6379@16379 master - 0 0 2 connected 8192-16383",
				idC+" 2001:db8::12:6379@16379 slave "+idA+" 0 0 1 connected",
			),
			wantIPs:   map[string]string{"2001:db8::10": idA, "2001:db8::11": idB, "2001:db8::12": idC},
			wantSlots: map[string]string{"0-8191": "2001:db8::10", "8192-16383": "2001:db8::11"},
		},
		{
			// Single-slot tokens used to be dropped; split ranges are also keyed
			// by their comma-joined form, matching ShardMappingEntry.SlotsRanges.
			name: "single slot and split ranges",
			nodes: lines(
				idA+" 10.0.0.1:6379@16379 master - 0 0 1 connected 100",
				idB+" 10.0.0.2:6379@16379 master - 0 0 2 connected 0-99 101-200",
			),
			wantIPs: map[string]string{"10.0.0.1": idA, "10.0.0.2": idB},
			wantSlots: map[string]string{
				"100":          "10.0.0.1",
				"0-99":         "10.0.0.2",
				"101-200":      "10.0.0.2",
				"0-99,101-200": "10.0.0.2",
			},
		},
		{
			// "nofailover" contains "fail" but marks a healthy node.
			name: "nofailover owner is healthy, fail and fail? owners are not",
			nodes: lines(
				idA+" 10.0.0.1:6379@16379 myself,master,nofailover - 0 0 1 connected 0-5460",
				idB+" 10.0.0.2:6379@16379 master,fail - 0 0 2 connected 5461-10922",
				idC+" 10.0.0.3:6379@16379 master,fail? - 0 0 3 connected 10923-16383",
			),
			wantIPs:   map[string]string{"10.0.0.1": idA, "10.0.0.2": idB, "10.0.0.3": idC},
			wantSlots: map[string]string{"0-5460": "10.0.0.1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ipToNodeID, slotRangeToPrimaryIP := bluegreen.ParseClusterNodesOutput(tc.nodes)
			assert.Equal(t, tc.wantIPs, ipToNodeID)
			assert.Equal(t, tc.wantSlots, slotRangeToPrimaryIP)
		})
	}
}
