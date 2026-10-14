// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseClusterNodesFromBackfillsOnlyEmptySelfAddress(t *testing.T) {
	out := "" +
		"self-id :6379@16379 myself,master - 0 0 1 connected 0-10\n" +
		"empty-peer :6379@16379 master - 0 0 2 connected\n" +
		"addressed-peer 10.0.0.9:6379@16379 master - 0 0 3 connected"

	nodes := parseClusterNodesFrom(out, "10.0.0.7")

	require.Len(t, nodes, 3)
	require.Equal(t, "10.0.0.7", nodes[0].IP)
	require.Empty(t, nodes[1].IP)
	require.Equal(t, "10.0.0.9", nodes[2].IP)
}

func TestParseClusterNodesFromPreservesExplicitSelfAddress(t *testing.T) {
	out := "self-id 10.96.0.42:6379@16379 myself,master - 0 0 1 connected 0-10"

	nodes := parseClusterNodesFrom(out, "10.0.0.7")

	require.Len(t, nodes, 1)
	require.Equal(t, "10.96.0.42", nodes[0].IP)
}
