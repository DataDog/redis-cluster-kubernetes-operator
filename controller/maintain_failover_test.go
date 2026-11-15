// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
)

// A primary pod recreated at a new IP shows up as a zombie (no pod matches the
// new IP) while peers still see it as a connected primary. Forcing its replica
// over in that window lost acknowledged writes in TestMeasure_PrimaryDeleteDuringScaleUp.
func TestRedisReportsPrimaryHealthy(t *testing.T) {
	view := func(linkState string, flags ...string) *redis_cluster.NodeView {
		return &redis_cluster.NodeView{Flags: flags, LinkState: linkState}
	}
	cases := []struct {
		name string
		node *redis_cluster.ClusterNode
		want bool
	}{
		{"recreated primary seen as connected master by all peers",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("connected", "master"), 2: view("connected", "master")}}, true},
		{"dead primary in FAIL",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("disconnected", "master", "fail")}}, false},
		{"one peer reports PFAIL",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("connected", "master"), 2: view("connected", "master", "fail?")}}, false},
		{"link down without flags yet",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("disconnected", "master")}}, false},
		{"handshake",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("connected", "master", "handshake")}}, false},
		{"replica is not a primary",
			&redis_cluster.ClusterNode{Views: map[int]*redis_cluster.NodeView{1: view("connected", "slave")}}, false},
		{"no views falls back to node flags",
			&redis_cluster.ClusterNode{Flags: []string{"master"}, LinkState: "connected"}, true},
		{"no views and no flags",
			&redis_cluster.ClusterNode{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, redisReportsPrimaryHealthy(tc.node))
		})
	}
}
