// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/toolbox"
	"github.com/stretchr/testify/assert"
)

// An unreachable node (exec never answers) must not stall the reconcile: a
// terminating pod once blocked maintenance for minutes, past the FORGET ban.
func TestToolbox_PingIPIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stub := toolbox.New().OnRedisCommand("PING", toolbox.Response{Stdout: "PONG"}).DelayNext(time.Hour)
		tb := newToolboxWithStub(stub)

		start := time.Now()
		assert.False(t, tb.PingIP("10.0.0.1"))
		assert.Equal(t, clusterViewPerNodeTimeout, time.Since(start))
	})
}
