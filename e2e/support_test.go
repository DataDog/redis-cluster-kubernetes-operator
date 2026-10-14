// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//go:build kind_e2e

package e2e

import (
	"os"
	"testing"
	"time"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
)

const (
	dataIntegrityKeyCount  = 100
	topologySettleTimeout  = 3 * time.Minute
	dataReadRetry          = 3 * time.Minute
	replicationWriteBatch  = 50
	replicationPollTimeout = 60 * time.Second
	gcAfterDeleteTimeout   = 2 * time.Minute
	failoverTimeout        = 5 * time.Minute
	scaleConvergeTimeout   = 10 * time.Minute
	clusterReadyTimeout    = 5 * time.Minute
	bgConvergeTimeout      = 15 * time.Minute

	scaleTargetPrimaries = int32(4)

	loadgenFaultMinSuccess = 0.90
	blueGreenMinSuccess    = 0.95
	loadgenWarmupTimeout   = time.Minute
)

func newCluster(t *testing.T, opts ...redisbuilder.ClusterOpt) *rckind.Suite {
	t.Helper()
	s := rckind.NewSuite(t)
	s.CreateCluster(t, rckind.NewRedisCluster(clusterName, s.Namespace, opts...), clusterReadyTimeout)
	return s
}

func requireSlow(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("E2E_SLOW") == "" {
		t.Skipf("set E2E_SLOW=1 to run: %s", reason)
	}
}

func requireLoadgen(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_LOADGEN") == "" {
		t.Skip("set E2E_LOADGEN=1 and load the loadgen image to run QPS tests")
	}
}
