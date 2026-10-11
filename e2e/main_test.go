//go:build kind_e2e

package e2e

import (
	"os"
	"testing"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clusterName is the RedisCluster name the e2e CRs use.
const clusterName = "redis-cluster-tester"

// clusterObjectKey is the key of the suite's RedisCluster CR.
func clusterObjectKey(s *rckind.Suite) client.ObjectKey {
	return client.ObjectKey{Namespace: s.Namespace, Name: clusterName}
}

// TestMain tears down the shared KIND cluster after the suite. Set
// KIND_E2E_VERBOSE=1 to retain controller and client logs while debugging.
func TestMain(m *testing.M) {
	// Set BACKUP_BUCKET process-wide before the shared operator starts. Per-test
	// t.Setenv races the shared operator goroutine's os.Getenv; this never unsets,
	// and only backup-enabled CRs read it, so non-backup tests are unaffected.
	_ = os.Setenv("BACKUP_BUCKET", backupBucket)
	code := m.Run()
	rckind.StopShared()
	os.Exit(code)
}
