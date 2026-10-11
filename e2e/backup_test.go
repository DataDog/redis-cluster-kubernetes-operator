//go:build kind_e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/require"
)

const (
	backupBucket          = "rco-e2e-backups"
	backupFrequencySecond = int64(1)
	// restoredClusterName is the restore-target cluster (distinct name → fresh PVCs).
	restoredClusterName = "redis-cluster-restored"
	// restoreDataPrefix namespaces the keys the restore round-trip seeds and verifies.
	restoreDataPrefix = "restore-data"
)

// TestE2E_Backup_RotatePreviousBackup_RealS3 runs the operator's S3 backup path
// against an in-namespace MinIO and asserts the rotation layout: after two
// backups, both <namespace>/<name>/latest and <namespace>/<name>/previous
// prefixes exist in the bucket.
func TestE2E_Backup_RotatePreviousBackup_RealS3(t *testing.T) {
	s := rckind.NewSuite(t)
	backupRoot := s.Namespace + "/" + clusterName
	deployMinIO(t, s, backupBucket)
	// BACKUP_BUCKET is set process-wide in TestMain (before the shared operator
	// starts) to avoid the t.Setenv-vs-shared-operator race.

	rc := rckind.NewRedisCluster(clusterName, s.Namespace,
		redisbuilder.WithReplicas(0),
		rckind.WithAppendOnly(false),
		redisbuilder.WithBackupEnabled(),
		rckind.WithBackupFrequency(backupFrequencySecond),
		rckind.WithS3BackupSidecar(awsCliImage(), minIOEndpoint(), minIOAccessKey, minIOSecretKey),
	)
	s.CreateCluster(t, rc, clusterReadyTimeout)

	first := waitForBackupAfter(t, s, 0)
	s.EventuallyT(t, "next backup second elapsed", clusterReadyTimeout, func() bool {
		return time.Now().Unix() > first+backupFrequencySecond
	})
	s.TriggerReconcile(t, clusterName, scaleConvergeTimeout)
	waitForBackupAfter(t, s, first)

	s.EventuallyT(t, "latest backup prefix exists", clusterReadyTimeout, func() bool {
		return s3PrefixExists(t, s, backupRoot+"/latest")
	})
	s.EventuallyT(t, "previous backup prefix exists", clusterReadyTimeout, func() bool {
		return s3PrefixExists(t, s, backupRoot+"/previous")
	})

	backupNodeCount := s.GetCluster(t, clusterName).Status.LastBackupNodeCount
	require.Positive(t, backupNodeCount)
	for _, generation := range []string{"latest", "previous"} {
		prefix := backupRoot + "/" + generation
		keys, ok := s3PrefixKeys(t, s, prefix)
		require.True(t, ok, "failed to list %s", prefix)
		require.ElementsMatch(t, expectedBackupKeys(prefix, backupNodeCount), keys,
			"%s must contain direct per-node objects without a nested in-progress directory", prefix)
	}
	inProgressKeys, ok := s3PrefixKeys(t, s, backupRoot+"/in-progress")
	require.True(t, ok, "failed to list in-progress prefix")
	require.Empty(t, inProgressKeys, "completed promotion must remove the standalone in-progress backup")
}

// waitForBackupAfter blocks until the CR reports a LastBackupTime newer than
// previous with the canonical latest path, returning the new backup time.
func waitForBackupAfter(t *testing.T, s *rckind.Suite, previous int64) int64 {
	t.Helper()
	var seen int64
	s.EventuallyT(t, "backup completed", clusterReadyTimeout, func() bool {
		rc := s.GetCluster(t, clusterName)
		seen = rc.Status.LastBackupTime
		return seen > previous && rc.Status.LastBackupPath == s.Namespace+"/"+clusterName+"/latest"
	})
	return seen
}

// s3PrefixExists reports whether the bucket prefix holds ≥1 object, via aws-cli
// in the backup sidecar of a redis pod.
func s3PrefixExists(t *testing.T, s *rckind.Suite, prefix string) bool {
	t.Helper()
	keys, ok := s3PrefixKeys(t, s, prefix)
	return ok && len(keys) > 0
}

// s3PrefixKeys lists every object directly or recursively under prefix.
func s3PrefixKeys(t *testing.T, s *rckind.Suite, prefix string) ([]string, bool) {
	t.Helper()
	pods := s.RedisPods(t, clusterName)
	if len(pods) == 0 {
		return nil, false
	}
	cmd := fmt.Sprintf(
		"aws ${BACKUP_S3_ENDPOINT_URL:+--endpoint-url $BACKUP_S3_ENDPOINT_URL} s3api list-objects-v2 --bucket %q --prefix %q --query 'Contents[].Key' --output json",
		backupBucket, prefix+"/")
	rc, stdout, _, err := s.ExecOnPod(s.Ctx, pods[0], "backup", []string{"sh", "-c", cmd})
	if err != nil || rc != 0 {
		return nil, false
	}

	var keys []string
	if err := json.Unmarshal([]byte(stdout), &keys); err != nil {
		return nil, false
	}
	return keys, true
}

func expectedBackupKeys(prefix string, nodeCount int) []string {
	keys := []string{prefix + "/manifest.json"}
	for i := 0; i < nodeCount; i++ {
		keys = append(keys,
			fmt.Sprintf("%s/%d/dump.rdb", prefix, i),
			fmt.Sprintf("%s/%d/nodes.conf", prefix, i),
		)
	}
	return keys
}

// TestE2E_BackupRestore is the disaster-recovery round-trip: seed a cluster, back
// it up to S3, then bring up a second cluster from RestoreFrom and assert every
// key and all hash slots came back. Exercises restoreNodes + slot re-own from the
// backup manifest — the guarantee the backup machinery exists to uphold.
func TestE2E_BackupRestore(t *testing.T) {
	requireSlow(t, "slow: two cluster formations + a backup round-trip")
	s := rckind.NewSuite(t)
	deployMinIO(t, s, backupBucket)

	// Source cluster: seed keys, then take a backup whose BGSAVE runs after the seed.
	src := rckind.NewRedisCluster(clusterName, s.Namespace,
		redisbuilder.WithReplicas(0),
		rckind.WithAppendOnly(false),
		redisbuilder.WithBackupEnabled(),
		rckind.WithBackupFrequency(backupFrequencySecond),
		rckind.WithS3BackupSidecar(awsCliImage(), minIOEndpoint(), minIOAccessKey, minIOSecretKey),
	)
	s.CreateCluster(t, src, clusterReadyTimeout)

	s.WriteStringData(t, restoreDataPrefix, dataIntegrityKeyCount, dataReadRetry)
	// Two backup cycles so a BGSAVE that began strictly after the seed (capturing
	// every key) lands in <namespace>/<name>/latest, not one that was mid-flight during it.
	b1 := waitForBackupAfter(t, s, s.GetCluster(t, clusterName).Status.LastBackupTime)
	waitForBackupAfter(t, s, b1)

	// Tear the source down so the restored cluster is the only one the redis-cli
	// helpers reach, and so the target forms with fresh data volumes (no on-disk
	// RDB masking the S3 restore).
	s.DeleteClusterAndWait(t, clusterName, gcAfterDeleteTimeout)

	// Restored cluster (distinct name → fresh PVCs): seeded purely from S3.
	dst := rckind.NewRedisCluster(restoredClusterName, s.Namespace,
		redisbuilder.WithReplicas(0),
		rckind.WithAppendOnly(false),
		rckind.WithS3BackupSidecar(awsCliImage(), minIOEndpoint(), minIOAccessKey, minIOSecretKey),
		redisbuilder.WithRestoreFrom(s.Namespace+"/"+clusterName+"/latest"),
	)
	s.CreateCluster(t, dst, clusterReadyTimeout)

	s.RequireStringData(t, restoreDataPrefix, dataIntegrityKeyCount, dataReadRetry)
	s.RequireSlotCoverage(t, restoredClusterName, topologySettleTimeout)
}
