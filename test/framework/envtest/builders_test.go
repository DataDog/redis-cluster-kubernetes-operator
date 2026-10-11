package envtest_test

import (
	"os"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/envtest"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/assert"
)

// TestNewRedisCluster_Opts covers envtest and shared builder options.
func TestNewRedisCluster_Opts(t *testing.T) {
	rc := envtest.NewRedisCluster("foo",
		envtest.WithNamespace("redis-ns"),
		redisbuilder.WithPrimaries(5),
		redisbuilder.WithReplicas(2),
		redisbuilder.WithImage("redis:7.4.0"),
		redisbuilder.WithBackupEnabled(),
		redisbuilder.WithRestoreFrom("my-bucket/202612181430/"),
	)
	assert.Equal(t, "redis-ns", rc.Namespace)
	assert.Equal(t, "redis-ns", rc.Spec.StatefulsetTemplate.Namespace)
	assert.Equal(t, int32(5), rc.Spec.Primaries)
	assert.Equal(t, int32(2), rc.Spec.Replicas)
	assert.Equal(t, int32(15), *rc.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, "redis:7.4.0", rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image)
	assert.True(t, rc.Spec.BackupEnabled)
	assert.Equal(t, "my-bucket/202612181430/", rc.Spec.RestoreFrom)
}

// TestSetupBackupBucket_SetsProcessEnv confirms the helper exports BACKUP_BUCKET.
func TestSetupBackupBucket_SetsProcessEnv(t *testing.T) {
	envtest.SetupBackupBucket(t, "my-test-bucket")
	assert.Equal(t, "my-test-bucket", os.Getenv("BACKUP_BUCKET"))
}
