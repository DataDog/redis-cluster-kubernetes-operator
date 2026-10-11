package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func clusterWithSpec(spec cachev1.RedisClusterSpec) *redis_cluster.Cluster {
	return &redis_cluster.Cluster{RedisCluster: &cachev1.RedisCluster{Spec: spec}}
}

func TestWaitForBackupPollStopsWhenReconciliationIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err := waitForBackupPoll(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(started), time.Second)
}

func TestRedisInfoString(t *testing.T) {
	info := "# Server\r\nredis_version:8.6.0\r\nrun_id:abc123\r\n"
	assert.Equal(t, "abc123", redisInfoString(info, "run_id"))
	assert.Empty(t, redisInfoString(info, "missing"))
}

func TestBackupBucketIsNotSanitizedIntoAnotherResource(t *testing.T) {
	for _, value := range []string{"s3://my-bucket", "gs://my-bucket", "my-bucket/", " my-bucket "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("BACKUP_BUCKET", value)
			r := &RedisClusterReconciler{}
			_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "s3"}))
			require.Error(t, err)
		})
	}
}

func TestBackupBucketUnsetReturnsEmpty(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "")
	assert.Empty(t, (&RedisClusterReconciler{}).backupBucket(clusterWithSpec(cachev1.RedisClusterSpec{})))
}

func TestBackupBackendSelection(t *testing.T) {
	for _, tc := range []struct {
		kind, bucket string
		typ          any
	}{
		{"", "my-bucket", &s3Backend{}}, {"s3", "my-bucket", &s3Backend{}}, {"gcs", "my-bucket", &gcsBackend{}}, {"azure", "my-container", &azureBackend{}},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Setenv("BACKUP_BUCKET", tc.bucket)
			backend, err := (&RedisClusterReconciler{}).backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: tc.kind}))
			require.NoError(t, err)
			assert.IsType(t, tc.typ, backend)
		})
	}
}

func TestBackupBackendRejectsMissingAndUnknown(t *testing.T) {
	r := &RedisClusterReconciler{}
	t.Setenv("BACKUP_BUCKET", "")
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "s3"}))
	require.Error(t, err)
	t.Setenv("BACKUP_BUCKET", "my-bucket")
	_, err = r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "nfs"}))
	require.Error(t, err)
}

func TestBackupContainerSelection(t *testing.T) {
	r := &RedisClusterReconciler{}
	withSidecar := cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis"}, {Name: "backup"}}}}}}}
	without := cachev1.RedisClusterSpec{StatefulsetTemplate: appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "redis"}}}}}}}
	assert.Equal(t, "backup", r.backupContainer(clusterWithSpec(withSidecar)))
	assert.Equal(t, "redis", r.backupContainer(clusterWithSpec(without)))
}

func TestBackupPathsDistinguishLatestAndPrevious(t *testing.T) {
	r := &RedisClusterReconciler{}
	c := &redis_cluster.Cluster{RedisCluster: &cachev1.RedisCluster{}, StatefulSet: &appsv1.StatefulSet{}}
	c.RedisCluster.Namespace = "cache-team"
	c.RedisCluster.Name = "rco-test"
	assert.Equal(t, "cache-team/rco-test/latest", r.latestBackupPath(c))
	assert.Equal(t, "cache-team/rco-test/previous", r.previousBackupPath(c))
	assert.Equal(t, "cache-team/rco-test/in-progress", r.inProgressBackupPath(c))

	otherNamespace := &redis_cluster.Cluster{RedisCluster: c.RedisCluster.DeepCopy(), StatefulSet: &appsv1.StatefulSet{}}
	otherNamespace.RedisCluster.Namespace = "other-team"
	assert.NotEqual(t, r.latestBackupPath(c), r.latestBackupPath(otherNamespace))
}

type promotionStore struct {
	calls     []string
	failures  map[string]error
	exists    bool
	existsErr error
}

func (s *promotionStore) UploadFile(context.Context, string, string) error      { return nil }
func (s *promotionStore) DownloadFile(context.Context, string, string) error    { return nil }
func (s *promotionStore) UploadBytes(context.Context, []byte, string) error     { return nil }
func (s *promotionStore) DownloadBytes(context.Context, string) ([]byte, error) { return nil, nil }
func (s *promotionStore) CopyPrefix(_ context.Context, src, dst string) error {
	key := "copy:" + src + ":" + dst
	s.calls = append(s.calls, key)
	return s.failures[key]
}
func (s *promotionStore) DeletePrefix(_ context.Context, p string) error {
	key := "delete:" + p
	s.calls = append(s.calls, key)
	return s.failures[key]
}
func (s *promotionStore) Exists(context.Context, string) (bool, error) { return s.exists, s.existsErr }

func TestReplaceLatestPropagatesPromotionAndRollbackFailures(t *testing.T) {
	promotionErr := errors.New("promotion failed")
	rollbackErr := errors.New("rollback failed")
	store := &promotionStore{failures: map[string]error{
		"copy:cluster/in-progress:cluster/latest": promotionErr,
		"copy:cluster/previous:cluster/latest":    rollbackErr,
	}}
	err := replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", true)
	require.Error(t, err)
	assert.ErrorIs(t, err, promotionErr)
	assert.Contains(t, err.Error(), "rollback failed")
	assert.NotContains(t, store.calls, "delete:cluster/in-progress")
}

func TestReplaceLatestPropagatesCleanupFailure(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	store := &promotionStore{failures: map[string]error{"delete:cluster/in-progress": cleanupErr}}
	err := replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", false)
	require.ErrorIs(t, err, cleanupErr)
}

func TestReplaceLatestSuccessCleansInProgress(t *testing.T) {
	store := &promotionStore{failures: map[string]error{}}
	require.NoError(t, replaceLatestWithInProgress(context.Background(), store, "cluster/in-progress", "cluster/latest", "cluster/previous", false))
	assert.Contains(t, store.calls, "delete:cluster/in-progress")
}

func TestConfirmedNotFoundIsTheOnlyAbsenceCategory(t *testing.T) {
	notFound := &BackendError{Kind: BackendNotFound, Operation: "exists"}
	auth := &BackendError{Kind: BackendAuthentication, Operation: "exists"}
	assert.True(t, IsBackendNotFound(notFound))
	assert.False(t, IsBackendNotFound(auth))
}

func TestRotateLatestFirstBackupDoesNotCopyAbsentLatest(t *testing.T) {
	store := &promotionStore{failures: map[string]error{}, exists: false}
	rotated, err := rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.NoError(t, err)
	assert.False(t, rotated)
	assert.Empty(t, store.calls)
}

func TestRotateLatestPropagatesProbeAndRotationFailures(t *testing.T) {
	authErr := &BackendError{Kind: BackendAuthentication, Operation: "exists"}
	store := &promotionStore{failures: map[string]error{}, existsErr: authErr}
	_, err := rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.ErrorIs(t, err, authErr)
	assert.Empty(t, store.calls)

	copyErr := errors.New("rotation copy failed")
	store = &promotionStore{exists: true, failures: map[string]error{"copy:cluster/latest:cluster/previous": copyErr}}
	_, err = rotateLatestToPrevious(context.Background(), store, "cluster/latest", "cluster/previous")
	require.ErrorIs(t, err, copyErr)
}
