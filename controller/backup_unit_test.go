package controller

import (
	"context"
	"errors"
	"testing"

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

func TestBackupLocationUnsetReturnsEmpty(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "")
	assert.Empty(t, (&RedisClusterReconciler{}).backupLocation())
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

func TestBackupBackendUsesInjectedFactory(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "")
	var gotProvider, gotLocation string
	r := &RedisClusterReconciler{BackupBackendFactory: BackupBackendFactoryFunc(func(provider, location string) (BackupProvider, error) {
		gotProvider, gotLocation = provider, location
		return stubBackupBackend{}, nil
	})}

	backend, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.NoError(t, err)
	assert.IsType(t, stubBackupBackend{}, backend)
	assert.Equal(t, "custom", gotProvider)
	assert.Empty(t, gotLocation)
}

func TestBackupBackendRejectsTypedNilFactory(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "provider-location")
	var factory *BackupBackendRegistry
	r := &RedisClusterReconciler{BackupBackendFactory: factory}
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.ErrorContains(t, err, "factory is nil")
}

func TestBackupBackendRejectsNilFactoryResult(t *testing.T) {
	t.Setenv("BACKUP_BUCKET", "provider-location")
	r := &RedisClusterReconciler{BackupBackendFactory: BackupBackendFactoryFunc(func(string, string) (BackupProvider, error) {
		return nil, nil
	})}
	_, err := r.backupBackend(clusterWithSpec(cachev1.RedisClusterSpec{BackupBackend: "custom"}))
	require.ErrorContains(t, err, "returned a nil backend")
}

type backupProviderFunc func(BackendCommandExecutor) BackupStore

func (f backupProviderFunc) Bind(executor BackendCommandExecutor) BackupStore {
	return f(executor)
}

func TestBindBackupStoreRejectsNilStore(t *testing.T) {
	var typedNil *promotionStore
	tests := []struct {
		name     string
		provider BackupProvider
	}{
		{
			name: "nil",
			provider: backupProviderFunc(func(BackendCommandExecutor) BackupStore {
				return nil
			}),
		},
		{
			name: "typed nil",
			provider: backupProviderFunc(func(BackendCommandExecutor) BackupStore {
				return typedNil
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store, err := (&RedisClusterReconciler{}).bindBackupStore(
				clusterWithSpec(cachev1.RedisClusterSpec{}),
				tc.provider,
				&redis_cluster.ClusterNode{PodName: "redis-0"},
			)
			require.ErrorContains(t, err, "returned a nil store")
			assert.Nil(t, store)
		})
	}
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
	c.RedisCluster.Name = "rco-test"
	assert.NotEqual(t, r.latestBackupPath(c), r.previousBackupPath(c))
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
