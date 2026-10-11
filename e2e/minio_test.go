//go:build kind_e2e

package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	rckind "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// In-namespace MinIO S3 backend for the backup/restore tests — no real S3 or AWS
// creds needed. The operator's backup sidecar points at MinIOEndpoint() through
// BACKUP_S3_ENDPOINT_URL with the fixed minioadmin credentials.

const (
	minIOName      = "minio"
	minIOAccessKey = "minioadmin"
	minIOSecretKey = "minioadmin"
	// minioBucketTimeout bounds the create-bucket Job reporting success.
	minioBucketTimeout = 2 * time.Minute
)

// minioImage / awsCliImage default to public pinned images and can be
// overridden for mirrors or controlled image updates.
func minioImage() string {
	return envOr("E2E_MINIO_IMAGE", "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e")
}
func awsCliImage() string { return envOr("E2E_AWS_CLI_IMAGE", "amazon/aws-cli:2.15.57") }

func minIOEndpoint() string { return fmt.Sprintf("http://%s:9000", minIOName) }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// deployMinIO stands up a single-replica MinIO + Service and creates the bucket.
func deployMinIO(t *testing.T, s *rckind.Suite, bucket string) {
	t.Helper()
	deployMinIOService(t, s)
	deployMinIOServer(t, s)
	waitMinIOReady(t, s)
	createMinIOBucket(t, s, bucket)
}

func deployMinIOService(t *testing.T, s *rckind.Suite) {
	t.Helper()
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: minIOName, Namespace: s.Namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": minIOName},
			Ports:    []corev1.ServicePort{{Name: "s3", Port: 9000, TargetPort: intstr.FromString("s3")}},
		},
	}
	if err := s.Client.Create(s.Ctx, svc); err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
}

func deployMinIOServer(t *testing.T, s *rckind.Suite) {
	t.Helper()
	replicas := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: minIOName, Namespace: s.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": minIOName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": minIOName}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  minIOName,
						Image: minioImage(),
						Args:  []string{"server", "/data"},
						Env: []corev1.EnvVar{
							{Name: "MINIO_ROOT_USER", Value: minIOAccessKey},
							{Name: "MINIO_ROOT_PASSWORD", Value: minIOSecretKey},
						},
						Ports: []corev1.ContainerPort{{Name: "s3", ContainerPort: 9000}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Path: "/minio/health/ready", Port: intstr.FromString("s3")},
							},
							PeriodSeconds: 1,
						},
						VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
				},
			},
		},
	}
	if err := s.Client.Create(s.Ctx, dep); err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
}

func createMinIOBucket(t *testing.T, s *rckind.Suite, bucket string) {
	t.Helper()
	backoff := int32(3)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "minio-create-bucket", Namespace: s.Namespace},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:    "aws",
						Image:   awsCliImage(),
						Command: []string{"sh", "-c"},
						Args:    []string{fmt.Sprintf("aws --endpoint-url %s s3 mb s3://%s", minIOEndpoint(), bucket)},
						Env:     s3Env(),
					}},
				},
			},
		},
	}
	if err := s.Client.Create(s.Ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
	s.EventuallyT(t, "minio bucket created", minioBucketTimeout, func() bool {
		current := &batchv1.Job{}
		if err := s.Client.Get(s.Ctx, types.NamespacedName{Name: job.Name, Namespace: s.Namespace}, current); err != nil {
			return false
		}
		return current.Status.Succeeded > 0
	})
}

func waitMinIOReady(t *testing.T, s *rckind.Suite) {
	t.Helper()
	s.EventuallyT(t, "minio ready", clusterReadyTimeout, func() bool {
		dep := &appsv1.Deployment{}
		if err := s.Client.Get(s.Ctx, types.NamespacedName{Name: minIOName, Namespace: s.Namespace}, dep); err != nil {
			return false
		}
		return dep.Status.AvailableReplicas >= 1
	})
}

func s3Env() []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "AWS_ACCESS_KEY_ID", Value: minIOAccessKey},
		{Name: "AWS_SECRET_ACCESS_KEY", Value: minIOSecretKey},
		{Name: "AWS_DEFAULT_REGION", Value: "us-east-1"},
		{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"},
		{Name: "BACKUP_S3_ENDPOINT_URL", Value: minIOEndpoint()},
	}
}
