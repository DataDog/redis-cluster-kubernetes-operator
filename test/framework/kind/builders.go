package kind

import (
	"fmt"
	"os"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// defaultRedisImage has no fallback; NewSuite fails fast when it is unset.
var defaultRedisImage = func() string {
	if v := os.Getenv("KIND_E2E_REDIS_IMAGE"); v != "" {
		return v
	}
	return os.Getenv("E2E_REDIS_IMAGE")
}()

// Redis ports for the headless Service the suite deploys; gossip is client + 10000.
const (
	redisPort  = redisbuilder.RedisPort
	gossipPort = redisbuilder.GossipPort
)

// toolboxPodName is the fixed name of the toolbox pod the suite deploys and the
// reconciler execs into; the operator finds it by label, the name is for waits.
const toolboxPodName = "toolbox"

const (
	// dataVolumeName is the per-pod data dir, a hostPath subdir keyed by pod name,
	// so nodes.conf + AOF/RDB survive the pod recreation scale-up triggers.
	dataVolumeName     = "data"
	dataHostPathPrefix = "/var/local/rco-e2e"
)

// fixtureDataGID is added as a supplemental group to every fixture container,
// allowing public Redis images with different runtime UIDs to share /data.
const fixtureDataGID = 1000

const (
	// chartVersionLabel is the label the operator diffs between the CR and its
	// StatefulSet to decide a blue-green rollout is due (controller/reconcile_utils.go).
	chartVersionLabel = "chart_version"
	// initialChartVersion is the version the freshly-built CR + STS agree on, so
	// the cluster bootstraps without a blue-green until a test bumps it.
	initialChartVersion = "e2e-initial"
)

// NewRedisCluster returns a RedisCluster CR ready for `Create` on KIND: the
// resolved chart redis image, 3 primaries × 1 replica, with
// the selector/template labels the operator needs to own its StatefulSet. Layers the
// KIND-specific fields (chart_version, data volume, OnDelete) onto the shared
// redisbuilder base (the Helm chart bakes these in production).
func NewRedisCluster(name, namespace string, opts ...redisbuilder.ClusterOpt) *cachev1.RedisCluster {
	rc := redisbuilder.NewBase(name, namespace, defaultRedisImage)
	// chart_version on the CR + STS so the operator's CR-vs-STS version diff starts matched.
	rc.Labels = map[string]string{chartVersionLabel: initialChartVersion}
	// Seed the annotation shouldReconcile writes, else it panics on a nil map.
	rc.Annotations = map[string]string{"operator/reconcile_count": "0"}
	rc.Spec.StatefulsetTemplate.Labels[chartVersionLabel] = initialChartVersion
	// Role label on the pod template only: the operator toggles it on the template
	// during scale-up, so it must stay out of the immutable selector.
	rc.Spec.StatefulsetTemplate.Spec.Template.Labels[constants.BlueGreenRoleLabel] = constants.BlueGreenRoleActive
	// OnDelete so the operator's scale-up role-label toggle doesn't roll every pod
	// (RollingUpdate would): the churn races the post-scale rebalance and severs
	// replica links. The operator manages pod labels directly, so pods still get
	// role=active without a template-driven recreation.
	rc.Spec.StatefulsetTemplate.Spec.UpdateStrategy = appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}
	attachDataVolume(&rc.Spec.StatefulsetTemplate.Spec, name, namespace)
	for _, opt := range opts {
		opt(rc)
	}
	return rc
}

// WithAppendOnly sets the Redis appendonly flag in the container args.
func WithAppendOnly(enabled bool) redisbuilder.ClusterOpt {
	return func(rc *cachev1.RedisCluster) {
		setRedisArg(&rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0], "--appendonly", redisBool(enabled))
	}
}

// WithBackupFrequency sets the automatic backup cadence in seconds.
func WithBackupFrequency(seconds int64) redisbuilder.ClusterOpt {
	return func(rc *cachev1.RedisCluster) { rc.Spec.BackupFrequency = seconds }
}

// WithS3BackupSidecar adds the operator's S3 backup sidecar pointed at the given
// (in-cluster MinIO) endpoint, mounting the per-pod data PVC at /mnt/redis on the
// redis and backup containers so the sidecar reads redis's on-disk RDB/AOF.
func WithS3BackupSidecar(image, endpoint, accessKey, secretKey string) redisbuilder.ClusterOpt {
	return func(rc *cachev1.RedisCluster) {
		podSpec := &rc.Spec.StatefulsetTemplate.Spec.Template.Spec
		redis := &podSpec.Containers[0]
		// Mount the same per-pod data subdir at /mnt/redis (SubPathExpr matches the
		// /data mount) so the sidecar reads/writes this pod's actual RDB/AOF.
		redis.VolumeMounts = append(redis.VolumeMounts,
			corev1.VolumeMount{Name: dataVolumeName, MountPath: "/mnt/redis", SubPathExpr: "$(POD_NAME)"},
		)
		podSpec.Containers = append(podSpec.Containers, corev1.Container{
			Name:    "backup",
			Image:   image,
			Command: []string{"sh", "-c", "sleep infinity"},
			Env: []corev1.EnvVar{
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				{Name: "HOME", Value: "/tmp"},
				{Name: "AWS_ACCESS_KEY_ID", Value: accessKey},
				{Name: "AWS_SECRET_ACCESS_KEY", Value: secretKey},
				{Name: "AWS_DEFAULT_REGION", Value: "us-east-1"},
				{Name: "AWS_EC2_METADATA_DISABLED", Value: "true"},
				// aws-cli v2 honors AWS_ENDPOINT_URL_S3 — routes the operator's
				// `aws s3 cp` (no --endpoint-url) at the in-cluster MinIO instead of
				// real AWS. Prod (real S3) uses no builder, so it's unaffected.
				{Name: "AWS_ENDPOINT_URL_S3", Value: endpoint},
			},
			VolumeMounts: []corev1.VolumeMount{{Name: dataVolumeName, MountPath: "/mnt/redis", SubPathExpr: "$(POD_NAME)"}},
		})
	}
}

// attachDataVolume gives each pod a stable PVC at /data (KIND's default
// StorageClass), so nodes.conf + on-disk data survive the pod recreation the
// operator triggers on scale-up — without it recreated pods get fresh node IDs
// and the old ones become zombies, exploding the cluster.
func attachDataVolume(sts *appsv1.StatefulSetSpec, name, namespace string) {
	sts.Template.Spec.Volumes = append(sts.Template.Spec.Volumes, corev1.Volume{
		Name: dataVolumeName,
		VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: fmt.Sprintf("%s/%s/%s", dataHostPathPrefix, namespace, name),
			Type: ptr.To(corev1.HostPathDirectoryOrCreate),
		}},
	})
	redis := &sts.Template.Spec.Containers[0]
	redis.Env = append(redis.Env, corev1.EnvVar{
		Name:      "POD_NAME",
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
	})
	dataMount := corev1.VolumeMount{Name: dataVolumeName, MountPath: "/data", SubPathExpr: "$(POD_NAME)"}
	redis.VolumeMounts = append(redis.VolumeMounts, dataMount)
	// HostPath ownership is not normalized by the storage driver. Give every
	// fixture container a shared supplemental group and prepare the directory as
	// group-writable without relying on an image-specific Redis UID.
	dataGID := int64(fixtureDataGID)
	if sts.Template.Spec.SecurityContext == nil {
		sts.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	sts.Template.Spec.SecurityContext.FSGroup = &dataGID
	root := int64(0)
	sts.Template.Spec.InitContainers = append(sts.Template.Spec.InitContainers, corev1.Container{
		Name:            "init-data-perms",
		Image:           defaultRedisImage,
		Command:         []string{"sh", "-c", fmt.Sprintf("chown 0:%d /data && chmod 0770 /data", fixtureDataGID)},
		SecurityContext: &corev1.SecurityContext{RunAsUser: &root},
		Env: []corev1.EnvVar{{
			Name:      "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}},
		}},
		VolumeMounts: []corev1.VolumeMount{dataMount},
	})
}

// setRedisArg sets or replaces a "--key value" pair in the redis container args.
func setRedisArg(c *corev1.Container, key, value string) {
	for i := 0; i < len(c.Args)-1; i++ {
		if c.Args[i] == key {
			c.Args[i+1] = value
			return
		}
	}
	c.Args = append(c.Args, key, value)
}

// redisBool renders a Go bool as a redis-server yes/no flag value.
func redisBool(enabled bool) string {
	if enabled {
		return "yes"
	}
	return "no"
}

func setChartVersion(rc *cachev1.RedisCluster, v string) {
	if rc.Labels == nil {
		rc.Labels = map[string]string{}
	}
	rc.Labels[chartVersionLabel] = v
	if rc.Spec.StatefulsetTemplate.Labels == nil {
		rc.Spec.StatefulsetTemplate.Labels = map[string]string{}
	}
	rc.Spec.StatefulsetTemplate.Labels[chartVersionLabel] = v
}
