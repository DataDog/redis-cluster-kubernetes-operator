// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Package redisbuilder contains RedisCluster builders shared by envtest and KIND.
package redisbuilder

import (
	"fmt"
	"maps"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// DefaultRedisImage is an envtest-only placeholder; the KIND e2e resolves the real image.
const DefaultRedisImage = "redis:8.6.0"

// RedisPort / GossipPort are the cluster-enabled pod ports; gossip is the client
// port + 10000.
const (
	RedisPort  = 6379
	GossipPort = 16379
)

// DefaultPrimaries / DefaultReplicas mirror production: 3 primaries, 1 replica
// each (6 pods).
const (
	DefaultPrimaries = 3
	DefaultReplicas  = 1
)

// ClusterOpt configures a RedisCluster.
type ClusterOpt func(*cachev1.RedisCluster)

// BaseLabels are the app and selector labels used by both test environments.
func BaseLabels(name string) map[string]string {
	return map[string]string{
		"app":                           "rco",
		"chart_name":                    name,
		constants.LabelRedisClusterName: name,
	}
}

// CopyLabels returns an independent copy so selector/template/object label maps
// can be mutated without aliasing one another.
func CopyLabels(src map[string]string) map[string]string {
	out := make(map[string]string, len(src))
	maps.Copy(out, src)
	return out
}

// RedisContainer builds the cluster-enabled redis-server container. Without
// these args the operator's CLUSTER NODES / CLUSTER MEET fail and bootstrap
// never converges.
func RedisContainer(image string) corev1.Container {
	return corev1.Container{
		Name:    constants.RedisContainerName,
		Image:   image,
		Command: []string{"redis-server"},
		Args: []string{
			"--cluster-enabled", "yes",
			"--cluster-config-file", "/data/nodes.conf",
			"--cluster-node-timeout", "5000",
			// Keep AOF + RDB under the writable /data mount; the prod image is
			// non-root, so redis's default dir (cwd) isn't writable.
			"--dir", "/data",
			"--appendonly", "yes",
			"--port", fmt.Sprintf("%d", RedisPort),
			"--bind", "0.0.0.0",
			"--protected-mode", "no",
		},
		Ports: []corev1.ContainerPort{
			{ContainerPort: RedisPort, Name: "redis"},
			{ContainerPort: GossipPort, Name: "gossip"},
		},
	}
}

// NewBase returns the RedisCluster spec shared by both test environments:
// topology, a cluster-enabled redis container, and matched selector/template
// base labels. Callers layer tier-specific fields on top.
func NewBase(name, namespace, image string) *cachev1.RedisCluster {
	base := BaseLabels(name)
	return &cachev1.RedisCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "cache.datadoghq.com/v1", Kind: "RedisCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: cachev1.RedisClusterSpec{
			Primaries: DefaultPrimaries,
			Replicas:  DefaultReplicas,
			StatefulsetTemplate: appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: CopyLabels(base)},
				Spec: appsv1.StatefulSetSpec{
					ServiceName: name,
					Replicas:    ptr.To(PodCount(DefaultPrimaries, DefaultReplicas)),
					Selector:    &metav1.LabelSelector{MatchLabels: CopyLabels(base)},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: CopyLabels(base)},
						Spec:       corev1.PodSpec{Containers: []corev1.Container{RedisContainer(image)}},
					},
				},
			},
		},
	}
}

// PodCount returns the StatefulSet size for a Redis topology.
func PodCount(primaries, replicas int32) int32 { return primaries * (1 + replicas) }

// SyncSTSReplicas keeps the StatefulSet replica count at primaries*(1+replicas).
func SyncSTSReplicas(rc *cachev1.RedisCluster) {
	rc.Spec.StatefulsetTemplate.Spec.Replicas = ptr.To(PodCount(rc.Spec.Primaries, rc.Spec.Replicas))
}

// SetPrimaries sets the primary count and resyncs the STS replica count.
func SetPrimaries(rc *cachev1.RedisCluster, n int32) {
	rc.Spec.Primaries = n
	SyncSTSReplicas(rc)
}

// SetReplicas sets the replica-per-primary count and resyncs the STS replicas.
func SetReplicas(rc *cachev1.RedisCluster, n int32) {
	rc.Spec.Replicas = n
	SyncSTSReplicas(rc)
}

// SetTopology updates primary and replica counts together.
func SetTopology(rc *cachev1.RedisCluster, primaries, replicas int32) {
	rc.Spec.Primaries = primaries
	rc.Spec.Replicas = replicas
	SyncSTSReplicas(rc)
}

// WithPrimaries overrides the primary count.
func WithPrimaries(n int32) ClusterOpt {
	return func(rc *cachev1.RedisCluster) { SetPrimaries(rc, n) }
}

// WithReplicas overrides the replicas per primary.
func WithReplicas(n int32) ClusterOpt {
	return func(rc *cachev1.RedisCluster) { SetReplicas(rc, n) }
}

// SetImage overrides the redis container image.
func SetImage(rc *cachev1.RedisCluster, image string) {
	rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image = image
}

// WithImage overrides the Redis image.
func WithImage(image string) ClusterOpt {
	return func(rc *cachev1.RedisCluster) { SetImage(rc, image) }
}

// AppendToolboxACL appends the operator's toolbox ACL user to the redis args.
func AppendToolboxACL(rc *cachev1.RedisCluster, password string) {
	args := &rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Args
	*args = append(*args, "--user", "toolbox", "on", ">"+password, "~*", "&*", "+@all")
}

// WithToolboxACL enables the toolbox user.
func WithToolboxACL(password string) ClusterOpt {
	return func(rc *cachev1.RedisCluster) { AppendToolboxACL(rc, password) }
}

// EnableBackup flips Spec.BackupEnabled.
func EnableBackup(rc *cachev1.RedisCluster) { rc.Spec.BackupEnabled = true }

// WithBackupEnabled enables backups.
func WithBackupEnabled() ClusterOpt {
	return func(rc *cachev1.RedisCluster) { EnableBackup(rc) }
}

// SetRestoreFrom seeds the cluster from a backup path.
func SetRestoreFrom(rc *cachev1.RedisCluster, path string) { rc.Spec.RestoreFrom = path }

// WithRestoreFrom sets the backup path to restore.
func WithRestoreFrom(path string) ClusterOpt {
	return func(rc *cachev1.RedisCluster) { SetRestoreFrom(rc, path) }
}
