package envtest

import (
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/constants"
	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
)

// DefaultRedisImage is the cluster-enabled image used by the test builders.
const DefaultRedisImage = redisbuilder.DefaultRedisImage

// NewRedisCluster returns a v1.RedisCluster with sane defaults: 3 primaries,
// 1 replica each, namespace "default", redis:8.6.0 in cluster-enabled mode.
// Override via opts. Selector, template, and object labels are independent
// map copies so the reconciler can mutate one without breaking the others.
func NewRedisCluster(name string, opts ...redisbuilder.ClusterOpt) *cachev1.RedisCluster {
	rc := redisbuilder.NewBase(name, "default", DefaultRedisImage)
	for _, opt := range opts {
		opt(rc)
	}
	return rc
}

// WithNamespace overrides the default namespace.
func WithNamespace(ns string) redisbuilder.ClusterOpt {
	return func(rc *cachev1.RedisCluster) {
		rc.Namespace = ns
		rc.Spec.StatefulsetTemplate.Namespace = ns
	}
}

// InitializeRequiredStatus sets Status.Nodes to a non-nil empty slice so the
// CRD (which marks `nodes` required) accepts subsequent Status().Update calls.
func InitializeRequiredStatus(rc *cachev1.RedisCluster) {
	if rc.Status.Nodes == nil {
		rc.Status.Nodes = []cachev1.RedisClusterNode{}
	}
}

// ReadyForCreate restores StatefulsetTemplate fields the CRD prunes on
// admission (name, namespace, template.Labels) and mirrors the role label
// across selector and template so the API server's selector-matches-template
// check passes when ActivateRole runs during STS creation. In production the
// Helm chart bakes these in; envtest tests must call this instead.
func ReadyForCreate(rc *cachev1.RedisCluster) *cachev1.RedisCluster {
	rc.Spec.StatefulsetTemplate.Name = rc.Name
	rc.Spec.StatefulsetTemplate.Namespace = rc.Namespace

	sel := rc.Spec.StatefulsetTemplate.Spec.Selector
	if sel == nil || sel.MatchLabels == nil {
		return rc
	}

	tpl := &rc.Spec.StatefulsetTemplate.Spec.Template
	if tpl.Labels == nil {
		tpl.Labels = map[string]string{}
	}
	for k, v := range sel.MatchLabels {
		tpl.Labels[k] = v
	}
	sel.MatchLabels[constants.BlueGreenRoleLabel] = constants.BlueGreenRoleActive
	tpl.Labels[constants.BlueGreenRoleLabel] = constants.BlueGreenRoleActive
	return rc
}

// SetupBackupBucket sets BACKUP_BUCKET for the test via t.Setenv — what the
// operator's backup code reads (controller/backup.go calls os.Getenv).
func SetupBackupBucket(t *testing.T, bucket string) {
	t.Helper()
	t.Setenv("BACKUP_BUCKET", bucket)
}
