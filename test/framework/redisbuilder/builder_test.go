// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redisbuilder_test

import (
	"testing"

	rb "github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/redisbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewBase_Defaults pins the shared CR core: topology, container, STS replica count.
func TestNewBase_Defaults(t *testing.T) {
	rc := rb.NewBase("foo", "default", rb.DefaultRedisImage)
	assert.Equal(t, "foo", rc.Name)
	assert.Equal(t, "default", rc.Namespace)
	assert.Equal(t, int32(rb.DefaultPrimaries), rc.Spec.Primaries)
	assert.Equal(t, int32(rb.DefaultReplicas), rc.Spec.Replicas)

	require.NotNil(t, rc.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, int32(rb.DefaultPrimaries*(1+rb.DefaultReplicas)), *rc.Spec.StatefulsetTemplate.Spec.Replicas)

	require.NotEmpty(t, rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers)
	c := rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0]
	assert.Equal(t, rb.DefaultRedisImage, c.Image)
	// Cluster-enabled args + client/gossip ports are what let the operator's
	// CLUSTER NODES / MEET converge; regressing them silently breaks bootstrap.
	assert.Contains(t, c.Args, "--cluster-enabled")
	assert.Len(t, c.Ports, 2)
}

// TestNewBase_LabelMapsAreIndependent guards map aliasing: selector/STS/template maps are independent.
func TestNewBase_LabelMapsAreIndependent(t *testing.T) {
	rc := rb.NewBase("foo", "default", rb.DefaultRedisImage)
	rc.Spec.StatefulsetTemplate.Spec.Template.Labels["probe-only"] = "x"

	assert.NotContains(t, rc.Spec.StatefulsetTemplate.Spec.Selector.MatchLabels, "probe-only")
	assert.NotContains(t, rc.Spec.StatefulsetTemplate.Labels, "probe-only")
}

// TestSetters covers the tier-agnostic mutators both builders delegate to.
func TestSetters(t *testing.T) {
	rc := rb.NewBase("foo", "default", rb.DefaultRedisImage)

	rb.SetPrimaries(rc, 5)
	rb.SetReplicas(rc, 2)
	assert.Equal(t, int32(5), rc.Spec.Primaries)
	assert.Equal(t, int32(2), rc.Spec.Replicas)
	require.NotNil(t, rc.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, int32(15), *rc.Spec.StatefulsetTemplate.Spec.Replicas, "STS replicas track primaries*(1+replicas)")

	rb.SetTopology(rc, 4, 1)
	assert.Equal(t, int32(4), rc.Spec.Primaries)
	assert.Equal(t, int32(1), rc.Spec.Replicas)
	assert.Equal(t, int32(8), *rc.Spec.StatefulsetTemplate.Spec.Replicas)
	assert.Equal(t, int32(8), rb.PodCount(4, 1))

	rb.SetImage(rc, "redis:7.4.0")
	assert.Equal(t, "redis:7.4.0", rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Image)

	rb.EnableBackup(rc)
	assert.True(t, rc.Spec.BackupEnabled)

	rb.SetRestoreFrom(rc, "my-bucket/202612181430/")
	assert.Equal(t, "my-bucket/202612181430/", rc.Spec.RestoreFrom)

	rb.AppendToolboxACL(rc, "s3cr3t")
	assert.Contains(t, rc.Spec.StatefulsetTemplate.Spec.Template.Spec.Containers[0].Args, "toolbox")
}
