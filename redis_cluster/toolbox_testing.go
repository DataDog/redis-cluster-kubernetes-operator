// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package redis_cluster

import (
	"context"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/DataDog/redis-cluster-kubernetes-operator/metrics"
)

// NewToolboxForTesting returns a Toolbox that runs every command through exec
// instead of a real toolbox pod. It exists so tests in other packages can
// drive code that talks to Redis through the Toolbox; do not use it in
// production code.
func NewToolboxForTesting(ctx context.Context, exec PodExecutor, redisPort int) *Toolbox {
	return &Toolbox{
		kubeExec:           exec,
		toolboxPod:         &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "toolbox-test"}},
		ctx:                ctx,
		RedisPort:          redisPort,
		Log:                logr.Discard(),
		Metrics:            metrics.CreateNoop(),
		CredentialProvider: NoAuthentication{},
	}
}
