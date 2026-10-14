// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"os"
	"testing"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/envtest"
)

// TestMain stops the shared envtest control plane after all tests in this
// package complete. Without it the etcd + apiserver child processes leak past
// test exit.
func TestMain(m *testing.M) {
	code := m.Run()
	envtest.StopShared()
	os.Exit(code)
}
