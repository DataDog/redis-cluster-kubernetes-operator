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
