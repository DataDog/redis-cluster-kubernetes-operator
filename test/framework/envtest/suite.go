// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Package envtest provides the operator's controller-runtime envtest harness.
package envtest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crenvtest "sigs.k8s.io/controller-runtime/pkg/envtest"
)

var errNoAssets = errors.New("envtest: no control-plane binaries found")

// Suite holds a running envtest control plane and its connected client.
type Suite struct {
	Cfg    *rest.Config
	Client client.Client
	Scheme *kruntime.Scheme
	env    *crenvtest.Environment
}

type sharedSuite struct {
	once sync.Once
	s    *Suite
	err  error
}

// NewNamespace creates an isolated namespace and removes it during cleanup.
func (s *Suite) NewNamespace(t *testing.T) string {
	t.Helper()
	name := "envtest-" + sanitizeName(t.Name())
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	require.NoError(t, s.Client.Create(context.Background(), ns))
	t.Cleanup(func() { _ = s.Client.Delete(context.Background(), ns) })
	return name
}

func sanitizeName(value string) string {
	const maxLen = 63 - len("envtest-")
	out := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+('a'-'A'))
		default:
			out = append(out, '-')
		}
	}
	if len(out) > maxLen {
		out = out[:maxLen]
	}
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	return string(out)
}

var shared sharedSuite

func crdDir() string {
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Join(filepath.Dir(self), "..", "..", "..", "crds")
}

func start() (*Suite, error) {
	assetsDir := os.Getenv("KUBEBUILDER_ASSETS")
	if assetsDir == "" {
		return nil, errNoAssets
	}
	if _, err := os.Stat(filepath.Join(assetsDir, "etcd")); err != nil {
		return nil, errNoAssets
	}

	scheme := kruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cachev1.AddToScheme(scheme))
	env := &crenvtest.Environment{
		BinaryAssetsDirectory: assetsDir,
		CRDDirectoryPaths:     []string{crdDir()},
		ErrorIfCRDPathMissing: true,
		Scheme:                scheme,
	}
	cfg, err := env.Start()
	if err != nil {
		return nil, err
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = env.Stop()
		return nil, err
	}
	return &Suite{Cfg: cfg, Client: c, Scheme: scheme, env: env}, nil
}

// SharedSuite returns a lazily started envtest control plane for this process.
func SharedSuite(t *testing.T) *Suite {
	t.Helper()
	shared.once.Do(func() { shared.s, shared.err = start() })
	if errors.Is(shared.err, errNoAssets) {
		if os.Getenv("CI") != "" {
			t.Fatal("envtest binaries not found in CI: set KUBEBUILDER_ASSETS")
		}
		t.Skip("envtest binaries not found: set KUBEBUILDER_ASSETS")
	}
	require.NoError(t, shared.err, "envtest shared suite failed to start")
	return shared.s
}

// StopShared stops the shared control plane; call from TestMain after m.Run().
func StopShared() {
	if shared.s != nil && shared.s.env != nil {
		_ = shared.s.env.Stop()
	}
}
