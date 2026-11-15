// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Cluster is a running KIND cluster connected via controller-runtime.
//
// E2E_CREATE_KIND_CLUSTER=true boots a fresh cluster torn down on t.Cleanup;
// unset connects to the CI-booted cluster via KUBECONFIG. Overrides:
// KIND_CLUSTER_NAME pins the cluster, KIND_NODE_IMAGE pins the kindest image.
type Cluster struct {
	Name       string
	Cfg        *rest.Config
	Client     client.Client
	Scheme     *runtime.Scheme
	Kubeconfig string
}

type prerequisiteError struct{ err error }

func (e prerequisiteError) Error() string { return e.err.Error() }
func (e prerequisiteError) Unwrap() error { return e.err }

// ClusterOpt configures NewCluster.
type ClusterOpt func(*clusterCfg)

type clusterCfg struct {
	scheme   *runtime.Scheme
	crdPaths []string
}

// WithScheme sets the client scheme (default: client-go scheme only).
func WithScheme(scheme *runtime.Scheme) ClusterOpt { return func(c *clusterCfg) { c.scheme = scheme } }

// WithCRDPaths installs the given CRD directories before tests run.
func WithCRDPaths(paths ...string) ClusterOpt {
	return func(c *clusterCfg) { c.crdPaths = append(c.crdPaths, paths...) }
}

// NewCluster returns a Cluster handle (see Cluster). Skips the test when KIND/docker are absent locally.
func NewCluster(t *testing.T, opts ...ClusterOpt) *Cluster {
	t.Helper()
	c, cleanup, err := newCluster(opts...)
	if err != nil {
		var prereq prerequisiteError
		if errors.As(err, &prereq) {
			t.Skip(prereq.Error())
		}
		require.NoError(t, err)
	}
	t.Cleanup(cleanup)
	return c
}

// NewClusterForTestMain is NewCluster for use from TestMain, where
// t.Cleanup is unavailable; the caller runs the returned cleanup.
func NewClusterForTestMain(opts ...ClusterOpt) (*Cluster, func(), error) {
	return newCluster(opts...)
}

func newCluster(opts ...ClusterOpt) (*Cluster, func(), error) {
	// Set the logger before cluster setup logs: controller-runtime ignores a
	// logger set after it stops waiting, 30 s after process start.
	quietTestLogs()
	cfg := &clusterCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	name := os.Getenv("KIND_CLUSTER_NAME")
	if name == "" {
		name = "redis-operator-test-" + randSuffix()
	}
	c := &Cluster{Name: name}
	cleanup := func() {}

	if os.Getenv("E2E_CREATE_KIND_CLUSTER") == "true" {
		kubeconfig, destroy, err := createCluster(c.Name)
		if err != nil {
			return nil, cleanup, err
		}
		c.Kubeconfig = kubeconfig
		cleanup = destroy
	} else {
		kubeconfig, err := useExternalKubeconfig(c.Name)
		if err != nil {
			return nil, cleanup, err
		}
		c.Kubeconfig = kubeconfig
	}

	restCfg, err := restConfig(c.Kubeconfig)
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	c.Cfg = restCfg
	scheme := cfg.scheme
	if scheme == nil {
		if scheme, err = newScheme(); err != nil {
			cleanup()
			return nil, func() {}, err
		}
	}
	c.Scheme = scheme
	cl, err := client.New(c.Cfg, client.Options{Scheme: c.Scheme})
	if err != nil {
		cleanup()
		return nil, func() {}, err
	}
	c.Client = cl

	if len(cfg.crdPaths) > 0 {
		if err := installCRDs(c.Cfg, cfg.crdPaths); err != nil {
			cleanup()
			return nil, func() {}, err
		}
	}
	return c, cleanup, nil
}

// createCluster boots a fresh cluster via the kind CLI.
func createCluster(name string) (string, func(), error) {
	if err := requireKindAndDocker(); err != nil {
		return "", func() {}, err
	}
	if exists, err := kindClusterExists(name); err != nil {
		return "", func() {}, err
	} else if exists {
		return "", func() {}, fmt.Errorf("refusing to replace existing KIND cluster %s", name)
	}

	args := []string{"create", "cluster", "--name", name, "--wait", "5m"}
	if image := os.Getenv("KIND_NODE_IMAGE"); image != "" {
		args = append(args, "--image", image)
	}
	if err := run("kind", args...); err != nil {
		return "", func() {}, fmt.Errorf("kind create cluster %s: %w", name, err)
	}
	cleanup := func() { _ = run("kind", "delete", "cluster", "--name", name) }
	kubeconfigPath := filepath.Join(os.TempDir(), name+".kubeconfig")
	if err := run("kind", "export", "kubeconfig", "--name", name, "--kubeconfig", kubeconfigPath); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return kubeconfigPath, cleanup, nil
}

func kindClusterExists(name string) (bool, error) {
	out, err := exec.Command("kind", "get", "clusters").CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("list KIND clusters: %w\n%s", err, out)
	}
	for _, cluster := range strings.Fields(string(out)) {
		if cluster == name {
			return true, nil
		}
	}
	return false, nil
}

// useExternalKubeconfig is the CI path: the test inherits the CI-booted cluster via KUBECONFIG.
func useExternalKubeconfig(name string) (string, error) {
	if v := os.Getenv("KUBECONFIG"); v != "" {
		if err := requireKindContext(v, name); err != nil {
			return "", err
		}
		return v, nil
	}
	path := filepath.Join(os.TempDir(), name+".kubeconfig")
	if err := run("kind", "export", "kubeconfig", "--name", name, "--kubeconfig", path); err != nil {
		return "", err
	}
	return path, nil
}

// requireKindContext guards against a misforwarded staging/prod KUBECONFIG.
func requireKindContext(kubeconfig, clusterName string) error {
	cfg, err := clientcmd.LoadFromFile(kubeconfig)
	if err != nil {
		return fmt.Errorf("load KUBECONFIG %s: %w", kubeconfig, err)
	}
	want := "kind-" + clusterName
	if cfg.CurrentContext != want {
		return fmt.Errorf("refusing KUBECONFIG %s: current context %q does not match %q; "+
			"set E2E_CREATE_KIND_CLUSTER=true to boot the requested cluster", kubeconfig, cfg.CurrentContext, want)
	}
	return nil
}

func requireKindAndDocker() error {
	if _, err := exec.LookPath("kind"); err != nil {
		return prerequisiteError{err: fmt.Errorf("kind not on PATH: %w", err)}
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		return prerequisiteError{err: fmt.Errorf("docker not running: %w\n%s", err, out)}
	}
	return nil
}

func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, string(out))
	}
	return nil
}

// LoadDockerImage makes a locally built image available to every node in a
// harness-owned KIND cluster.
func LoadDockerImage(clusterName, image string) error {
	if err := run("kind", "load", "docker-image", image, "--name", clusterName); err != nil {
		return fmt.Errorf("load image %s into KIND cluster %s: %w", image, clusterName, err)
	}
	return nil
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func newScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// installCRDs applies the operator CRDs via the Go client (no kubectl in the
// test process) and waits for each to be Established. The Go client also
// sidesteps the last-applied annotation size limit for large CRDs.
func installCRDs(cfg *rest.Config, crdPaths []string) error {
	if _, err := envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths:        crdPaths,
		MaxTime:      60 * time.Second,
		PollInterval: time.Second,
	}); err != nil {
		return fmt.Errorf("install CRDs: %w", err)
	}
	return nil
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

func randSuffix() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
