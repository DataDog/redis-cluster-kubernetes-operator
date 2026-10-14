// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// SchemeFromBuilders returns client-go's scheme plus each operator scheme.
func SchemeFromBuilders(addToScheme ...func(*runtime.Scheme) error) (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		return nil, err
	}
	for _, add := range addToScheme {
		if err := add(s); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// SharedCluster is one KIND cluster reused across a test binary.
type SharedCluster struct {
	once    sync.Once
	scheme  *runtime.Scheme
	crdDirs []string
	cluster *Cluster
	cleanup func()
	err     error
}

// NewSharedCluster returns a lazily booted cluster using repository-relative
// CRD files and the supplied runtime schemes.
func NewSharedCluster(crdFiles []string, addToScheme ...func(*runtime.Scheme) error) *SharedCluster {
	crdDirs, crdErr := crdDirectories(crdFiles)
	scheme, schemeErr := SchemeFromBuilders(addToScheme...)
	return &SharedCluster{
		scheme:  scheme,
		crdDirs: crdDirs,
		cleanup: func() {},
		err:     errors.Join(crdErr, schemeErr),
	}
}

func crdDirectories(files []string) ([]string, error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var dirs []string
	for _, file := range files {
		path := file
		if !filepath.IsAbs(path) {
			path = filepath.Join(root, path)
		}
		if _, err := os.Stat(path); err != nil {
			return nil, fmt.Errorf("required CRD %s: %w", file, err)
		}
		dir := filepath.Dir(path)
		if !seen[dir] {
			seen[dir] = true
			dirs = append(dirs, dir)
		}
	}
	return dirs, nil
}

// Scheme is the shared client and manager scheme.
func (sc *SharedCluster) Scheme() *runtime.Scheme { return sc.scheme }

// Ensure boots or connects to the cluster on first use.
func (sc *SharedCluster) Ensure(t *testing.T) *Cluster {
	t.Helper()
	sc.once.Do(func() {
		if sc.err != nil {
			return
		}
		opts := []ClusterOpt{WithScheme(sc.scheme)}
		if len(sc.crdDirs) > 0 {
			opts = append(opts, WithCRDPaths(sc.crdDirs...))
		}
		sc.cluster, sc.cleanup, sc.err = NewClusterForTestMain(opts...)
		if sc.err == nil && os.Getenv("E2E_LOADGEN") == "1" {
			image := os.Getenv("LOADGEN_IMAGE")
			if image == "" {
				image = "rco-loadgen:test"
			}
			sc.err = LoadDockerImage(sc.cluster.Name, image)
		}
	})
	if sc.err != nil {
		if os.Getenv("CI") != "" {
			t.Fatalf("kind cluster unavailable in CI: %v", sc.err)
		}
		t.Skipf("kind cluster unavailable: %v", sc.err)
	}
	return sc.cluster
}

// Stop tears down the shared cluster.
func (sc *SharedCluster) Stop() { sc.cleanup() }
