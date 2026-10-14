// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCRDDirectoriesRequireEveryFile(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "present.yaml")
	if err := os.WriteFile(present, []byte("apiVersion: v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	dirs, err := crdDirectories([]string{present})
	if err != nil {
		t.Fatalf("existing CRD rejected: %v", err)
	}
	if len(dirs) != 1 || dirs[0] != dir {
		t.Fatalf("crdDirectories() = %v, want [%s]", dirs, dir)
	}

	if _, err := crdDirectories([]string{filepath.Join(dir, "missing.yaml")}); err == nil {
		t.Fatal("expected a missing required CRD to fail")
	}
}
