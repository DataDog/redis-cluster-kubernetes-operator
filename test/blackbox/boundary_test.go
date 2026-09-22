package blackbox

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlackboxImportBoundary(t *testing.T) {
	t.Parallel()
	forbidden := []string{
		"github.com/DataDog/redis-cluster-kubernetes-operator/controller",
		"github.com/DataDog/redis-cluster-kubernetes-operator/mains",
		"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind",
		"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/internal/kindharness",
	}
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imported := range parsed.Imports {
			value := strings.Trim(imported.Path.Value, `"`)
			for _, suffix := range forbidden {
				if strings.Contains(value, suffix) {
					t.Errorf("%s imports forbidden in-process package %q", path, value)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
