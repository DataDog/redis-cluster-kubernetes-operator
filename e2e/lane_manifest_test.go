// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package e2e

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type testGates struct {
	slow    bool
	loadgen bool
}

// TestCILaneManifestShape ensures the shell runner exposes only the public
// lanes and keeps smoke as a non-empty subset of the default lane.
func TestCILaneManifestMatchesE2EManifest(t *testing.T) {
	dir := e2eSourceDir(t)
	ciLanes := readCILanes(t, filepath.Join(dir, "..", "ci", "e2e-lanes.txt"))
	expected := map[string]bool{"smoke": true, "default": true, "slow": true, "bg-stuck": true, "loadgen": true}
	for _, lane := range []string{"smoke", "default", "slow", "bg-stuck", "loadgen"} {
		if _, ok := ciLanes[lane]; !ok {
			t.Errorf("required CI lane %q is missing", lane)
		}
	}
	smoke := ciLanes["smoke"]
	if len(smoke) == 0 {
		t.Error("smoke lane must contain at least one default-lane test")
	}
	defaultTests := make(map[string]bool, len(ciLanes["default"]))
	for _, name := range ciLanes["default"] {
		defaultTests[name] = true
	}
	for _, name := range smoke {
		if !defaultTests[name] {
			t.Errorf("smoke test %s is not in the default lane", name)
		}
	}
	if len(ciLanes) != 5 {
		t.Errorf("CI manifest has %d lanes, want 5", len(ciLanes))
	}
	for lane := range ciLanes {
		if !expected[lane] {
			t.Errorf("unknown CI lane %q", lane)
		}
	}
}

// TestWorkflowUsesPublicLanes keeps the event wiring honest without requiring
// KIND, Docker, a YAML parser, or private CI helpers.
func TestWorkflowUsesPublicLanes(t *testing.T) {
	dir := e2eSourceDir(t)
	contents, err := os.ReadFile(filepath.Join(dir, "..", ".github", "workflows", "test.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(contents)
	for _, fragment := range []string{
		"pull_request:",
		"schedule:",
		"engine: [redis, valkey]",
		"valkey/valkey:8.1.11",
		"ci/run-e2e-lane.sh smoke",
		"lane: [default, slow, bg-stuck, loadgen]",
		"ci/run-e2e-lane.sh \"${{ matrix.lane }}\"",
	} {
		if !strings.Contains(workflow, fragment) {
			t.Errorf("public workflow missing %q", fragment)
		}
	}
}

func readCILanes(t *testing.T, path string) map[string][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lanes := map[string][]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if _, exists := lanes[fields[0]]; exists {
			t.Fatalf("duplicate CI lane %q", fields[0])
		}
		lanes[fields[0]] = fields[1:]
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return lanes
}

// TestLaneManifestOwnsEveryE2ETest keeps every top-level E2E in exactly one
// public Go test lane without depending on an external manifest.
func TestLaneManifestOwnsEveryE2ETest(t *testing.T) {
	dir := e2eSourceDir(t)
	gates := sourceTestGates(t, dir)
	lanes := readCILanes(t, filepath.Join(dir, "..", "ci", "e2e-lanes.txt"))
	delete(lanes, "smoke")
	owner := map[string]string{}
	for lane, tests := range lanes {
		for _, name := range tests {
			gate, ok := gates[name]
			if !ok {
				t.Errorf("%s lane names missing test %s", lane, name)
				continue
			}
			if prior := owner[name]; prior != "" {
				t.Errorf("test %s appears in both %s and %s lanes", name, prior, lane)
			}
			owner[name] = lane
			switch lane {
			case "default":
				if gate.slow || gate.loadgen {
					t.Errorf("default lane test %s must not use an opt-in gate", name)
				}
			case "loadgen":
				if !gate.loadgen {
					t.Errorf("loadgen lane test %s must call requireLoadgen", name)
				}
			default:
				if !gate.slow || gate.loadgen {
					t.Errorf("%s lane test %s must call requireSlow and not requireLoadgen", lane, name)
				}
			}
		}
	}
	for name := range gates {
		if owner[name] == "" {
			t.Errorf("test %s is not owned by any lane", name)
		}
	}
}

func e2eSourceDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Redis Cluster e2e sources")
	}
	return filepath.Dir(file)
}

func sourceTestGates(t *testing.T, dir string) map[string]testGates {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]testGates{}
	for _, path := range paths {
		if filepath.Base(path) == "lane_manifest_test.go" {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") || fn.Name.Name == "TestMain" {
				continue
			}
			gate := testGates{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok {
					return true
				}
				switch name.Name {
				case "requireSlow":
					gate.slow = true
				case "requireLoadgen":
					gate.loadgen = true
				}
				return true
			})
			tests[fn.Name.Name] = gate
		}
	}
	return tests
}
