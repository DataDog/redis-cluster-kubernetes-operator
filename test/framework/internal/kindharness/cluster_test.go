// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"path/filepath"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestRequireKindContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	write := func(context string) {
		t.Helper()
		if err := clientcmd.WriteToFile(clientcmdapi.Config{CurrentContext: context}, path); err != nil {
			t.Fatal(err)
		}
	}

	write("kind-redis-test")
	if err := requireKindContext(path, "redis-test"); err != nil {
		t.Fatalf("expected matching KIND context: %v", err)
	}

	write("prod-kind-migration")
	if err := requireKindContext(path, "redis-test"); err == nil {
		t.Fatal("expected a non-matching context to be rejected")
	}
}
