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
