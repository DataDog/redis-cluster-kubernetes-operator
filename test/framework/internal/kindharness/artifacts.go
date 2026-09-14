package kind

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const artifactTimeout = 10 * time.Second

// dumpFailureArtifacts captures namespace state before cleanup.
func dumpFailureArtifacts(t cleanupReporter, f *Fixture) {
	if f == nil || f.Namespace == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), artifactTimeout)
	defer cancel()
	if f.Client != nil {
		lists := []struct {
			name string
			list client.ObjectList
		}{
			{"client-pods.json", &corev1.PodList{}},
			{"client-services.json", &corev1.ServiceList{}},
			{"client-events.json", &corev1.EventList{}},
			{"client-deployments.json", &appsv1.DeploymentList{}},
			{"client-statefulsets.json", &appsv1.StatefulSetList{}},
			{"client-jobs.json", &batchv1.JobList{}},
		}
		for _, extra := range f.cfg.failureArtifactLists {
			if extra.newList == nil {
				writeArtifact(t, f.Namespace, extra.filename, `{"error":"failure artifact list factory is nil"}`)
				continue
			}
			list := extra.newList()
			if list == nil {
				writeArtifact(t, f.Namespace, extra.filename, `{"error":"failure artifact list factory returned nil"}`)
				continue
			}
			lists = append(lists, struct {
				name string
				list client.ObjectList
			}{name: extra.filename, list: list})
		}
		for _, d := range lists {
			writeArtifact(t, f.Namespace, d.name, clientResourceOutput(ctx, f.Client, f.Namespace, d.list))
		}
	}
	if f.Cluster != nil && f.Cluster.Kubeconfig != "" {
		if _, err := exec.LookPath("kubectl"); err == nil {
			for _, d := range []struct {
				name string
				args []string
			}{
				{"kubectl-get.txt", []string{"get", "all,events", "-o", "wide", "-n", f.Namespace}},
				{"kubectl-describe-pods.txt", []string{"describe", "pods", "-n", f.Namespace}},
				{"kubectl-events.txt", []string{"get", "events", "-n", f.Namespace, "--sort-by=.lastTimestamp"}},
			} {
				writeArtifact(t, f.Namespace, d.name, kubectlOutput(ctx, f.Cluster.Kubeconfig, d.args...))
			}
		}
	}
}

func clientResourceOutput(ctx context.Context, c client.Client, namespace string, list client.ObjectList) string {
	var value any = list
	if err := c.List(ctx, list, client.InNamespace(namespace)); err != nil {
		value = map[string]string{"error": err.Error()}
	}
	out, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		out, _ = json.Marshal(map[string]string{"error": fmt.Sprintf("marshal client resources: %v", err)})
	}
	return string(out)
}

func writeArtifact(t cleanupReporter, namespace, name, out string) {
	dir := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if dir != "" {
		path := filepath.Join(dir, namespace+"-"+name)
		if err := os.WriteFile(path, []byte(out), 0o644); err == nil {
			return
		} else {
			t.Logf("write failure artifact %s: %v", path, err)
		}
	}
	t.Logf("=== %s (ns=%s) ===\n%s", name, namespace, out)
}

func kubectlOutput(ctx context.Context, kubeconfig string, args ...string) string {
	out, err := exec.CommandContext(ctx, "kubectl", append([]string{"--kubeconfig", kubeconfig}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Sprintf("%s\n(kubectl error: %v)", out, err)
	}
	return string(out)
}
