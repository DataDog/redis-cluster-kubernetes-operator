package kind

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind/loadgen"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
)

// The rueidis loadgen runs in-cluster and exposes stats through a port-forward.

const (
	// loadgenPodName is the fixed name of the in-cluster loadgen pod.
	loadgenPodName = "loadgen"
	// loadgenReadyTimeout bounds the loadgen pod reporting Ready.
	loadgenReadyTimeout = 2 * time.Minute
)

// StartRueidisLoadgen deploys the in-cluster loadgen pod (seeded with the
// cluster's ready redis pod IPs), waits for it Ready, port-forwards, and returns
// a connected continuous-QPS generator that runs until the test's Cleanup.
func (s *Suite) StartRueidisLoadgen(t *testing.T, name string) *loadgen.RueidisGenerator {
	t.Helper()
	s.deployLoadgenPod(t, s.redisSeedAddrs(t, name))
	s.waitLoadgenPodReady(t)
	return loadgen.StartRueidis(t, loadgen.RueidisConfig{
		Kubeconfig: s.Cluster.Kubeconfig,
		Namespace:  s.Namespace,
		PodName:    loadgenPodName,
	})
}

// redisSeedAddrs is the cluster's ready redis pod IPs as host:port seeds for the
// loadgen's rueidis cluster client.
func (s *Suite) redisSeedAddrs(t *testing.T, name string) string {
	t.Helper()
	ips, err := s.tryReadyRedisPodIPs(name)
	require.NoError(t, err)
	require.NotEmpty(t, ips, "no ready redis pods to seed the loadgen")
	addrs := make([]string, len(ips))
	for i, ip := range ips {
		addrs[i] = net.JoinHostPort(ip, strconv.Itoa(redisPort))
	}
	return strings.Join(addrs, ",")
}

// deployLoadgenPod creates the loadgen pod (rueidis workload, HTTP control on
// :8080) pointed at the given seed addresses.
func (s *Suite) deployLoadgenPod(t *testing.T, initAddrs string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      loadgenPodName,
			Namespace: s.Namespace,
			Labels:    map[string]string{"app": loadgenPodName},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:            loadgen.LoadgenContainerName,
				Image:           resolvedLoadgenImage(),
				ImagePullPolicy: corev1.PullIfNotPresent,
				Ports:           []corev1.ContainerPort{{ContainerPort: loadgen.LoadgenPodPort}},
				Env:             []corev1.EnvVar{{Name: "LOADGEN_INIT_ADDRS", Value: initAddrs}},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/healthz",
							Port: intstr.FromInt(loadgen.LoadgenPodPort),
						},
					},
					InitialDelaySeconds: 1,
					PeriodSeconds:       2,
				},
			}},
		},
	}
	require.NoError(t, s.Fixture.Client.Create(s.Ctx, pod))
}

func resolvedLoadgenImage() string {
	if image := os.Getenv("LOADGEN_IMAGE"); image != "" {
		return image
	}
	return loadgen.LoadgenPodImage
}

// waitLoadgenPodReady blocks until the loadgen pod is Running+Ready.
func (s *Suite) waitLoadgenPodReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(s.Ctx, loadgenReadyTimeout)
	defer cancel()
	err := wait.PollUntilContextTimeout(ctx, toolboxPollInterval, loadgenReadyTimeout, true,
		func(ctx context.Context) (bool, error) {
			pod := &corev1.Pod{}
			if err := s.Fixture.Client.Get(ctx, types.NamespacedName{Namespace: s.Namespace, Name: loadgenPodName}, pod); err != nil {
				return false, nil
			}
			return podRunningReady(*pod), nil
		})
	require.NoError(t, err, "loadgen pod %q never became Ready", loadgenPodName)
}
