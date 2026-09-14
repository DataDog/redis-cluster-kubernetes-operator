package loadgen

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	// loadgenHTTPTimeout bounds a single HTTP call to the loadgen pod.
	loadgenHTTPTimeout = 10 * time.Second
	// loadgenHealthzTimeout bounds the post-port-forward wait for /healthz.
	loadgenHealthzTimeout = 30 * time.Second
	// loadgenHealthzPoll is the /healthz retry cadence during that wait.
	loadgenHealthzPoll = 250 * time.Millisecond
	// loadgenStatsPoll is the stats assertion cadence.
	loadgenStatsPoll = time.Second
	// maxErrorBodyBytes bounds diagnostic response reads.
	maxErrorBodyBytes = 64 << 10
)

// RueidisGenerator is the in-cluster rueidis loadgen pod's Go client, reached
// over an in-process SPDY port-forward (rueidis runs inside KIND to follow MOVED).
type RueidisGenerator struct {
	baseURL string // http://127.0.0.1:<forwarded-port>
	http    *http.Client
}

// RueidisConfig drives StartRueidis.
type RueidisConfig struct {
	Kubeconfig string // path to KIND cluster's kubeconfig (required)
	Namespace  string // namespace where the loadgen pod lives
	PodName    string // loadgen pod name; defaults to "loadgen"
	LocalPort  int    // local-side port for the in-process port-forward; 0 = auto
}

// StartRueidis sets up the port-forward and returns a connected
// client. The test's Cleanup tears the port-forward down.
func StartRueidis(t *testing.T, cfg RueidisConfig) *RueidisGenerator {
	t.Helper()
	if cfg.Kubeconfig == "" {
		t.Fatal("loadgen.StartRueidis: Kubeconfig is required")
	}
	if cfg.PodName == "" {
		cfg.PodName = "loadgen"
	}
	if cfg.LocalPort == 0 {
		cfg.LocalPort = freeLocalPort(t)
	}

	stopPF := startPortForward(t, cfg)

	g := &RueidisGenerator{
		baseURL: "http://127.0.0.1:" + strconv.Itoa(cfg.LocalPort),
		http:    &http.Client{Timeout: loadgenHTTPTimeout},
	}

	t.Cleanup(func() {
		_, _ = g.http.Post(g.baseURL+"/stop", "application/json", nil)
		stopPF()
	})

	// Wait for the port-forward + loadgen /healthz; bounded so a broken pod surfaces fast.
	deadline := time.Now().Add(loadgenHealthzTimeout)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("loadgen: pod /healthz never became ready within %s", loadgenHealthzTimeout)
		}
		resp, err := g.http.Get(g.baseURL + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return g
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(loadgenHealthzPoll)
	}
}

// startPortForward opens an in-process SPDY port-forward from 127.0.0.1:LocalPort
// to the loadgen pod's :8080. Using client-go's forwarder (not an external
// `kubectl port-forward`) avoids requiring an external kubectl process.
// Returns a stop func that tears the tunnel down.
func startPortForward(t *testing.T, cfg RueidisConfig) func() {
	t.Helper()
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	require.NoError(t, err, "build rest config from kubeconfig")
	clientset, err := kubernetes.NewForConfig(restCfg)
	require.NoError(t, err, "build clientset")

	roundTripper, upgrader, err := spdy.RoundTripperFor(restCfg)
	require.NoError(t, err, "spdy round tripper")
	pfURL := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(cfg.Namespace).Name(cfg.PodName).SubResource("portforward").URL()
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: roundTripper}, http.MethodPost, pfURL)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	ports := []string{fmt.Sprintf("%d:%d", cfg.LocalPort, LoadgenPodPort)}
	fw, err := portforward.New(dialer, ports, stopCh, readyCh, io.Discard, io.Discard)
	require.NoError(t, err, "new port forwarder")

	errCh := make(chan error, 1)
	go func() { errCh <- fw.ForwardPorts() }()

	select {
	case <-readyCh:
		return func() { close(stopCh) }
	case err := <-errCh:
		close(stopCh)
		t.Fatalf("loadgen port-forward exited early: %v", err)
	case <-time.After(loadgenHealthzTimeout):
		close(stopCh)
		t.Fatalf("loadgen port-forward to pod %s not ready within %s", cfg.PodName, loadgenHealthzTimeout)
	}
	return func() {}
}

func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "allocate local port for loadgen port-forward")
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok, "local listener address has type %T, want *net.TCPAddr", ln.Addr())
	return addr.Port
}

// RueidisStats mirrors the pod's Stats JSON shape.
type RueidisStats struct {
	QPS      int64  `json:"qps"`
	Workers  int64  `json:"workers"`
	SetsOK   int64  `json:"sets_ok"`
	SetsFail int64  `json:"sets_fail"`
	GetsOK   int64  `json:"gets_ok"`
	GetsFail int64  `json:"gets_fail"`
	Mismatch int64  `json:"mismatch"`
	LastErr  string `json:"last_err,omitempty"`
}

// DeltaSince returns counters added after before and rejects resets.
func (s RueidisStats) DeltaSince(before RueidisStats) (RueidisStats, error) {
	counters := []struct {
		name         string
		after, prior int64
	}{
		{"sets_ok", s.SetsOK, before.SetsOK},
		{"sets_fail", s.SetsFail, before.SetsFail},
		{"gets_ok", s.GetsOK, before.GetsOK},
		{"gets_fail", s.GetsFail, before.GetsFail},
		{"mismatch", s.Mismatch, before.Mismatch},
	}
	for _, counter := range counters {
		if counter.after < counter.prior {
			return RueidisStats{}, fmt.Errorf("loadgen counter %s reset between snapshots: before=%d after=%d",
				counter.name, counter.prior, counter.after)
		}
	}
	return RueidisStats{
		QPS:      s.QPS,
		Workers:  s.Workers,
		SetsOK:   s.SetsOK - before.SetsOK,
		SetsFail: s.SetsFail - before.SetsFail,
		GetsOK:   s.GetsOK - before.GetsOK,
		GetsFail: s.GetsFail - before.GetsFail,
		Mismatch: s.Mismatch - before.Mismatch,
		LastErr:  s.LastErr,
	}, nil
}

// CompletedOps is the number of successful, failed, or mismatched operations.
func (s RueidisStats) CompletedOps() int64 {
	return s.SetsOK + s.SetsFail + s.GetsOK + s.GetsFail + s.Mismatch
}

// SuccessRate is the fraction of completed operations that succeeded.
func (s RueidisStats) SuccessRate() float64 {
	if s.CompletedOps() == 0 {
		return 0
	}
	return float64(s.SetsOK+s.GetsOK) / float64(s.CompletedOps())
}

// Stats fetches a counter snapshot from the loadgen pod.
func (g *RueidisGenerator) Stats() (RueidisStats, error) {
	var s RueidisStats
	resp, err := g.http.Get(g.baseURL + "/stats")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return s, fmt.Errorf("GET /stats: %s: %s", resp.Status, readErrorBody(resp.Body))
	}
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

// WaitForTraffic waits until the generator completes a successful operation.
func (g *RueidisGenerator) WaitForTraffic(t *testing.T, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		stats, err := g.Stats()
		return err == nil && stats.SetsOK+stats.GetsOK > 0
	}, timeout, loadgenStatsPoll, "loadgen produced no successful operations")
}

// MustStats returns one counter snapshot or fails the test.
func (g *RueidisGenerator) MustStats(t *testing.T) RueidisStats {
	t.Helper()
	stats, err := g.Stats()
	require.NoError(t, err, "fetch loadgen stats")
	return stats
}

// RequireSuccessRateSince validates traffic after a counter snapshot.
func (g *RueidisGenerator) RequireSuccessRateSince(t *testing.T, before RueidisStats, minRate float64) {
	t.Helper()
	require.NoError(t, g.CheckSuccessRateSince(before, minRate))
}

// CheckSuccessRateSince validates traffic after a counter snapshot.
func (g *RueidisGenerator) CheckSuccessRateSince(before RueidisStats, minRate float64) error {
	stats, err := g.Stats()
	if err != nil {
		return fmt.Errorf("fetch loadgen stats: %w", err)
	}
	delta, err := stats.DeltaSince(before)
	if err != nil {
		return err
	}
	if delta.CompletedOps() <= 0 {
		return fmt.Errorf("loadgen completed no operations after snapshot: %+v", delta)
	}
	if delta.Mismatch != 0 {
		return fmt.Errorf("loadgen observed wrong values after snapshot: %+v", delta)
	}
	if rate := delta.SuccessRate(); rate < minRate {
		return fmt.Errorf(
			"loadgen success rate %.3f below %.2f after snapshot: %+v",
			rate, minRate, delta,
		)
	}
	return nil
}

func readErrorBody(body io.Reader) string {
	var out strings.Builder
	_, _ = io.CopyN(&out, body, maxErrorBodyBytes)
	return strings.TrimSpace(out.String())
}

// LoadgenPodImage is the kind-loaded loadgen image tag (ci/build-loadgen-image.sh).
const LoadgenPodImage = "rco-loadgen:test"

// LoadgenPodPort is the in-pod HTTP control-surface port.
const LoadgenPodPort = 8080

// LoadgenContainerName is the loadgen pod's only container name.
const LoadgenContainerName = "loadgen"
