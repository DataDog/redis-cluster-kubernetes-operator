package kind

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// dns1123MaxLen is the Kubernetes object-name limit (an RFC 1123 label).
	dns1123MaxLen = 63
	// nsRunIDLen sizes the random suffix appended to every derived namespace.
	// Go's test timeout panics from a separate goroutine, so the test goroutine's
	// t.Cleanup never runs and the namespace leaks on the shared cluster. A unique
	// suffix lets the retry (a fresh process) proceed without colliding with it.
	nsRunIDLen = 5
	// nsBaseMaxLen caps the readable part so "<base>-<runID>" fits the limit.
	nsBaseMaxLen = dns1123MaxLen - 1 - nsRunIDLen

	// defaultReadyPollInterval is the default cadence for the fixture's wait loops.
	defaultReadyPollInterval = 500 * time.Millisecond
	// saReadyTimeout/saReadyPollInterval bound the wait for a new namespace's
	// default ServiceAccount, which kube-controller-manager creates asynchronously.
	saReadyTimeout      = 30 * time.Second
	saReadyPollInterval = 200 * time.Millisecond
	// nsDeleteTimeout/nsDeletePollInterval bound namespace teardown; the owning
	// manager must be alive within this window to clear CR finalizers.
	nsDeleteTimeout      = 90 * time.Second
	nsDeletePollInterval = 500 * time.Millisecond
)

// Fixture bundles a KIND cluster, a fresh namespace, and a connected client
// for one test. The manager is NOT started here — call StartManager after
// creating CRs so reconcile sees them in order.
type Fixture struct {
	T         *testing.T
	Bootstrap Bootstrap
	Cluster   *Cluster
	Client    client.Client
	Cfg       *rest.Config
	Namespace string
	Ctx       context.Context

	cfg    fixtureCfg
	runner *Runner
}

type cleanupReporter interface {
	Failed() bool
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Opt configures New.
type Opt func(*fixtureCfg)

type fixtureCfg struct {
	namespace            string
	existing             *Cluster
	pollEvery            time.Duration
	failureArtifactLists []failureArtifactList
}

type failureArtifactList struct {
	filename string
	newList  func() client.ObjectList
}

// WithNamespace sets the namespace name (default: derived from the test name).
func WithNamespace(ns string) Opt { return func(c *fixtureCfg) { c.namespace = ns } }

// UseCluster reuses an existing cluster, skipping the per-test boot.
func UseCluster(c *Cluster) Opt { return func(o *fixtureCfg) { o.existing = c } }

// WithPollInterval sets the WaitReady cadence (default 500ms).
func WithPollInterval(d time.Duration) Opt { return func(c *fixtureCfg) { c.pollEvery = d } }

// WithFailureArtifactList adds an object list to failure artifacts.
func WithFailureArtifactList(filename string, newList func() client.ObjectList) Opt {
	return func(c *fixtureCfg) {
		c.failureArtifactLists = append(c.failureArtifactLists, failureArtifactList{
			filename: filename,
			newList:  newList,
		})
	}
}

// New boots (or reuses) a KIND cluster, creates a fresh namespace, and
// connects a client. Cleanup is registered on t.
func New(t *testing.T, b Bootstrap, opts ...Opt) *Fixture {
	t.Helper()
	quietTestLogs() // silence per-test manager/informer chatter (KIND_E2E_VERBOSE=1 to keep)
	require.NotNil(t, b, "kind.New: Bootstrap is required")

	cfg := fixtureCfg{pollEvery: defaultReadyPollInterval}
	for _, o := range opts {
		o(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := &Fixture{T: t, Bootstrap: b, Ctx: ctx, cfg: cfg}
	if cfg.existing != nil {
		f.Cluster = cfg.existing
		t.Logf("kind: reused cluster %s", cfg.existing.Name)
	} else {
		f.Cluster = NewCluster(t)
	}
	f.Cfg = f.Cluster.Cfg
	f.Client = f.Cluster.Client
	f.Namespace = cfg.namespace
	if f.Namespace == "" {
		// Unique per run so a namespace leaked by a timed-out attempt (t.Cleanup
		// can't run on Go's timeout panic) never collides with the retry.
		f.Namespace = uniqueNamespace(sanitizeName(t.Name()))
	}
	// Register cleanup before namespace creation to cover partial setup.
	t.Cleanup(func() { f.cleanup(t) })
	ensureNamespace(t, ctx, f.Client, f.Namespace)
	return f
}

func (f *Fixture) cleanup(t cleanupReporter) {
	artifactsAttempted := false
	dumpArtifacts := func() {
		if !artifactsAttempted {
			artifactsAttempted = true
			dumpFailureArtifacts(t, f)
		}
	}
	if t.Failed() {
		dumpArtifacts()
	}
	if f.Client != nil && f.Namespace != "" {
		if err := deleteNamespaceAndWait(f.Client, f.Namespace); err != nil {
			t.Errorf("delete namespace %s: %v", f.Namespace, err)
			dumpArtifacts()
		}
	}
	if err := f.stopRunner(); err != nil {
		t.Errorf("stop manager: %v", err)
		dumpArtifacts()
	}
}

func (f *Fixture) stopRunner() error {
	if f == nil || f.runner == nil {
		return nil
	}
	runner := f.runner
	f.runner = nil
	return runner.Stop()
}

// WaitReady polls Bootstrap.Ready until true or timeout.
func (f *Fixture) WaitReady(key client.ObjectKey, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(f.Ctx, timeout)
	defer cancel()
	interval := f.poll()
	return wait.PollUntilContextTimeout(ctx, interval, timeout, true,
		func(ctx context.Context) (bool, error) {
			return f.Bootstrap.Ready(ctx, f.Client, key)
		})
}

// ensureNamespace idempotently creates ns and waits for its default
// ServiceAccount, so shared clusters don't race.
func ensureNamespace(t *testing.T, ctx context.Context, c client.Client, ns string) {
	t.Helper()
	err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name: ns,
	}})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		require.NoError(t, err)
	}
	err = wait.PollUntilContextTimeout(ctx, saReadyPollInterval, saReadyTimeout, true,
		func(ctx context.Context) (bool, error) {
			sa := &corev1.ServiceAccount{}
			err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: "default"}, sa)
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return err == nil, err
		})
	require.NoError(t, err, "default ServiceAccount not created in namespace %s", ns)
}

// deleteNamespaceAndWait deletes ns and waits for it to terminate. Must run
// while the owning manager is alive so controllers clear finalizers; otherwise
// the namespace hangs in Terminating.
func deleteNamespaceAndWait(c client.Client, ns string) error {
	ctx, cancel := context.WithTimeout(context.Background(), nsDeleteTimeout)
	defer cancel()
	var lastDeleteErr error
	err := wait.PollUntilContextTimeout(ctx, nsDeletePollInterval, nsDeleteTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := c.Delete(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
			if err == nil || apierrors.IsNotFound(err) {
				return true, nil
			}
			if isPermanentAPIError(err) {
				return false, err
			}
			lastDeleteErr = err
			return false, nil
		})
	if err != nil {
		if lastDeleteErr != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			return fmt.Errorf("request deletion (last error: %w): %w", lastDeleteErr, err)
		}
		return fmt.Errorf("request deletion: %w", err)
	}
	var lastGetErr error
	err = wait.PollUntilContextTimeout(ctx, nsDeletePollInterval, nsDeleteTimeout, true,
		func(ctx context.Context) (bool, error) {
			err := c.Get(ctx, client.ObjectKey{Name: ns}, &corev1.Namespace{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if isPermanentAPIError(err) {
				return false, err
			}
			// Retry transient reads until the deletion deadline.
			lastGetErr = err
			if err == nil {
				lastGetErr = nil
			}
			return false, nil
		})
	if err != nil {
		if lastGetErr != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
			return fmt.Errorf("wait for deletion (last read error: %w): %w", lastGetErr, err)
		}
		return fmt.Errorf("wait for deletion: %w", err)
	}
	return nil
}

func isPermanentAPIError(err error) bool {
	return apierrors.IsBadRequest(err) ||
		apierrors.IsForbidden(err) ||
		apierrors.IsInvalid(err) ||
		apierrors.IsMethodNotSupported(err) ||
		apierrors.IsUnauthorized(err)
}

// uniqueNamespace appends a random suffix to base (truncated to fit the 63-char
// limit) so every test-binary run gets fresh namespaces. See nsRunIDLen.
func uniqueNamespace(base string) string {
	if len(base) > nsBaseMaxLen {
		base = base[:nsBaseMaxLen]
	}
	return base + "-" + rand.String(nsRunIDLen)
}

// sanitizeName turns t.Name() into a DNS-1123 namespace fragment.
func sanitizeName(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r >= 'A' && r <= 'Z':
			out = append(out, r+32)
		case r == '-' || r == '/' || r == '_':
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "kv"
	}
	return string(out)
}
