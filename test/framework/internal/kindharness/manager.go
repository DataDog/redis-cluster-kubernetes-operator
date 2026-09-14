package kind

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// cacheSyncTimeout bounds the initial informer cache sync so a manager that can
// never reach the API server fails fast instead of hanging to the test timeout.
const cacheSyncTimeout = 60 * time.Second

// Runner is a started in-process manager.
type Runner struct {
	cancel   context.CancelFunc
	done     <-chan error
	stopOnce sync.Once
	stopErr  error
}

// Stop cancels the manager and is safe to call more than once.
func (r *Runner) Stop() error {
	if r == nil {
		return nil
	}
	r.stopOnce.Do(func() {
		if r.cancel != nil {
			r.cancel()
		}
		if r.done != nil {
			r.stopErr = <-r.done
		}
	})
	return r.stopErr
}

// StartManager replaces the fixture's namespace-scoped manager.
func StartManager(t *testing.T, f *Fixture, b Bootstrap) {
	t.Helper()
	require.NotNil(t, f, "kind.StartManager: Fixture is required")

	mgr, err := ctrl.NewManager(f.Cfg, ctrl.Options{
		Scheme:         b.Scheme(),
		Logger:         ctrl.Log.WithName("manager"),
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
		Controller:     config.Controller{SkipNameValidation: ptr.To(true)},
		Cache:          cache.Options{DefaultNamespaces: map[string]cache.Config{f.Namespace: {}}},
	})
	require.NoError(t, err)
	require.NoError(t, b.Register(mgr))
	// Keep the old manager alive until its replacement is ready to start.
	require.NoError(t, f.stopRunner(), "stop previous manager")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- mgr.Start(ctx)
		close(done)
	}()
	r := &Runner{cancel: cancel, done: done}
	f.runner = r
	syncCtx, syncCancel := context.WithTimeout(ctx, cacheSyncTimeout)
	defer syncCancel()
	require.NoErrorf(t, waitForManagerCache(syncCtx, r, mgr.GetCache().WaitForCacheSync),
		"manager cache did not sync within %s", cacheSyncTimeout)
}

func waitForManagerCache(ctx context.Context, r *Runner, waitForSync func(context.Context) bool) error {
	if waitForSync(ctx) {
		return nil
	}
	if err := r.Stop(); err != nil {
		return fmt.Errorf("cache sync failed and manager stop returned: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("cache sync failed: %w", err)
	}
	return errors.New("cache sync failed before context completed")
}
