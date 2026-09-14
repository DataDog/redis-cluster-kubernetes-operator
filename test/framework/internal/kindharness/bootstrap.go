// Package kind is a reusable, test-only KIND e2e harness shared across
// operators: it owns cluster lifecycle, namespace setup, and an
// in-process controller-runtime manager. Each operator supplies one
// Bootstrap in a seam package (apps/<op>/test/framework/kind).
package kind

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Bootstrap is the operator's plug-in seam — the only thing an adopting operator implements.
type Bootstrap interface {
	// Scheme returns the operator's CRD types plus any watched externals.
	Scheme() *runtime.Scheme
	// Register wires the operator's reconcilers onto mgr.
	Register(mgr ctrl.Manager) error
	// Ready reports whether the named CR reached its healthy state.
	Ready(ctx context.Context, c client.Client, key client.ObjectKey) (bool, error)
}
