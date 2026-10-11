package kind

import (
	"io"
	"os"
	"sync"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
)

// VerboseEnv, when set to any non-empty value, keeps full controller-runtime and
// client-go logs instead of silencing them. See quietTestLogs.
const VerboseEnv = "KIND_E2E_VERBOSE"

var quietLogsOnce sync.Once

// quietTestLogs routes controller-runtime and client-go (klog) output to a
// discard sink, once per test binary. Each test runs its own manager whose
// informers are cancelled at teardown, so client-go's reflector otherwise floods
// stderr with "watch ended with error: context canceled" — one line per watched
// type per test. klog is separate from the manager's logr logger, so silencing it
// needs klog's own switches rather than a logr discard.
//
// Set KIND_E2E_VERBOSE=1 to keep the logs (routed through klog to stderr) when
// debugging a test.
func quietTestLogs() {
	quietLogsOnce.Do(func() {
		if os.Getenv(VerboseEnv) != "" {
			ctrl.SetLogger(klog.NewKlogr()) // controller-runtime -> klog -> stderr
			return
		}
		ctrl.SetLogger(logr.Discard())
		klog.LogToStderr(false)    // stop the default stderr path...
		klog.SetOutput(io.Discard) // ...and send the redirected output nowhere
	})
}
