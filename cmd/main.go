// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime/debug"

	cachev1 "github.com/DataDog/redis-cluster-kubernetes-operator/apiv1"
	"github.com/DataDog/redis-cluster-kubernetes-operator/mains"
	"github.com/DataDog/redis-cluster-kubernetes-operator/utils"
	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/features"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme    = runtime.NewScheme()
	manageCmd *flag.FlagSet
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(cachev1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func createLog() logr.Logger {
	opts := func(o *zap.Options) {
		o.Development = true
		o.DestWriter = os.Stdout
		// zapcore.TimeEncoder defaults to RFC3339TimeEncoder.
		// time.RFC3339Nano is "2006-01-02T15:04:05.999999999Z07:00"
		// Keep millisecond precision for consistent structured-log parsing.
		// see https://go.dev/src/time/format.go
		o.TimeEncoder = zapcore.TimeEncoderOfLayout("2006-01-02T15:04:05.000Z07:00")
	}
	return zap.New(opts)
}

func main() {
	// Crash handler: log stack trace on panic for debugging
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "FATAL: panic recovered: %v\n", r)
			fmt.Fprintf(os.Stderr, "Stack trace:\n%s\n", debug.Stack())
			os.Exit(1)
		}
	}()

	flag.CommandLine.Init(flag.CommandLine.Name(), flag.ContinueOnError)
	flag.CommandLine.Usage = func() {}

	manageCmd = flag.NewFlagSet("manage", flag.ContinueOnError)
	manageMetricsAddr := manageCmd.String("metrics-addr", ":8080", "The address the metric endpoint binds to.")
	manageHealthProbeAddr := manageCmd.String("health-probe-bind-address", ":8081", "The address the liveness and readiness endpoints bind to.")
	manageEnableLeaderElection := manageCmd.Bool("enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	manageCmd.Usage = func() {}

	cliCmd := flag.NewFlagSet("cli", flag.ContinueOnError)
	cliCmd.Usage = func() {}

	ctrl.SetLogger(createLog())
	log := ctrl.Log.WithName("main")
	log.Info("Operator started", "build_info", utils.BuildInfo())

	// Disable the WatchListClient feature gate
	disableWatchListClient(log)

	// note: top-level 'kubeconfig' flag added by k8

	err := flag.CommandLine.Parse(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = fmt.Fprintf(os.Stderr, "err: %s\n", err)
		}
		usageAndExit()
	}
	args := flag.Args()

	if len(args) < 1 {
		_, _ = fmt.Fprintf(os.Stderr, "err: missing subcommand\n")
		usageAndExit()
	}

	switch args[0] {
	case "manage":
		err := manageCmd.Parse(args[1:])
		if err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				_, _ = fmt.Fprintf(os.Stderr, "err: %s\n", err)
			}
			usageAndExit()
		}
		mains.Manage(scheme, *manageMetricsAddr, *manageHealthProbeAddr, *manageEnableLeaderElection, log)
	case "cli":
		err := cliCmd.Parse(args[1:])
		if err != nil {
			if !errors.Is(err, flag.ErrHelp) {
				_, _ = fmt.Fprintf(os.Stderr, "err: %s\n", err)
			}
			usageAndExit()
		}
		mains.Cli(scheme, "", log)
	default:
		_, _ = fmt.Fprintf(os.Stderr, "err: missing subcommand\n")
		usageAndExit()
	}
}

// watchListDisabledFeatureGates delegates every feature gate except
// WatchListClient, which is always disabled.
type watchListDisabledFeatureGates struct {
	features.Gates
}

func (g watchListDisabledFeatureGates) Enabled(feature features.Feature) bool {
	if feature == features.WatchListClient {
		return false
	}
	return g.Gates.Enabled(feature)
}

// disableWatchListClient turns off the WatchListClient feature gate before
// the manager creates informers.
//
// client-go v0.35+ defaults WatchListClient to enabled, which makes informers
// use WatchList (streaming WATCH for initial sync) instead of traditional
// LIST+WATCH. On several clusters the API server accepts the WatchList
// request but never sends the initial-events-end bookmark, causing the
// reflector to hang indefinitely waiting for it. The reflector's built-in
// fallback only triggers on watch errors, not on a missing bookmark, so
// cache sync times out:
//
//	failed to wait for <kind> caches to sync: timed out waiting for cache
//	to be synced for Kind *v1.RedisCluster
//
// which exits the controller and puts it into CrashLoopBackOff.
func disableWatchListClient(log logr.Logger) {
	features.ReplaceFeatureGates(watchListDisabledFeatureGates{
		Gates: features.FeatureGates(),
	})
	log.Info("disabled WatchListClient feature gate to ensure cache sync compatibility with CRDs")
}

func usageAndExit() {
	_, _ = fmt.Fprintf(os.Stdout, "usage: %s <common-flags> manage|cli ...\n", os.Args[0])
	_, _ = fmt.Fprintf(os.Stdout, "common-flags:\n")
	flag.PrintDefaults()
	_, _ = fmt.Fprintf(os.Stdout, "manage: Run the manager\n")
	manageCmd.PrintDefaults()
	_, _ = fmt.Fprintf(os.Stdout, "cli: Run the CLI\n")
	_, _ = fmt.Fprintf(os.Stdout, "  (no options)\n")
	os.Exit(1)
}
