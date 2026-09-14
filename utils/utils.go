package utils

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

// BuildInfo returns commit SHA, commit time, and Go version of the running
// binary. Fields fall back to "unknown" if build info is missing (e.g. `go run`).
func BuildInfo() map[string]string {
	info := map[string]string{
		"go_version": runtime.Version(),
		"revision":   "unknown",
		"vcs_time":   "unknown",
		"modified":   "unknown",
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			info["revision"] = s.Value
		case "vcs.time":
			info["vcs_time"] = s.Value
		case "vcs.modified":
			info["modified"] = s.Value
		}
	}
	return info
}

// ContainsString determines whether an array of strings contains a specific string
func ContainsString(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// ContainsLine determines whether a multilines string contains a specified line
func ContainsLine(lines string, line string) bool {
	return ContainsString(strings.Split(lines, "\n"), line)
}

// Retry retries a function until it returns true, or exceeds the retry limit.
func Retry(f func() bool, retryLimit int, wait time.Duration) bool {
	retries := 0
	for {
		retries++

		if f() {
			break
		}

		if retries >= retryLimit {
			return false
		}

		time.Sleep(wait)
	}
	return true
}

// RetryWithBackoff retries a function with exponential backoff.
// - f: function to retry, returns error (nil = success)
// - isRetryable: checks if error is worth retrying
// - maxRetries: maximum number of attempts
// - baseWait: base wait time (multiplied by attempt number for backoff)
// - log: logger for retry messages (optional, can be nil)
func RetryWithBackoff(f func() error, isRetryable func(error) bool, maxRetries int, baseWait time.Duration, log logr.Logger) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := f()
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryable(err) {
			return err
		}

		if log.Enabled() {
			log.Info("Retrying due to transient error", "attempt", attempt, "maxRetries", maxRetries, "error", err.Error())
		}

		if attempt < maxRetries {
			time.Sleep(baseWait * time.Duration(attempt))
		}
	}
	return lastErr
}

// WrapErr wraps an error.
func WrapErr(e error, format string, a ...any) error {
	if e == nil {
		return nil
	}
	return fmt.Errorf(format+": %w", append(a, e)...)
}

// XLogger represents an extended logr.Logger that e.g. supports Debug level.
type XLogger struct {
	logr.Logger
}

// XLog extends a logr.Logger into an XLogger.
func XLog(log logr.Logger) XLogger {
	return XLogger{Logger: log}
}

// Debug logs a Debug level statement.
func (log *XLogger) Debug(msg string, keysAndValues ...any) {
	// from zapr's doc: "Zap uses semantically named levels for logging (DebugLevel, InfoLevel,
	// WarningLevel, ...). Logr uses arbitrary numeric levels. By default logr's V(0) is zap's
	// InfoLevel and V(1) is zap's DebugLevel (which is numerically -1)."
	log.V(1).Info(msg, keysAndValues...)
}
