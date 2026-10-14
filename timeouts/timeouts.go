// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package timeouts

import "time"

// Config holds timeout values scaled based on cluster size
type Config struct {
	PollInterval          time.Duration // How often to check cluster state while waiting
	OperationTimeout      time.Duration // Max time for long operations (failover, add nodes, etc.)
	MaxMaintainIterations int           // Max fix operations per reconcile cycle
	ForgetRetries         int           // Number of times to retry FORGET commands
	ForgetRetryDelay      time.Duration // Sleep between FORGET retries
}

// NewConfig creates timeout configuration scaled for the given cluster size
func NewConfig(nodeCount int32) *Config {
	return &Config{
		PollInterval:          calculatePollInterval(nodeCount),
		OperationTimeout:      calculateOperationTimeout(nodeCount),
		MaxMaintainIterations: calculateMaxMaintainIterations(nodeCount),
		ForgetRetries:         calculateForgetRetries(nodeCount),
		ForgetRetryDelay:      calculateForgetRetryDelay(nodeCount),
	}
}

// calculatePollInterval: How often to poll cluster state
// Small clusters: gossip propagates quickly (10s)
// Large clusters: gossip takes longer (up to 30s)
// Formula: min(30s, max(5s, 10s + (nodeCount/20)*1s))
func calculatePollInterval(nodeCount int32) time.Duration {
	seconds := 10 + nodeCount/20
	if seconds < 5 {
		seconds = 5
	}
	if seconds > 30 {
		seconds = 30
	}
	return time.Duration(seconds) * time.Second
}

// calculateOperationTimeout: Total timeout for operations
// Small clusters: operations complete in ~2min
// Large clusters: need more time for gossip convergence
// Formula: min(10min, max(1min, 2min + (nodeCount/10)*10s))
func calculateOperationTimeout(nodeCount int32) time.Duration {
	seconds := 120 + (nodeCount/10)*10 // 2min base + scale with node count
	if seconds < 60 {
		seconds = 60 // min 1 minute
	}
	if seconds > 600 {
		seconds = 600 // max 10 minutes
	}
	return time.Duration(seconds) * time.Second
}

// calculateMaxMaintainIterations: Max fixes per reconcile cycle
// Small clusters: rarely need many fixes
// Large clusters: degraded state can have many issues
// Formula: min(100, max(10, nodeCount/3))
func calculateMaxMaintainIterations(nodeCount int32) int {
	iterations := int(nodeCount / 3)
	if iterations < 10 {
		iterations = 10
	}
	if iterations > 100 {
		iterations = 100
	}
	return iterations
}

// calculateForgetRetries: FORGET command retry attempts
// Small clusters: gossip settles quickly
// Large clusters: gossip re-propagates more
// Formula: min(10, max(3, 3 + nodeCount/30))
func calculateForgetRetries(nodeCount int32) int {
	retries := 3 + int(nodeCount/30)
	if retries < 3 {
		retries = 3
	}
	if retries > 10 {
		retries = 10
	}
	return retries
}

// calculateForgetRetryDelay: Delay between FORGET retries
// Small clusters: gossip propagates quickly (~1s)
// Large clusters: gossip takes longer (up to 5s)
// Formula: min(5s, max(500ms, 1s + (nodeCount/50)*1s))
func calculateForgetRetryDelay(nodeCount int32) time.Duration {
	milliseconds := 1000 + (nodeCount/50)*1000
	if milliseconds < 500 {
		milliseconds = 500
	}
	if milliseconds > 5000 {
		milliseconds = 5000
	}
	return time.Duration(milliseconds) * time.Millisecond
}
