// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package health

// ClusterHealth defines the cluster health enum.
type ClusterHealth int

// Unknown cluster health.
const Unknown ClusterHealth = 0

// OK the cluster is in good health.
const OK ClusterHealth = 1

// Warn the cluster has issues.
const Warn ClusterHealth = 2

// Error the cluster is broken.
const Error ClusterHealth = 3

// Pending the cluster is initializing (expected, not an error).
const Pending ClusterHealth = 4

func (h ClusterHealth) String() string {
	switch h {
	case Unknown:
		return "unknown"
	case OK:
		return "OK"
	case Warn:
		return "Warn"
	case Error:
		return "Error"
	case Pending:
		return "Pending"
	default:
		return "Unknown"
	}
}
