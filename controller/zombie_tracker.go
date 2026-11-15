// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package controller

import (
	"maps"
	"sync"
	"time"

	"github.com/DataDog/redis-cluster-kubernetes-operator/redis_cluster"
)

// zombieFirstSeenTracker remembers when each zombie node ID was first seen, per
// cluster. It is process-local: losing it on restart only restarts the wait.
type zombieFirstSeenTracker struct {
	mu        sync.Mutex
	firstSeen map[string]map[string]time.Time // cluster key -> node ID -> first seen
}

// Observe records the zombie IDs seen now, drops IDs no longer seen, and
// returns when each current ID was first seen.
func (t *zombieFirstSeenTracker) Observe(clusterKey string, ids []string, now time.Time) map[string]time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ids) == 0 {
		delete(t.firstSeen, clusterKey)
		return map[string]time.Time{}
	}
	if t.firstSeen == nil {
		t.firstSeen = make(map[string]map[string]time.Time)
	}
	previous := t.firstSeen[clusterKey]
	current := make(map[string]time.Time, len(ids))
	for _, id := range ids {
		if seen, ok := previous[id]; ok {
			current[id] = seen
		} else {
			current[id] = now
		}
	}
	t.firstSeen[clusterKey] = current
	return maps.Clone(current)
}

func (r *RedisClusterReconciler) zombieTracker() *zombieFirstSeenTracker {
	if r.zombieFirstSeen == nil {
		r.zombieFirstSeen = &zombieFirstSeenTracker{}
	}
	return r.zombieFirstSeen
}

func zombieTrackerKey(cluster *redis_cluster.Cluster) string {
	if cluster.RedisCluster == nil {
		return ""
	}
	return cluster.RedisCluster.Namespace + "/" + cluster.RedisCluster.Name
}
