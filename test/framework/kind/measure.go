// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package kind

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/DataDog/redis-cluster-kubernetes-operator/test/framework/kind/loadgen"
)

// MeasureReport is one measured run: client-observed impact of a fault window.
type MeasureReport struct {
	Scenario        string    `json:"scenario"`
	Run             int       `json:"run"`
	Primaries       int32     `json:"primaries"`
	TargetPrimaries int32     `json:"target_primaries"`
	QPS             int       `json:"qps"`
	SeedKeys        int       `json:"seed_keys"`
	Commit          string    `json:"commit,omitempty"`
	FaultStart      time.Time `json:"fault_start"`
	FaultEnd        time.Time `json:"fault_end"`

	// UnavailableSeconds spans the first to the last second with a failed request.
	UnavailableSeconds int64   `json:"unavailable_seconds"`
	Scheduled          int64   `json:"scheduled"`
	Failed             int64   `json:"failed"`
	ErrorRate          float64 `json:"error_rate"`
	BaselineP99Ms      float64 `json:"baseline_p99_ms"`
	FaultP99Ms         float64 `json:"fault_p99_ms"`
	FaultMaxMs         float64 `json:"fault_max_ms"`
	AckedLost          int     `json:"acked_lost"`

	Errors   map[string]string     `json:"errors,omitempty"`
	Verify   loadgen.VerifyResult  `json:"verify"`
	Timeline []loadgen.TimelineRow `json:"timeline"`
}

// AnalyzeFault summarises the timeline inside [start, end]. Baseline p99 is
// the median per-second p99 before start.
func AnalyzeFault(rows []loadgen.TimelineRow, start, end time.Time) MeasureReport {
	r := MeasureReport{FaultStart: start, FaultEnd: end, Timeline: rows}
	var baseline []float64
	firstFail, lastFail := int64(0), int64(0)
	for _, row := range rows {
		if row.Unix < start.Unix() {
			baseline = append(baseline, row.P99Ms)
			continue
		}
		if row.Unix > end.Unix() {
			continue
		}
		r.Scheduled += row.OK + row.Fail
		r.Failed += row.Fail
		r.FaultP99Ms = max(r.FaultP99Ms, row.P99Ms)
		r.FaultMaxMs = max(r.FaultMaxMs, row.MaxMs)
		if row.Fail == 0 {
			continue
		}
		if firstFail == 0 {
			firstFail = row.Unix
		}
		lastFail = row.Unix
		for kind, sample := range row.Errors {
			if r.Errors == nil {
				r.Errors = map[string]string{}
			}
			if _, seen := r.Errors[kind]; !seen {
				r.Errors[kind] = sample
			}
		}
	}
	if firstFail != 0 {
		r.UnavailableSeconds = lastFail - firstFail + 1
	}
	if r.Scheduled > 0 {
		r.ErrorRate = float64(r.Failed) / float64(r.Scheduled)
	}
	if len(baseline) > 0 {
		slices.Sort(baseline)
		r.BaselineP99Ms = baseline[len(baseline)/2]
	}
	return r
}

// Summary is the one-line form logged after each run.
func (r MeasureReport) Summary() string {
	return fmt.Sprintf("%s run %d: unavailable %ds, errors %d/%d (%.2f%%), p99 %.1f→%.1f ms, max %.1f ms, acked lost %d",
		r.Scenario, r.Run, r.UnavailableSeconds, r.Failed, r.Scheduled, 100*r.ErrorRate,
		r.BaselineP99Ms, r.FaultP99Ms, r.FaultMaxMs, r.AckedLost)
}

// WriteMeasureReport writes the report as JSON under dir.
func WriteMeasureReport(t *testing.T, dir string, r MeasureReport) string {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	name := fmt.Sprintf("%s-%dp-run%d.json", strings.ToLower(r.Scenario), r.Primaries, r.Run)
	path := filepath.Join(dir, name)
	data, err := json.MarshalIndent(r, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path
}

// SlotMigrationInProgress reports whether any primary shows an open
// importing or migrating slot.
func SlotMigrationInProgress(nodes []ClusterNode) bool {
	for _, node := range nodes {
		for _, token := range node.Slots {
			if strings.HasPrefix(token, "[") {
				return true
			}
		}
	}
	return false
}

// BusiestPrimaryPod returns the pod of the live primary owning the most slots.
func (s *Suite) BusiestPrimaryPod(t *testing.T, name string) string {
	t.Helper()
	nodes, _, err := s.TryClusterView()
	require.NoError(t, err)
	pods, err := s.tryRedisPodList(name)
	require.NoError(t, err)
	best, bestSlots := "", -1
	for _, node := range nodes {
		if !node.IsMaster || node.Failed || node.OwnedSlots() <= bestSlots {
			continue
		}
		for _, pod := range pods {
			if pod.Status.PodIP == node.IP {
				best, bestSlots = pod.Name, node.OwnedSlots()
			}
		}
	}
	require.NotEmpty(t, best, "no live primary pod owns slots")
	return best
}
