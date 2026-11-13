// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

package main

import (
	"context"
	"errors"
	mathrand "math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

const (
	// defaultInflight bounds concurrent open-loop requests; a backlog beyond it
	// still counts from each request's scheduled time.
	defaultInflight = 256
	// verifyWorkers is the /verify read-back parallelism.
	verifyWorkers = 32
	// maxErrorKindLen keeps per-second error kinds short; the full first message
	// is kept as the sample.
	maxErrorKindLen = 48
	// maxVerifySamples bounds missing-key samples in the /verify response.
	maxVerifySamples = 10
)

// errNothingWritten means a read had no acknowledged key to read yet.
var errNothingWritten = errors.New("no acknowledged key to read yet")

// runOpenLoop schedules requests at the target rate and records each request's
// latency from its scheduled time, so a stall shows as latency, not as fewer
// requests (no coordinated omission).
func (g *generator) runOpenLoop(ctx context.Context, done chan<- struct{}, inflight int) {
	defer close(done)
	qps := max(g.qps, 1)
	interval := time.Second / time.Duration(qps)
	jobs := make(chan scheduledOp, max(inflight, 1))

	var workers sync.WaitGroup
	for range max(inflight, 1) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for op := range jobs {
				g.runScheduled(ctx, op)
			}
		}()
	}
	defer func() {
		close(jobs)
		workers.Wait()
	}()

	start := time.Now()
	for n := int64(0); ; n++ {
		at := start.Add(time.Duration(n) * interval)
		time.Sleep(time.Until(at))
		select {
		case <-ctx.Done():
			return
		case jobs <- scheduledOp{seq: n, at: at}:
		}
	}
}

type scheduledOp struct {
	seq int64
	at  time.Time
}

// runScheduled runs the 70% GET / 25% SET / 5% MGET mix; reads fall back to a
// SET until a key has been acknowledged.
func (g *generator) runScheduled(ctx context.Context, op scheduledOp) {
	var err error
	switch r := mathrand.IntN(100); {
	case r < 70:
		err = g.doGet(ctx)
	case r >= 95:
		err = g.doMGet(ctx)
	default:
		err = errNothingWritten
	}
	if errors.Is(err, errNothingWritten) {
		err = g.doSet(ctx, op.seq)
	}
	g.timeline.record(op.at, time.Since(op.at), err)
}

// timeline aggregates open-loop results by scheduled second.
type timeline struct {
	mu      sync.Mutex
	seconds map[int64]*secondBucket
}

type secondBucket struct {
	ok, fail  int64
	latencies []time.Duration
	errors    map[string]string
}

// TimelineRow is one scheduled second in GET /timeline.
type TimelineRow struct {
	Unix   int64             `json:"t"`
	OK     int64             `json:"ok"`
	Fail   int64             `json:"fail"`
	P50Ms  float64           `json:"p50_ms"`
	P99Ms  float64           `json:"p99_ms"`
	MaxMs  float64           `json:"max_ms"`
	Errors map[string]string `json:"errors,omitempty"`
}

func newTimeline() *timeline {
	return &timeline{seconds: make(map[int64]*secondBucket)}
}

func (tl *timeline) record(scheduled time.Time, latency time.Duration, err error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	sec := scheduled.Unix()
	b := tl.seconds[sec]
	if b == nil {
		b = &secondBucket{}
		tl.seconds[sec] = b
	}
	b.latencies = append(b.latencies, latency)
	if err == nil {
		b.ok++
		return
	}
	b.fail++
	if b.errors == nil {
		b.errors = make(map[string]string)
	}
	kind := errorKind(err)
	if _, seen := b.errors[kind]; !seen {
		b.errors[kind] = err.Error()
	}
}

// rows returns every recorded second in time order.
func (tl *timeline) rows() []TimelineRow {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	out := make([]TimelineRow, 0, len(tl.seconds))
	for sec, b := range tl.seconds {
		lat := slices.Clone(b.latencies)
		slices.Sort(lat)
		out = append(out, TimelineRow{
			Unix:   sec,
			OK:     b.ok,
			Fail:   b.fail,
			P50Ms:  quantileMs(lat, 0.50),
			P99Ms:  quantileMs(lat, 0.99),
			MaxMs:  quantileMs(lat, 1),
			Errors: b.errors,
		})
	}
	slices.SortFunc(out, func(a, b TimelineRow) int { return int(a.Unix - b.Unix) })
	return out
}

// quantileMs returns the nearest-rank quantile of sorted latencies.
func quantileMs(sorted []time.Duration, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q*float64(len(sorted))+0.5) - 1
	i = min(max(i, 0), len(sorted)-1)
	return float64(sorted[i]) / float64(time.Millisecond)
}

// errorKind is the error text before its first detail (separator or digit), so
// messages that differ only by key, host or address share one kind.
func errorKind(err error) string {
	kind := err.Error()
	if i := strings.IndexAny(kind, ":(0123456789"); i > 0 {
		kind = kind[:i]
	}
	if len(kind) > maxErrorKindLen {
		kind = kind[:maxErrorKindLen]
	}
	return strings.TrimSpace(kind)
}

// VerifyResult is the GET /verify shape: acknowledged SETs read back.
type VerifyResult struct {
	Checked  int      `json:"checked"`
	Missing  int      `json:"missing"`
	Mismatch int      `json:"mismatch"`
	Errors   int      `json:"errors"`
	Samples  []string `json:"samples,omitempty"`
}

// verify reads back every acknowledged SET. Callers stop load first so the
// set of acknowledged keys is final.
func (g *generator) verify(ctx context.Context) VerifyResult {
	g.mu.Lock()
	want := make(map[string]string, len(g.written))
	for k, v := range g.written {
		want[k] = v
	}
	g.mu.Unlock()

	keys := make(chan string)
	var (
		mu  sync.Mutex
		res = VerifyResult{Checked: len(want)}
		wg  sync.WaitGroup
	)
	for range verifyWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keys {
				got, err := g.client.Do(ctx, g.client.B().Get().Key(key).Build()).ToString()
				mu.Lock()
				switch {
				case rueidis.IsRedisNil(err):
					res.Missing++
					if len(res.Samples) < maxVerifySamples {
						res.Samples = append(res.Samples, "missing "+key)
					}
				case err != nil:
					res.Errors++
				case got != want[key]:
					res.Mismatch++
				}
				mu.Unlock()
			}
		}()
	}
	for key := range want {
		keys <- key
	}
	close(keys)
	wg.Wait()
	return res
}

func (g *generator) handleTimeline(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, g.timeline.rows())
}

func (g *generator) handleVerify(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, g.verify(r.Context()))
}
