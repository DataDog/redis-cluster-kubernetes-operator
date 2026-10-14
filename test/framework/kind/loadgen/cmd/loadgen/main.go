// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Loadgen drives rueidis traffic and exposes health, stats, and shutdown endpoints.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/rueidis"
)

const (
	defaultShardsRefreshInterval = time.Second
	// shutdownTimeout bounds graceful HTTP shutdown on SIGTERM/SIGINT.
	shutdownTimeout = 5 * time.Second
)

func main() {
	addrs := flag.String("addrs", os.Getenv("LOADGEN_INIT_ADDRS"),
		"comma-separated Redis cluster seed addresses (host:port,host:port)")
	listen := flag.String("listen", ":8080", "HTTP control-surface listen address")
	startQPS := flag.Int("qps", 200, "target QPS across all workers")
	startWorkers := flag.Int("workers", 2, "worker count")
	keyPrefix := flag.String("key-prefix", "loadgen", "key prefix for all SET/GET ops")
	password := flag.String("password", os.Getenv("LOADGEN_PASSWORD"),
		"Redis ACL password (empty disables auth)")
	shardsRefreshInterval := flag.Duration("shards-refresh-interval",
		envDuration("LOADGEN_SHARDS_REFRESH_INTERVAL", defaultShardsRefreshInterval),
		"rueidis cluster topology refresh interval")
	flag.Parse()

	if *addrs == "" {
		log.Fatal("at least one Redis seed address is required (use -addrs or LOADGEN_INIT_ADDRS)")
	}
	seeds := strings.Split(*addrs, ",")

	opt := rueidis.ClientOption{
		InitAddress: seeds,
		// rueidis defaults to RESP3; production Redis 8.6 supports it.
		// SelectDB is unset (no SELECT in cluster mode).
		ShuffleInit: true,
		ClusterOption: rueidis.ClusterOption{
			ShardsRefreshInterval: *shardsRefreshInterval,
		},
	}
	if *password != "" {
		opt.Password = *password
		opt.Username = "toolbox"
	}

	client, err := rueidis.NewClient(opt)
	if err != nil {
		log.Fatalf("rueidis: %v", err)
	}
	defer client.Close()

	g := &generator{
		client:    client,
		keyPrefix: *keyPrefix,
		qps:       int64(*startQPS),
		workers:   int64(*startWorkers),
		written:   make(map[string]string, 4096),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopCh := make(chan struct{})
	go g.run(ctx, stopCh)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/stats", g.handleStats)
	mux.HandleFunc("/stop", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("stopping"))
		go func() {
			cancel()
			<-stopCh
			os.Exit(0)
		}()
	})

	// Bound requests from a broken port-forward or client.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		shutdownCtx, sc := context.WithTimeout(context.Background(), shutdownTimeout)
		defer sc()
		_ = srv.Shutdown(shutdownCtx)
		cancel()
	}()

	log.Printf("loadgen: listening on %s, seeds=%v workers=%d qps=%d shards_refresh_interval=%s",
		*listen, seeds, *startWorkers, *startQPS, shardsRefreshInterval.String())
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server: %v", err)
	}
	<-stopCh
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return fallback
}

// generator drives the workload. Counters are atomic so the HTTP
// handlers can read them without coordinating with the worker pool.
type generator struct {
	client    rueidis.Client
	keyPrefix string
	qps       int64
	workers   int64

	setsOK   atomic.Int64
	setsFail atomic.Int64
	getsOK   atomic.Int64
	getsFail atomic.Int64
	mismatch atomic.Int64
	lastErr  atomic.Pointer[string]

	mu      sync.Mutex
	written map[string]string // key → expected value (for GET verification)
}

// Stats is the JSON shape returned by GET /stats.
type Stats struct {
	QPS      int64  `json:"qps"`
	Workers  int64  `json:"workers"`
	SetsOK   int64  `json:"sets_ok"`
	SetsFail int64  `json:"sets_fail"`
	GetsOK   int64  `json:"gets_ok"`
	GetsFail int64  `json:"gets_fail"`
	Mismatch int64  `json:"mismatch"`
	LastErr  string `json:"last_err,omitempty"`
}

func (g *generator) snapshot() Stats {
	s := Stats{
		QPS:      g.qps,
		Workers:  g.workers,
		SetsOK:   g.setsOK.Load(),
		SetsFail: g.setsFail.Load(),
		GetsOK:   g.getsOK.Load(),
		GetsFail: g.getsFail.Load(),
		Mismatch: g.mismatch.Load(),
	}
	if p := g.lastErr.Load(); p != nil {
		s.LastErr = *p
	}
	return s
}

func (g *generator) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	var workers sync.WaitGroup
	for i := int64(0); i < g.workers; i++ {
		workers.Add(1)
		go g.runWorker(ctx, &workers, i)
	}
	<-ctx.Done()
	workers.Wait()
}

func (g *generator) runWorker(ctx context.Context, wg *sync.WaitGroup, workerID int64) {
	defer wg.Done()
	workers := g.workers
	if workers <= 0 {
		workers = 1
	}
	perWorker := g.qps / workers
	if perWorker <= 0 {
		perWorker = 1
	}
	ticker := time.NewTicker(time.Second / time.Duration(perWorker))
	defer ticker.Stop()
	var counter int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		counter++
		// Run the configured workload mix:
		//   70% GET, 25% SET, 5% MGET.
		// Implementation: bucket the [0,100) random integer; mix is
		// driven by per-op math.rand to avoid a shared RNG mutex.
		switch r := mathrand.IntN(100); {
		case r < 70:
			g.doGet(ctx)
		case r < 95:
			g.doSet(ctx, workerID*1_000_000+counter)
		default:
			g.doMGet(ctx)
		}
	}
}

// loadgenKeyTags is how many distinct hash tags SET keys spread across. Tagging
// by a rolling counter (not the worker id, which yields only `workers` tags, 2 by
// default) keeps every primary owning some loadgen slots, so a primary kill always
// drops a measurable share of traffic — otherwise a fault on a primary owning none
// of the loadgen's slots is invisible and the success-rate floor passes vacuously.
// MGET stays valid: doMGet still groups keys by their (now more numerous) hash tag.
const loadgenKeyTags = 256

func (g *generator) doSet(ctx context.Context, n int64) {
	key := fmt.Sprintf("%s:{k-%d}:%d", g.keyPrefix, n%loadgenKeyTags, n)
	val := randomValue()
	cmd := g.client.B().Set().Key(key).Value(val).Build()
	if err := g.client.Do(ctx, cmd).Error(); err != nil {
		g.setsFail.Add(1)
		g.recordErr("SET", err)
		return
	}
	g.mu.Lock()
	g.written[key] = val
	g.mu.Unlock()
	g.setsOK.Add(1)
}

func (g *generator) doGet(ctx context.Context) {
	g.mu.Lock()
	var key, want string
	for k, v := range g.written {
		key, want = k, v
		break
	}
	g.mu.Unlock()
	if key == "" {
		return // nothing to GET yet
	}
	cmd := g.client.B().Get().Key(key).Build()
	got, err := g.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			// Key may have been evicted; not a hard failure.
			g.getsFail.Add(1)
			g.recordErr("GET nil", err)
			return
		}
		g.getsFail.Add(1)
		g.recordErr("GET", err)
		return
	}
	if got != want {
		g.mismatch.Add(1)
		s := fmt.Sprintf("key=%s want=%q got=%q", key, want, got)
		g.lastErr.Store(&s)
		return
	}
	g.getsOK.Add(1)
}

// doMGet picks up to 16 keys from one hash slot and issues a single
// MGET. Redis Cluster rejects cross-slot multi-key commands, so the
// loadgen key format includes a hash tag and this selector keeps MGETs
// valid while still exercising cluster routing.
func (g *generator) doMGet(ctx context.Context) {
	g.mu.Lock()
	keys := make([]string, 0, 16)
	var tag string
	for k := range g.written {
		if tag == "" {
			tag = hashTag(k)
		}
		if hashTag(k) != tag {
			continue
		}
		keys = append(keys, k)
		if len(keys) == 16 {
			break
		}
	}
	g.mu.Unlock()
	if len(keys) == 0 {
		return
	}
	cmd := g.client.B().Mget().Key(keys...).Build()
	_, err := g.client.Do(ctx, cmd).ToArray()
	if err != nil {
		g.getsFail.Add(1)
		g.recordErr("MGET", err)
		return
	}
	g.getsOK.Add(1)
}

func hashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start == -1 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end == -1 {
		return key
	}
	return key[start : start+end+2]
}

func (g *generator) recordErr(op string, err error) {
	s := fmt.Sprintf("%s err=%v", op, err)
	g.lastErr.Store(&s)
}

func (g *generator) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, g.snapshot())
}

func randomValue() string {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		// crypto/rand should never fail; fall back to math/rand so the
		// loadgen keeps producing keys instead of dying mid-test.
		return fmt.Sprintf("%016x", mathrand.Uint64())
	}
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
