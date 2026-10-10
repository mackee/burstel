// Command burstel-compact runs the bounded, crash-recoverable compactor that
// turns the durable raw OTLP journal into immutable, checksummed DuckDB
// segments (spec 015), and the production, DO-mediated compactor service
// (spec 018).
//
// It has two modes:
//
//   - Filesystem/trial mode (default): the engine runs against a filesystem
//     mirror of the bucket (`-store-dir`) plus a durable DuckDB manifest. This
//     is what the 60 s / 180 s trials and the failure-injection tests use.
//
//   - Service mode (`-serve`): a short-lived HTTP service the coordinator
//     Durable Object drives. It never talks to R2; the DO streams bounded raw
//     journal bodies in and streams the immutable segment bytes back out, so no
//     account-wide R2/S3 credential ever reaches the Container instance.
//
// Typical short-lived Container invocation:
//
//	burstel-compact -serve -addr :8080
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mackee/burstel/internal/compaction"
)

func main() {
	var (
		storeDir  = flag.String("store-dir", "", "filesystem mirror of the R2 bucket (required unless -serve)")
		manifest  = flag.String("manifest", "", "path to the durable manifest database (required unless -serve)")
		workDir   = flag.String("work-dir", "", "scratch directory for segment builds (default: temp)")
		window    = flag.Duration("window", 60*time.Second, "target receive-time segment window")
		grace     = flag.Duration("grace", 90*time.Second, "finalize watermark lag")
		retention = flag.Duration("retention", 24*time.Hour, "unreferenced journal retention (<=0 disables time-based GC)")
		loop      = flag.Duration("loop", 0, "if >0, run continuously with this interval")
		doGC      = flag.Bool("gc", false, "run garbage collection after compaction")
		forceGC   = flag.Bool("force-gc", false, "GC regardless of retention (respects shot pins)")
		reconcile = flag.Bool("reconcile", true, "reconcile the manifest against the bucket")
		repair    = flag.Bool("repair", false, "rebuild missing/damaged segments from the raw journal")
		asJSON    = flag.Bool("json", true, "emit a machine-readable report")
		serve     = flag.Bool("serve", false, "run the bounded DO-mediated compactor HTTP service")
		addr      = flag.String("addr", envString("BURSTEL_ADDR", ":8080"), "listen address for -serve")
	)
	flag.Parse()

	if *serve {
		runService(*addr, *workDir)
		return
	}

	if *storeDir == "" || *manifest == "" {
		fmt.Fprintln(os.Stderr, "burstel-compact: -store-dir and -manifest are required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := compaction.NewFakeR2(*storeDir)
	if err != nil {
		fatal(err)
	}
	m, err := compaction.OpenManifest(ctx, *manifest)
	if err != nil {
		fatal(err)
	}
	defer m.Close()

	comp := compaction.New(store, m, compaction.Config{
		Window:    *window,
		Grace:     *grace,
		Retention: *retention,
		WorkDir:   *workDir,
	})

	runOnce := func() error {
		stats, err := comp.Run(ctx)
		if err != nil {
			return fmt.Errorf("compact: %w", err)
		}
		report := map[string]any{"stats": stats, "calls": store.Calls()}
		if *reconcile {
			rep, err := comp.Reconcile(ctx)
			if err != nil {
				return fmt.Errorf("reconcile: %w", err)
			}
			report["reconcile"] = rep
		}
		if *repair {
			rep, err := comp.Repair(ctx)
			if err != nil {
				return fmt.Errorf("repair: %w", err)
			}
			report["repair"] = rep
		}
		if *doGC || *forceGC {
			gc, err := comp.GC(ctx, *forceGC)
			if err != nil {
				return fmt.Errorf("gc: %w", err)
			}
			report["gc"] = gc
		}
		totals, err := m.Totals(ctx)
		if err != nil {
			return err
		}
		report["totals"] = totals
		report["cost"] = compaction.DefaultCostModel().EstimateCost(store.Calls(), totals.CoveredBytes)
		return emit(report, *asJSON)
	}

	if *loop <= 0 {
		if err := runOnce(); err != nil {
			fatal(err)
		}
		return
	}
	ticker := time.NewTicker(*loop)
	defer ticker.Stop()
	for {
		if err := runOnce(); err != nil {
			fatal(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// runService starts the bounded compactor HTTP service used by the coordinator
// Durable Object (spec 018). It fails closed without BURSTEL_INTERNAL_TOKEN
// unless the explicit local-development opt-in is set.
func runService(addr, workDir string) {
	cfg := compaction.DefaultServiceConfig()
	cfg.InternalToken = os.Getenv("BURSTEL_INTERNAL_TOKEN")
	cfg.DevInsecureAuth = envBool("BURSTEL_DEV_INSECURE_AUTH")
	if workDir == "" {
		workDir = envString("BURSTEL_COMPACT_WORK_DIR", "")
	}
	cfg.WorkDir = workDir
	cfg.MaxBatchObjects = envInt("BURSTEL_COMPACT_MAX_BATCH_OBJECTS", cfg.MaxBatchObjects)
	cfg.MaxBatchBytes = envInt64("BURSTEL_COMPACT_MAX_BATCH_BYTES", cfg.MaxBatchBytes)
	cfg.MaxSegmentBytes = envInt64("BURSTEL_COMPACT_MAX_SEGMENT_BYTES", cfg.MaxSegmentBytes)
	cfg.SegmentTTL = envDuration("BURSTEL_COMPACT_SEGMENT_TTL", cfg.SegmentTTL)

	svc := compaction.NewService(cfg)
	srv := &http.Server{
		Addr:              addr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("burstel-compact: service listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal(err)
	}
}

func emit(report map[string]any, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(os.Stdout, "%+v\n", report)
	return nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "burstel-compact: %v\n", err)
	os.Exit(1)
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
