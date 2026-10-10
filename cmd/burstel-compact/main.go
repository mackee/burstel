// Command burstel-compact runs the bounded, crash-recoverable compactor that
// turns the durable raw OTLP journal into immutable, checksummed DuckDB
// segments (spec 015).
//
// It is deliberately storage-agnostic: the compactor engine works against an
// R2-compatible object store. This entrypoint drives it against a filesystem
// mirror of the bucket (`-store-dir`), which is exactly the store the 60 s /
// 180 s trials use; wiring a production bucket is a matter of supplying an
// R2-backed ObjectStore, not of changing the engine.
//
// Typical short-lived Container invocation:
//
//	burstel-compact -store-dir /mnt/r2 -manifest /work/manifest.duckdb \
//	    -window 60s -grace 90s -retention 24h -gc
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mackee/burstel/internal/compaction"
)

func main() {
	var (
		storeDir  = flag.String("store-dir", "", "filesystem mirror of the R2 bucket (required)")
		manifest  = flag.String("manifest", "", "path to the durable manifest database (required)")
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
	)
	flag.Parse()

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
