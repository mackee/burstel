package storage

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCheckpointIfNeededBoundsWAL proves the spec-024 R4 guard: a WAL below the
// configured bound is left alone, and a WAL at/above it is checkpointed on the
// writer connection without losing committed rows.
func TestCheckpointIfNeededBoundsWAL(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal.duckdb")
	st, err := OpenWithOptions(ctx, path, Options{
		WALCheckpointBytes: 1 << 30, // effectively disabled for the first write
		WALPollInterval:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if err := st.InsertBatch(ctx, internalSpanBatch(50)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	info, err := os.Stat(st.walPath)
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	before := info.Size()
	if before == 0 {
		t.Fatal("expected a non-empty WAL after a committed batch")
	}

	// Still below the (1 GiB) bound: no checkpoint.
	checked, err := st.CheckpointIfNeeded(ctx)
	if err != nil {
		t.Fatalf("checkpoint check: %v", err)
	}
	if checked {
		t.Fatal("checkpointed while the WAL was below the configured bound")
	}

	// Lower the bound below the current WAL: the next check must checkpoint.
	st.mu.Lock()
	st.walCheckpointBytes = before / 2
	st.mu.Unlock()
	checked, err = st.CheckpointIfNeeded(ctx)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if !checked {
		t.Fatal("expected a checkpoint at/above the configured bound")
	}
	info, err = os.Stat(st.walPath)
	after := int64(0)
	switch {
	case err == nil:
		after = info.Size()
	case errors.Is(err, os.ErrNotExist):
		after = 0 // DuckDB may unlink the WAL after a full checkpoint
	default:
		t.Fatalf("stat wal after checkpoint: %v", err)
	}
	if after >= before {
		t.Fatalf("wal size after checkpoint = %d, want < %d", after, before)
	}

	var n int64
	if err := st.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM spans").Scan(&n); err != nil {
		t.Fatalf("count after checkpoint: %v", err)
	}
	if n != 50 {
		t.Fatalf("spans after checkpoint = %d, want 50", n)
	}
}

// TestWALCheckpointLoopRunsInBackground verifies the guard is actually armed
// without any explicit Checkpoint call.
func TestWALCheckpointLoopRunsInBackground(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal-loop.duckdb")
	st, err := OpenWithOptions(ctx, path, Options{
		WALCheckpointBytes: 1,
		WALPollInterval:    10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if err := st.InsertBatch(ctx, internalSpanBatch(25)); err != nil {
		t.Fatalf("insert: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		info, statErr := os.Stat(st.walPath)
		if errors.Is(statErr, os.ErrNotExist) {
			break // a full CHECKPOINT removed the WAL file
		}
		if statErr == nil && info.Size() < 1 {
			break // CHECKPOINT truncated the WAL
		}
		if time.Now().After(deadline) {
			size := int64(-1)
			if info != nil {
				size = info.Size()
			}
			t.Fatalf("background WAL checkpoint did not run (size=%d, err=%v)", size, statErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int64
	if err := db.QueryRow("SELECT COUNT(*) FROM spans").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 25 {
		t.Fatalf("spans after background checkpoint = %d, want 25", n)
	}
}
