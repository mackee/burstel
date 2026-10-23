package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/otlp"
)

func internalSpanBatch(n int) *Batch {
	spans := make([]otlp.Span, 0, n)
	for i := 0; i < n; i++ {
		spans = append(spans, otlp.Span{
			TraceID:            fmt.Sprintf("%032x", i),
			SpanID:             fmt.Sprintf("%016x", i),
			Name:               fmt.Sprintf("span-%d", i),
			StartTimeUnixNano:  uint64(i),
			EndTimeUnixNano:    uint64(i + 1),
			ServiceName:        "bench",
			ResourceAttributes: `{}`,
			ScopeAttributes:    `{}`,
			Attributes:         `{}`,
		})
	}
	return &Batch{
		ShotID:          "shot-atomic",
		Signal:          otlp.Traces,
		ReceivedAt:      time.Now().UTC(),
		ResourceSummary: `{}`,
		Payload:         []byte("raw-otlp-payload"),
		Spans:           spans,
	}
}

// TestInsertBatchRollsBackOnAppendFailure poisons the cached span appender so
// the projection rows fail after the raw payload row was already appended. The
// batch must roll back atomically: no raw row, no span rows, and the writer must
// recover on the next batch.
func TestInsertBatchRollsBackOnAppendFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "atomic.duckdb")
	st, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}

	st.mu.Lock()
	spans, err := st.appenderLocked(tableSpans)
	st.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := spans.Close(); err != nil {
		t.Fatal(err)
	}

	if err := st.InsertBatch(ctx, internalSpanBatch(4)); err == nil {
		t.Fatal("expected the poisoned appender to fail the batch")
	}

	// The writer must still work after the rollback.
	if err := st.InsertBatch(ctx, internalSpanBatch(4)); err != nil {
		t.Fatalf("insert after rollback: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for table, want := range map[string]int64{"otlp_batches": 1, "spans": 4} {
		var n int64
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != want {
			t.Errorf("%s rows = %d, want %d (failed batch leaked partial rows)", table, n, want)
		}
	}
}

// BenchmarkInsertBatch512RowAtATimeBaseline documents the *previous* ingest
// path (one prepared INSERT per row) so the appender speedup in spec 022 Part B
// can be measured on the same machine. It is not used by production code.
func BenchmarkInsertBatch512RowAtATimeBaseline(b *testing.B) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", filepath.Join(b.TempDir(), "baseline.duckdb"))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			b.Fatal(err)
		}
	}
	insertBatch, err := db.PrepareContext(ctx, `INSERT INTO otlp_batches
		(shot_id, ordinal, signal, received_at, content_encoding, record_count, payload_length, resource_summary, payload)
		VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer insertBatch.Close()
	insertSpan, err := db.PrepareContext(ctx, `INSERT INTO spans
		(shot_id, ordinal, trace_id, span_id, parent_span_id, trace_state, name, kind,
		 start_time_unix_nano, end_time_unix_nano, status_code, status_message,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 flags, dropped_attributes_count, dropped_events_count, dropped_links_count, events_count, links_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		b.Fatal(err)
	}
	defer insertSpan.Close()

	batch := internalSpanBatch(512)
	for i := 0; i < 8; i++ {
		writeRowAtATime(ctx, b, db, insertBatch, insertSpan, batch, int64(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writeRowAtATime(ctx, b, db, insertBatch, insertSpan, batch, int64(i+8)); err != nil {
			b.Fatal(err)
		}
	}
}

func writeRowAtATime(
	ctx context.Context,
	tb testing.TB,
	db *sql.DB,
	insertBatch, insertSpan *sql.Stmt,
	b *Batch,
	ordinal int64,
) error {
	tb.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.StmtContext(ctx, insertBatch).ExecContext(ctx,
		b.ShotID, ordinal, string(b.Signal), b.ReceivedAt, b.ContentEncoding,
		b.RecordCount(), int64(len(b.Payload)), b.ResourceSummary, b.Payload,
	); err != nil {
		return err
	}
	for _, sp := range b.Spans {
		if _, err := tx.StmtContext(ctx, insertSpan).ExecContext(ctx, anyArgs(spanArgs(b.ShotID, ordinal, sp))...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func anyArgs(values []driver.Value) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}
