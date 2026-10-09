// Package storage owns the per-shot DuckDB connection: schema management,
// durable writes of raw OTLP batches plus their relational projections, and
// the checkpoint/close sequence that freezes a shot database.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/otlp"
)

// ErrClosed is returned when a write is attempted after Close.
var ErrClosed = errors.New("storage: closed")

// Batch is one accepted OTLP export request ready to be persisted. Payload is
// the decompressed protobuf request bytes; the projection slices are derived
// from it by the otlp package.
type Batch struct {
	ShotID          string
	Signal          otlp.Signal
	ReceivedAt      time.Time
	ContentEncoding string
	ResourceSummary string
	Payload         []byte
	Spans           []otlp.Span
	LogRecords      []otlp.LogRecord
	MetricPoints    []otlp.MetricPoint
}

// RecordCount returns the number of logical records carried by the batch.
func (b *Batch) RecordCount() int64 {
	switch b.Signal {
	case otlp.Traces:
		return int64(len(b.Spans))
	case otlp.Logs:
		return int64(len(b.LogRecords))
	case otlp.Metrics:
		return int64(len(b.MetricPoints))
	default:
		return 0
	}
}

// Storage is a single-writer DuckDB wrapper. DuckDB itself serializes writers;
// we additionally cap the sql.DB pool at one connection and guard ordinal
// assignment with a mutex so ordinal numbers are gap-free per process run.
type Storage struct {
	db   *sql.DB
	path string

	mu          sync.Mutex
	nextOrdinal int64
	closed      bool

	insertBatch  *sql.Stmt
	insertSpan   *sql.Stmt
	insertLog    *sql.Stmt
	insertMetric *sql.Stmt
}

// Open creates (or opens) the DuckDB file at dbPath, applies the schema and
// prepares the write statements.
func Open(ctx context.Context, dbPath string) (*Storage, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", dbPath, err)
	}
	// DuckDB allows a single writer. Serialising the pool also makes the
	// CHECKPOINT/close sequence deterministic.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: ping %s: %w", dbPath, err)
	}

	s := &Storage{db: db, path: dbPath}
	for _, stmt := range schemaStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			s.Close()
			return nil, fmt.Errorf("storage: apply schema: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, SchemaVersion); err != nil {
		s.Close()
		return nil, fmt.Errorf("storage: write schema version: %w", err)
	}

	if err := s.prepare(ctx); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Storage) prepare(ctx context.Context) error {
	var err error
	if s.insertBatch, err = s.db.PrepareContext(ctx, `INSERT INTO otlp_batches
		(shot_id, ordinal, signal, received_at, content_encoding, record_count, payload_length, resource_summary, payload)
		VALUES (?,?,?,?,?,?,?,?,?)`); err != nil {
		return fmt.Errorf("storage: prepare batch insert: %w", err)
	}
	if s.insertSpan, err = s.db.PrepareContext(ctx, `INSERT INTO spans
		(shot_id, ordinal, trace_id, span_id, parent_span_id, trace_state, name, kind,
		 start_time_unix_nano, end_time_unix_nano, status_code, status_message,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 flags, dropped_attributes_count, dropped_events_count, dropped_links_count, events_count, links_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); err != nil {
		return fmt.Errorf("storage: prepare span insert: %w", err)
	}
	if s.insertLog, err = s.db.PrepareContext(ctx, `INSERT INTO log_records
		(shot_id, ordinal, time_unix_nano, observed_time_unix_nano, severity_number, severity_text, body,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 trace_id, span_id, event_name, flags, dropped_attributes_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); err != nil {
		return fmt.Errorf("storage: prepare log insert: %w", err)
	}
	if s.insertMetric, err = s.db.PrepareContext(ctx, `INSERT INTO metric_points
		(shot_id, ordinal, metric_name, metric_type, metric_unit, metric_description,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 start_time_unix_nano, time_unix_nano, flags, value_double, value_int,
		 aggregation_temporality, is_monotonic, count, sum, min, max, bucket_counts, explicit_bounds, detail)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`); err != nil {
		return fmt.Errorf("storage: prepare metric insert: %w", err)
	}
	return nil
}

// Path returns the on-disk DuckDB file path.
func (s *Storage) Path() string { return s.path }

// InsertBatch durably writes the raw batch and its projections in one
// transaction. A nil return means the data is committed to the DuckDB WAL and
// will survive CHECKPOINT/close (and a crash, given DuckDB's WAL).
func (s *Storage) InsertBatch(ctx context.Context, b *Batch) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	ordinal := s.nextOrdinal
	s.nextOrdinal++
	s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	receivedAt := b.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}
	if _, err := tx.StmtContext(ctx, s.insertBatch).ExecContext(ctx,
		b.ShotID, ordinal, string(b.Signal), receivedAt.UTC(), b.ContentEncoding,
		b.RecordCount(), int64(len(b.Payload)), b.ResourceSummary, b.Payload,
	); err != nil {
		return fmt.Errorf("storage: insert batch: %w", err)
	}

	switch b.Signal {
	case otlp.Traces:
		stmt := tx.StmtContext(ctx, s.insertSpan)
		for _, sp := range b.Spans {
			if _, err := stmt.ExecContext(ctx, spanArgs(b.ShotID, ordinal, sp)...); err != nil {
				return fmt.Errorf("storage: insert span: %w", err)
			}
		}
	case otlp.Logs:
		stmt := tx.StmtContext(ctx, s.insertLog)
		for _, lr := range b.LogRecords {
			if _, err := stmt.ExecContext(ctx, logArgs(b.ShotID, ordinal, lr)...); err != nil {
				return fmt.Errorf("storage: insert log: %w", err)
			}
		}
	case otlp.Metrics:
		stmt := tx.StmtContext(ctx, s.insertMetric)
		for _, mp := range b.MetricPoints {
			if _, err := stmt.ExecContext(ctx, metricArgs(b.ShotID, ordinal, mp)...); err != nil {
				return fmt.Errorf("storage: insert metric point: %w", err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit: %w", err)
	}
	return nil
}

// Checkpoint flushes the WAL into the main database file. It must be called
// after all writers have drained and before Close.
func (s *Storage) Checkpoint(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, err := s.db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("storage: checkpoint: %w", err)
	}
	return nil
}

// Close releases prepared statements and the underlying connection. It is safe
// to call more than once.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for _, stmt := range []*sql.Stmt{s.insertBatch, s.insertSpan, s.insertLog, s.insertMetric} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	return s.db.Close()
}

func spanArgs(shotID string, ordinal int64, sp otlp.Span) []any {
	return []any{
		shotID, ordinal,
		sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.TraceState, sp.Name, sp.Kind,
		int64(sp.StartTimeUnixNano), int64(sp.EndTimeUnixNano),
		sp.StatusCode, sp.StatusMessage,
		sp.ServiceName, sp.ScopeName, sp.ScopeVersion,
		sp.ResourceAttributes, sp.ScopeAttributes, sp.Attributes,
		int64(sp.Flags), int64(sp.DroppedAttributesCount), int64(sp.DroppedEventsCount),
		int64(sp.DroppedLinksCount), int64(sp.EventsCount), int64(sp.LinksCount),
	}
}

func logArgs(shotID string, ordinal int64, lr otlp.LogRecord) []any {
	return []any{
		shotID, ordinal,
		int64(lr.TimeUnixNano), int64(lr.ObservedTimeUnixNano),
		lr.SeverityNumber, lr.SeverityText, lr.Body,
		lr.ServiceName, lr.ScopeName, lr.ScopeVersion,
		lr.ResourceAttributes, lr.ScopeAttributes, lr.Attributes,
		lr.TraceID, lr.SpanID, lr.EventName,
		int64(lr.Flags), int64(lr.DroppedAttributesCount),
	}
}

func metricArgs(shotID string, ordinal int64, mp otlp.MetricPoint) []any {
	return []any{
		shotID, ordinal,
		mp.MetricName, mp.MetricType, mp.MetricUnit, mp.MetricDescription,
		mp.ServiceName, mp.ScopeName, mp.ScopeVersion,
		mp.ResourceAttributes, mp.ScopeAttributes, mp.Attributes,
		int64(mp.StartTimeUnixNano), int64(mp.TimeUnixNano), int64(mp.Flags),
		float64Ptr(mp.ValueDouble), int64Ptr(mp.ValueInt),
		int64(mp.AggregationTemporality), mp.IsMonotonic,
		uint64Ptr(mp.Count), float64Ptr(mp.Sum), float64Ptr(mp.Min), float64Ptr(mp.Max),
		mp.BucketCounts, mp.ExplicitBounds, mp.Detail,
	}
}

func float64Ptr(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

func int64Ptr(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func uint64Ptr(v *uint64) any {
	if v == nil {
		return nil
	}
	return int64(*v)
}
