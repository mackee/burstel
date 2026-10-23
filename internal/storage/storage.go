// Package storage owns the per-shot DuckDB connection: schema management,
// durable writes of raw OTLP batches plus their relational projections, and
// the checkpoint/close sequence that freezes a shot database.
package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

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

// Table names used by the DuckDB appender path.
const (
	tableBatches = "otlp_batches"
	tableSpans   = "spans"
	tableLogs    = "log_records"
	tableMetrics = "metric_points"
)

const (
	// DefaultWALCheckpointBytes bounds the standalone WAL at 256 MiB
	// (spec 024, R4). Below this size a crash-recovery replay stays short while
	// checkpoints remain infrequent enough not to perturb ingest.
	DefaultWALCheckpointBytes int64 = 256 << 20
	// defaultWALPollInterval is how often the background guard inspects the WAL.
	defaultWALPollInterval = 5 * time.Second
)

// Options tune the standalone writer. The zero value uses production defaults.
type Options struct {
	// WALCheckpointBytes triggers a background CHECKPOINT once the WAL reaches
	// this size. Values <= 0 use DefaultWALCheckpointBytes.
	WALCheckpointBytes int64
	// WALPollInterval is the size-check cadence. Values <= 0 use 5s.
	WALPollInterval time.Duration
}

// Storage is a single-writer DuckDB wrapper. DuckDB itself serializes writers;
// we additionally pin the database to exactly one connection, hold it for the
// lifetime of the Storage and serialise every batch behind one mutex so ordinal
// numbers stay gap-free and BEGIN/COMMIT never interleave.
//
// Writes use the DuckDB Appender (a chunk-based bulk path) instead of
// row-at-a-time prepared INSERTs. Every batch still runs inside one explicit
// transaction on the single writer connection, so the raw payload row and its
// projection rows commit atomically or not at all.
type Storage struct {
	db   *sql.DB
	conn *sql.Conn
	path string

	mu          sync.Mutex
	nextOrdinal int64
	closed      bool

	driverConn driver.Conn
	appenders  map[string]*duckdb.Appender

	walPath            string
	walCheckpointBytes int64

	stopOnce sync.Once
	stop     chan struct{}
	walWG    sync.WaitGroup
}

// Open creates (or opens) the DuckDB file at dbPath with the default WAL bound.
func Open(ctx context.Context, dbPath string) (*Storage, error) {
	return OpenWithOptions(ctx, dbPath, Options{})
}

// OpenWithOptions is Open with an explicit standalone WAL checkpoint bound.
func OpenWithOptions(ctx context.Context, dbPath string, opts Options) (*Storage, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", dbPath, err)
	}
	// DuckDB allows a single writer. Serialising the pool also makes the
	// CHECKPOINT/close sequence deterministic, and lets the appender and the
	// transaction share one session.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: connect %s: %w", dbPath, err)
	}
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		db.Close()
		return nil, fmt.Errorf("storage: ping %s: %w", dbPath, err)
	}

	s := &Storage{db: db, conn: conn, path: dbPath, appenders: map[string]*duckdb.Appender{}}
	walBytes := opts.WALCheckpointBytes
	if walBytes <= 0 {
		walBytes = DefaultWALCheckpointBytes
	}
	poll := opts.WALPollInterval
	if poll <= 0 {
		poll = defaultWALPollInterval
	}
	s.walPath = dbPath + ".wal"
	s.walCheckpointBytes = walBytes
	for _, stmt := range schemaStatements {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			s.Close()
			return nil, fmt.Errorf("storage: apply schema: %w", err)
		}
	}
	// Ingest must stay predictable: DuckDB's default 16MB WAL auto-checkpoint
	// blocks a commit for hundreds of milliseconds, which overflows an OTLP
	// exporter's batch queue and silently drops spans. Push the checkpoint to
	// the explicit Checkpoint at Stop instead (the WAL still guarantees
	// durability of every acknowledged batch).
	if _, err := conn.ExecContext(ctx, `SET wal_autocheckpoint='1GB'`); err != nil {
		s.Close()
		return nil, fmt.Errorf("storage: configure WAL checkpoint threshold: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		`INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
		 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, SchemaVersion); err != nil {
		s.Close()
		return nil, fmt.Errorf("storage: write schema version: %w", err)
	}

	if err := s.prepare(ctx); err != nil {
		s.Close()
		return nil, err
	}
	s.stop = make(chan struct{})
	s.walWG.Add(1)
	go s.runWALCheckpointLoop(poll)
	return s, nil
}

// prepare resolves the raw driver connection the DuckDB appender needs. The
// appenders themselves are created lazily per table on first use.
func (s *Storage) prepare(_ context.Context) error {
	if err := s.conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("storage: unexpected driver connection type %T", dc)
		}
		s.driverConn = driverConn
		return nil
	}); err != nil {
		return fmt.Errorf("storage: resolve driver connection: %w", err)
	}
	return nil
}

// Path returns the on-disk DuckDB file path.
func (s *Storage) Path() string { return s.path }

// appenderLocked returns the cached appender for table, creating it on first
// use. Callers must hold s.mu and must not keep the appender across Close.
func (s *Storage) appenderLocked(table string) (*duckdb.Appender, error) {
	if a, ok := s.appenders[table]; ok {
		return a, nil
	}
	a, err := duckdb.NewAppenderFromConn(s.driverConn, "", table)
	if err != nil {
		return nil, fmt.Errorf("storage: open appender for %s: %w", table, err)
	}
	s.appenders[table] = a
	return a, nil
}

// dropAppendersLocked discards every cached appender. A failed append leaves an
// appender in an invalidated state, so the next batch starts from a fresh one.
func (s *Storage) dropAppendersLocked() {
	for table, a := range s.appenders {
		_ = a.Close()
		delete(s.appenders, table)
	}
}

// InsertBatch durably writes the raw batch and its projections in one
// transaction. A nil return means the data is committed to the DuckDB WAL and
// will survive CHECKPOINT/close (and a crash, given DuckDB's WAL).
func (s *Storage) InsertBatch(ctx context.Context, b *Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}

	receivedAt := b.ReceivedAt
	if receivedAt.IsZero() {
		receivedAt = time.Now().UTC()
	}

	if _, err := s.conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("storage: begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// The connection is serialised by s.mu, so a best-effort rollback on
			// a background context cannot race another writer.
			_, _ = s.conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	ordinal := s.nextOrdinal
	// The raw payload row keeps the batch-level metadata; the projections are
	// appended through the same transaction.
	if err := s.appendBatchRowLocked(ctx, b, ordinal, receivedAt); err != nil {
		s.dropAppendersLocked()
		return err
	}

	switch b.Signal {
	case otlp.Traces:
		if err := s.appendSpansLocked(ctx, b.ShotID, ordinal, b.Spans); err != nil {
			s.dropAppendersLocked()
			return err
		}
	case otlp.Logs:
		if err := s.appendLogsLocked(ctx, b.ShotID, ordinal, b.LogRecords); err != nil {
			s.dropAppendersLocked()
			return err
		}
	case otlp.Metrics:
		if err := s.appendMetricsLocked(ctx, b.ShotID, ordinal, b.MetricPoints); err != nil {
			s.dropAppendersLocked()
			return err
		}
	}

	if err := s.flushAppendersLocked(tableBatches); err != nil {
		s.dropAppendersLocked()
		return err
	}
	switch b.Signal {
	case otlp.Traces:
		if err := s.flushAppendersLocked(tableSpans); err != nil {
			s.dropAppendersLocked()
			return err
		}
	case otlp.Logs:
		if err := s.flushAppendersLocked(tableLogs); err != nil {
			s.dropAppendersLocked()
			return err
		}
	case otlp.Metrics:
		if err := s.flushAppendersLocked(tableMetrics); err != nil {
			s.dropAppendersLocked()
			return err
		}
	}

	if _, err := s.conn.ExecContext(ctx, "COMMIT"); err != nil {
		s.dropAppendersLocked()
		return fmt.Errorf("storage: commit: %w", err)
	}
	committed = true
	s.nextOrdinal++
	return nil
}

func (s *Storage) appendBatchRowLocked(ctx context.Context, b *Batch, ordinal int64, receivedAt time.Time) error {
	a, err := s.appenderLocked(tableBatches)
	if err != nil {
		return err
	}
	if err := a.AppendRow(
		b.ShotID, ordinal, string(b.Signal), receivedAt.UTC(), b.ContentEncoding,
		b.RecordCount(), int64(len(b.Payload)), b.ResourceSummary, b.Payload,
	); err != nil {
		return fmt.Errorf("storage: append batch: %w", err)
	}
	return nil
}

func (s *Storage) appendSpansLocked(_ context.Context, shotID string, ordinal int64, spans []otlp.Span) error {
	if len(spans) == 0 {
		return nil
	}
	a, err := s.appenderLocked(tableSpans)
	if err != nil {
		return err
	}
	for _, sp := range spans {
		if err := a.AppendRow(spanArgs(shotID, ordinal, sp)...); err != nil {
			return fmt.Errorf("storage: append span: %w", err)
		}
	}
	return nil
}

func (s *Storage) appendLogsLocked(_ context.Context, shotID string, ordinal int64, records []otlp.LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	a, err := s.appenderLocked(tableLogs)
	if err != nil {
		return err
	}
	for _, lr := range records {
		if err := a.AppendRow(logArgs(shotID, ordinal, lr)...); err != nil {
			return fmt.Errorf("storage: append log: %w", err)
		}
	}
	return nil
}

func (s *Storage) appendMetricsLocked(_ context.Context, shotID string, ordinal int64, points []otlp.MetricPoint) error {
	if len(points) == 0 {
		return nil
	}
	a, err := s.appenderLocked(tableMetrics)
	if err != nil {
		return err
	}
	for _, mp := range points {
		if err := a.AppendRow(metricArgs(shotID, ordinal, mp)...); err != nil {
			return fmt.Errorf("storage: append metric point: %w", err)
		}
	}
	return nil
}

func (s *Storage) flushAppendersLocked(table string) error {
	a, ok := s.appenders[table]
	if !ok {
		return nil
	}
	if err := a.Flush(); err != nil {
		return fmt.Errorf("storage: flush %s: %w", table, err)
	}
	return nil
}

// runWALCheckpointLoop bounds the WAL off the ingest hot path (spec 024, R4).
// It only inspects the file size, then takes the writer mutex to CHECKPOINT;
// an in-flight batch is never interrupted and the caller never waits.
func (s *Storage) runWALCheckpointLoop(interval time.Duration) {
	defer s.walWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, _ = s.CheckpointIfNeeded(ctx)
			cancel()
		}
	}
}

// CheckpointIfNeeded runs CHECKPOINT when the WAL has reached the configured
// bound. It is safe to call from tests and the background guard.
func (s *Storage) CheckpointIfNeeded(ctx context.Context) (bool, error) {
	info, err := os.Stat(s.walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("storage: stat wal: %w", err)
	}
	if info.Size() < s.walCheckpointBytes {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, ErrClosed
	}
	// Re-check under the writer lock: the WAL may already have been truncated.
	info, err = os.Stat(s.walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("storage: stat wal: %w", err)
	}
	if info.Size() < s.walCheckpointBytes {
		return false, nil
	}
	if _, err := s.conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return false, fmt.Errorf("storage: wal checkpoint: %w", err)
	}
	return true, nil
}

// Checkpoint flushes the WAL into the main database file. It must be called
// after all writers have drained and before Close.
func (s *Storage) Checkpoint(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if _, err := s.conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("storage: checkpoint: %w", err)
	}
	return nil
}

// Close releases appenders, prepared statements and the underlying connection.
// It is safe to call more than once.
func (s *Storage) Close() error {
	if s.stop != nil {
		s.stopOnce.Do(func() { close(s.stop) })
		s.walWG.Wait()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.dropAppendersLocked()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return s.db.Close()
}

func spanArgs(shotID string, ordinal int64, sp otlp.Span) []driver.Value {
	return []driver.Value{
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

func logArgs(shotID string, ordinal int64, lr otlp.LogRecord) []driver.Value {
	return []driver.Value{
		shotID, ordinal,
		int64(lr.TimeUnixNano), int64(lr.ObservedTimeUnixNano),
		lr.SeverityNumber, lr.SeverityText, lr.Body,
		lr.ServiceName, lr.ScopeName, lr.ScopeVersion,
		lr.ResourceAttributes, lr.ScopeAttributes, lr.Attributes,
		lr.TraceID, lr.SpanID, lr.EventName,
		int64(lr.Flags), int64(lr.DroppedAttributesCount),
	}
}

func metricArgs(shotID string, ordinal int64, mp otlp.MetricPoint) []driver.Value {
	return []driver.Value{
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

func float64Ptr(v *float64) driver.Value {
	if v == nil {
		return nil
	}
	return *v
}

func int64Ptr(v *int64) driver.Value {
	if v == nil {
		return nil
	}
	return *v
}

func uint64Ptr(v *uint64) driver.Value {
	if v == nil {
		return nil
	}
	return int64(*v)
}
