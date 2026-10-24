package compaction

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/otlp"
)

// SchemaVersion is the segment schema version, stored in schema_meta.
const SchemaVersion = "015"

// JournalRecord is one raw journal object, already fetched and decoded.
type JournalRecord struct {
	Object  JournalObject
	Wire    []byte // exactly the bytes stored in R2 (possibly gzip)
	Payload []byte // decompressed OTLP protobuf request bytes
	Decoded *otlp.Decoded
	// Spec 034: a projection-only part never needs the raw bytes after decode.
	// The materializer hashes them while they are in hand and drops both blobs
	// so a large part does not hold hundreds of MiB of payload in memory.
	WireSHA256    string
	PayloadSHA256 string
	PayloadLength int64
}

// wireSHA256 returns the stored-bytes checksum, falling back to hashing the
// retained wire blob (continuous compaction keeps it for the raw tables).
func (r JournalRecord) wireSHA256() string {
	if r.WireSHA256 != "" {
		return r.WireSHA256
	}
	return sha256Hex(r.Wire)
}

// payloadSHA256 returns the decompressed-payload checksum.
func (r JournalRecord) payloadSHA256() string {
	if r.PayloadSHA256 != "" {
		return r.PayloadSHA256
	}
	return sha256Hex(r.Payload)
}

// payloadLength returns the decompressed payload length.
func (r JournalRecord) payloadLength() int64 {
	if r.PayloadLength > 0 || r.PayloadSHA256 != "" {
		return r.PayloadLength
	}
	return int64(len(r.Payload))
}

// SegmentInfo describes a freshly built immutable segment.
type SegmentInfo struct {
	Key             string
	Path            string
	Size            int64
	SHA256          string
	FirstReceivedAt int64
	LastReceivedAt  int64
	RequestCount    int64
	RecordCount     int64
	SpanCount       int64
	LogCount        int64
	MetricCount     int64
	EventMinNS      *int64
	EventMaxNS      *int64
}

const segmentSchema = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key   VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS segment_meta (
  key   VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS otlp_batches (
  instance_id      VARCHAR NOT NULL,
  source_key       VARCHAR NOT NULL,
  request_id       VARCHAR NOT NULL,
  signal           VARCHAR NOT NULL,
  received_at      TIMESTAMP NOT NULL,
  content_encoding VARCHAR,
  wire_sha256      VARCHAR NOT NULL,
  payload_sha256   VARCHAR NOT NULL,
  record_count     BIGINT  NOT NULL,
  payload_length   BIGINT  NOT NULL,
  resource_summary VARCHAR,
  wire_payload     BLOB    NOT NULL,
  payload          BLOB    NOT NULL,
  PRIMARY KEY (source_key)
);
CREATE TABLE IF NOT EXISTS spans (
  instance_id              VARCHAR NOT NULL,
  source_key               VARCHAR NOT NULL,
  ordinal                  BIGINT  NOT NULL,
  trace_id                 VARCHAR,
  span_id                  VARCHAR,
  parent_span_id           VARCHAR,
  trace_state              VARCHAR,
  name                     VARCHAR,
  kind                     VARCHAR,
  start_time_unix_nano     BIGINT,
  end_time_unix_nano       BIGINT,
  status_code              VARCHAR,
  status_message           VARCHAR,
  service_name             VARCHAR,
  scope_name               VARCHAR,
  scope_version            VARCHAR,
  resource_attributes      VARCHAR,
  scope_attributes         VARCHAR,
  attributes               VARCHAR,
  flags                    BIGINT,
  dropped_attributes_count BIGINT,
  dropped_events_count     BIGINT,
  dropped_links_count      BIGINT,
  events_count             BIGINT,
  links_count              BIGINT,
);
CREATE TABLE IF NOT EXISTS log_records (
  instance_id              VARCHAR NOT NULL,
  source_key               VARCHAR NOT NULL,
  ordinal                  BIGINT  NOT NULL,
  time_unix_nano           BIGINT,
  observed_time_unix_nano  BIGINT,
  severity_number          VARCHAR,
  severity_text            VARCHAR,
  body                     VARCHAR,
  service_name             VARCHAR,
  scope_name               VARCHAR,
  scope_version            VARCHAR,
  resource_attributes      VARCHAR,
  scope_attributes         VARCHAR,
  attributes               VARCHAR,
  trace_id                 VARCHAR,
  span_id                  VARCHAR,
  event_name               VARCHAR,
  flags                    BIGINT,
  dropped_attributes_count BIGINT,
);
CREATE TABLE IF NOT EXISTS metric_points (
  instance_id             VARCHAR NOT NULL,
  source_key              VARCHAR NOT NULL,
  ordinal                 BIGINT  NOT NULL,
  metric_name             VARCHAR,
  metric_type             VARCHAR,
  metric_unit             VARCHAR,
  metric_description      VARCHAR,
  service_name            VARCHAR,
  scope_name              VARCHAR,
  scope_version           VARCHAR,
  resource_attributes     VARCHAR,
  scope_attributes        VARCHAR,
  attributes              VARCHAR,
  start_time_unix_nano    BIGINT,
  time_unix_nano          BIGINT,
  flags                   BIGINT,
  value_double            DOUBLE,
  value_int               BIGINT,
  aggregation_temporality INTEGER,
  is_monotonic            BOOLEAN,
  count                   BIGINT,
  sum                     DOUBLE,
  min                     DOUBLE,
  max                     DOUBLE,
  bucket_counts           VARCHAR,
  explicit_bounds         VARCHAR,
  detail                  VARCHAR,
);
`

// Secondary indexes are built once after the bulk append. Creating them before
// the insert makes every appended row maintain three ART indexes, which is the
// dominant cost for a multi-million-span build (spec 031 D2).
const segmentIndexes = `
CREATE INDEX IF NOT EXISTS idx_seg_spans_service ON spans (service_name);
CREATE INDEX IF NOT EXISTS idx_seg_logs_trace ON log_records (trace_id);
CREATE INDEX IF NOT EXISTS idx_seg_metrics_name ON metric_points (metric_name);
`

// NewScratchSegment creates a private scratch directory for exactly one build
// and returns the segment path plus its cleanup function.
//
// The path must never be deterministic per segment key: BuildSegment removes
// and recreates its target, and the caller removes it after hashing. Two
// concurrent builds of the same key (for example a Worker timeout retry that
// overlaps the original request) would otherwise delete each other's file
// between CHECKPOINT/close and the SHA-256 read, surfacing as
// `open /work/seg-<hash>.duckdb: no such file or directory`. A private
// directory per request makes concurrent same-key builds independent.
func NewScratchSegment(workDir, key string) (string, func(), error) {
	if workDir == "" {
		workDir = os.TempDir()
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp(workDir, "seg-*")
	if err != nil {
		return "", nil, fmt.Errorf("compaction: create scratch dir: %w", err)
	}
	return filepath.Join(dir, shortHash(key)+".duckdb"), func() {
		_ = os.RemoveAll(dir)
	}, nil
}

// BuildSegment writes an immutable DuckDB segment at path containing the raw
// wire bytes (byte-for-byte), the decompressed payload, and the relational
// projections. It CHECKPOINTs and closes before hashing the file so the caller
// uploads a stable object.
func BuildSegment(ctx context.Context, path, key, instanceID, instanceName string, records []JournalRecord) (SegmentInfo, error) {
	return buildSegment(ctx, path, key, instanceID, instanceName, records, false, nil)
}

// BuildProfile is the per-phase timing breakdown of one segment/part build
// (spec 033). All values are milliseconds; a zero value means the phase was
// skipped (for example an empty part still checkpoints but appends no rows).
type BuildProfile struct {
	AppendMS     int64
	IndexMS      int64
	CheckpointMS int64
	CloseMS      int64
	HashMS       int64
}

// BuildPart writes one projection-only shot-materialization part (spec 034).
// Unlike a compacted segment it has no raw wire/payload blobs and no secondary
// indexes; it may legitimately contain zero records (for example when every
// journal object in a chunk was excluded). The analysis tables and the
// checksum/metadata contract are identical, so analysis can ATTACH a part
// exactly like a segment.
func BuildPart(ctx context.Context, path, key, instanceID, instanceName string, records []JournalRecord) (SegmentInfo, error) {
	return buildPart(ctx, path, key, instanceID, instanceName, records, nil)
}

// BuildPartProfile is BuildPart plus the per-phase timing breakdown used by the
// materializer's structured logs (spec 033 requirement 5).
func BuildPartProfile(ctx context.Context, path, key, instanceID, instanceName string, records []JournalRecord) (SegmentInfo, BuildProfile, error) {
	var profile BuildProfile
	info, err := buildPart(ctx, path, key, instanceID, instanceName, records, &profile)
	return info, profile, err
}

func buildSegment(
	ctx context.Context,
	path, key, instanceID, instanceName string,
	records []JournalRecord,
	allowEmpty bool,
	profile *BuildProfile,
) (SegmentInfo, error) {
	if len(records) == 0 && !allowEmpty {
		return SegmentInfo{}, fmt.Errorf("compaction: cannot build an empty segment")
	}
	_ = os.Remove(path)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: open segment: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	if _, err := db.ExecContext(ctx, segmentSchema); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: segment schema: %w", err)
	}

	// A single connection owns the transaction and the appenders. DuckDB
	// appenders write through the session's current transaction, so every raw
	// batch row and its projection rows commit (or roll back) atomically.
	conn, err := db.Conn(ctx)
	if err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: segment connection: %w", err)
	}
	defer conn.Close()

	var driverConn driver.Conn
	if err := conn.Raw(func(dc any) error {
		resolved, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type %T", dc)
		}
		driverConn = resolved
		return nil
	}); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: resolve driver connection: %w", err)
	}

	appenders := map[string]*duckdb.Appender{}
	dropAppenders := func() {
		for table, a := range appenders {
			_ = a.Close()
			delete(appenders, table)
		}
	}
	appender := func(table string) (*duckdb.Appender, error) {
		if a, ok := appenders[table]; ok {
			return a, nil
		}
		a, err := duckdb.NewAppenderFromConn(driverConn, "", table)
		if err != nil {
			return nil, fmt.Errorf("compaction: open appender %s: %w", table, err)
		}
		appenders[table] = a
		return a, nil
	}

	appendStart := time.Now()
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: begin segment: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			// Flush/close the appenders into the open transaction first, then
			// roll the whole unit back. The reverse order would let an appender
			// flush buffered rows *after* ROLLBACK.
			dropAppenders()
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	info := SegmentInfo{Key: key, Path: path}
	if len(records) > 0 {
		info.FirstReceivedAt = records[0].Object.ReceivedAtMS
		info.LastReceivedAt = records[0].Object.ReceivedAtMS
	}
	var minEvent, maxEvent int64
	haveEvent := false
	seenSources := make(map[string]struct{}, len(records))

	for _, rec := range records {
		o := rec.Object
		if _, duplicate := seenSources[o.Key]; duplicate {
			return SegmentInfo{}, fmt.Errorf("compaction: duplicate source key %s", o.Key)
		}
		seenSources[o.Key] = struct{}{}
		if info.RequestCount == 0 || o.ReceivedAtMS < info.FirstReceivedAt {
			info.FirstReceivedAt = o.ReceivedAtMS
		}
		if info.RequestCount == 0 || o.ReceivedAtMS > info.LastReceivedAt {
			info.LastReceivedAt = o.ReceivedAtMS
		}
		// A cross-instance bucket keeps each row's real instance attribution.
		rowInstanceID := o.InstanceID
		if rowInstanceID == "" {
			rowInstanceID = instanceID
		}
		wireSHA := rec.wireSHA256()
		payloadSHA := rec.payloadSHA256()
		rq := rec.Decoded
		recordCount := int64(0)
		if rq != nil {
			recordCount = rq.RecordCount
		}
		batchAppender, err := appender("otlp_batches")
		if err != nil {
			return SegmentInfo{}, err
		}
		if err := batchAppender.AppendRow(
			rowInstanceID, o.Key, o.RequestID, string(o.Signal), o.ReceivedAt.UTC(),
			o.ContentEncoding, wireSHA, payloadSHA, recordCount, rec.payloadLength(),
			resourceSummaryOf(rq), rec.Wire, rec.Payload,
		); err != nil {
			return SegmentInfo{}, fmt.Errorf("compaction: append batch: %w", err)
		}
		info.RequestCount++
		info.RecordCount += recordCount

		if rq == nil {
			continue
		}
		if len(rq.Spans) > 0 {
			a, err := appender("spans")
			if err != nil {
				return SegmentInfo{}, err
			}
			for i, sp := range rq.Spans {
				if err := a.AppendRow(driverValues(append([]any{rowInstanceID, o.Key, int64(i)}, spanArgs(sp)...))...); err != nil {
					return SegmentInfo{}, fmt.Errorf("compaction: append span: %w", err)
				}
				if sp.StartTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.StartTimeUnixNano), &haveEvent)
				}
				if sp.EndTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.EndTimeUnixNano), &haveEvent)
				}
			}
		}
		info.SpanCount += int64(len(rq.Spans))
		if len(rq.LogRecords) > 0 {
			a, err := appender("log_records")
			if err != nil {
				return SegmentInfo{}, err
			}
			for i, lr := range rq.LogRecords {
				if err := a.AppendRow(driverValues(append([]any{rowInstanceID, o.Key, int64(i)}, logArgs(lr)...))...); err != nil {
					return SegmentInfo{}, fmt.Errorf("compaction: append log: %w", err)
				}
				if lr.TimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.TimeUnixNano), &haveEvent)
				}
				if lr.ObservedTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.ObservedTimeUnixNano), &haveEvent)
				}
			}
		}
		info.LogCount += int64(len(rq.LogRecords))
		if len(rq.MetricPoints) > 0 {
			a, err := appender("metric_points")
			if err != nil {
				return SegmentInfo{}, err
			}
			for i, mp := range rq.MetricPoints {
				if err := a.AppendRow(driverValues(append([]any{rowInstanceID, o.Key, int64(i)}, metricArgs(mp)...))...); err != nil {
					return SegmentInfo{}, fmt.Errorf("compaction: append metric: %w", err)
				}
				if mp.StartTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.StartTimeUnixNano), &haveEvent)
				}
				if mp.TimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.TimeUnixNano), &haveEvent)
				}
			}
		}
		info.MetricCount += int64(len(rq.MetricPoints))
	}

	for table, a := range appenders {
		if err := a.Flush(); err != nil {
			return SegmentInfo{}, fmt.Errorf("compaction: flush %s: %w", table, err)
		}
	}

	meta := map[string]string{
		"schema_version":    SchemaVersion,
		"segment_key":       key,
		"instance_id":       instanceID,
		"instance":          instanceName,
		"first_received_at": fmt.Sprint(info.FirstReceivedAt),
		"last_received_at":  fmt.Sprint(info.LastReceivedAt),
		"request_count":     fmt.Sprint(info.RequestCount),
		"record_count":      fmt.Sprint(info.RecordCount),
	}
	for k, v := range meta {
		if _, err := conn.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return SegmentInfo{}, err
		}
		if _, err := conn.ExecContext(ctx, `INSERT INTO segment_meta (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return SegmentInfo{}, err
		}
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, SchemaVersion); err != nil {
		return SegmentInfo{}, err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: commit segment: %w", err)
	}
	committed = true
	dropAppenders()
	if profile != nil {
		profile.AppendMS = time.Since(appendStart).Milliseconds()
	}
	indexStart := time.Now()
	if _, err := conn.ExecContext(ctx, segmentIndexes); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: index segment: %w", err)
	}
	if profile != nil {
		profile.IndexMS = time.Since(indexStart).Milliseconds()
	}
	checkpointStart := time.Now()
	if _, err := conn.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: checkpoint segment: %w", err)
	}
	if profile != nil {
		profile.CheckpointMS = time.Since(checkpointStart).Milliseconds()
	}
	closeStart := time.Now()
	if err := conn.Close(); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: close segment connection: %w", err)
	}
	if err := db.Close(); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: close segment: %w", err)
	}
	if profile != nil {
		profile.CloseMS = time.Since(closeStart).Milliseconds()
	}

	if haveEvent {
		info.EventMinNS = &minEvent
		info.EventMaxNS = &maxEvent
	}
	hashStart := time.Now()
	size, sum, err := fileSizeAndSHA256(path)
	if err != nil {
		return SegmentInfo{}, err
	}
	if profile != nil {
		profile.HashMS = time.Since(hashStart).Milliseconds()
	}
	info.Size = size
	info.SHA256 = sum
	return info, nil
}

func resourceSummaryOf(d *otlp.Decoded) string {
	if d == nil {
		return ""
	}
	return d.ResourceSummary
}

func extend(minV, maxV, v int64, have *bool) (int64, int64) {
	if !*have {
		*have = true
		return v, v
	}
	if v < minV {
		minV = v
	}
	if v > maxV {
		maxV = v
	}
	return minV, maxV
}

func driverValues(args []any) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, arg := range args {
		out[i] = arg
	}
	return out
}

func spanArgs(sp otlp.Span) []any {
	return []any{
		sp.TraceID, sp.SpanID, sp.ParentSpanID, sp.TraceState, sp.Name, sp.Kind,
		int64(sp.StartTimeUnixNano), int64(sp.EndTimeUnixNano),
		sp.StatusCode, sp.StatusMessage,
		sp.ServiceName, sp.ScopeName, sp.ScopeVersion,
		sp.ResourceAttributes, sp.ScopeAttributes, sp.Attributes,
		int64(sp.Flags), int64(sp.DroppedAttributesCount), int64(sp.DroppedEventsCount),
		int64(sp.DroppedLinksCount), int64(sp.EventsCount), int64(sp.LinksCount),
	}
}

func logArgs(lr otlp.LogRecord) []any {
	return []any{
		int64(lr.TimeUnixNano), int64(lr.ObservedTimeUnixNano),
		lr.SeverityNumber, lr.SeverityText, lr.Body,
		lr.ServiceName, lr.ScopeName, lr.ScopeVersion,
		lr.ResourceAttributes, lr.ScopeAttributes, lr.Attributes,
		lr.TraceID, lr.SpanID, lr.EventName,
		int64(lr.Flags), int64(lr.DroppedAttributesCount),
	}
}

func metricArgs(mp otlp.MetricPoint) []any {
	return []any{
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

// SegmentReader is a verified, read-only view of one segment object.
type SegmentReader struct {
	db   *sql.DB
	path string
	SHA  string
}

// OpenSegmentReadOnly verifies the file's SHA-256 (when wantSHA is non-empty)
// and opens it read-only.
func OpenSegmentReadOnly(ctx context.Context, path, wantSHA string) (*SegmentReader, error) {
	size, sum, err := fileSizeAndSHA256(path)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, fmt.Errorf("compaction: segment %s is empty", path)
	}
	if wantSHA != "" && !strings.EqualFold(sum, wantSHA) {
		return nil, fmt.Errorf("compaction: segment checksum mismatch: got %s want %s", sum, wantSHA)
	}
	db, err := sql.Open("duckdb", path+"?access_mode=read_only")
	if err != nil {
		return nil, fmt.Errorf("compaction: open read-only %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("compaction: ping read-only %s: %w", path, err)
	}
	return &SegmentReader{db: db, path: path, SHA: sum}, nil
}

// DB exposes the read-only connection for bounded inspection queries.
func (r *SegmentReader) DB() *sql.DB { return r.db }

// Path returns the restored segment path.
func (r *SegmentReader) Path() string { return r.path }

// Close releases the read-only connection.
func (r *SegmentReader) Close() error { return r.db.Close() }

// Counts reads the per-signal row counts back from the restored segment or
// part. A projection-only part (spec 034) has `part_meta` instead of
// `otlp_batches`, so the batch count falls back to the provenance rows.
func (r *SegmentReader) Counts(ctx context.Context) (spans, logs, metrics, batches int64, err error) {
	for _, q := range []struct {
		sql string
		dst *int64
	}{
		{`SELECT COUNT(*) FROM spans`, &spans},
		{`SELECT COUNT(*) FROM log_records`, &logs},
		{`SELECT COUNT(*) FROM metric_points`, &metrics},
		{`SELECT COUNT(*) FROM otlp_batches`, &batches},
	} {
		if err := r.db.QueryRowContext(ctx, q.sql).Scan(q.dst); err != nil {
			if q.dst == &batches && isMissingTableError(err) {
				if fallbackErr := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM part_meta`).Scan(q.dst); fallbackErr != nil {
					return 0, 0, 0, 0, fallbackErr
				}
				continue
			}
			return 0, 0, 0, 0, err
		}
	}
	return spans, logs, metrics, batches, nil
}

// WireHashes returns source_key -> wire payload SHA-256 as stored in the
// segment, for byte-for-byte reconciliation.
func (r *SegmentReader) WireHashes(ctx context.Context) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT source_key, wire_sha256 FROM otlp_batches`)
	if err != nil {
		if !isMissingTableError(err) {
			return nil, err
		}
		rows, err = r.db.QueryContext(ctx, `SELECT source_key, wire_sha256 FROM part_meta`)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, h string
		if err := rows.Scan(&k, &h); err != nil {
			return nil, err
		}
		out[k] = h
	}
	return out, rows.Err()
}

// RawPayloadHashes returns source_key -> SHA-256 of the stored wire BLOB by
// re-hashing the bytes read back out of the DuckDB file.
func (r *SegmentReader) RawPayloadHashes(ctx context.Context) (map[string]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT source_key, wire_payload FROM otlp_batches`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k string
		var b []byte
		if err := rows.Scan(&k, &b); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(b)
		out[k] = hex.EncodeToString(sum[:])
	}
	return out, rows.Err()
}

// MetaValue reads a segment_meta value.
func (r *SegmentReader) MetaValue(ctx context.Context, key string) (string, error) {
	var v string
	err := r.db.QueryRowContext(ctx, `SELECT value FROM segment_meta WHERE key = ?`, key).Scan(&v)
	return v, err
}

// RestoreSegment downloads a segment object from the store, verifies its size
// and SHA-256, writes it to dir and opens it read-only. It never depends on the
// compactor process being alive.
func RestoreSegment(ctx context.Context, store ObjectStore, m *Manifest, segmentKey, dir string) (*SegmentReader, SegmentManifest, error) {
	seg, ok, err := m.GetSegment(ctx, segmentKey)
	if err != nil {
		return nil, SegmentManifest{}, err
	}
	if !ok {
		return nil, SegmentManifest{}, fmt.Errorf("compaction: unknown segment %s", segmentKey)
	}
	data, obj, err := store.Get(ctx, segmentKey)
	if err != nil {
		return nil, seg, fmt.Errorf("compaction: fetch segment %s: %w", segmentKey, err)
	}
	if int64(len(data)) != seg.Size || obj.Size != seg.Size {
		return nil, seg, fmt.Errorf("compaction: segment %s size mismatch", segmentKey)
	}
	got := sha256Hex(data)
	if got != seg.SHA256 || obj.SHA256 != seg.SHA256 {
		return nil, seg, fmt.Errorf("compaction: segment %s checksum mismatch", segmentKey)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, seg, err
	}
	path := dir + "/" + sanitizeKey(segmentKey)
	if err := os.WriteFile(path, data, 0o444); err != nil {
		return nil, seg, err
	}
	reader, err := OpenSegmentReadOnly(ctx, path, seg.SHA256)
	if err != nil {
		return nil, seg, err
	}
	return reader, seg, nil
}

func sanitizeKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:]) + ".duckdb"
}

func fileSizeAndSHA256(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", fmt.Errorf("compaction: open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", fmt.Errorf("compaction: hash %s: %w", path, err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
