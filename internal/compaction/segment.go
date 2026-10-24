package compaction

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"

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
  PRIMARY KEY (source_key, ordinal)
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
  PRIMARY KEY (source_key, ordinal)
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
  PRIMARY KEY (source_key, ordinal)
);
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
	if len(records) == 0 {
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

	info := SegmentInfo{Key: key, Path: path, FirstReceivedAt: records[0].Object.ReceivedAtMS}
	info.LastReceivedAt = records[0].Object.ReceivedAtMS
	var minEvent, maxEvent int64
	haveEvent := false

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	insertBatch, err := tx.PrepareContext(ctx, `INSERT INTO otlp_batches
		(instance_id, source_key, request_id, signal, received_at, content_encoding,
		 wire_sha256, payload_sha256, record_count, payload_length, resource_summary, wire_payload, payload)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer insertBatch.Close()
	insertSpan, err := tx.PrepareContext(ctx, `INSERT INTO spans
		(instance_id, source_key, ordinal, trace_id, span_id, parent_span_id, trace_state, name, kind,
		 start_time_unix_nano, end_time_unix_nano, status_code, status_message,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 flags, dropped_attributes_count, dropped_events_count, dropped_links_count, events_count, links_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer insertSpan.Close()
	insertLog, err := tx.PrepareContext(ctx, `INSERT INTO log_records
		(instance_id, source_key, ordinal, time_unix_nano, observed_time_unix_nano, severity_number, severity_text, body,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 trace_id, span_id, event_name, flags, dropped_attributes_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer insertLog.Close()
	insertMetric, err := tx.PrepareContext(ctx, `INSERT INTO metric_points
		(instance_id, source_key, ordinal, metric_name, metric_type, metric_unit, metric_description,
		 service_name, scope_name, scope_version, resource_attributes, scope_attributes, attributes,
		 start_time_unix_nano, time_unix_nano, flags, value_double, value_int,
		 aggregation_temporality, is_monotonic, count, sum, min, max, bucket_counts, explicit_bounds, detail)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer insertMetric.Close()

	for _, rec := range records {
		o := rec.Object
		if o.ReceivedAtMS < info.FirstReceivedAt {
			info.FirstReceivedAt = o.ReceivedAtMS
		}
		if o.ReceivedAtMS > info.LastReceivedAt {
			info.LastReceivedAt = o.ReceivedAtMS
		}
		// A cross-instance bucket keeps each row's real instance attribution.
		rowInstanceID := o.InstanceID
		if rowInstanceID == "" {
			rowInstanceID = instanceID
		}
		wireSHA := sha256Hex(rec.Wire)
		payloadSHA := sha256Hex(rec.Payload)
		rq := rec.Decoded
		recordCount := int64(0)
		if rq != nil {
			recordCount = rq.RecordCount
		}
		if _, err := insertBatch.ExecContext(ctx,
			rowInstanceID, o.Key, o.RequestID, string(o.Signal), o.ReceivedAt.UTC(),
			o.ContentEncoding, wireSHA, payloadSHA, recordCount, int64(len(rec.Payload)),
			resourceSummaryOf(rq), rec.Wire, rec.Payload,
		); err != nil {
			return SegmentInfo{}, fmt.Errorf("compaction: insert batch: %w", err)
		}
		info.RequestCount++
		info.RecordCount += recordCount

		if rq == nil {
			continue
		}
		for i, sp := range rq.Spans {
			if _, err := insertSpan.ExecContext(ctx, append([]any{rowInstanceID, o.Key, int64(i)}, spanArgs(sp)...)...); err != nil {
				return SegmentInfo{}, fmt.Errorf("compaction: insert span: %w", err)
			}
			if sp.StartTimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.StartTimeUnixNano), &haveEvent)
			}
			if sp.EndTimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.EndTimeUnixNano), &haveEvent)
			}
		}
		info.SpanCount += int64(len(rq.Spans))
		for i, lr := range rq.LogRecords {
			if _, err := insertLog.ExecContext(ctx, append([]any{rowInstanceID, o.Key, int64(i)}, logArgs(lr)...)...); err != nil {
				return SegmentInfo{}, fmt.Errorf("compaction: insert log: %w", err)
			}
			if lr.TimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.TimeUnixNano), &haveEvent)
			}
			if lr.ObservedTimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.ObservedTimeUnixNano), &haveEvent)
			}
		}
		info.LogCount += int64(len(rq.LogRecords))
		for i, mp := range rq.MetricPoints {
			if _, err := insertMetric.ExecContext(ctx, append([]any{rowInstanceID, o.Key, int64(i)}, metricArgs(mp)...)...); err != nil {
				return SegmentInfo{}, fmt.Errorf("compaction: insert metric: %w", err)
			}
			if mp.StartTimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.StartTimeUnixNano), &haveEvent)
			}
			if mp.TimeUnixNano > 0 {
				minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.TimeUnixNano), &haveEvent)
			}
		}
		info.MetricCount += int64(len(rq.MetricPoints))
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return SegmentInfo{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO segment_meta (key, value) VALUES (?, ?)
			ON CONFLICT (key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return SegmentInfo{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, SchemaVersion); err != nil {
		return SegmentInfo{}, err
	}
	if err := tx.Commit(); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: commit segment: %w", err)
	}
	if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: checkpoint segment: %w", err)
	}
	if err := db.Close(); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: close segment: %w", err)
	}

	if haveEvent {
		info.EventMinNS = &minEvent
		info.EventMaxNS = &maxEvent
	}
	size, sum, err := fileSizeAndSHA256(path)
	if err != nil {
		return SegmentInfo{}, err
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

// Counts reads the per-signal row counts back from the restored segment.
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
		return nil, err
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
