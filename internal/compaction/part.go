package compaction

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"

	"github.com/mackee/burstel/internal/otlp"
)

// Shot materialization parts are *projection-only* (spec 034, R1): they keep the
// analysis tables plus a small `part_meta` provenance row per journal object,
// but never the raw wire bytes or the decompressed payload. The raw bytes are
// already durable in the R2 journal, so duplicating them inside every part only
// inflated the build, the file size and the analysis download.
//
// The table names and projection columns are identical to a compacted segment,
// so analysis can ATTACH a part exactly like a segment.
const partSchema = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key   VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS segment_meta (
  key   VARCHAR PRIMARY KEY,
  value VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS part_meta (
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

// partTables are the tables appended by one build. Each has its own appender
// goroutine(s) (spec 034, R3); DuckDB appenders must never be shared between
// goroutines, but independent tables (and shards of one table) may append
// concurrently on separate connections of the same process.
var partTables = []string{"part_meta", "spans", "log_records", "metric_points"}

// defaultAppendShards is how many concurrent appenders one analysis table gets.
// DuckDB's row-wise appender is the build bottleneck (~3-4 us per span row on a
// 4-vCPU host), but it serializes writes inside one database.
func defaultAppendShards() int {
	// Spec 034 R3 asks for one appender goroutine per table, and measurement
	// agrees: DuckDB serializes writes inside one database, so extra shards per
	// table only add connections and contention. The knob is kept (configurable
	// via MATERIALIZE_APPEND_SHARDS) for future container sizing.
	return 1
}

func appendShardsFor(table string, shards int) int {
	if table == "part_meta" {
		// One row per object: sharding only adds contention/overhead.
		return 1
	}
	if shards <= 0 {
		shards = defaultAppendShards()
	}
	return shards
}

// partAppendBatch is one table's rows for a single journal object. Batching per
// object keeps the channel traffic proportional to the object count instead of
// the row count.
type partAppendBatch [][]driver.Value

const (
	spanColumns   = 25
	logColumns    = 19
	metricColumns = 27
)

// newPartAppendBatch allocates one contiguous value buffer and returns row views
// into it: a part can contain hundreds of thousands of rows, so one allocation
// per object instead of three per row keeps the appender path allocation-free.
func newPartAppendBatch(rows, columns int) (partAppendBatch, []driver.Value) {
	flat := make([]driver.Value, rows*columns)
	batch := make(partAppendBatch, rows)
	for i := range batch {
		batch[i] = flat[i*columns : (i+1)*columns]
	}
	return batch, flat
}

// insertMetaRows upserts every key/value pair of one meta table in a single
// statement.
func insertMetaRows(ctx context.Context, db *sql.DB, table string, rows map[string]string) error {
	if len(rows) == 0 {
		return nil
	}
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(table)
	b.WriteString(" (key, value) VALUES ")
	args := make([]any, 0, len(rows)*2)
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString("(?, ?)")
		args = append(args, k, rows[k])
	}
	b.WriteString(" ON CONFLICT (key) DO UPDATE SET value = excluded.value")
	if _, err := db.ExecContext(ctx, b.String(), args...); err != nil {
		return fmt.Errorf("compaction: write %s: %w", table, err)
	}
	return nil
}

func appendSpanRow(dst []driver.Value, instanceID, sourceKey string, ordinal int64, sp otlp.Span) {
	dst[0] = instanceID
	dst[1] = sourceKey
	dst[2] = ordinal
	dst[3] = sp.TraceID
	dst[4] = sp.SpanID
	dst[5] = sp.ParentSpanID
	dst[6] = sp.TraceState
	dst[7] = sp.Name
	dst[8] = sp.Kind
	dst[9] = int64(sp.StartTimeUnixNano)
	dst[10] = int64(sp.EndTimeUnixNano)
	dst[11] = sp.StatusCode
	dst[12] = sp.StatusMessage
	dst[13] = sp.ServiceName
	dst[14] = sp.ScopeName
	dst[15] = sp.ScopeVersion
	dst[16] = sp.ResourceAttributes
	dst[17] = sp.ScopeAttributes
	dst[18] = sp.Attributes
	dst[19] = int64(sp.Flags)
	dst[20] = int64(sp.DroppedAttributesCount)
	dst[21] = int64(sp.DroppedEventsCount)
	dst[22] = int64(sp.DroppedLinksCount)
	dst[23] = int64(sp.EventsCount)
	dst[24] = int64(sp.LinksCount)
}

func appendLogRow(dst []driver.Value, instanceID, sourceKey string, ordinal int64, lr otlp.LogRecord) {
	dst[0] = instanceID
	dst[1] = sourceKey
	dst[2] = ordinal
	dst[3] = int64(lr.TimeUnixNano)
	dst[4] = int64(lr.ObservedTimeUnixNano)
	dst[5] = lr.SeverityNumber
	dst[6] = lr.SeverityText
	dst[7] = lr.Body
	dst[8] = lr.ServiceName
	dst[9] = lr.ScopeName
	dst[10] = lr.ScopeVersion
	dst[11] = lr.ResourceAttributes
	dst[12] = lr.ScopeAttributes
	dst[13] = lr.Attributes
	dst[14] = lr.TraceID
	dst[15] = lr.SpanID
	dst[16] = lr.EventName
	dst[17] = int64(lr.Flags)
	dst[18] = int64(lr.DroppedAttributesCount)
}

func appendMetricRow(dst []driver.Value, instanceID, sourceKey string, ordinal int64, mp otlp.MetricPoint) {
	values := metricArgs(mp)
	copy(dst[3:], driverValues(values))
	dst[0] = instanceID
	dst[1] = sourceKey
	dst[2] = ordinal
}

// BuildPart writes one projection-only shot-materialization part from an
// already-decoded record slice.
func buildPart(
	ctx context.Context,
	path, key, instanceID, instanceName string,
	records []JournalRecord,
	profile *BuildProfile,
) (SegmentInfo, error) {
	return buildPartStream(ctx, path, key, instanceID, instanceName, profile, 0, func(emit func(JournalRecord) error) error {
		for _, rec := range records {
			if err := emit(rec); err != nil {
				return err
			}
		}
		return nil
	})
}

// buildPartStream writes one projection-only part. `produce` calls `emit` once
// per journal object in final row order; a non-nil return from `emit` aborts
// the build. Streaming lets the materializer decode ahead of the appenders
// without retaining a whole part's decoded rows (spec 034, R3).
//
// appendShards selects how many concurrent appenders each analysis table gets
// (0 = defaultAppendShards). Object rows are dispatched round-robin by object
// index, so each shard keeps a deterministic per-object row order.
func buildPartStream(
	ctx context.Context,
	path, key, instanceID, instanceName string,
	profile *BuildProfile,
	appendShards int,
	produce func(emit func(JournalRecord) error) error,
) (SegmentInfo, error) {
	_ = os.Remove(path)
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: open part: %w", err)
	}
	// One connection per appender shard plus one for schema/meta/checkpoint.
	connections := 1
	for _, table := range partTables {
		connections += appendShardsFor(table, appendShards)
	}
	db.SetMaxOpenConns(connections)
	db.SetMaxIdleConns(connections)
	defer db.Close()

	// A part is scanned once by analysis, so keep DuckDB's own (compact)
	// compression: it keeps a 100k-span part around 5 MiB instead of tens of
	// MiB, which directly bounds the analysis download. No secondary indexes are
	// created (spec 034, R3).
	if _, err := db.ExecContext(ctx, partSchema); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: part schema: %w", err)
	}

	appendCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type appenderResult struct {
		table string
		err   error
	}
	queues := make(map[string][]chan partAppendBatch, len(partTables))
	totalAppenders := 0
	for _, table := range partTables {
		totalAppenders += appendShardsFor(table, appendShards)
	}
	results := make(chan appenderResult, totalAppenders)
	for _, table := range partTables {
		shards := appendShardsFor(table, appendShards)
		tableQueues := make([]chan partAppendBatch, shards)
		for shard := 0; shard < shards; shard++ {
			queue := make(chan partAppendBatch, 32)
			tableQueues[shard] = queue
			go func(table string, queue <-chan partAppendBatch) {
				err := appendPartRows(appendCtx, db, table, queue)
				if err != nil {
					// Unblock a producer that is waiting on this table's queue.
					cancel()
				}
				results <- appenderResult{table: table, err: err}
			}(table, queue)
		}
		queues[table] = tableQueues
	}

	appendStart := time.Now()
	info := SegmentInfo{Key: key, Path: path}
	var minEvent, maxEvent int64
	haveEvent := false
	seenSources := make(map[string]struct{})
	objectIndex := 0

	dispatch := func(table string, batch partAppendBatch) error {
		if len(batch) == 0 {
			return nil
		}
		shards := queues[table]
		queue := shards[objectIndex%len(shards)]
		select {
		case queue <- batch:
			return nil
		case <-appendCtx.Done():
			if err := appendCtx.Err(); err != nil {
				return err
			}
			return context.Canceled
		}
	}

	produceErr := produce(func(rec JournalRecord) error {
		o := rec.Object
		objectIndex++
		if _, duplicate := seenSources[o.Key]; duplicate {
			return fmt.Errorf("compaction: duplicate source key %s", o.Key)
		}
		seenSources[o.Key] = struct{}{}
		if info.RequestCount == 0 || o.ReceivedAtMS < info.FirstReceivedAt {
			info.FirstReceivedAt = o.ReceivedAtMS
		}
		if info.RequestCount == 0 || o.ReceivedAtMS > info.LastReceivedAt {
			info.LastReceivedAt = o.ReceivedAtMS
		}
		rowInstanceID := o.InstanceID
		if rowInstanceID == "" {
			rowInstanceID = instanceID
		}
		rq := rec.Decoded
		recordCount := int64(0)
		if rq != nil {
			recordCount = rq.RecordCount
		}
		meta := partAppendBatch{driverValues([]any{
			rowInstanceID, o.Key, o.RequestID, string(o.Signal), o.ReceivedAt.UTC(),
			o.ContentEncoding, rec.wireSHA256(), rec.payloadSHA256(), recordCount,
			rec.payloadLength(), resourceSummaryOf(rq),
		})}
		if err := dispatch("part_meta", meta); err != nil {
			return err
		}
		info.RequestCount++
		info.RecordCount += recordCount

		if rq == nil {
			return nil
		}
		if len(rq.Spans) > 0 {
			rows, _ := newPartAppendBatch(len(rq.Spans), spanColumns)
			for i, sp := range rq.Spans {
				appendSpanRow(rows[i], rowInstanceID, o.Key, int64(i), sp)
				if sp.StartTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.StartTimeUnixNano), &haveEvent)
				}
				if sp.EndTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(sp.EndTimeUnixNano), &haveEvent)
				}
			}
			if err := dispatch("spans", rows); err != nil {
				return err
			}
		}
		info.SpanCount += int64(len(rq.Spans))
		if len(rq.LogRecords) > 0 {
			rows, _ := newPartAppendBatch(len(rq.LogRecords), logColumns)
			for i, lr := range rq.LogRecords {
				appendLogRow(rows[i], rowInstanceID, o.Key, int64(i), lr)
				if lr.TimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.TimeUnixNano), &haveEvent)
				}
				if lr.ObservedTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(lr.ObservedTimeUnixNano), &haveEvent)
				}
			}
			if err := dispatch("log_records", rows); err != nil {
				return err
			}
		}
		info.LogCount += int64(len(rq.LogRecords))
		if len(rq.MetricPoints) > 0 {
			rows, _ := newPartAppendBatch(len(rq.MetricPoints), metricColumns)
			for i, mp := range rq.MetricPoints {
				appendMetricRow(rows[i], rowInstanceID, o.Key, int64(i), mp)
				if mp.StartTimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.StartTimeUnixNano), &haveEvent)
				}
				if mp.TimeUnixNano > 0 {
					minEvent, maxEvent = extend(minEvent, maxEvent, int64(mp.TimeUnixNano), &haveEvent)
				}
			}
			if err := dispatch("metric_points", rows); err != nil {
				return err
			}
		}
		info.MetricCount += int64(len(rq.MetricPoints))
		return nil
	})

	for _, table := range partTables {
		for _, queue := range queues[table] {
			close(queue)
		}
	}
	var appendErr error
	for i := 0; i < totalAppenders; i++ {
		res := <-results
		if res.err != nil && appendErr == nil {
			appendErr = fmt.Errorf("compaction: append %s: %w", res.table, res.err)
			cancel()
		}
	}
	if appendErr != nil {
		return SegmentInfo{}, appendErr
	}
	if produceErr != nil {
		return SegmentInfo{}, produceErr
	}
	if profile != nil {
		profile.AppendMS = time.Since(appendStart).Milliseconds()
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
		"part_layout":       "projection_only",
	}

	// One multi-row upsert per meta table: one statement per key cost ~300 ms
	// per part in the measured build (each is its own transaction).
	if err := insertMetaRows(ctx, db, "schema_meta", meta); err != nil {
		return SegmentInfo{}, err
	}
	if err := insertMetaRows(ctx, db, "segment_meta", meta); err != nil {
		return SegmentInfo{}, err
	}

	// No secondary indexes (spec 034, R3): the previous part build maintained
	// three ART indexes per appended row, which dominated the build time and is
	// useless for a single-scan analysis of an immutable part.
	checkpointStart := time.Now()
	if _, err := db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: checkpoint part: %w", err)
	}
	if profile != nil {
		profile.CheckpointMS = time.Since(checkpointStart).Milliseconds()
	}
	closeStart := time.Now()
	if err := db.Close(); err != nil {
		return SegmentInfo{}, fmt.Errorf("compaction: close part: %w", err)
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

// appendPartRows drains one table's batches into its own appender and its own
// connection. A connection per table is what lets the analysis tables be
// appended concurrently while every individual appender stays single-goroutine.
func appendPartRows(ctx context.Context, db *sql.DB, table string, queue <-chan partAppendBatch) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("connection: %w", err)
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
		return fmt.Errorf("resolve driver connection: %w", err)
	}
	appender, err := duckdb.NewAppenderFromConn(driverConn, "", table)
	if err != nil {
		return fmt.Errorf("open appender: %w", err)
	}
	// One explicit transaction per table: DuckDB's appender auto-flushes every
	// data chunk, and without a wrapping transaction each flush is its own
	// commit. On a multi-100k-row part that commit overhead dominated the build
	// (spec 034, R3). Tables are independent, so one transaction per table is
	// sufficient for a scratch part that is only uploaded after verification.
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		_ = appender.Close()
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = appender.Close()
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	for batch := range queue {
		for _, row := range batch {
			if err := appender.AppendRow(row...); err != nil {
				return fmt.Errorf("append row: %w", err)
			}
		}
	}
	if err := appender.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	if err := appender.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}
