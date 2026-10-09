package storage

// SchemaVersion is stored in the schema_meta table and surfaced in
// /internal/metadata. Bump it whenever table definitions change in a way that
// downstream queries must account for.
const SchemaVersion = "1"

// schemaStatements are executed (idempotently) when a shot database is opened.
//
// Design notes:
//   - otlp_batches.payload is the lossless source of truth: the decompressed
//     OTLP Export*ServiceRequest protobuf bytes exactly as received. Unknown
//     fields, exemplars, exponential histograms and other variants survive
//     because we never re-serialize.
//   - The other tables are convenience projections that make common analysis
//     queries (top slow spans, error spans, metric summaries) cheap.
//   - Nanosecond timestamps are stored as BIGINT (int64) so they never pass
//     through a float64/JS-number boundary and lose precision.
var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS schema_meta (
		key   VARCHAR PRIMARY KEY,
		value VARCHAR NOT NULL
	)`,

	`CREATE TABLE IF NOT EXISTS otlp_batches (
		shot_id          VARCHAR NOT NULL,
		ordinal          BIGINT  NOT NULL,
		signal           VARCHAR NOT NULL,
		received_at      TIMESTAMP NOT NULL,
		content_encoding VARCHAR,
		record_count     BIGINT  NOT NULL,
		payload_length   BIGINT  NOT NULL,
		resource_summary VARCHAR,
		payload          BLOB    NOT NULL,
		PRIMARY KEY (shot_id, ordinal)
	)`,

	`CREATE TABLE IF NOT EXISTS spans (
		shot_id                  VARCHAR NOT NULL,
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
		links_count              BIGINT
	)`,

	`CREATE TABLE IF NOT EXISTS log_records (
		shot_id                  VARCHAR NOT NULL,
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
		dropped_attributes_count BIGINT
	)`,

	`CREATE TABLE IF NOT EXISTS metric_points (
		shot_id                 VARCHAR NOT NULL,
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
		detail                  VARCHAR
	)`,

	`CREATE INDEX IF NOT EXISTS idx_spans_service_name ON spans (service_name)`,
	`CREATE INDEX IF NOT EXISTS idx_spans_duration ON spans (end_time_unix_nano, start_time_unix_nano)`,
	`CREATE INDEX IF NOT EXISTS idx_log_records_trace_id ON log_records (trace_id)`,
	`CREATE INDEX IF NOT EXISTS idx_metric_points_name ON metric_points (metric_name)`,
}
