-- Example read-only analysis queries for a frozen burstel shot database.
-- Open with:
--   duckdb -readonly /data/<shot>/telemetry.duckdb
-- or from the Go driver:
--   sql.Open("duckdb", "/data/<shot>/telemetry.duckdb?access_mode=read_only")

-- Schema/provenance.
SELECT * FROM schema_meta;

-- How many OTLP requests and logical records were captured per signal.
SELECT signal, count(*) AS batches, sum(record_count) AS records,
       sum(payload_length) AS bytes
FROM otlp_batches
GROUP BY signal
ORDER BY signal;

-- Top 10 slowest spans (duration in nanoseconds, no float precision loss).
SELECT service_name, name, kind, status_code,
       (end_time_unix_nano - start_time_unix_nano) AS duration_ns
FROM spans
ORDER BY duration_ns DESC
LIMIT 10;

-- Error spans grouped by operation.
SELECT service_name, name, count(*) AS errors
FROM spans
WHERE status_code = 'STATUS_CODE_ERROR'
GROUP BY service_name, name
ORDER BY errors DESC;

-- Logs correlated to a span by trace id.
SELECT l.time_unix_nano, l.severity_text, l.body
FROM log_records l
WHERE l.trace_id = '<hex-trace-id>'
ORDER BY l.time_unix_nano;

-- Metric overview (aggregation types captured per metric).
SELECT metric_name, metric_type, count(*) AS points,
       sum(count) AS histogram_count, avg(value_double) AS avg_double
FROM metric_points
GROUP BY metric_name, metric_type
ORDER BY metric_name;

-- Raw batch inspection: the lossless source of truth for anything not
-- projected (exemplars, exponential histograms, unknown fields).
SELECT ordinal, signal, record_count, payload_length, resource_summary
FROM otlp_batches
ORDER BY ordinal;
