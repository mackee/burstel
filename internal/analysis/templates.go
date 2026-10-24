package analysis

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type sectionDef struct {
	name        string
	description string
	unit        string
	countSQL    string
	sql         func(topN int) string
}

const spanDurationPredicate = "end_time_unix_nano IS NOT NULL AND start_time_unix_nano IS NOT NULL"

var sections = []sectionDef{
	{
		name:        "overview",
		description: "Raw record volume and time range actually captured in the shot.",
		unit:        "count",
		countSQL:    "SELECT 1",
		sql: func(int) string {
			return `SELECT
  (SELECT count(*) FROM otlp_batches) AS batches,
  (SELECT coalesce(sum(record_count), 0) FROM otlp_batches) AS records,
  (SELECT count(*) FROM spans) AS spans,
  (SELECT count(*) FROM log_records) AS log_records,
  (SELECT count(*) FROM metric_points) AS metric_points,
  (SELECT count(DISTINCT trace_id) FROM spans WHERE trace_id IS NOT NULL) AS distinct_traces,
  (SELECT count(*) FROM spans WHERE status_code = 'STATUS_CODE_ERROR') AS error_spans,
  (SELECT min(start_time_unix_nano) FROM spans) AS first_start_ns,
  (SELECT max(end_time_unix_nano) FROM spans) AS last_end_ns`
		},
	},
	{
		name:        "latency",
		description: "Per service/operation span latency percentiles.",
		unit:        "ms",
		countSQL:    "SELECT count(*) FROM spans WHERE " + spanDurationPredicate,
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT service_name, name,
  count(*) AS n,
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.50), 3) AS p50_ms,
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95), 3) AS p95_ms,
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.99), 3) AS p99_ms,
  round(max((end_time_unix_nano - start_time_unix_nano) / 1e6), 3) AS max_ms,
  sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) AS errors
FROM spans
WHERE %s
GROUP BY service_name, name
ORDER BY p95_ms DESC NULLS LAST, n DESC
LIMIT %d`, spanDurationPredicate, topN)
		},
	},
	{
		name:        "slow_spans",
		description: "Slowest individual spans with their trace/span ids.",
		unit:        "ms",
		countSQL:    "SELECT count(*) FROM spans WHERE " + spanDurationPredicate,
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT trace_id, span_id, service_name, name, kind, status_code,
  round((end_time_unix_nano - start_time_unix_nano) / 1e6, 3) AS duration_ms,
  start_time_unix_nano
FROM spans
WHERE %s
ORDER BY duration_ms DESC NULLS LAST
LIMIT %d`, spanDurationPredicate, topN)
		},
	},
	{
		name:        "errors",
		description: "Error concentration per service/operation.",
		unit:        "count",
		countSQL:    "SELECT count(*) FROM spans WHERE " + spanDurationPredicate,
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT service_name, name,
  sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) AS errors,
  count(*) AS total,
  round(100.0 * sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) / count(*), 3) AS error_pct
FROM spans
WHERE %s
GROUP BY service_name, name
HAVING sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) > 0
ORDER BY errors DESC, total DESC
LIMIT %d`, spanDurationPredicate, topN)
		},
	},
	{
		name:        "db_operations",
		description: "Database/client operations inferred from span kind and attributes.",
		unit:        "ms",
		countSQL:    "SELECT count(*) FROM spans WHERE kind = 'SPAN_KIND_CLIENT' OR attributes LIKE '%db.system%'",
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT service_name, name,
  count(*) AS n,
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95), 3) AS p95_ms,
  sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) AS errors
FROM spans
WHERE %s AND (kind = 'SPAN_KIND_CLIENT' OR attributes LIKE '%%db.system%%')
GROUP BY service_name, name
ORDER BY p95_ms DESC NULLS LAST, n DESC
LIMIT %d`, spanDurationPredicate, topN)
		},
	},
	{
		name:        "log_correlation",
		description: "Log records grouped by the trace ids they correlate to.",
		unit:        "count",
		countSQL:    "SELECT count(*) FROM log_records WHERE trace_id IS NOT NULL",
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT l.trace_id,
  count(*) AS log_count,
  sum(CASE WHEN upper(coalesce(l.severity_text, '')) IN ('WARN', 'ERROR', 'FATAL') THEN 1 ELSE 0 END) AS warn_or_worse,
  min(l.severity_text) AS example_severity,
  max(l.body) AS example_body
FROM log_records l
WHERE l.trace_id IS NOT NULL
GROUP BY l.trace_id
ORDER BY warn_or_worse DESC, log_count DESC
LIMIT %d`, topN)
		},
	},
	{
		name:        "log_severity",
		description: "Log record counts by severity.",
		unit:        "count",
		countSQL:    "SELECT count(*) FROM log_records",
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT coalesce(severity_text, severity_number) AS severity, count(*) AS records
FROM log_records
GROUP BY 1
ORDER BY records DESC
LIMIT %d`, topN)
		},
	},
	{
		name:        "metrics",
		description: "Metric points grouped by metric name and type.",
		unit:        "value",
		countSQL:    "SELECT count(*) FROM metric_points",
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT metric_name, metric_type, count(*) AS points,
  round(avg(value_double), 6) AS avg_double,
  sum(count) AS histogram_count,
  round(sum(sum), 6) AS histogram_sum,
  round(min(min), 6) AS min_value,
  round(max(max), 6) AS max_value
FROM metric_points
GROUP BY metric_name, metric_type
ORDER BY points DESC
LIMIT %d`, topN)
		},
	},
	{
		name:        "signals",
		description: "Raw OTLP batch counts per signal (the lossless source of truth).",
		unit:        "count",
		countSQL:    "SELECT count(*) FROM otlp_batches",
		sql: func(topN int) string {
			return fmt.Sprintf(`SELECT signal, count(*) AS batches, sum(record_count) AS records,
  sum(payload_length) AS payload_bytes,
  min(received_at) AS first_received, max(received_at) AS last_received
FROM otlp_batches
GROUP BY signal
ORDER BY signal
LIMIT %d`, topN)
		},
	},
}

func sectionByName(name string) (sectionDef, bool) {
	for _, s := range sections {
		if s.name == name {
			return s, true
		}
	}
	return sectionDef{}, false
}

// Templates returns the documented analysis section list.
func Templates() []TemplateInfo {
	out := make([]TemplateInfo, 0, len(sections))
	for _, s := range sections {
		out = append(out, TemplateInfo{Name: s.name, Description: s.description, Unit: s.unit})
	}
	return out
}

// Analyze runs the requested SQL-backed sections (all by default).
func (e *Engine) Analyze(ctx context.Context, policy Policy, shotID string, req AnalyzeRequest) (*AnalyzeResult, error) {
	topN := req.TopN
	if topN <= 0 || topN > policy.MaxRows {
		topN = 20
	}
	wanted := req.Sections
	if len(wanted) == 0 {
		for _, s := range sections {
			wanted = append(wanted, s.name)
		}
	}

	startedAt := time.Now()
	result := &AnalyzeResult{
		ShotID:        shotID,
		SchemaVersion: e.schema,
		RawCounts:     map[string]int64{},
		Timings:       &AnalyzeTimings{SectionsMS: map[string]int64{}},
	}
	rawStart := time.Now()
	for _, table := range []string{"otlp_batches", "spans", "log_records", "metric_points"} {
		qr, err := e.run(ctx, "SELECT count(*) FROM "+table, 1, policy)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) == 1 {
			result.RawCounts[table] = asInt64(qr.Rows[0][0])
		}
	}
	result.Timings.RawCountsMS = time.Since(rawStart).Milliseconds()

	for _, name := range wanted {
		sectionStart := time.Now()
		def, ok := sectionByName(name)
		if !ok {
			return nil, policyErr("unknown_section", "unknown analysis section "+name)
		}
		if def.name == "metric_cumulative_delta" {
			sec, err := e.cumulativeSection(ctx, topN, policy)
			if err != nil {
				return nil, err
			}
			if sec.Observed {
				result.Observed = append(result.Observed, name)
			}
			result.Sections = append(result.Sections, sec)
			result.Timings.SectionsMS[name] = time.Since(sectionStart).Milliseconds()
			continue
		}
		section := Section{
			Name:        def.name,
			Description: def.description,
			Unit:        def.unit,
		}
		if def.countSQL != "" {
			qr, err := e.run(ctx, "SELECT * FROM ( "+def.countSQL+" ) AS _burstel_sub LIMIT 1", 1, policy)
			if err == nil && len(qr.Rows) == 1 {
				section.SampleSize = asInt64(qr.Rows[0][0])
			}
		}
		qr, err := e.run(ctx, def.sql(topN), topN, policy)
		if err != nil {
			return nil, fmt.Errorf("analysis: section %s: %w", name, err)
		}
		section.Columns = qr.Columns
		section.Rows = qr.Rows
		section.GroupCount = qr.RowCount
		section.Observed = qr.RowCount > 0
		section.Notes = qr.Notes
		if !section.Observed {
			section.Notes = appendUnique(section.Notes, "no rows for this section in the frozen shot")
		}
		if section.Observed {
			result.Observed = append(result.Observed, name)
		}
		result.Sections = append(result.Sections, section)
		result.Timings.SectionsMS[name] = time.Since(sectionStart).Milliseconds()
	}
	result.Timings.TotalMS = time.Since(startedAt).Milliseconds()

	result.Notes = []string{
		"Values in sections and raw_counts are query-derived from the frozen DuckDB snapshot.",
		"Hypotheses, when present, are explicitly separated and are not observations.",
	}
	return result, nil
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case string:
		var n int64
		_, _ = fmt.Sscan(t, &n)
		return n
	default:
		return 0
	}
}

// SchemaDocuments returns the documented schema for model discovery.
func SchemaDocuments(policy Policy) SchemaInfo {
	return SchemaInfo{
		SchemaVersion: SchemaVersion,
		Tables:        tableDocs(),
		Templates:     Templates(),
		Safety: SafetyInfo{
			ReadOnly:        true,
			SingleStatement: true,
			MaxRows:         policy.MaxRows,
			MaxValueBytes:   policy.MaxValueBytes,
			AllowedTables:   policy.AllowedTables(),
			Forbidden: []string{
				"multiple statements", "COPY/ATTACH/INSTALL/LOAD/PRAGMA/SET",
				"CREATE/INSERT/UPDATE/DELETE/DROP/ALTER", "unknown tables",
				"unknown or file/network functions (read_csv, read_parquet, glob, httpfs, getenv, ...)",
			},
		},
	}
}

// SchemaVersion documents the schema this build understands.
const SchemaVersion = "1"

func tableDocs() []TableInfo {
	return []TableInfo{
		{Name: "schema_meta", Description: "Schema provenance key/value pairs.", Columns: cols("key VARCHAR", "value VARCHAR")},
		{Name: "otlp_batches", Description: "Lossless raw OTLP request bytes (source of truth).", Columns: cols(
			"shot_id VARCHAR", "ordinal BIGINT", "signal VARCHAR", "received_at TIMESTAMP",
			"content_encoding VARCHAR", "record_count BIGINT", "payload_length BIGINT",
			"resource_summary VARCHAR", "payload BLOB")},
		{Name: "spans", Description: "Trace span projection.", Columns: cols(
			"shot_id VARCHAR", "ordinal BIGINT", "trace_id VARCHAR", "span_id VARCHAR",
			"parent_span_id VARCHAR", "trace_state VARCHAR", "name VARCHAR", "kind VARCHAR",
			"start_time_unix_nano BIGINT", "end_time_unix_nano BIGINT", "status_code VARCHAR",
			"status_message VARCHAR", "service_name VARCHAR", "scope_name VARCHAR", "scope_version VARCHAR",
			"resource_attributes VARCHAR", "scope_attributes VARCHAR", "attributes VARCHAR",
			"flags BIGINT", "dropped_attributes_count BIGINT", "dropped_events_count BIGINT",
			"dropped_links_count BIGINT", "events_count BIGINT", "links_count BIGINT")},
		{Name: "log_records", Description: "Log record projection with trace correlation.", Columns: cols(
			"shot_id VARCHAR", "ordinal BIGINT", "time_unix_nano BIGINT", "observed_time_unix_nano BIGINT",
			"severity_number VARCHAR", "severity_text VARCHAR", "body VARCHAR", "service_name VARCHAR",
			"scope_name VARCHAR", "scope_version VARCHAR", "resource_attributes VARCHAR",
			"scope_attributes VARCHAR", "attributes VARCHAR", "trace_id VARCHAR", "span_id VARCHAR",
			"event_name VARCHAR", "flags BIGINT", "dropped_attributes_count BIGINT")},
		{Name: "metric_points", Description: "Metric point projection (number + histogram).", Columns: cols(
			"shot_id VARCHAR", "ordinal BIGINT", "metric_name VARCHAR", "metric_type VARCHAR",
			"metric_unit VARCHAR", "metric_description VARCHAR", "service_name VARCHAR", "scope_name VARCHAR",
			"scope_version VARCHAR", "resource_attributes VARCHAR", "scope_attributes VARCHAR",
			"attributes VARCHAR", "start_time_unix_nano BIGINT", "time_unix_nano BIGINT", "flags BIGINT",
			"value_double DOUBLE", "value_int BIGINT", "aggregation_temporality INTEGER",
			"is_monotonic BOOLEAN", "count BIGINT", "sum DOUBLE", "min DOUBLE", "max DOUBLE",
			"bucket_counts VARCHAR", "explicit_bounds VARCHAR", "detail VARCHAR")},
	}
}

func cols(defs ...string) []Column {
	out := make([]Column, 0, len(defs))
	for _, d := range defs {
		parts := strings.SplitN(d, " ", 2)
		c := Column{Name: parts[0]}
		if len(parts) > 1 {
			c.Type = parts[1]
		}
		out = append(out, c)
	}
	return out
}
