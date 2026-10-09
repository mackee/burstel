package analysis

import (
	"context"
	"fmt"
	"sort"
)

type latencyRow struct {
	service, operation string
	n                  int64
	p50, p95, p99      float64
}

type errorRow struct {
	service, operation string
	errors, total      int64
	rate               float64
}

// CompareEngines diffs two restored frozen shots using SQL aggregates only.
func CompareEngines(ctx context.Context, policy Policy, left, right *Engine, leftID, rightID string, topN int) (*CompareResult, error) {
	if topN <= 0 || topN > policy.MaxRows {
		topN = 20
	}
	leftCounts, leftSeconds, err := sideCounts(ctx, left, policy)
	if err != nil {
		return nil, err
	}
	rightCounts, rightSeconds, err := sideCounts(ctx, right, policy)
	if err != nil {
		return nil, err
	}

	leftLat, err := latencyRows(ctx, left, policy, topN)
	if err != nil {
		return nil, err
	}
	rightLat, err := latencyRows(ctx, right, policy, topN)
	if err != nil {
		return nil, err
	}
	leftErr, err := errorRows(ctx, left, policy, topN)
	if err != nil {
		return nil, err
	}
	rightErr, err := errorRows(ctx, right, policy, topN)
	if err != nil {
		return nil, err
	}

	res := &CompareResult{
		Left: CompareSide{
			RestoreID:   leftID,
			ShotID:      leftID,
			Counts:      leftCounts,
			SchemaVer:   left.SchemaVersion(),
			SpanSeconds: leftSeconds,
		},
		Right: CompareSide{
			RestoreID:   rightID,
			ShotID:      rightID,
			Counts:      rightCounts,
			SchemaVer:   right.SchemaVersion(),
			SpanSeconds: rightSeconds,
		},
		SampleSizes: SampleSizes{Left: leftCounts, Right: rightCounts},
	}

	// Latency join keyed by (service, operation).
	rightByKey := map[string]latencyRow{}
	for _, r := range rightLat {
		rightByKey[key(r.service, r.operation)] = r
	}
	seen := map[string]bool{}
	for _, l := range leftLat {
		k := key(l.service, l.operation)
		seen[k] = true
		r, ok := rightByKey[k]
		if !ok {
			res.Latency = append(res.Latency, LatencyDelta{
				Service: l.service, Operation: l.operation, Unit: "ms",
				LeftN: l.n, LeftP50: l.p50, LeftP95: l.p95, LeftP99: l.p99,
				Note: "only present in left shot",
			})
			continue
		}
		res.Latency = append(res.Latency, LatencyDelta{
			Service: l.service, Operation: l.operation, Unit: "ms",
			LeftN: l.n, RightN: r.n,
			LeftP50: l.p50, RightP50: r.p50, DeltaP50: round3(r.p50 - l.p50),
			LeftP95: l.p95, RightP95: r.p95, DeltaP95: round3(r.p95 - l.p95),
			LeftP99: l.p99, RightP99: r.p99, DeltaP99: round3(r.p99 - l.p99),
			PctP95: pctChange(l.p95, r.p95),
		})
	}
	for _, r := range rightLat {
		if !seen[key(r.service, r.operation)] {
			res.Latency = append(res.Latency, LatencyDelta{
				Service: r.service, Operation: r.operation, Unit: "ms",
				RightN: r.n, RightP50: r.p50, RightP95: r.p95, RightP99: r.p99,
				Note: "only present in right shot",
			})
		}
	}
	sort.SliceStable(res.Latency, func(i, j int) bool {
		return absf(res.Latency[i].DeltaP95) > absf(res.Latency[j].DeltaP95)
	})

	// Error join keyed by (service, operation).
	rightErrByKey := map[string]errorRow{}
	for _, r := range rightErr {
		rightErrByKey[key(r.service, r.operation)] = r
	}
	seenErr := map[string]bool{}
	for _, l := range leftErr {
		k := key(l.service, l.operation)
		seenErr[k] = true
		r, ok := rightErrByKey[k]
		if !ok {
			res.Errors = append(res.Errors, ErrorDelta{
				Service: l.service, Operation: l.operation,
				LeftErrors: l.errors, LeftRate: l.rate,
				DeltaErrors: -l.errors, DeltaRate: -l.rate,
				Note: "only present in left shot",
			})
			continue
		}
		res.Errors = append(res.Errors, ErrorDelta{
			Service: l.service, Operation: l.operation,
			LeftErrors: l.errors, RightErrors: r.errors, DeltaErrors: r.errors - l.errors,
			LeftRate: l.rate, RightRate: r.rate, DeltaRate: round3(r.rate - l.rate),
		})
	}
	for _, r := range rightErr {
		if !seenErr[key(r.service, r.operation)] {
			res.Errors = append(res.Errors, ErrorDelta{
				Service: r.service, Operation: r.operation,
				RightErrors: r.errors, RightRate: r.rate,
				DeltaErrors: r.errors, DeltaRate: r.rate,
				Note: "only present in right shot",
			})
		}
	}
	sort.SliceStable(res.Errors, func(i, j int) bool {
		return absi(res.Errors[i].DeltaErrors) > absi(res.Errors[j].DeltaErrors)
	})

	res.Disclosure = []string{
		"All values are SQL aggregates computed independently over each frozen snapshot; they describe observed differences, not causes.",
		fmt.Sprintf("Sample sizes: left spans=%d, right spans=%d.", leftCounts["spans"], rightCounts["spans"]),
	}
	if leftCounts["spans"] != rightCounts["spans"] {
		res.Disclosure = append(res.Disclosure, "Span sample sizes differ; percentile comparisons are sensitive to sample size.")
	}
	if left.SchemaVersion() != right.SchemaVersion() {
		res.Disclosure = append(res.Disclosure, fmt.Sprintf("Schema versions differ (left=%s, right=%s).", left.SchemaVersion(), right.SchemaVersion()))
	}
	for _, d := range res.Latency {
		if d.Note != "" {
			res.Disclosure = append(res.Disclosure, fmt.Sprintf("%s/%s %s.", d.Service, d.Operation, d.Note))
		}
	}
	res.Hypotheses = []string{
		"A positive p95 delta may indicate a latency regression, but workload mix, cache state and sample size can also explain it.",
	}
	return res, nil
}

func sideCounts(ctx context.Context, e *Engine, policy Policy) (map[string]int64, float64, error) {
	counts := map[string]int64{}
	queries := map[string]string{
		"batches":       "SELECT count(*) FROM otlp_batches",
		"spans":         "SELECT count(*) FROM spans",
		"traces":        "SELECT count(DISTINCT trace_id) FROM spans WHERE trace_id IS NOT NULL",
		"log_records":   "SELECT count(*) FROM log_records",
		"metric_points": "SELECT count(*) FROM metric_points",
		"error_spans":   "SELECT count(*) FROM spans WHERE status_code = 'STATUS_CODE_ERROR'",
	}
	for k, q := range queries {
		qr, err := e.run(ctx, q, 1, policy)
		if err != nil {
			return nil, 0, err
		}
		if len(qr.Rows) == 1 {
			counts[k] = asInt64(qr.Rows[0][0])
		}
	}
	seconds := 0.0
	qr, err := e.run(ctx, "SELECT coalesce(sum((end_time_unix_nano - start_time_unix_nano) / 1e9), 0) FROM spans WHERE "+spanDurationPredicate, 1, policy)
	if err != nil {
		return nil, 0, err
	}
	if len(qr.Rows) == 1 {
		if f, ok := qr.Rows[0][0].(float64); ok {
			seconds = f
		}
	}
	return counts, seconds, nil
}

func latencyRows(ctx context.Context, e *Engine, policy Policy, topN int) ([]latencyRow, error) {
	qr, err := e.run(ctx, fmt.Sprintf(`SELECT service_name, name, count(*) AS n,
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.50), 3),
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.95), 3),
  round(quantile_cont((end_time_unix_nano - start_time_unix_nano) / 1e6, 0.99), 3)
FROM spans WHERE %s
GROUP BY service_name, name
ORDER BY 5 DESC NULLS LAST
LIMIT %d`, spanDurationPredicate, topN), topN, policy)
	if err != nil {
		return nil, err
	}
	out := make([]latencyRow, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		out = append(out, latencyRow{
			service:   str(row[0]),
			operation: str(row[1]),
			n:         asInt64(row[2]),
			p50:       asFloat(row[3]),
			p95:       asFloat(row[4]),
			p99:       asFloat(row[5]),
		})
	}
	return out, nil
}

func errorRows(ctx context.Context, e *Engine, policy Policy, topN int) ([]errorRow, error) {
	qr, err := e.run(ctx, fmt.Sprintf(`SELECT service_name, name,
  sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) AS errors,
  count(*) AS total,
  round(100.0 * sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) / count(*), 3) AS rate
FROM spans WHERE %s
GROUP BY service_name, name
HAVING sum(CASE WHEN status_code = 'STATUS_CODE_ERROR' THEN 1 ELSE 0 END) > 0
ORDER BY errors DESC
LIMIT %d`, spanDurationPredicate, topN), topN, policy)
	if err != nil {
		return nil, err
	}
	out := make([]errorRow, 0, len(qr.Rows))
	for _, row := range qr.Rows {
		out = append(out, errorRow{
			service:   str(row[0]),
			operation: str(row[1]),
			errors:    asInt64(row[2]),
			total:     asInt64(row[3]),
			rate:      asFloat(row[4]),
		})
	}
	return out, nil
}

func key(a, b string) string { return a + "\x00" + b }

func str(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case string:
		var f float64
		_, _ = fmt.Sscan(t, &f)
		return f
	default:
		return 0
	}
}

func round3(f float64) float64 { return float64(int64(f*1000+sign(f)*0.5)) / 1000 }

func pctChange(from, to float64) float64 {
	if from == 0 {
		if to == 0 {
			return 0
		}
		return 0
	}
	return round3((to - from) / from * 100)
}

func sign(f float64) float64 {
	if f < 0 {
		return -1
	}
	return 1
}

func absf(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func absi(i int64) int64 {
	if i < 0 {
		return -i
	}
	return i
}
