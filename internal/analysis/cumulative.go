package analysis

import (
	"context"
	"fmt"
)

// Cumulative counters and histograms are reported with OTLP temporality. A
// cumulative monotonic sum carries a value that keeps growing for the lifetime
// of the process, so the *per-shot* value is the delta between the last point in
// the window and the last point before the window.
//
// The pre-window baseline lives only in the private `_burstel_baseline_metrics`
// table. It is read here solely to compute the derived delta; pre-window rows
// are never exposed through the shot-scoped query surface.

func init() {
	sections = append(sections, sectionDef{
		name:        "metric_cumulative_delta",
		description: "Cumulative (monotonic) series delta over the window, using a pre-window baseline; derived only, never raw pre-window rows.",
		unit:        "value",
	})
}

type cumulativeRow struct {
	metricName string
	instanceID string
	service    string
	attributes string
	last       float64
	baseline   float64
}

// cumulativeSection computes the derived delta for cumulative metric series.
func (e *Engine) cumulativeSection(ctx context.Context, topN int, policy Policy) (Section, error) {
	section := Section{
		Name:        "metric_cumulative_delta",
		Description: "Cumulative (monotonic) series delta over the window, using a pre-window baseline; derived only, never raw pre-window rows.",
		Unit:        "value",
	}
	if !e.scoped {
		section.Notes = append(section.Notes, "cumulative delta requires a logical window snapshot with a pre-window baseline")
		return section, nil
	}
	latestSQL := `SELECT metric_name, instance_id, coalesce(service_name, ''), coalesce(attributes, ''),
		coalesce(sum, value_double, value_int)
	FROM %s
	WHERE aggregation_temporality = 2
	QUALIFY row_number() OVER (PARTITION BY metric_name, instance_id, coalesce(service_name, ''), coalesce(attributes, '') ORDER BY time_unix_nano DESC) = 1`
	win, err := e.run(ctx, fmt.Sprintf(latestSQL, "metric_points"), topN, policy)
	if err != nil {
		return section, fmt.Errorf("analysis: cumulative window query: %w", err)
	}
	base, err := e.run(ctx, fmt.Sprintf(latestSQL, "_burstel_baseline_metrics"), topN, policy)
	if err != nil {
		return section, fmt.Errorf("analysis: cumulative baseline query: %w", err)
	}
	baseByKey := map[string]float64{}
	for _, row := range base.Rows {
		baseByKey[cumKey(row)] = asFloat(row[4])
	}
	var rows [][]any
	for _, row := range win.Rows {
		w := asFloat(row[4])
		b := baseByKey[cumKey(row)]
		delta := round6(w - b)
		rows = append(rows, []any{row[0], row[1], row[2], round6(w), round6(b), delta})
	}
	section.Columns = []Column{
		{Name: "metric_name", Type: "VARCHAR"},
		{Name: "instance_id", Type: "VARCHAR"},
		{Name: "service_name", Type: "VARCHAR"},
		{Name: "window_last", Type: "DOUBLE"},
		{Name: "baseline_last", Type: "DOUBLE"},
		{Name: "delta", Type: "DOUBLE"},
	}
	section.Rows = rows
	section.GroupCount = len(rows)
	section.SampleSize = int64(len(rows))
	section.Observed = len(rows) > 0
	if !section.Observed {
		section.Notes = append(section.Notes, "no cumulative metric series in this window")
	}
	section.Notes = appendUnique(section.Notes, "delta values are derived; pre-window baseline rows are not exposed to queries")
	return section, nil
}

func cumKey(row []any) string {
	return fmt.Sprint(row[0]) + "\x00" + fmt.Sprint(row[1]) + "\x00" + fmt.Sprint(row[2]) + "\x00" + fmt.Sprint(row[3])
}

func round6(f float64) float64 { return float64(int64(f*1e6+sign(f)*0.5)) / 1e6 }
