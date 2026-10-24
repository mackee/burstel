package analysis

// Column describes one projected column.
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryResult is a bounded, typed result set. Rows are arrays aligned with
// Columns. 64-bit integers that cannot be represented exactly as an IEEE-754
// double are emitted as decimal strings so nanosecond timestamps survive a
// JSON round-trip through a JavaScript client.
type QueryResult struct {
	Columns   []Column `json:"columns"`
	Rows      [][]any  `json:"rows"`
	RowCount  int      `json:"row_count"`
	Truncated bool     `json:"truncated"`
	Limit     int      `json:"limit"`
	Bytes     int      `json:"bytes"`
	Notes     []string `json:"notes,omitempty"`
}

// Section is one analytical summary backed by a real SQL query.
type Section struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Unit        string   `json:"unit,omitempty"`
	Columns     []Column `json:"columns"`
	Rows        [][]any  `json:"rows"`
	// SampleSize is the number of underlying records the section considered.
	SampleSize int64 `json:"sample_size"`
	// GroupCount is the number of grouped rows returned.
	GroupCount int `json:"group_count"`
	// Observed is true when the section is backed by rows in the frozen shot.
	Observed bool `json:"observed"`
	// Notes disclose missing data or truncation for this section.
	Notes []string `json:"notes,omitempty"`
}

// AnalyzeResult is the structured output of shot_analyze. Values under
// Sections and RawCounts are query-derived observations; Hypotheses are
// explicitly separated, non-observed interpretations.
type AnalyzeResult struct {
	ShotID        string           `json:"shot_id"`
	SchemaVersion string           `json:"schema_version"`
	RawCounts     map[string]int64 `json:"raw_counts"`
	Sections      []Section        `json:"sections"`
	Observed      []string         `json:"observed"`
	Hypotheses    []string         `json:"hypotheses,omitempty"`
	Notes         []string         `json:"notes,omitempty"`
	Timings       *AnalyzeTimings  `json:"timings,omitempty"`
}

// AnalyzeTimings exposes per-section SQL time (spec 033 requirement 4).
type AnalyzeTimings struct {
	RawCountsMS int64            `json:"raw_counts_ms"`
	SectionsMS  map[string]int64 `json:"sections_ms"`
	TotalMS     int64            `json:"total_ms"`
}

// AnalyzeRequest selects which sections to compute.
type AnalyzeRequest struct {
	Sections []string `json:"sections,omitempty"`
	TopN     int      `json:"top_n,omitempty"`
}

// QueryRequest is a single constrained query.
type QueryRequest struct {
	SQL     string `json:"sql"`
	MaxRows int    `json:"max_rows,omitempty"`
}

// CompareRequest asks for a SQL-derived diff between two restored shots.
type CompareRequest struct {
	LeftRestoreID  string `json:"left_restore_id"`
	RightRestoreID string `json:"right_restore_id"`
	TopN           int    `json:"top_n,omitempty"`
}

// CompareSide summarises the raw volume of one side of a comparison.
type CompareSide struct {
	RestoreID   string           `json:"restore_id"`
	ShotID      string           `json:"shot_id"`
	Counts      map[string]int64 `json:"counts"`
	SchemaVer   string           `json:"schema_version"`
	SpanSeconds float64          `json:"span_seconds,omitempty"`
}

// LatencyDelta compares one (service, operation) group across two shots.
type LatencyDelta struct {
	Service   string  `json:"service"`
	Operation string  `json:"operation"`
	Unit      string  `json:"unit"`
	LeftN     int64   `json:"left_n"`
	RightN    int64   `json:"right_n"`
	LeftP50   float64 `json:"left_p50"`
	RightP50  float64 `json:"right_p50"`
	DeltaP50  float64 `json:"delta_p50"`
	LeftP95   float64 `json:"left_p95"`
	RightP95  float64 `json:"right_p95"`
	DeltaP95  float64 `json:"delta_p95"`
	LeftP99   float64 `json:"left_p99"`
	RightP99  float64 `json:"right_p99"`
	DeltaP99  float64 `json:"delta_p99"`
	PctP95    float64 `json:"pct_change_p95"`
	Note      string  `json:"note,omitempty"`
}

// ErrorDelta compares one (service, operation) error group.
type ErrorDelta struct {
	Service     string  `json:"service"`
	Operation   string  `json:"operation"`
	LeftErrors  int64   `json:"left_errors"`
	RightErrors int64   `json:"right_errors"`
	DeltaErrors int64   `json:"delta_errors"`
	LeftRate    float64 `json:"left_error_rate"`
	RightRate   float64 `json:"right_error_rate"`
	DeltaRate   float64 `json:"delta_error_rate"`
	Note        string  `json:"note,omitempty"`
}

// SampleSizes exposes the distinct record counts behind each side so a caller
// can judge whether a difference is meaningful.
type SampleSizes struct {
	Left  map[string]int64 `json:"left"`
	Right map[string]int64 `json:"right"`
}

// CompareResult is the full diff. Disclosure always lists the caveats that
// apply; it never presents a causal conclusion as an observation.
type CompareResult struct {
	Left        CompareSide    `json:"left"`
	Right       CompareSide    `json:"right"`
	SampleSizes SampleSizes    `json:"sample_sizes"`
	Latency     []LatencyDelta `json:"latency_deltas"`
	Errors      []ErrorDelta   `json:"error_deltas"`
	Disclosure  []string       `json:"disclosure"`
	Hypotheses  []string       `json:"hypotheses,omitempty"`
}

// RestoreInfo is returned after a frozen artifact is verified and restored.
type RestoreInfo struct {
	RestoreID     string `json:"restore_id"`
	SizeBytes     int64  `json:"size_bytes"`
	SHA256        string `json:"sha256"`
	SchemaVersion string `json:"schema_version"`
	Reused        bool   `json:"reused"`
}

// SchemaInfo documents the queryable tables and analysis templates.
type SchemaInfo struct {
	SchemaVersion string         `json:"schema_version"`
	Tables        []TableInfo    `json:"tables"`
	Templates     []TemplateInfo `json:"templates"`
	Safety        SafetyInfo     `json:"safety"`
}

// TableInfo documents one table.
type TableInfo struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Columns     []Column `json:"columns"`
}

// TemplateInfo documents one analysis section.
type TemplateInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Unit        string `json:"unit,omitempty"`
}

// SafetyInfo documents the enforced query policy.
type SafetyInfo struct {
	ReadOnly        bool     `json:"read_only"`
	SingleStatement bool     `json:"single_statement"`
	MaxRows         int      `json:"max_rows"`
	MaxValueBytes   int      `json:"max_value_bytes"`
	AllowedTables   []string `json:"allowed_tables"`
	Forbidden       []string `json:"forbidden"`
}
