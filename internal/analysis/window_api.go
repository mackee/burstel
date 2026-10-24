package analysis

// WindowOpenRequest starts a logical-window analysis session. The catalog is the
// caller's view of the verified segment metadata; the store selects the
// candidate segments itself so the selection rule lives in one place.
type WindowOpenRequest struct {
	SessionID string        `json:"session_id"`
	Window    TimeWindow    `json:"window"`
	Catalog   []SegmentMeta `json:"catalog,omitempty"`
	// Download asks the analysis container to fetch the selected parts directly
	// from R2 with its bucket-scoped credential (spec 035). The Worker then
	// never streams part bytes.
	Download bool `json:"download,omitempty"`
	// ExpectedParts and ExpectedSpans are the materialization manifest totals
	// (spec 036). When set, commit refuses the session unless every part is
	// attached and the attached span total matches exactly.
	ExpectedParts int   `json:"expected_parts,omitempty"`
	ExpectedSpans int64 `json:"expected_spans,omitempty"`
}

// Manifest returns the caller's authoritative part/span totals.
func (r WindowOpenRequest) Manifest() WindowManifest {
	return WindowManifest{Parts: r.ExpectedParts, Spans: r.ExpectedSpans}
}

// WindowManifest is the caller's authoritative expectation for a session: the
// materialization part count and its declared span total (spec 036).
type WindowManifest struct {
	Parts int   `json:"parts,omitempty"`
	Spans int64 `json:"spans,omitempty"`
}

func (m WindowManifest) isEmpty() bool { return m.Parts <= 0 && m.Spans <= 0 }

// WindowCommitRequest commits a window session. ExpectedParts/ExpectedSpans are
// the manifest totals from the shot's materialization run; a commit whose
// attached set does not match is refused with `409 analysis_incomplete`.
type WindowCommitRequest struct {
	SessionID     string `json:"session_id"`
	ExpectedParts int    `json:"expected_parts,omitempty"`
	ExpectedSpans int64  `json:"expected_spans,omitempty"`
}

// Manifest returns the caller's authoritative part/span totals.
func (r WindowCommitRequest) Manifest() WindowManifest {
	return WindowManifest{Parts: r.ExpectedParts, Spans: r.ExpectedSpans}
}

// PartRowCounts is the verified row count of one attached part (spec 036). The
// counts come from the attached part catalog itself, before any window
// predicate is applied, so the Worker can compare the attached total with the
// materialization manifest.
type PartRowCounts struct {
	Key          string `json:"key"`
	Spans        int64  `json:"spans"`
	LogRecords   int64  `json:"log_records"`
	MetricPoints int64  `json:"metric_points"`
	Batches      int64  `json:"batches"`
}

// WindowCommit is the result of a committed window session. It is both the
// commit response and the verification record: AttachedParts and PartRows prove
// what the container actually attached, AttachedSpans/ViewSpans are the
// self-check counts, and Verified is true only when the attached set matches the
// declared manifest.
type WindowCommit struct {
	ScopedCoverage
	Timings ScopedTimings `json:"timings"`
	// AttachedParts is the number of selected parts present locally with a
	// verified digest.
	AttachedParts int `json:"attached_parts"`
	// PartRows is the per-part row count of every attached part.
	PartRows []PartRowCounts `json:"part_rows,omitempty"`
	// AttachedSpans is the sum of the per-part span counts.
	AttachedSpans int64 `json:"attached_spans"`
	// ViewSpans is `SELECT count(*) FROM spans` over the committed views.
	ViewSpans int64 `json:"view_spans"`
	// ExpectedParts and ExpectedSpans echo the manifest totals, when supplied.
	ExpectedParts int   `json:"expected_parts,omitempty"`
	ExpectedSpans int64 `json:"expected_spans,omitempty"`
	// ViewMatchesManifest reports whether the committed view count equals the
	// manifest. Informational: a span whose start precedes the window is stored
	// in a part but excluded by the start-in-shot predicate.
	ViewMatchesManifest bool `json:"view_matches_manifest"`
	// Verified is true only when every selected part is attached and the
	// attached span total equals the manifest.
	Verified bool `json:"verified"`
}

// WindowCommitResponse is the wire form of a commit (spec 036). It keeps the
// historical name used by the Worker and the server tests.
type WindowCommitResponse = WindowCommit

// IncompleteError refuses a commit whose attached part set does not satisfy the
// declared manifest. It is never a partial result: the session is discarded and
// the caller must not return rows.
type IncompleteError struct {
	Message       string   `json:"message"`
	MissingParts  []string `json:"missing_parts,omitempty"`
	AttachedParts int      `json:"attached_parts"`
	ExpectedParts int      `json:"expected_parts,omitempty"`
	AttachedSpans int64    `json:"attached_spans"`
	ExpectedSpans int64    `json:"expected_spans,omitempty"`
}

func (e *IncompleteError) Error() string { return e.Message }

// Code is the stable machine-readable code the Worker maps to
// `409 analysis_incomplete`.
func (e *IncompleteError) Code() string { return "analysis_incomplete" }

func incompleteError(message string) *IncompleteError {
	return &IncompleteError{Message: message}
}

// WindowOpenResponse returns the justified candidate selection plus the part
// cache hits (spec 033/035) and the open-phase timing.
type WindowOpenResponse struct {
	SessionID  string           `json:"session_id"`
	Window     TimeWindow       `json:"window"`
	Selection  SegmentSelection `json:"selection"`
	CachedKeys []string         `json:"cached_keys,omitempty"`
	Timings    ScopedTimings    `json:"timings"`
}

// WindowPreloadResponse is the result of opening, downloading and committing a
// shot session from the MaterializeDO alarm (spec 035/036).
type WindowPreloadResponse struct {
	SessionID  string           `json:"session_id"`
	Selection  SegmentSelection `json:"selection"`
	CachedKeys []string         `json:"cached_keys,omitempty"`
	Coverage   ScopedCoverage   `json:"coverage"`
	Timings    ScopedTimings    `json:"timings"`
	// Spec 036 verification fields, copied from the commit result.
	AttachedParts       int             `json:"attached_parts"`
	PartRows            []PartRowCounts `json:"part_rows,omitempty"`
	AttachedSpans       int64           `json:"attached_spans"`
	ViewSpans           int64           `json:"view_spans"`
	ExpectedParts       int             `json:"expected_parts,omitempty"`
	ExpectedSpans       int64           `json:"expected_spans,omitempty"`
	ViewMatchesManifest bool            `json:"view_matches_manifest"`
	Verified            bool            `json:"verified"`
}
