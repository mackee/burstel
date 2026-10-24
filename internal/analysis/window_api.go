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

// WindowCommitResponse flattens the coverage summary and adds attach/materialize
// phase timings for the Worker log (spec 033).
type WindowCommitResponse struct {
	ScopedCoverage
	Timings ScopedTimings `json:"timings"`
}

// WindowPreloadResponse is the result of opening, downloading and committing a
// shot session from the MaterializeDO alarm (spec 035).
type WindowPreloadResponse struct {
	SessionID  string           `json:"session_id"`
	Selection  SegmentSelection `json:"selection"`
	CachedKeys []string         `json:"cached_keys,omitempty"`
	Coverage   ScopedCoverage   `json:"coverage"`
	Timings    ScopedTimings    `json:"timings"`
}
