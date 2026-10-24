package analysis

// WindowOpenRequest starts a logical-window analysis session. The catalog is the
// caller's view of the verified segment metadata; the store selects the
// candidate segments itself so the selection rule lives in one place.
type WindowOpenRequest struct {
	SessionID string        `json:"session_id"`
	Window    TimeWindow    `json:"window"`
	Catalog   []SegmentMeta `json:"catalog,omitempty"`
}

// WindowOpenResponse returns the justified candidate selection plus the part
// cache hits (spec 033) and the open-phase timing.
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
