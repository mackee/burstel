package analysis

// WindowOpenRequest starts a logical-window analysis session. The catalog is the
// caller's view of the verified segment metadata; the store selects the
// candidate segments itself so the selection rule lives in one place.
type WindowOpenRequest struct {
	SessionID string        `json:"session_id"`
	Window    TimeWindow    `json:"window"`
	Catalog   []SegmentMeta `json:"catalog,omitempty"`
}

// WindowOpenResponse returns the justified candidate selection.
type WindowOpenResponse struct {
	SessionID string           `json:"session_id"`
	Window    TimeWindow       `json:"window"`
	Selection SegmentSelection `json:"selection"`
}
