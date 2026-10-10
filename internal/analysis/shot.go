package analysis

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// ShotState is the lifecycle of a logical window shot.
//
// Unlike the legacy recording shot (created/recording/stopping/frozen/failed),
// a window shot never owns a per-shot ingest token or recording container. Its
// lifecycle is:
//
//	open --stop--> finalizing --ready--> ready
//	                          \--missing/incomplete--> incomplete
//
// A backdated externally supplied window is created directly in `ready` (or
// `incomplete` when coverage is known to be partial).
type ShotState string

const (
	ShotOpen       ShotState = "open"
	ShotFinalizing ShotState = "finalizing"
	ShotReady      ShotState = "ready"
	ShotIncomplete ShotState = "incomplete"
	ShotFailed     ShotState = "failed"
)

// Terminal reports whether no further transition is expected.
func (s ShotState) Terminal() bool { return s == ShotReady || s == ShotIncomplete || s == ShotFailed }

// CoverageSummary describes how complete a shot's data is. It is always
// disclosed; missing segments are never silently filtered out.
type CoverageSummary struct {
	// ExpectedSegments is the number of candidate segments the window needs.
	ExpectedSegments int `json:"expected_segments"`
	// AvailableSegments is the number of those actually restored and verified.
	AvailableSegments int `json:"available_segments"`
	// MissingSegments lists candidate segment keys that could not be restored.
	MissingSegments []string `json:"missing_segments,omitempty"`
	// Instances is the resolved set of instances included in this shot.
	Instances []string `json:"instances,omitempty"`
	// LateArrivals counts rows whose event time is in the window but whose
	// receive time is after the window end.
	LateArrivals int64 `json:"late_arrivals,omitempty"`
	// LogObservedFallbacks counts log rows counted on observed time because the
	// event time was absent.
	LogObservedFallbacks int64 `json:"log_observed_fallbacks,omitempty"`
	// Complete is true only when every expected segment was available.
	Complete bool `json:"complete"`
}

// Shot is the durable metadata of a logical window shot. It is a metadata
// record only: it never carries instance credentials or a per-shot ingest
// token.
type Shot struct {
	ShotID     string         `json:"shot_id"`
	Name       string         `json:"name,omitempty"`
	Owner      string         `json:"owner"`
	State      ShotState      `json:"state"`
	Provenance Provenance     `json:"provenance"`
	Window     TimeWindow     `json:"window"`
	Instances  []string       `json:"instances,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	// QueryWatermark is the newest receive time known to be compacted into a
	// verified segment at finalize time; it justifies late-arrival selection.
	QueryWatermark time.Time       `json:"query_watermark,omitzero"`
	Coverage       CoverageSummary `json:"coverage"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// NewShot builds a shot from a window, normalizing identity and provenance.
func NewShot(shotID, name, owner string, window TimeWindow, metadata map[string]any, now time.Time) Shot {
	instances := normalizeInstances(window.Instances)
	return Shot{
		ShotID:     shotID,
		Name:       strings.TrimSpace(name),
		Owner:      normalizeOwner(owner),
		State:      ShotOpen,
		Provenance: window.Provenance,
		Window:     window,
		Instances:  instances,
		Metadata:   metadata,
		CreatedAt:  now.UTC(),
		UpdatedAt:  now.UTC(),
	}
}

// Finalize transitions an open shot to ready (or incomplete). It is idempotent:
// finalizing an already-terminal shot is a no-op that returns the current shot.
func (s *Shot) Finalize(now time.Time) {
	if s.State.Terminal() {
		return
	}
	if s.Coverage.Complete {
		s.State = ShotReady
	} else {
		s.State = ShotIncomplete
	}
	s.UpdatedAt = now.UTC()
}

// MarkFinalizing records the intent to stop. It is idempotent.
func (s *Shot) MarkFinalizing(now time.Time) {
	if s.State.Terminal() {
		return
	}
	s.State = ShotFinalizing
	s.UpdatedAt = now.UTC()
}

// SetCoverage replaces the coverage summary and derives the terminal state for
// a shot that is being finalized.
func (s *Shot) SetCoverage(c CoverageSummary, now time.Time) {
	c.Instances = normalizeInstances(append(s.Instances, c.Instances...))
	s.Coverage = c
	if s.State.Terminal() {
		s.State = stateForCoverage(c)
	}
	s.UpdatedAt = now.UTC()
}

func stateForCoverage(c CoverageSummary) ShotState {
	if c.Complete {
		return ShotReady
	}
	return ShotIncomplete
}

// ExpectedSegments returns the candidate count a complete shot needs.
func (s Shot) ExpectedSegments() int { return s.Coverage.ExpectedSegments }

// WindowString renders the half-open window for diagnostics.
func (s Shot) WindowString() string {
	return fmt.Sprintf("[%s,%s)", s.Window.Start.Format(time.RFC3339Nano), s.Window.End.Format(time.RFC3339Nano))
}

// CoverageNotes produces human-readable completeness disclosures. The result
// always mentions the window bounds and instance set so callers cannot mistake
// an empty result for "no traffic".
func (s Shot) CoverageNotes() []string {
	notes := []string{
		fmt.Sprintf("Half-open window [%s,%s); start included, end excluded.",
			s.Window.Start.Format(time.RFC3339Nano), s.Window.End.Format(time.RFC3339Nano)),
	}
	if s.Provenance == ProvenanceExternal {
		notes = append(notes, "Window timestamps were supplied externally and are untrusted client-clock values.")
	}
	if len(s.Instances) == 0 {
		notes = append(notes, "No instance filter was recorded; results cover every instance the caller queried.")
	} else {
		notes = append(notes, "Included instances: "+strings.Join(s.Instances, ", ")+".")
	}
	if !s.Coverage.Complete {
		if len(s.Coverage.MissingSegments) > 0 {
			notes = append(notes, "Missing segments (data not available): "+strings.Join(s.Coverage.MissingSegments, ", ")+".")
		}
		notes = append(notes, "Coverage is incomplete; sample counts are lower bounds.")
	}
	return notes
}

// CompareDisclosure lists the caveats that make a two-window comparison honest.
func CompareDisclosure(left, right Shot) []string {
	out := []string{
		"All values are SQL aggregates computed independently over each window; they describe observed differences, not causes.",
	}
	out = append(out, "left: "+left.WindowString()+" ("+string(left.Provenance)+", "+leftStateLabel(left)+")")
	out = append(out, "right: "+right.WindowString()+" ("+string(right.Provenance)+", "+rightStateLabel(right)+")")
	if d := left.Window.Duration(); d != right.Window.Duration() {
		out = append(out, fmt.Sprintf("Window durations differ (left=%s, right=%s); volume comparisons are duration-sensitive.",
			d, right.Window.Duration()))
	}
	if !sameStrings(left.Instances, right.Instances) {
		out = append(out, "Instance sets differ (left=["+strings.Join(left.Instances, ",")+"], right=["+strings.Join(right.Instances, ",")+"]).")
	}
	if !left.Coverage.Complete || !right.Coverage.Complete {
		out = append(out, "At least one side has incomplete coverage; percentile comparisons are sensitive to missing data.")
	}
	if left.Provenance != right.Provenance {
		out = append(out, "Provenance differs between the two windows.")
	}
	return out
}

func leftStateLabel(s Shot) string {
	if s.State == "" {
		return string(ShotReady)
	}
	return string(s.State)
}

func rightStateLabel(s Shot) string { return leftStateLabel(s) }

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func normalizeOwner(owner string) string {
	v := strings.TrimSpace(owner)
	if v == "" {
		return "legacy"
	}
	return v
}
