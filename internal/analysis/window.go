package analysis

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Phase 16 makes a "shot" an immutable logical time window over the continuous
// per-instance telemetry streams (spec 014/015) rather than a recording target.
//
// This file defines the window value type, its half-open [start,end) semantics
// and the validation rules for externally supplied (backdated) windows.
//
// The central rule is: a window is *half-open*. `start` is included and `end` is
// excluded for every time basis. All analysis predicates are written from this
// type so an edge record is never counted twice or dropped.

// Provenance records who assigned a window's timestamps.
type Provenance string

const (
	// ProvenanceServer means the timestamps came from the burstel server clock.
	ProvenanceServer Provenance = "server"
	// ProvenanceExternal means the timestamps were supplied by a client (for
	// example an external benchmark marker) and are therefore not trusted.
	ProvenanceExternal Provenance = "external"
)

// Valid reports whether p is a known provenance value.
func (p Provenance) Valid() bool { return p == ProvenanceServer || p == ProvenanceExternal }

// ClockTrust classifies how much a window's timestamps can be trusted.
type ClockTrust string

const (
	// TrustServer marks server-clock timestamps.
	TrustServer ClockTrust = "server"
	// TrustClient marks externally supplied, untrusted client-clock timestamps.
	TrustClient ClockTrust = "untrusted_client"
)

// Default window bounds. They bound abuse (an enormous or ancient window) while
// remaining generous for real 1-3 minute benchmark markers plus ingest lag.
const (
	DefaultMaxWindowDuration = time.Hour
	DefaultMaxWindowPast     = 7 * 24 * time.Hour
	DefaultMaxClockSkew      = 5 * time.Minute
)

// WindowBounds bounds what a window may cover relative to "now".
type WindowBounds struct {
	// MaxDuration is the largest allowed (end-start).
	MaxDuration time.Duration
	// MaxPast is how far before now a window start may be (retention).
	MaxPast time.Duration
	// MaxFuture is how far in the future an end may be (clock-skew tolerance).
	MaxFuture time.Duration
}

// DefaultWindowBounds returns the conservative production bounds.
func DefaultWindowBounds() WindowBounds {
	return WindowBounds{
		MaxDuration: DefaultMaxWindowDuration,
		MaxPast:     DefaultMaxWindowPast,
		MaxFuture:   DefaultMaxClockSkew,
	}
}

func (b WindowBounds) withDefaults() WindowBounds {
	if b.MaxDuration <= 0 {
		b.MaxDuration = DefaultMaxWindowDuration
	}
	if b.MaxPast <= 0 {
		b.MaxPast = DefaultMaxWindowPast
	}
	if b.MaxFuture <= 0 {
		b.MaxFuture = DefaultMaxClockSkew
	}
	return b
}

// TimeWindow is an immutable, half-open [start,end) logical time window.
//
// Start is inclusive, End is exclusive, and both are UTC instants. The window
// carries explicit provenance so downstream results can disclose whether the
// timestamps are server or client sourced.
type TimeWindow struct {
	// Start is the inclusive lower bound (UTC).
	Start time.Time `json:"start"`
	// End is the exclusive upper bound (UTC).
	End time.Time `json:"end"`
	// Provenance records who assigned the timestamps.
	Provenance Provenance `json:"provenance"`
	// Trust is derived from Provenance; it is never caller-controlled.
	Trust ClockTrust `json:"clock_trust"`
	// Instances selects the included instance names. Empty means "all
	// appropriate registered instances", resolved by the caller.
	Instances []string `json:"instances,omitempty"`
	// Name is an optional human label for the marker.
	Name string `json:"name,omitempty"`
}

// NewServerWindow builds a window from trusted server timestamps.
func NewServerWindow(start, end time.Time, instances []string, name string) TimeWindow {
	return TimeWindow{
		Start:      start.UTC(),
		End:        end.UTC(),
		Provenance: ProvenanceServer,
		Trust:      TrustServer,
		Instances:  normalizeInstances(instances),
		Name:       strings.TrimSpace(name),
	}
}

// NewExternalWindow builds a window from untrusted, externally supplied
// timestamps. The caller MUST validate it with Validate before use.
func NewExternalWindow(start, end time.Time, instances []string, name string) TimeWindow {
	return TimeWindow{
		Start:      start.UTC(),
		End:        end.UTC(),
		Provenance: ProvenanceExternal,
		Trust:      TrustClient,
		Instances:  normalizeInstances(instances),
		Name:       strings.TrimSpace(name),
	}
}

// WindowValidationError is a stable, machine-readable rejection.
type WindowValidationError struct {
	Code    string
	Message string
}

func (e *WindowValidationError) Error() string { return e.Message }

func windowErr(code, message string) *WindowValidationError {
	return &WindowValidationError{Code: code, Message: message}
}

// Validate enforces the ordering, magnitude and retention bounds for a window
// against the current server time. The returned error (when non-nil) is a
// *WindowValidationError with a stable code.
func (w TimeWindow) Validate(now time.Time, bounds WindowBounds) error {
	b := bounds.withDefaults()
	if !w.Provenance.Valid() {
		return windowErr("invalid_provenance", "provenance must be server or external")
	}
	if w.Start.IsZero() || w.End.IsZero() {
		return windowErr("window_bounds_required", "start and end are required")
	}
	if !w.End.After(w.Start) {
		return windowErr("window_not_ordered", "end must be strictly after start (half-open [start,end))")
	}
	if d := w.End.Sub(w.Start); d > b.MaxDuration {
		return windowErr("window_too_long", fmt.Sprintf("window duration %s exceeds the %s limit", d, b.MaxDuration))
	}
	now = now.UTC()
	if w.Start.Before(now.Add(-b.MaxPast)) {
		return windowErr("window_too_old", "window start is outside the retained range")
	}
	if w.End.After(now.Add(b.MaxFuture)) {
		return windowErr("window_in_future", "window end is beyond the allowed clock skew")
	}
	for _, inst := range w.Instances {
		if !isValidInstanceLabel(inst) {
			return windowErr("invalid_instance", "instance name "+inst+" is not valid")
		}
	}
	return nil
}

// StartNS is the inclusive lower bound in Unix nanoseconds.
func (w TimeWindow) StartNS() int64 { return w.Start.UTC().UnixNano() }

// EndNS is the exclusive upper bound in Unix nanoseconds.
func (w TimeWindow) EndNS() int64 { return w.End.UTC().UnixNano() }

// ContainsNS reports whether ns is inside the half-open window.
func (w TimeWindow) ContainsNS(ns int64) bool { return ns >= w.StartNS() && ns < w.EndNS() }

// Duration is End-Start.
func (w TimeWindow) Duration() time.Duration { return w.End.Sub(w.Start) }

// Equal reports whether two windows describe the same half-open interval.
func (w TimeWindow) Equal(other TimeWindow) bool {
	return w.Start.Equal(other.Start) && w.End.Equal(other.End)
}

// Overlaps reports whether two half-open windows share any instant.
func (w TimeWindow) Overlaps(other TimeWindow) bool {
	return w.Start.Before(other.End) && other.Start.Before(w.End)
}

// TimeBasis selects the per-row event timestamp a signal is filtered on.
type TimeBasis string

const (
	// BasisSpanStart uses spans.start_time_unix_nano (start-in-shot default).
	BasisSpanStart TimeBasis = "span_start"
	// BasisMetricTime uses metric_points.time_unix_nano.
	BasisMetricTime TimeBasis = "metric_time"
	// BasisLogTime uses log_records.time_unix_nano, falling back to
	// observed_time_unix_nano when the event time is absent.
	BasisLogTime TimeBasis = "log_event_time"
	// BasisReceivedAt uses otlp_batches.received_at (receive time).
	BasisReceivedAt TimeBasis = "received_at"
)

// SpanBoundaryPolicy selects how a span that crosses the window edge is treated.
type SpanBoundaryPolicy string

const (
	// SpanStartInShot (default) counts a span when its start is in the window.
	SpanStartInShot SpanBoundaryPolicy = "start_in_shot"
	// SpanOverlapDiagnostics additionally reports crossing spans without
	// changing the default counted set.
	SpanOverlapDiagnostics SpanBoundaryPolicy = "overlap_diagnostics"
)

// LogTimeExpr is the default log time basis: event time, falling back to the
// observed time. It is emitted as SQL so the exact same predicate is used in
// every scoped view and in coverage accounting.
const LogTimeExpr = "COALESCE(NULLIF(time_unix_nano, 0), observed_time_unix_nano)"

// MetricTimeExpr is the default metric time basis.
const MetricTimeExpr = "time_unix_nano"

// SpanTimeExpr is the default span time basis (span start).
const SpanTimeExpr = "start_time_unix_nano"

func normalizeInstances(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(strings.ToLower(raw))
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// isValidInstanceLabel matches the named-instance wire form from spec 014.
func isValidInstanceLabel(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return false
		}
	}
	if name[0] == '-' || name[len(name)-1] == '-' {
		return false
	}
	return true
}
