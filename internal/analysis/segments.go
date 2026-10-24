package analysis

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// SegmentMeta is the analysis-side view of one immutable compacted segment
// (spec 015). It deliberately mirrors only the fields needed to select
// candidate segments for a window; the byte-level verification happens when
// the segment object is actually restored.
type SegmentMeta struct {
	Key             string `json:"key"`
	InstanceID      string `json:"instance_id,omitempty"`
	Instance        string `json:"instance,omitempty"`
	Status          string `json:"status,omitempty"`
	Size            int64  `json:"size,omitempty"`
	SHA256          string `json:"sha256,omitempty"`
	FirstReceivedAt int64  `json:"first_received_at"`
	LastReceivedAt  int64  `json:"last_received_at"`
	EventMinNS      *int64 `json:"event_min_ns,omitempty"`
	EventMaxNS      *int64 `json:"event_max_ns,omitempty"`
}

// Verified reports whether the segment is advertised as verified.
func (m SegmentMeta) Verified() bool { return m.Status == "" || m.Status == "verified" }

// SelectionOptions tunes candidate-segment selection.
type SelectionOptions struct {
	// IngestLag widens the receive-time range to allow for exports that arrive
	// after their events. A late export may carry shot-timestamped records whose
	// receive time falls outside the nominal window.
	IngestLag time.Duration
	// WatermarkMS, when > 0, is the finalized receive-time watermark (epoch ms)
	// known to be compacted. Segments beyond it may still be in flight.
	WatermarkMS int64
}

// DefaultIngestLag is a conservative default for export batching plus compaction
// latency.
const DefaultIngestLag = 2 * time.Minute

func (o SelectionOptions) lagMS() int64 {
	lag := o.IngestLag
	if lag <= 0 {
		lag = DefaultIngestLag
	}
	return lag.Milliseconds()
}

// SegmentSelection is the justified candidate set for a window.
type SegmentSelection struct {
	// Window is the window the selection was made for.
	Window TimeWindow
	// Selected is the ordered candidate set (by receive time then key).
	Selected []SegmentMeta
	// Missing lists advertised-but-unavailable segment keys.
	Missing []string
	// Expected is len(Selected)+len(Missing): the segments the window needs.
	Expected int
	// Justification explains why the selection is safe (ingest lag / event range).
	Justification string
}

// SelectSegments chooses the candidate segments for a window.
//
// A segment is a candidate when *either*:
//
//   - its receive-time range overlaps the window widened by the ingest lag, so
//     an export that arrived late but carries in-window records is included; or
//   - its event-time min/max overlaps the half-open window, which catches a
//     late export whose receive time is far after the window and would otherwise
//     be missed by the receive-time test alone.
//
// A segment that is advertised but not verified is reported in Missing and is
// still counted in Expected, so coverage cannot silently under-report.
func SelectSegments(window TimeWindow, segments []SegmentMeta, opts SelectionOptions) SegmentSelection {
	startMS := window.Start.UnixMilli()
	endMS := window.End.UnixMilli()
	lag := opts.lagMS()
	loMS := startMS - lag
	hiMS := endMS + lag

	sel := SegmentSelection{Window: window}
	// Instance filter: an empty window instance list means "all appropriate
	// registered instances", i.e. every segment.
	want := map[string]struct{}{}
	for _, inst := range window.Instances {
		want[inst] = struct{}{}
	}
	for _, seg := range segments {
		if len(want) > 0 {
			// A cross-instance segment scope is the synthetic `all`; its rows
			// carry real instance ids and the SQL view filters them. Spec 031
			// materialization parts use the same scope so ATTACH-based analysis
			// never drops a multi-instance part.
			if seg.InstanceID == "all" {
				// keep
			} else if _, ok := want[strings.ToLower(seg.Instance)]; !ok {
				// Fall back to matching the instance id so a caller that only
				// knows ids is still scoped correctly.
				if _, ok := want[strings.ToLower(seg.InstanceID)]; !ok {
					continue
				}
			}
		}
		receiveOverlap := seg.LastReceivedAt >= loMS && seg.FirstReceivedAt < hiMS
		eventOverlap := false
		if seg.EventMinNS != nil && seg.EventMaxNS != nil {
			// [min,max] event range intersects half-open [start,end).
			eventOverlap = *seg.EventMaxNS >= window.StartNS() && *seg.EventMinNS < window.EndNS()
		}
		if !receiveOverlap && !eventOverlap {
			continue
		}
		if !seg.Verified() {
			sel.Missing = append(sel.Missing, seg.Key)
			continue
		}
		sel.Selected = append(sel.Selected, seg)
	}
	sort.SliceStable(sel.Selected, func(i, j int) bool {
		if sel.Selected[i].FirstReceivedAt != sel.Selected[j].FirstReceivedAt {
			return sel.Selected[i].FirstReceivedAt < sel.Selected[j].FirstReceivedAt
		}
		return sel.Selected[i].Key < sel.Selected[j].Key
	})
	sort.Strings(sel.Missing)
	sel.Expected = len(sel.Selected) + len(sel.Missing)
	sel.Justification = fmt.Sprintf(
		"candidates selected by receive-time overlap with [%d,%d]ms (window widened by ingest lag %dms) OR event-time overlap with the half-open window",
		loMS, hiMS, lag,
	)
	if opts.WatermarkMS > 0 {
		sel.Justification += fmt.Sprintf("; finalized watermark=%dms", opts.WatermarkMS)
	}
	return sel
}
