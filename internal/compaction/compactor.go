package compaction

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mackee/burstel/internal/otlp"
)

// JournalPrefix is the R2 prefix of the durable raw journal (spec 014).
const JournalPrefix = "journal"

// SegmentsPrefix is the R2 prefix of immutable compacted segments.
const SegmentsPrefix = "segments"

// JournalObject is one raw OTLP journal object as advertised by R2 metadata.
type JournalObject struct {
	Key             string
	InstanceID      string
	Instance        string
	Signal          otlp.Signal
	ReceivedAt      time.Time
	ReceivedAtMS    int64
	ContentEncoding string
	ContentType     string
	SHA256          string
	Size            int64
	RequestID       string
}

// Config bounds a compactor run.
type Config struct {
	// Window is the target receive-time width of one segment.
	Window time.Duration
	// Grace is the finalize watermark lag: data younger than Grace is not
	// compacted until it is unambiguous (spec 015 recommends 90 s).
	Grace time.Duration
	// MaxWindowBytes and MaxWindowObjects split a window that grows too large.
	MaxWindowBytes   int64
	MaxWindowObjects int
	// Retention is the unreferenced-telemetry retention for GC. <= 0 disables
	// time-based GC (unlimited retention).
	Retention time.Duration
	// WorkDir holds temporary segment files. Empty uses os.TempDir.
	WorkDir string
}

// DefaultConfig returns the operational defaults from spec 015.
func DefaultConfig() Config {
	return Config{
		Window:           60 * time.Second,
		Grace:            90 * time.Second,
		MaxWindowBytes:   256 << 20,
		MaxWindowObjects: 4096,
		Retention:        24 * time.Hour,
	}
}

// Hooks let a caller inject deterministic failures at the durability
// boundaries the spec calls out.
type Hooks struct {
	// BeforeUpload runs before the R2 PUT (crash before upload).
	BeforeUpload func(seg SegmentInfo) error
	// AfterUpload runs after the R2 PUT returns (crash after upload, before the
	// manifest commit).
	AfterUpload func(seg SegmentInfo) error
	// BeforeManifest runs after the object verifies but before the manifest
	// commit.
	BeforeManifest func(seg SegmentInfo) error
	// DuringGC runs for each journal object GC is about to delete (crash
	// mid-GC).
	DuringGC func(journalKey string) error
}

// Stats summarizes one compaction pass.
type Stats struct {
	JournalObjects  int64
	Pending         int64
	Eligible        int64
	Windows         int64
	SegmentsCreated int64
	SegmentsReused  int64
	LateObjects     int64
	RecordsCovered  int64
	BytesCovered    int64
	GCDeleted       int64
	GCProtected     int64
	Duration        time.Duration
}

// Compactor builds and publishes segments from the raw journal.
type Compactor struct {
	store    ObjectStore
	manifest *Manifest
	cfg      Config
	now      func() time.Time
	hooks    Hooks
}

// New creates a compactor.
func New(store ObjectStore, manifest *Manifest, cfg Config) *Compactor {
	if cfg.Window <= 0 {
		cfg.Window = 60 * time.Second
	}
	if cfg.Grace < 0 {
		cfg.Grace = 0
	}
	if cfg.MaxWindowBytes <= 0 {
		cfg.MaxWindowBytes = 256 << 20
	}
	if cfg.MaxWindowObjects <= 0 {
		cfg.MaxWindowObjects = 4096
	}
	return &Compactor{store: store, manifest: manifest, cfg: cfg, now: func() time.Time { return time.Now().UTC() }}
}

// SetClock overrides the compactor clock (deterministic tests/trials).
func (c *Compactor) SetClock(fn func() time.Time) { c.now = fn }

// SetHooks installs failure-injection hooks.
func (c *Compactor) SetHooks(h Hooks) { c.hooks = h }

// Manifest exposes the durable catalog.
func (c *Compactor) Manifest() *Manifest { return c.manifest }

// Config returns the effective configuration.
func (c *Compactor) Config() Config { return c.cfg }

// Run performs one compaction pass over the pending, finalized raw journal.
func (c *Compactor) Run(ctx context.Context) (Stats, error) {
	start := time.Now()
	var st Stats

	objects, err := c.listJournal(ctx)
	if err != nil {
		return st, err
	}
	st.JournalObjects = int64(len(objects))
	keys := make([]string, 0, len(objects))
	for _, o := range objects {
		keys = append(keys, o.Key)
	}
	covered, err := c.manifest.IsCovered(ctx, keys)
	if err != nil {
		return st, err
	}

	watermark := c.now().Add(-c.cfg.Grace).UnixMilli()
	byInstance := map[string][]JournalObject{}
	for _, o := range objects {
		if covered[o.Key] {
			continue
		}
		st.Pending++
		if o.ReceivedAtMS > watermark {
			continue
		}
		st.Eligible++
		byInstance[o.InstanceID] = append(byInstance[o.InstanceID], o)
	}

	instances := make([]string, 0, len(byInstance))
	for id := range byInstance {
		instances = append(instances, id)
	}
	sort.Strings(instances)

	for _, instanceID := range instances {
		objs := byInstance[instanceID]
		sort.Slice(objs, func(i, j int) bool {
			if objs[i].ReceivedAtMS != objs[j].ReceivedAtMS {
				return objs[i].ReceivedAtMS < objs[j].ReceivedAtMS
			}
			return objs[i].Key < objs[j].Key
		})

		finalizedThrough, err := c.manifest.FinalizedThrough(ctx, instanceID)
		if err != nil {
			return st, err
		}

		for _, w := range c.windows(objs) {
			st.Windows++
			lateObjs := int64(0)
			for _, o := range w {
				if o.ReceivedAtMS <= finalizedThrough {
					lateObjs++
				}
			}
			seg, reused, err := c.compactWindow(ctx, instanceID, w, lateObjs > 0)
			if err != nil {
				return st, err
			}
			if reused {
				st.SegmentsReused++
			} else {
				st.SegmentsCreated++
			}
			st.RecordsCovered += seg.RecordCount
			st.BytesCovered += seg.Size
			st.LateObjects += lateObjs

			if err := c.manifest.SetFinalizedThrough(ctx, instanceID, w[len(w)-1].ReceivedAtMS); err != nil {
				return st, err
			}
			if lateObjs > 0 {
				_ = c.manifest.Bump(ctx, "late_events", lateObjs)
				_ = c.manifest.AddGap(ctx, Gap{
					ID:         "late:" + seg.Key,
					InstanceID: instanceID,
					SegmentKey: seg.Key,
					FromMS:     w[0].ReceivedAtMS,
					ToMS:       w[len(w)-1].ReceivedAtMS,
					Reason:     "late_reopen",
					DetectedAt: c.now().UnixMilli(),
					Resolved:   true,
				})
			}
		}
	}

	_ = c.manifest.Bump(ctx, "compaction_runs", 1)
	_ = c.manifest.Bump(ctx, "records_covered", st.RecordsCovered)
	st.Duration = time.Since(start)
	return st, nil
}

// compactWindow builds, uploads, verifies and publishes one segment. It returns
// the manifest view of the segment (which may have been reused from a previous
// abandoned upload).
func (c *Compactor) compactWindow(ctx context.Context, instanceID string, objs []JournalObject, late bool) (SegmentManifest, bool, error) {
	records := make([]JournalRecord, 0, len(objs))
	for _, o := range objs {
		rec, err := c.fetchRecord(ctx, o)
		if err != nil {
			return SegmentManifest{}, false, err
		}
		records = append(records, rec)
	}

	first, last := records[0].Object.ReceivedAtMS, records[0].Object.ReceivedAtMS
	srcKeys := make([]string, 0, len(records))
	for _, r := range records {
		if r.Object.ReceivedAtMS < first {
			first = r.Object.ReceivedAtMS
		}
		if r.Object.ReceivedAtMS > last {
			last = r.Object.ReceivedAtMS
		}
		srcKeys = append(srcKeys, r.Object.Key)
	}
	sort.Strings(srcKeys)
	key := segmentKey(instanceID, first, last, srcKeys)

	tmp, cleanup, err := NewScratchSegment(c.cfg.WorkDir, key)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	defer cleanup()
	info, err := BuildSegment(ctx, tmp, key, instanceID, records[0].Object.Instance, records)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	info.Key = key

	if c.hooks.BeforeUpload != nil {
		if err := c.hooks.BeforeUpload(info); err != nil {
			return SegmentManifest{}, false, err
		}
	}

	// Reuse a durable object from an earlier abandoned attempt when it is
	// present and its bytes match its advertised checksum.
	stored, reused, err := c.ensureObject(ctx, info, instanceID)
	if err != nil {
		return SegmentManifest{}, false, err
	}

	if c.hooks.AfterUpload != nil {
		if err := c.hooks.AfterUpload(info); err != nil {
			return SegmentManifest{}, false, err
		}
	}

	if c.hooks.BeforeManifest != nil {
		if err := c.hooks.BeforeManifest(info); err != nil {
			return SegmentManifest{}, false, err
		}
	}

	now := c.now().UnixMilli()
	seg := SegmentManifest{
		Key:             key,
		InstanceID:      instanceID,
		Size:            stored.Size,
		SHA256:          stored.SHA256,
		FirstReceivedAt: info.FirstReceivedAt,
		LastReceivedAt:  info.LastReceivedAt,
		RequestCount:    info.RequestCount,
		RecordCount:     info.RecordCount,
		SpanCount:       info.SpanCount,
		LogCount:        info.LogCount,
		MetricCount:     info.MetricCount,
		EventMinNS:      info.EventMinNS,
		EventMaxNS:      info.EventMaxNS,
		Late:            late,
		CreatedAt:       now,
	}
	coverage := make([]CoverageRow, 0, len(records))
	for _, r := range records {
		coverage = append(coverage, CoverageRow{
			JournalKey: r.Object.Key,
			SegmentKey: key,
			InstanceID: instanceID,
			Signal:     string(r.Object.Signal),
			ReceivedAt: r.Object.ReceivedAtMS,
			Size:       r.Object.Size,
		})
	}
	res, err := c.manifest.CommitSegment(ctx, seg, coverage, now)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	if res.Duplicate {
		existing, ok, err := c.manifest.GetSegment(ctx, key)
		if err != nil {
			return SegmentManifest{}, false, err
		}
		if !ok {
			return SegmentManifest{}, false, fmt.Errorf("compaction: duplicate segment %s vanished", key)
		}
		return existing, true, nil
	}
	return seg, reused, nil
}

// ensureObject makes the segment durable in R2 and re-verifies it. It returns
// the stored object, reusing a prior verified upload of the same key.
func (c *Compactor) ensureObject(ctx context.Context, info SegmentInfo, instanceID string) (ObjectInfo, bool, error) {
	if existing, ok, err := c.store.Head(ctx, info.Key); err != nil {
		return ObjectInfo{}, false, err
	} else if ok && existing.Size > 0 && existing.SHA256 != "" {
		data, _, err := c.store.Get(ctx, info.Key)
		if err == nil && int64(len(data)) == existing.Size && sha256Hex(data) == existing.SHA256 {
			return existing, true, nil
		}
	}
	data, err := os.ReadFile(info.Path)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	obj, err := c.store.Put(ctx, info.Key, data, PutOptions{
		ContentType: "application/vnd.duckdb",
		SHA256:      info.SHA256,
		Metadata: map[string]string{
			"sha256":            info.SHA256,
			"instance_id":       instanceID,
			"segment_key":       info.Key,
			"first_received_at": fmt.Sprint(info.FirstReceivedAt),
			"last_received_at":  fmt.Sprint(info.LastReceivedAt),
			"request_count":     fmt.Sprint(info.RequestCount),
			"record_count":      fmt.Sprint(info.RecordCount),
		},
	})
	if err != nil {
		return ObjectInfo{}, false, err
	}
	// Re-verify the stored object before it may be advertised (spec 015).
	head, ok, err := c.store.Head(ctx, info.Key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if !ok {
		return ObjectInfo{}, false, fmt.Errorf("compaction: uploaded segment %s is not present", info.Key)
	}
	stored, _, err := c.store.Get(ctx, info.Key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	if int64(len(stored)) != head.Size || sha256Hex(stored) != head.SHA256 {
		return ObjectInfo{}, false, fmt.Errorf("compaction: stored segment %s failed checksum verification", info.Key)
	}
	if obj.Size != head.Size || obj.SHA256 != head.SHA256 {
		return ObjectInfo{}, false, fmt.Errorf("compaction: stored segment %s metadata mismatch", info.Key)
	}
	return head, false, nil
}

// windows splits a time-ordered object slice into bounded windows.
func (c *Compactor) windows(objs []JournalObject) [][]JournalObject {
	if len(objs) == 0 {
		return nil
	}
	var out [][]JournalObject
	cur := []JournalObject{objs[0]}
	var curBytes, curFirst int64 = objs[0].Size, objs[0].ReceivedAtMS
	for _, o := range objs[1:] {
		tooWide := o.ReceivedAtMS-curFirst >= c.cfg.Window.Milliseconds()
		tooBig := curBytes+o.Size > c.cfg.MaxWindowBytes || len(cur) >= c.cfg.MaxWindowObjects
		if tooWide || tooBig {
			out = append(out, cur)
			cur = nil
			curFirst = o.ReceivedAtMS
			curBytes = 0
		}
		cur = append(cur, o)
		curBytes += o.Size
	}
	out = append(out, cur)
	return out
}

func (c *Compactor) listJournal(ctx context.Context) ([]JournalObject, error) {
	var out []JournalObject
	cursor := ""
	for {
		infos, next, err := c.store.List(ctx, JournalPrefix+"/", cursor, 1000)
		if err != nil {
			return nil, err
		}
		for _, info := range infos {
			o, err := parseJournalObject(info)
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
		if next == "" {
			return out, nil
		}
		cursor = next
	}
}

func (c *Compactor) fetchRecord(ctx context.Context, o JournalObject) (JournalRecord, error) {
	wire, obj, err := c.store.Get(ctx, o.Key)
	if err != nil {
		return JournalRecord{}, err
	}
	if obj.SHA256 != "" && sha256Hex(wire) != obj.SHA256 {
		return JournalRecord{}, fmt.Errorf("compaction: journal object %s failed checksum", o.Key)
	}
	payload := wire
	if o.ContentEncoding == "gzip" {
		payload, err = gunzipBytes(wire)
		if err != nil {
			return JournalRecord{}, fmt.Errorf("compaction: gunzip %s: %w", o.Key, err)
		}
	}
	decoded, err := otlp.Decode(o.Signal, payload)
	if err != nil {
		return JournalRecord{}, fmt.Errorf("compaction: decode %s: %w", o.Key, err)
	}
	return JournalRecord{Object: o, Wire: wire, Payload: payload, Decoded: decoded}, nil
}

// ReconcileReport summarizes a manifest/R2 reconciliation.
type ReconcileReport struct {
	SegmentsChecked   int64
	SegmentsMissing   int64
	SegmentsRecovered int64
	OrphanSegments    int64
	PendingObjects    int64
	ChecksumFailures  int64
}

// Reconcile re-verifies every advertised segment against R2 and rebuilds the
// gap/status state. It is safe to run after a DO eviction or compactor loss.
func (c *Compactor) Reconcile(ctx context.Context) (ReconcileReport, error) {
	var rep ReconcileReport
	segments, err := c.allSegments(ctx)
	if err != nil {
		return rep, err
	}
	known := map[string]bool{}
	for _, seg := range segments {
		known[seg.Key] = true
	}
	for _, seg := range segments {
		rep.SegmentsChecked++
		head, ok, err := c.store.Head(ctx, seg.Key)
		if err != nil {
			return rep, err
		}
		if !ok {
			rep.SegmentsMissing++
			if err := c.manifest.SetSegmentStatus(ctx, seg.Key, StatusMissing); err != nil {
				return rep, err
			}
			if err := c.manifest.AddGap(ctx, Gap{
				ID: "missing:" + seg.Key, InstanceID: seg.InstanceID, SegmentKey: seg.Key,
				FromMS: seg.FirstReceivedAt, ToMS: seg.LastReceivedAt,
				Reason: "missing_segment", DetectedAt: c.now().UnixMilli(),
			}); err != nil {
				return rep, err
			}
			continue
		}
		data, _, err := c.store.Get(ctx, seg.Key)
		if err != nil {
			return rep, err
		}
		if int64(len(data)) != seg.Size || sha256Hex(data) != seg.SHA256 || head.SHA256 != seg.SHA256 {
			rep.ChecksumFailures++
			if err := c.manifest.SetSegmentStatus(ctx, seg.Key, StatusMissing); err != nil {
				return rep, err
			}
			if err := c.manifest.AddGap(ctx, Gap{
				ID: "checksum:" + seg.Key, InstanceID: seg.InstanceID, SegmentKey: seg.Key,
				FromMS: seg.FirstReceivedAt, ToMS: seg.LastReceivedAt,
				Reason: "checksum_mismatch", DetectedAt: c.now().UnixMilli(),
			}); err != nil {
				return rep, err
			}
			continue
		}
		if seg.Status != StatusVerified {
			rep.SegmentsRecovered++
			if err := c.manifest.SetSegmentStatus(ctx, seg.Key, StatusVerified); err != nil {
				return rep, err
			}
			if err := c.manifest.ResolveGapsForSegment(ctx, seg.Key); err != nil {
				return rep, err
			}
		}
	}

	// Orphan segments (present in R2, absent from the manifest) are reported,
	// never silently adopted, because their coverage cannot be proven.
	cursor := ""
	for {
		infos, next, err := c.store.List(ctx, SegmentsPrefix+"/", cursor, 1000)
		if err != nil {
			return rep, err
		}
		for _, info := range infos {
			if !known[info.Key] {
				rep.OrphanSegments++
			}
		}
		if next == "" {
			break
		}
		cursor = next
	}

	objects, err := c.listJournal(ctx)
	if err != nil {
		return rep, err
	}
	keys := make([]string, 0, len(objects))
	for _, o := range objects {
		keys = append(keys, o.Key)
	}
	covered, err := c.manifest.IsCovered(ctx, keys)
	if err != nil {
		return rep, err
	}
	for _, o := range objects {
		if !covered[o.Key] {
			rep.PendingObjects++
		}
	}
	return rep, nil
}

// RepairReport summarizes a repair pass.
type RepairReport struct {
	Checked      int64
	Repaired     int64
	Unrepairable int64
	Errors       []string
}

// Repair rebuilds any advertised segment that is missing or fails verification,
// as long as its raw journal objects are still present. It is the recovery path
// after a compactor crash, a DO eviction or a lost segment object.
func (c *Compactor) Repair(ctx context.Context) (RepairReport, error) {
	var rep RepairReport
	segments, err := c.allSegments(ctx)
	if err != nil {
		return rep, err
	}
	for _, seg := range segments {
		_, ok, err := c.store.Head(ctx, seg.Key)
		if err != nil {
			return rep, err
		}
		healthy := ok
		if ok {
			data, _, err := c.store.Get(ctx, seg.Key)
			if err != nil {
				return rep, err
			}
			if int64(len(data)) != seg.Size || sha256Hex(data) != seg.SHA256 {
				healthy = false
			}
		}
		if healthy && seg.Status == StatusVerified {
			continue
		}
		rep.Checked++
		coverage, err := c.manifest.ListCoverageForSegment(ctx, seg.Key)
		if err != nil {
			return rep, err
		}
		records := make([]JournalRecord, 0, len(coverage))
		missing := false
		for _, cov := range coverage {
			info, ok, err := c.store.Head(ctx, cov.JournalKey)
			if err != nil {
				return rep, err
			}
			if !ok {
				missing = true
				break
			}
			obj, err := parseJournalObject(info)
			if err != nil {
				return rep, err
			}
			rec, err := c.fetchRecord(ctx, obj)
			if err != nil {
				return rep, err
			}
			records = append(records, rec)
		}
		if missing || len(records) == 0 {
			rep.Unrepairable++
			rep.Errors = append(rep.Errors, "raw journal objects for "+seg.Key+" are no longer available")
			continue
		}
		sort.Slice(records, func(i, j int) bool { return records[i].Object.Key < records[j].Object.Key })
		tmp, cleanup, err := NewScratchSegment(c.cfg.WorkDir, seg.Key)
		if err != nil {
			return rep, err
		}
		info, err := BuildSegment(ctx, tmp, seg.Key, seg.InstanceID, records[0].Object.Instance, records)
		if err != nil {
			cleanup()
			return rep, err
		}
		info.Key = seg.Key
		stored, _, err := c.ensureObject(ctx, info, seg.InstanceID)
		cleanup()
		if err != nil {
			return rep, err
		}
		rebuilt := seg
		rebuilt.Size = stored.Size
		rebuilt.SHA256 = stored.SHA256
		rebuilt.RequestCount = info.RequestCount
		rebuilt.RecordCount = info.RecordCount
		rebuilt.SpanCount = info.SpanCount
		rebuilt.LogCount = info.LogCount
		rebuilt.MetricCount = info.MetricCount
		rebuilt.EventMinNS = info.EventMinNS
		rebuilt.EventMaxNS = info.EventMaxNS
		if err := c.manifest.ReplaceSegment(ctx, rebuilt, c.now().UnixMilli()); err != nil {
			return rep, err
		}
		if err := c.manifest.ResolveGapsForSegment(ctx, seg.Key); err != nil {
			return rep, err
		}
		rep.Repaired++
	}
	_ = c.manifest.Bump(ctx, "segments_repaired", rep.Repaired)
	return rep, nil
}

func (c *Compactor) allSegments(ctx context.Context) ([]SegmentManifest, error) {
	var out []SegmentManifest
	offset := 0
	for {
		page, err := c.manifest.ListSegments(ctx, "", 1000, offset)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < 1000 {
			return out, nil
		}
		offset += len(page)
	}
}

// GCStats summarizes garbage collection.
type GCStats struct {
	Considered int64
	Deleted    int64
	Protected  int64
	Skipped    int64
	BytesFreed int64
}

// GC deletes raw journal objects that are covered by a verified segment.
// Covered-but-unreferenced objects are retained until the retention window
// elapses unless force is set; pinned (shot-protected) objects are never
// deleted.
func (c *Compactor) GC(ctx context.Context, force bool) (GCStats, error) {
	var st GCStats
	pins, err := c.manifest.Pins(ctx)
	if err != nil {
		return st, err
	}
	details, err := c.manifest.ListCoverage(ctx)
	if err != nil {
		return st, err
	}
	cutoff := c.now().Add(-c.cfg.Retention).UnixMilli()
	for _, d := range details {
		st.Considered++
		if d.SegmentStatus != StatusVerified {
			st.Skipped++
			continue
		}
		if pinned(pins, d) {
			st.Protected++
			continue
		}
		if !force {
			if c.cfg.Retention <= 0 || d.ReceivedAt > cutoff {
				st.Skipped++
				continue
			}
		}
		if c.hooks.DuringGC != nil {
			if err := c.hooks.DuringGC(d.JournalKey); err != nil {
				return st, err
			}
		}
		if err := c.store.Delete(ctx, d.JournalKey); err != nil {
			if err == ErrObjectNotFound {
				st.Deleted++
				continue
			}
			return st, err
		}
		st.Deleted++
		st.BytesFreed += d.Size
	}
	_ = c.manifest.Bump(ctx, "gc_deleted", st.Deleted)
	return st, nil
}

// Completeness reports whether a receive-time range is provably complete.
type Completeness struct {
	InstanceID  string
	FromMS      int64
	ToMS        int64
	Complete    bool
	Missing     []string
	Late        []string
	Gaps        []Gap
	PendingKeys []string
}

// CheckCompleteness reports explicit incompleteness for a range: missing
// segments, unresolved gaps and late events all make it not complete.
func (c *Compactor) CheckCompleteness(ctx context.Context, instanceID string, fromMS, toMS int64) (Completeness, error) {
	if toMS <= 0 {
		toMS = int64(^uint64(0) >> 1)
	}
	out := Completeness{InstanceID: instanceID, FromMS: fromMS, ToMS: toMS, Complete: true}
	segments, err := c.allSegments(ctx)
	if err != nil {
		return out, err
	}
	for _, seg := range segments {
		if instanceID != "" && seg.InstanceID != instanceID {
			continue
		}
		if seg.LastReceivedAt < fromMS || seg.FirstReceivedAt > toMS {
			continue
		}
		if seg.Status != StatusVerified {
			out.Missing = append(out.Missing, seg.Key)
		}
		if seg.Late {
			out.Late = append(out.Late, seg.Key)
		}
	}
	gaps, err := c.manifest.Gaps(ctx, false)
	if err != nil {
		return out, err
	}
	for _, g := range gaps {
		if instanceID != "" && g.InstanceID != "" && g.InstanceID != instanceID {
			continue
		}
		if g.ToMS < fromMS || g.FromMS > toMS {
			continue
		}
		out.Gaps = append(out.Gaps, g)
	}
	if len(out.Missing) > 0 || len(out.Gaps) > 0 || len(out.Late) > 0 {
		out.Complete = false
	}
	objects, err := c.listJournal(ctx)
	if err != nil {
		return out, err
	}
	keys := make([]string, 0, len(objects))
	for _, o := range objects {
		keys = append(keys, o.Key)
	}
	covered, err := c.manifest.IsCovered(ctx, keys)
	if err != nil {
		return out, err
	}
	for _, o := range objects {
		if covered[o.Key] {
			continue
		}
		if instanceID != "" && o.InstanceID != instanceID {
			continue
		}
		if o.ReceivedAtMS < fromMS || o.ReceivedAtMS > toMS {
			continue
		}
		out.PendingKeys = append(out.PendingKeys, o.Key)
	}
	if len(out.PendingKeys) > 0 {
		out.Complete = false
	}
	return out, nil
}

// ------------------------------------------------------------- helpers

// parseJournalObject decodes metadata, falling back to the key layout
// (`journal/{instance_id}/{signal}/{received_at}-{id}.bin`).
func parseJournalObject(info ObjectInfo) (JournalObject, error) {
	o := JournalObject{Key: info.Key, Size: info.Size, SHA256: info.SHA256}
	md := info.Metadata
	if md != nil {
		o.InstanceID = md["instance_id"]
		o.Instance = md["instance"]
		o.Signal = otlp.Signal(md["signal"])
		o.ContentEncoding = md["content_encoding"]
		o.ContentType = strings.ToLower(strings.TrimSpace(md["content_type"]))
		if v := md["received_at"]; v != "" {
			o.ReceivedAtMS = parseInt64(v)
		}
		if md["sha256"] != "" {
			o.SHA256 = md["sha256"]
		}
	}
	if o.Signal == "" || !o.Signal.Valid() || o.InstanceID == "" || o.ReceivedAtMS == 0 {
		if err := parseJournalKey(info.Key, &o); err != nil {
			return JournalObject{}, err
		}
	}
	o.ReceivedAt = time.UnixMilli(o.ReceivedAtMS).UTC()
	return o, nil
}

func parseJournalKey(key string, o *JournalObject) error {
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != JournalPrefix {
		return fmt.Errorf("compaction: unexpected journal key %q", key)
	}
	if o.InstanceID == "" {
		o.InstanceID = parts[1]
	}
	if o.Signal == "" {
		o.Signal = otlp.Signal(parts[2])
	}
	base := strings.TrimSuffix(parts[3], ".bin")
	if i := strings.IndexByte(base, '-'); i > 0 {
		o.RequestID = base[i+1:]
		if o.ReceivedAtMS == 0 {
			o.ReceivedAtMS = parseInt64(base[:i])
		}
	}
	if !o.Signal.Valid() {
		return fmt.Errorf("compaction: journal key %q has an invalid signal", key)
	}
	return nil
}

// SegmentKey returns the deterministic, content-addressed R2 key of the segment
// built from exactly these source journal keys for an instance. It is a pure
// function of the (sorted) source keys and their receive-time range, so a retry
// after a crash produces the same key and can reuse an already-uploaded object.
func SegmentKey(instanceID string, firstMS, lastMS int64, srcKeys []string) string {
	h := sha256.New()
	for _, k := range srcKeys {
		io.WriteString(h, k)
		io.WriteString(h, "\n")
	}
	fp := hex.EncodeToString(h.Sum(nil))[:16]
	return fmt.Sprintf("%s/%s/%d-%d-%s.duckdb", SegmentsPrefix, instanceID, firstMS, lastMS, fp)
}

func segmentKey(instanceID string, firstMS, lastMS int64, srcKeys []string) string {
	return SegmentKey(instanceID, firstMS, lastMS, srcKeys)
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:8])
}

func gunzipBytes(raw []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func parseInt64(s string) int64 {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

func pinned(pins []Pin, d CoverageDetail) bool {
	for _, p := range pins {
		if p.InstanceID != "" && p.InstanceID != d.InstanceID {
			continue
		}
		if p.ToMS < d.SegmentFirstReceivedAt || p.FromMS > d.SegmentLastReceivedAt {
			continue
		}
		return true
	}
	return false
}
