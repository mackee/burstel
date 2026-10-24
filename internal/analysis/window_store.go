package analysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WindowStore owns the in-flight logical-window analysis sessions exposed to the
// Worker over /internal/window/*. A session accepts verified segment bytes,
// materializes a shot-scoped read-only snapshot and answers bounded queries.
//
// It is deliberately separate from the frozen-shot restore Store: a window
// session is multi-segment and carries explicit coverage/selection diagnostics.

// DefaultMaxWindowBytes bounds the total bytes a single session may restore.
const DefaultMaxWindowBytes = int64(16) << 30

var sessionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// WindowStoreOptions configures a window session store.
type WindowStoreOptions struct {
	Policy          Policy
	Bounds          WindowBounds
	IngestLag       time.Duration
	BaselineHorizon time.Duration
	MaxSessionBytes int64
	Open            OpenOptions
	Now             func() time.Time
}

// WindowStore holds the active logical-window sessions.
type WindowStore struct {
	dir       string
	cacheDir  string
	policy    Policy
	bounds    WindowBounds
	ingestLag time.Duration
	baseline  time.Duration
	maxBytes  int64
	open      OpenOptions
	now       func() time.Time
	mu        sync.Mutex
	sessions  map[string]*WindowSession
	cacheSize int64
	closed    bool
}

// NewWindowStore creates a window session store rooted at dir.
func NewWindowStore(dir string, opts WindowStoreOptions) *WindowStore {
	policy := opts.Policy
	if policy.MaxRows == 0 {
		policy = DefaultPolicy()
	}
	bounds := opts.Bounds
	if bounds.MaxDuration <= 0 {
		bounds = DefaultWindowBounds()
	}
	maxBytes := opts.MaxSessionBytes
	if maxBytes <= 0 {
		maxBytes = DefaultMaxWindowBytes
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &WindowStore{
		dir:       dir,
		cacheDir:  filepath.Join(dir, "_parts"),
		policy:    policy,
		bounds:    bounds,
		ingestLag: opts.IngestLag,
		baseline:  opts.BaselineHorizon,
		maxBytes:  maxBytes,
		open:      opts.Open,
		now:       now,
		sessions:  map[string]*WindowSession{},
	}
}

// Policy returns the base query policy.
func (s *WindowStore) Policy() Policy { return s.policy }

// Session is one window analysis session.
type WindowSession struct {
	ID        string
	Window    TimeWindow
	Catalog   []SegmentMeta
	Selection SegmentSelection
	segments  []SegmentRef
	dir       string
	bytes     int64
	scoped    *ScopedEngine
	coverage  ScopedCoverage
	ready     bool
}

// OpenWindow validates a window and starts a session. catalog is the full set of
// segment metadata the caller knows about; the store selects candidates itself.
func (s *WindowStore) OpenWindow(id string, window TimeWindow, catalog []SegmentMeta) (SegmentSelection, error) {
	selection, _, err := s.OpenWindowWithCache(id, window, catalog)
	return selection, err
}

// OpenWindowWithCache also returns the keys whose bytes are already cached in
// this analysis container (spec 033). The Worker skips their R2 download and
// segment upload entirely; the session already references the cached files.
func (s *WindowStore) OpenWindowWithCache(id string, window TimeWindow, catalog []SegmentMeta) (SegmentSelection, []string, error) {
	if !sessionIDPattern.MatchString(id) {
		return SegmentSelection{}, nil, policyErr("invalid_session_id", "session id must match [A-Za-z0-9._-]{1,128}")
	}
	if err := window.Validate(s.now(), s.bounds); err != nil {
		return SegmentSelection{}, nil, err
	}
	selection := SelectSegments(window, catalog, SelectionOptions{IngestLag: s.ingestLag})
	dir := filepath.Join(s.dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SegmentSelection{}, nil, fmt.Errorf("analysis: window session dir: %w", err)
	}
	byKey := make(map[string]SegmentMeta, len(catalog))
	for _, meta := range catalog {
		byKey[meta.Key] = meta
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return SegmentSelection{}, nil, errors.New("analysis: window store is closed")
	}
	if prev, ok := s.sessions[id]; ok {
		_ = prev.scoped.Close()
		_ = os.RemoveAll(prev.dir)
	}
	sess := &WindowSession{ID: id, Window: window, Catalog: catalog, Selection: selection, dir: dir}
	var cached []string
	// A previous analysis of the same shot already wrote verified part files
	// into the container cache: reuse them without another download.
	for _, selected := range selection.Selected {
		meta, ok := byKey[selected.Key]
		if !ok {
			continue
		}
		if path, size, ok := s.cachedSegment(meta.Key, meta.Size, meta.SHA256); ok {
			sess.segments = append(sess.segments, SegmentRef{
				Key:      meta.Key,
				Instance: firstNonEmpty(meta.Instance, meta.InstanceID),
				Path:     path,
			})
			sess.bytes += size
			cached = append(cached, meta.Key)
		}
	}
	s.sessions[id] = sess
	return selection, cached, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (s *WindowStore) cachedSegment(key string, size int64, sha string) (string, int64, bool) {
	path := filepath.Join(s.cacheDir, hashName(key)+".duckdb")
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", 0, false
	}
	if size > 0 && info.Size() != size {
		return "", 0, false
	}
	norm := normalizeSHA(sha)
	if norm != "" {
		sidecar, err := os.ReadFile(path + ".sha256")
		if err != nil || strings.TrimSpace(string(sidecar)) != norm {
			return "", 0, false
		}
	}
	return path, info.Size(), true
}

// AddSegment writes one verified segment object into the session.
func (s *WindowStore) AddSegment(ctx context.Context, id string, ref SegmentRef, r io.Reader, size int64, sha string) error {
	s.mu.Lock()
	sess, ok := s.sessions[id]
	s.mu.Unlock()
	if !ok {
		return ErrRestoreNotFound
	}
	if err := validateSegmentPath(ref.Key); err != nil {
		return err
	}
	if size < 0 {
		return policyErr("invalid_segment_size", "segment size must be non-negative")
	}
	if sess.bytes+size > s.maxBytes {
		return policyErr("session_too_large", "window session exceeds the maximum byte budget")
	}
	normSHA := normalizeSHA(sha)
	if path, cachedSize, ok := s.cachedSegment(ref.Key, size, normSHA); ok {
		return s.appendCachedSegment(id, sess, ref, path, cachedSize)
	}
	if err := os.MkdirAll(s.cacheDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.cacheDir, hashName(ref.Key)+".duckdb")
	// Write to a unique temp file first: concurrent requests for the same key
	// (a retried Worker request overlapping the original) must not delete or
	// truncate each other's bytes.
	tmp := path + ".tmp-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	written, gotSHA, err := writeVerified(tmp, r, s.maxBytes)
	if err != nil {
		return err
	}
	if size > 0 && written != size {
		_ = os.Remove(tmp)
		return policyErr("size_mismatch", fmt.Sprintf("segment size %d does not match declared %d", written, size))
	}
	if normSHA != "" && gotSHA != normSHA {
		_ = os.Remove(tmp)
		return policyErr("sha256_mismatch", "segment sha256 does not match the declared checksum")
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("analysis: cache segment %s: %w", ref.Key, err)
	}
	if err := os.WriteFile(path+".sha256", []byte(gotSHA), 0o600); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("analysis: cache segment checksum %s: %w", ref.Key, err)
	}
	return s.appendCachedSegment(id, sess, ref, path, written)
}

func (s *WindowStore) appendCachedSegment(id string, sess *WindowSession, ref SegmentRef, path string, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.sessions[id]
	if !ok || cur != sess {
		return ErrRestoreNotFound
	}
	for _, existing := range cur.segments {
		if existing.Key == ref.Key {
			return nil
		}
	}
	ref.Path = path
	cur.segments = append(cur.segments, ref)
	cur.bytes += size
	s.cacheSize += size
	return nil
}

// Commit builds the scoped read-only snapshot for the session.
func (s *WindowStore) Commit(ctx context.Context, id string) (ScopedCoverage, error) {
	cov, _, err := s.CommitWithTimings(ctx, id)
	return cov, err
}

// CommitWithTimings also returns the attach/materialize/checkpoint phases.
func (s *WindowStore) CommitWithTimings(ctx context.Context, id string) (ScopedCoverage, ScopedTimings, error) {
	s.mu.Lock()
	sess, ok := s.sessions[id]
	s.mu.Unlock()
	if !ok {
		return ScopedCoverage{}, ScopedTimings{}, ErrRestoreNotFound
	}
	scoped, err := BuildScopedEngine(ctx, sess.Window, sess.segments, ScopedOptions{
		WorkDir:         sess.dir,
		BaselineHorizon: s.baseline,
		Open:            s.open,
	})
	if err != nil {
		return ScopedCoverage{}, ScopedTimings{}, err
	}
	provided := map[string]struct{}{}
	for _, ref := range sess.segments {
		provided[ref.Key] = struct{}{}
	}
	var missing []string
	for _, m := range sess.Selection.Missing {
		missing = append(missing, m)
	}
	for _, sel := range sess.Selection.Selected {
		if _, ok := provided[sel.Key]; !ok {
			missing = append(missing, sel.Key)
		}
	}
	scoped.Coverage.MissingSegments = missing
	scoped.Coverage.ExpectedSegments = sess.Selection.Expected
	scoped.Coverage.AvailableSegments = len(sess.segments)
	scoped.Coverage.Complete = len(missing) == 0
	scoped.Coverage.SelectionJustification = sess.Selection.Justification

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.sessions[id]
	if !ok || cur != sess {
		_ = scoped.Close()
		return ScopedCoverage{}, ScopedTimings{}, ErrRestoreNotFound
	}
	if cur.scoped != nil {
		_ = cur.scoped.Close()
	}
	cur.scoped = scoped
	cur.coverage = scoped.Coverage
	cur.ready = true
	return scoped.Coverage, scoped.Timings, nil
}

// Scoped returns the committed engine for a session.
func (s *WindowStore) Scoped(id string) (*ScopedEngine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, ErrRestoreNotFound
	}
	if sess.scoped == nil {
		return nil, policyErr("window_not_committed", "window session has not been committed")
	}
	return sess.scoped, nil
}

// Query runs a bounded, policy-validated query against the scoped snapshot.
func (s *WindowStore) Query(ctx context.Context, id string, req QueryRequest) (*QueryResult, error) {
	scoped, err := s.Scoped(id)
	if err != nil {
		return nil, err
	}
	return scoped.Engine.Query(ctx, ScopedPolicy(s.policy), req)
}

// Analyze runs the SQL-backed sections against the scoped snapshot.
func (s *WindowStore) Analyze(ctx context.Context, id, shotID string, req AnalyzeRequest) (*AnalyzeResult, error) {
	scoped, err := s.Scoped(id)
	if err != nil {
		return nil, err
	}
	res, err := scoped.Engine.Analyze(ctx, ScopedPolicy(s.policy), shotID, req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Compare compares two committed window sessions.
func (s *WindowStore) Compare(ctx context.Context, leftID, rightID string, topN int) (*CompareResult, error) {
	left, err := s.Scoped(leftID)
	if err != nil {
		return nil, err
	}
	right, err := s.Scoped(rightID)
	if err != nil {
		return nil, err
	}
	res, err := CompareEngines(ctx, ScopedPolicy(s.policy), left.Engine, right.Engine, leftID, rightID, topN)
	if err != nil {
		return nil, err
	}
	leftShot := Shot{Window: left.Window, Provenance: left.Window.Provenance, Instances: left.Window.Instances, Coverage: left.Coverage.CoverageSummary}
	rightShot := Shot{Window: right.Window, Provenance: right.Window.Provenance, Instances: right.Window.Instances, Coverage: right.Coverage.CoverageSummary}
	res.Disclosure = append(res.Disclosure, CompareDisclosure(leftShot, rightShot)...)
	if !left.Coverage.Complete || !right.Coverage.Complete {
		res.Disclosure = append(res.Disclosure, "Coverage diagnostics: left="+coverageLabel(left.Coverage)+", right="+coverageLabel(right.Coverage))
	}
	return res, nil
}

// Release tears down a window session.
func (s *WindowStore) Release(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[id]
	if !ok {
		return false
	}
	delete(s.sessions, id)
	_ = sess.scoped.Close()
	_ = os.RemoveAll(sess.dir)
	return true
}

// Close releases every session.
func (s *WindowStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for id, sess := range s.sessions {
		_ = sess.scoped.Close()
		_ = os.RemoveAll(sess.dir)
		delete(s.sessions, id)
	}
	return nil
}

func coverageLabel(c ScopedCoverage) string {
	return fmt.Sprintf("expected=%d available=%d missing=%d complete=%t",
		c.ExpectedSegments, c.AvailableSegments, len(c.MissingSegments), c.Complete)
}

func sanitizeSegmentKey(key string) string {
	// Keys are opaque but may contain '/', so hash to a stable, filesystem-safe
	// name.
	return hashName(key) + ".duckdb"
}

func hashName(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
