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
	policy    Policy
	bounds    WindowBounds
	ingestLag time.Duration
	baseline  time.Duration
	maxBytes  int64
	open      OpenOptions
	now       func() time.Time
	mu        sync.Mutex
	sessions  map[string]*WindowSession
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
	if !sessionIDPattern.MatchString(id) {
		return SegmentSelection{}, policyErr("invalid_session_id", "session id must match [A-Za-z0-9._-]{1,128}")
	}
	if err := window.Validate(s.now(), s.bounds); err != nil {
		return SegmentSelection{}, err
	}
	selection := SelectSegments(window, catalog, SelectionOptions{IngestLag: s.ingestLag})
	dir := filepath.Join(s.dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return SegmentSelection{}, fmt.Errorf("analysis: window session dir: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return SegmentSelection{}, errors.New("analysis: window store is closed")
	}
	if prev, ok := s.sessions[id]; ok {
		_ = prev.scoped.Close()
		_ = os.RemoveAll(prev.dir)
	}
	s.sessions[id] = &WindowSession{ID: id, Window: window, Catalog: catalog, Selection: selection, dir: dir}
	return selection, nil
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
	segDir := filepath.Join(sess.dir, "segments")
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(segDir, sanitizeSegmentKey(ref.Key))
	written, gotSHA, err := writeVerified(path, r, s.maxBytes)
	if err != nil {
		return err
	}
	if size > 0 && written != size {
		_ = os.Remove(path)
		return policyErr("size_mismatch", fmt.Sprintf("segment size %d does not match declared %d", written, size))
	}
	normSHA := normalizeSHA(sha)
	if normSHA != "" && gotSHA != normSHA {
		_ = os.Remove(path)
		return policyErr("sha256_mismatch", "segment sha256 does not match the declared checksum")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.sessions[id]
	if !ok || cur != sess {
		_ = os.Remove(path)
		return ErrRestoreNotFound
	}
	ref.Path = path
	cur.segments = append(cur.segments, ref)
	cur.bytes += written
	return nil
}

// Commit builds the scoped read-only snapshot for the session.
func (s *WindowStore) Commit(ctx context.Context, id string) (ScopedCoverage, error) {
	s.mu.Lock()
	sess, ok := s.sessions[id]
	s.mu.Unlock()
	if !ok {
		return ScopedCoverage{}, ErrRestoreNotFound
	}
	scoped, err := BuildScopedEngine(ctx, sess.Window, sess.segments, ScopedOptions{
		WorkDir:         sess.dir,
		BaselineHorizon: s.baseline,
		Open:            s.open,
	})
	if err != nil {
		return ScopedCoverage{}, err
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
		return ScopedCoverage{}, ErrRestoreNotFound
	}
	if cur.scoped != nil {
		_ = cur.scoped.Close()
	}
	cur.scoped = scoped
	cur.coverage = scoped.Coverage
	cur.ready = true
	return scoped.Coverage, nil
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
