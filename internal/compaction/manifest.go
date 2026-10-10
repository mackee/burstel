package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

// SegmentStatus values recorded in the manifest.
const (
	StatusVerified   = "verified"
	StatusMissing    = "missing"
	StatusIncomplete = "incomplete"
)

// SegmentManifest is one advertised immutable segment. A row is only ever
// inserted after the R2 object has been verified.
type SegmentManifest struct {
	Key             string
	InstanceID      string
	Size            int64
	SHA256          string
	FirstReceivedAt int64 // epoch ms, inclusive
	LastReceivedAt  int64 // epoch ms, inclusive
	RequestCount    int64
	RecordCount     int64
	SpanCount       int64
	LogCount        int64
	MetricCount     int64
	EventMinNS      *int64
	EventMaxNS      *int64
	Status          string
	Late            bool
	CreatedAt       int64
	VerifiedAt      *int64
}

// CoverageRow attributes one raw journal object to exactly one verified
// segment. The primary key on JournalKey makes duplicate coverage impossible.
type CoverageRow struct {
	JournalKey string
	SegmentKey string
	InstanceID string
	Signal     string
	ReceivedAt int64
	Size       int64
}

// CoverageDetail is a coverage row joined with its segment's status and range.
type CoverageDetail struct {
	JournalKey             string
	SegmentKey             string
	InstanceID             string
	Signal                 string
	ReceivedAt             int64
	Size                   int64
	SegmentStatus          string
	SegmentFirstReceivedAt int64
	SegmentLastReceivedAt  int64
}

// Gap is an explicit coverage hole: an advertised segment missing from R2, or
// a finalized window that a late event has invalidated.
type Gap struct {
	ID         string
	InstanceID string
	SegmentKey string
	FromMS     int64
	ToMS       int64
	Reason     string
	DetectedAt int64
	Resolved   bool
}

// CommitResult reports the outcome of a manifest commit.
type CommitResult struct {
	Committed      bool
	Duplicate      bool
	CoveredNew     int
	CoveredSkipped int
}

// Manifest is a durable, single-writer catalog of verified segments and the raw
// objects they cover.
type Manifest struct {
	db   *sql.DB
	path string
}

const manifestSchema = `
CREATE TABLE IF NOT EXISTS segments (
  segment_key       VARCHAR PRIMARY KEY,
  instance_id       VARCHAR NOT NULL,
  size_bytes        BIGINT  NOT NULL,
  sha256            VARCHAR NOT NULL,
  first_received_at BIGINT  NOT NULL,
  last_received_at  BIGINT  NOT NULL,
  request_count     BIGINT  NOT NULL,
  record_count      BIGINT  NOT NULL,
  span_count        BIGINT  NOT NULL,
  log_count         BIGINT  NOT NULL,
  metric_count      BIGINT  NOT NULL,
  event_min_ns      BIGINT,
  event_max_ns      BIGINT,
  status            VARCHAR NOT NULL,
  late              BOOLEAN NOT NULL,
  created_at        BIGINT  NOT NULL,
  verified_at       BIGINT
);
CREATE INDEX IF NOT EXISTS segments_instance ON segments (instance_id, first_received_at);
CREATE TABLE IF NOT EXISTS coverage (
  journal_key VARCHAR PRIMARY KEY,
  segment_key VARCHAR NOT NULL,
  instance_id VARCHAR NOT NULL,
  signal      VARCHAR NOT NULL,
  received_at BIGINT  NOT NULL,
  size_bytes  BIGINT  NOT NULL,
  covered_at  BIGINT  NOT NULL
);
CREATE INDEX IF NOT EXISTS coverage_segment ON coverage (segment_key);
CREATE TABLE IF NOT EXISTS finalized (
  instance_id VARCHAR PRIMARY KEY,
  through_ms  BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS segment_pins (
  shot_id     VARCHAR NOT NULL,
  instance_id VARCHAR,
  from_ms     BIGINT  NOT NULL,
  to_ms       BIGINT  NOT NULL,
  pinned_at   BIGINT  NOT NULL
);
CREATE TABLE IF NOT EXISTS segment_gaps (
  id          VARCHAR PRIMARY KEY,
  instance_id VARCHAR,
  segment_key VARCHAR,
  from_ms     BIGINT,
  to_ms       BIGINT,
  reason      VARCHAR NOT NULL,
  detected_at BIGINT  NOT NULL,
  resolved    BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS compile_counters (
  name  VARCHAR PRIMARY KEY,
  value BIGINT  NOT NULL
)
`

// OpenManifest opens (or creates) the manifest database at path.
func OpenManifest(ctx context.Context, path string) (*Manifest, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("compaction: open manifest %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("compaction: ping manifest %s: %w", path, err)
	}
	if _, err := db.ExecContext(ctx, manifestSchema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("compaction: manifest schema: %w", err)
	}
	return &Manifest{db: db, path: path}, nil
}

// Path returns the manifest file path.
func (m *Manifest) Path() string { return m.path }

// Close releases the manifest database.
func (m *Manifest) Close() error {
	if m.db == nil {
		return nil
	}
	return m.db.Close()
}

// GetSegment returns one segment, if present.
func (m *Manifest) GetSegment(ctx context.Context, key string) (SegmentManifest, bool, error) {
	rows, err := m.db.QueryContext(ctx, segmentSelect+` WHERE segment_key = ?`, key)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	defer rows.Close()
	list, err := scanSegments(rows)
	if err != nil {
		return SegmentManifest{}, false, err
	}
	if len(list) == 0 {
		return SegmentManifest{}, false, nil
	}
	return list[0], true, nil
}

// ListSegments returns segments, newest range first, optionally filtered by
// instance.
func (m *Manifest) ListSegments(ctx context.Context, instanceID string, limit, offset int) ([]SegmentManifest, error) {
	if limit <= 0 {
		limit = 100
	}
	q := segmentSelect
	args := []any{}
	if instanceID != "" {
		q += ` WHERE instance_id = ?`
		args = append(args, instanceID)
	}
	q += ` ORDER BY first_received_at ASC, segment_key ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := m.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSegments(rows)
}

const segmentSelect = `SELECT segment_key, instance_id, size_bytes, sha256, first_received_at,
       last_received_at, request_count, record_count, span_count, log_count, metric_count,
       event_min_ns, event_max_ns, status, late, created_at, verified_at FROM segments`

func scanSegments(rows *sql.Rows) ([]SegmentManifest, error) {
	var out []SegmentManifest
	for rows.Next() {
		var s SegmentManifest
		var minNS, maxNS, verified sql.NullInt64
		if err := rows.Scan(
			&s.Key, &s.InstanceID, &s.Size, &s.SHA256, &s.FirstReceivedAt, &s.LastReceivedAt,
			&s.RequestCount, &s.RecordCount, &s.SpanCount, &s.LogCount, &s.MetricCount,
			&minNS, &maxNS, &s.Status, &s.Late, &s.CreatedAt, &verified,
		); err != nil {
			return nil, err
		}
		s.EventMinNS = nullInt64Ptr(minNS)
		s.EventMaxNS = nullInt64Ptr(maxNS)
		s.VerifiedAt = nullInt64Ptr(verified)
		out = append(out, s)
	}
	return out, rows.Err()
}

// CommitSegment atomically inserts a segment row and its coverage rows. It is
// idempotent: a segment key that already exists with the same checksum is a
// no-op duplicate, and each journal key is covered at most once.
func (m *Manifest) CommitSegment(ctx context.Context, s SegmentManifest, coverage []CoverageRow, now int64) (CommitResult, error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return CommitResult{}, fmt.Errorf("compaction: begin manifest tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var existingSHA string
	err = tx.QueryRowContext(ctx, `SELECT sha256 FROM segments WHERE segment_key = ?`, s.Key).Scan(&existingSHA)
	switch {
	case err == nil:
		if existingSHA != s.SHA256 {
			return CommitResult{}, fmt.Errorf("compaction: segment %s exists with a different checksum", s.Key)
		}
		return CommitResult{Duplicate: true}, nil
	case err != sql.ErrNoRows:
		return CommitResult{}, err
	}

	s.Status = StatusVerified
	s.VerifiedAt = &now
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO segments (segment_key, instance_id, size_bytes, sha256, first_received_at,
		   last_received_at, request_count, record_count, span_count, log_count, metric_count,
		   event_min_ns, event_max_ns, status, late, created_at, verified_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		s.Key, s.InstanceID, s.Size, s.SHA256, s.FirstReceivedAt, s.LastReceivedAt,
		s.RequestCount, s.RecordCount, s.SpanCount, s.LogCount, s.MetricCount,
		ptrAny(s.EventMinNS), ptrAny(s.EventMaxNS), s.Status, s.Late, s.CreatedAt, now,
	); err != nil {
		return CommitResult{}, fmt.Errorf("compaction: insert segment: %w", err)
	}

	res := CommitResult{Committed: true}
	for _, c := range coverage {
		var one int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM coverage WHERE journal_key = ?`, c.JournalKey).Scan(&one)
		if err == nil {
			res.CoveredSkipped++
			continue
		}
		if err != sql.ErrNoRows {
			return CommitResult{}, err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO coverage (journal_key, segment_key, instance_id, signal, received_at, size_bytes, covered_at)
			 VALUES (?,?,?,?,?,?,?)`,
			c.JournalKey, s.Key, c.InstanceID, c.Signal, c.ReceivedAt, c.Size, now,
		); err != nil {
			return CommitResult{}, fmt.Errorf("compaction: insert coverage: %w", err)
		}
		res.CoveredNew++
	}
	if err := tx.Commit(); err != nil {
		return CommitResult{}, fmt.Errorf("compaction: commit manifest: %w", err)
	}
	return res, nil
}

// IsCovered reports, for each key, whether it is attributed to a segment.
func (m *Manifest) IsCovered(ctx context.Context, keys []string) (map[string]bool, error) {
	out := make(map[string]bool, len(keys))
	for _, k := range keys {
		out[k] = false
	}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := m.db.QueryContext(ctx, `SELECT journal_key FROM coverage`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		if _, ok := out[k]; ok {
			out[k] = true
		}
	}
	return out, rows.Err()
}

// CoverageSegments returns the set of segment keys referenced by coverage rows.
func (m *Manifest) CoverageSegments(ctx context.Context) (map[string]int64, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT segment_key, COUNT(*) FROM coverage GROUP BY segment_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

// ListCoverage returns every coverage row joined with its segment's status and
// receive-time range.
func (m *Manifest) ListCoverage(ctx context.Context) ([]CoverageDetail, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT c.journal_key, c.segment_key, c.instance_id, c.signal,
		c.received_at, c.size_bytes, s.status, s.first_received_at, s.last_received_at
		FROM coverage c JOIN segments s ON s.segment_key = c.segment_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CoverageDetail
	for rows.Next() {
		var d CoverageDetail
		if err := rows.Scan(&d.JournalKey, &d.SegmentKey, &d.InstanceID, &d.Signal, &d.ReceivedAt,
			&d.Size, &d.SegmentStatus, &d.SegmentFirstReceivedAt, &d.SegmentLastReceivedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// SetSegmentStatus updates a segment's status (verified|missing|incomplete).
func (m *Manifest) SetSegmentStatus(ctx context.Context, key, status string) error {
	_, err := m.db.ExecContext(ctx, `UPDATE segments SET status = ? WHERE segment_key = ?`, status, key)
	return err
}

// ListCoverageForSegment returns the coverage rows of one segment.
func (m *Manifest) ListCoverageForSegment(ctx context.Context, segmentKey string) ([]CoverageRow, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT journal_key, segment_key, instance_id, signal, received_at, size_bytes
		FROM coverage WHERE segment_key = ? ORDER BY journal_key ASC`, segmentKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CoverageRow
	for rows.Next() {
		var c CoverageRow
		if err := rows.Scan(&c.JournalKey, &c.SegmentKey, &c.InstanceID, &c.Signal, &c.ReceivedAt, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ReplaceSegment rewrites a segment's stored object metadata after a repair.
func (m *Manifest) ReplaceSegment(ctx context.Context, s SegmentManifest, now int64) error {
	_, err := m.db.ExecContext(ctx, `UPDATE segments SET
		size_bytes = ?, sha256 = ?, request_count = ?, record_count = ?, span_count = ?,
		log_count = ?, metric_count = ?, event_min_ns = ?, event_max_ns = ?, status = ?,
		verified_at = ? WHERE segment_key = ?`,
		s.Size, s.SHA256, s.RequestCount, s.RecordCount, s.SpanCount, s.LogCount, s.MetricCount,
		ptrAny(s.EventMinNS), ptrAny(s.EventMaxNS), StatusVerified, now, s.Key)
	return err
}

// FinalizedThrough returns the finalized receive-time watermark for an instance
// (0 when never finalized).
func (m *Manifest) FinalizedThrough(ctx context.Context, instanceID string) (int64, error) {
	var v int64
	err := m.db.QueryRowContext(ctx, `SELECT through_ms FROM finalized WHERE instance_id = ?`, instanceID).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return v, err
}

// SetFinalizedThrough advances the finalized watermark monotonically.
func (m *Manifest) SetFinalizedThrough(ctx context.Context, instanceID string, through int64) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO finalized (instance_id, through_ms) VALUES (?, ?)
		 ON CONFLICT (instance_id) DO UPDATE SET through_ms = GREATEST(finalized.through_ms, excluded.through_ms)`,
		instanceID, through)
	return err
}

// AddPin protects a receive-time range for a shot until it is explicitly
// released. GC never deletes journal objects whose segment range overlaps a pin.
func (m *Manifest) AddPin(ctx context.Context, shotID, instanceID string, fromMS, toMS, now int64) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO segment_pins (shot_id, instance_id, from_ms, to_ms, pinned_at) VALUES (?,?,?,?,?)`,
		shotID, instanceID, fromMS, toMS, now)
	return err
}

// RemovePins releases every pin held by a shot.
func (m *Manifest) RemovePins(ctx context.Context, shotID string) (int64, error) {
	res, err := m.db.ExecContext(ctx, `DELETE FROM segment_pins WHERE shot_id = ?`, shotID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// Pins returns every active pin.
func (m *Manifest) Pins(ctx context.Context) ([]Pin, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT shot_id, instance_id, from_ms, to_ms, pinned_at FROM segment_pins`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Pin
	for rows.Next() {
		var p Pin
		var inst sql.NullString
		if err := rows.Scan(&p.ShotID, &inst, &p.FromMS, &p.ToMS, &p.PinnedAt); err != nil {
			return nil, err
		}
		if inst.Valid {
			p.InstanceID = inst.String
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// AddGap records an explicit coverage gap (idempotent on id).
func (m *Manifest) AddGap(ctx context.Context, g Gap) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO segment_gaps (id, instance_id, segment_key, from_ms, to_ms, reason, detected_at, resolved)
		 VALUES (?,?,?,?,?,?,?,?)
		 ON CONFLICT (id) DO NOTHING`,
		g.ID, g.InstanceID, g.SegmentKey, g.FromMS, g.ToMS, g.Reason, g.DetectedAt, g.Resolved)
	return err
}

// ResolveGapsForSegment clears gaps when a segment is verified again.
func (m *Manifest) ResolveGapsForSegment(ctx context.Context, segmentKey string) error {
	_, err := m.db.ExecContext(ctx, `UPDATE segment_gaps SET resolved = TRUE WHERE segment_key = ?`, segmentKey)
	return err
}

// Gaps returns unresolved gaps.
func (m *Manifest) Gaps(ctx context.Context, includeResolved bool) ([]Gap, error) {
	q := `SELECT id, instance_id, segment_key, from_ms, to_ms, reason, detected_at, resolved FROM segment_gaps`
	if !includeResolved {
		q += ` WHERE resolved = FALSE`
	}
	q += ` ORDER BY detected_at ASC`
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Gap
	for rows.Next() {
		var g Gap
		var inst, seg sql.NullString
		var from, to sql.NullInt64
		if err := rows.Scan(&g.ID, &inst, &seg, &from, &to, &g.Reason, &g.DetectedAt, &g.Resolved); err != nil {
			return nil, err
		}
		if inst.Valid {
			g.InstanceID = inst.String
		}
		if seg.Valid {
			g.SegmentKey = seg.String
		}
		if from.Valid {
			g.FromMS = from.Int64
		}
		if to.Valid {
			g.ToMS = to.Int64
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Bump increments a named counter.
func (m *Manifest) Bump(ctx context.Context, name string, delta int64) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO compile_counters (name, value) VALUES (?, ?)
		 ON CONFLICT (name) DO UPDATE SET value = compile_counters.value + excluded.value`,
		name, delta)
	return err
}

// Counters returns all counters.
func (m *Manifest) Counters(ctx context.Context) (map[string]int64, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT name, value FROM compile_counters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, rows.Err()
}

// Totals returns aggregate counts used by health/status surfaces.
type Totals struct {
	Segments       int64
	Verified       int64
	Missing        int64
	CoveredObjects int64
	CoveredRecords int64
	CoveredBytes   int64
	LateSegments   int64
	GapsOpen       int64
}

// Totals computes the health summary.
func (m *Manifest) Totals(ctx context.Context) (Totals, error) {
	var t Totals
	if err := m.db.QueryRowContext(ctx, `SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN status = 'verified' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN status != 'verified' THEN 1 ELSE 0 END), 0),
		COALESCE(SUM(CASE WHEN late THEN 1 ELSE 0 END), 0)
		FROM segments`).Scan(&t.Segments, &t.Verified, &t.Missing, &t.LateSegments); err != nil {
		return t, err
	}
	if err := m.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM coverage`).Scan(&t.CoveredObjects); err != nil {
		return t, err
	}
	if err := m.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(record_count),0), COALESCE(SUM(size_bytes),0) FROM segments`).
		Scan(&t.CoveredRecords, &t.CoveredBytes); err != nil {
		return t, err
	}
	if err := m.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM segment_gaps WHERE resolved = FALSE`).Scan(&t.GapsOpen); err != nil {
		return t, err
	}
	return t, nil
}

// Pin is one shot protection range.
type Pin struct {
	ShotID     string
	InstanceID string
	FromMS     int64
	ToMS       int64
	PinnedAt   int64
}

func ptrAny(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullInt64Ptr(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	x := v.Int64
	return &x
}

// nowMS is a small helper for deterministic clocks.
func nowMS(t time.Time) int64 { return t.UTC().UnixMilli() }
