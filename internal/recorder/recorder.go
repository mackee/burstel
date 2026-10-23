// Package recorder implements the per-shot lifecycle state machine:
// created -> recording -> stopping -> frozen (or failed). It owns the DuckDB
// storage handle for the active shot and is the only place that mutates shot
// state, which keeps the start/ingest/stop/idempotency rules in one place.
package recorder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/storage"
)

// State is the lifecycle state of a shot.
type State string

const (
	StateCreated   State = "created"
	StateRecording State = "recording"
	StateStopping  State = "stopping"
	StateFrozen    State = "frozen"
	StateFailed    State = "failed"
)

// ArtifactName is the stable filename of a frozen shot database.
const ArtifactName = "telemetry.duckdb"

// Lifecycle errors. Handlers map these to HTTP status codes.
var (
	ErrInvalidShotID      = errors.New("recorder: shot_id is required")
	ErrAlreadyRecording   = errors.New("recorder: a different shot is already recording")
	ErrWrongShot          = errors.New("recorder: batch does not belong to the active shot")
	ErrNotRecording       = errors.New("recorder: no shot is recording")
	ErrStopping           = errors.New("recorder: shot is stopping")
	ErrNotStarted         = errors.New("recorder: shot has not started")
	ErrNotFrozen          = errors.New("recorder: shot is not frozen")
	ErrStorageUnavailable = errors.New("recorder: storage is unavailable")
)

// StartRequest is the body of POST /internal/start.
type StartRequest struct {
	ShotID   string         `json:"shot_id"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Info is returned by Start.
type Info struct {
	ShotID    string    `json:"shot_id"`
	State     State     `json:"state"`
	StartedAt time.Time `json:"started_at,omitempty"`
	StoppedAt time.Time `json:"stopped_at,omitempty"`
	SizeBytes int64     `json:"size_bytes,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	Artifact  string    `json:"artifact,omitempty"`
}

// Result is returned by Stop.
type Result struct {
	ShotID    string    `json:"shot_id"`
	State     State     `json:"state"`
	StartedAt time.Time `json:"started_at,omitempty"`
	StoppedAt time.Time `json:"stopped_at,omitempty"`
	SizeBytes int64     `json:"size_bytes,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	Artifact  string    `json:"artifact,omitempty"`
}

// ArtifactInfo describes the frozen database file.
type ArtifactInfo struct {
	Path   string
	Size   int64
	SHA256 string
	Name   string
}

// Counters tracks how much has been accepted for the active shot.
type Counters struct {
	Batches      map[string]int64 `json:"batches"`
	Records      map[string]int64 `json:"records"`
	Spans        int64            `json:"spans"`
	LogRecords   int64            `json:"log_records"`
	MetricPoints int64            `json:"metric_points"`
}

func newCounters() Counters {
	return Counters{
		Batches: map[string]int64{"traces": 0, "metrics": 0, "logs": 0},
		Records: map[string]int64{"traces": 0, "metrics": 0, "logs": 0},
	}
}

// Metadata is a point-in-time snapshot returned by /internal/metadata.
type Metadata struct {
	ShotID        string         `json:"shot_id"`
	State         State          `json:"state"`
	SchemaVersion string         `json:"schema_version"`
	StartedAt     time.Time      `json:"started_at,omitempty"`
	StoppedAt     time.Time      `json:"stopped_at,omitempty"`
	Artifact      string         `json:"artifact,omitempty"`
	SizeBytes     int64          `json:"size_bytes"`
	SHA256        string         `json:"sha256,omitempty"`
	Counters      Counters       `json:"counters"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	LastError     string         `json:"last_error,omitempty"`
	// DroppedOutsideShot counts records that reached the standalone OTLP
	// endpoint with no active shot. They were acknowledged (200 + OTLP
	// partial_success) and discarded instead of provoking an exporter retry
	// storm (spec 022, Part C).
	DroppedOutsideShot int64 `json:"dropped_outside_shot"`
}

// Recorder owns the active shot.
type Recorder struct {
	cfg config.Config

	// mu is held in read mode for the duration of every durable write and in
	// write mode while the lifecycle transitions, so Stop can prove that no
	// writer is in flight before it checkpoints the file.
	mu sync.RWMutex

	// stopMu serialises concurrent Stop calls.
	stopMu sync.Mutex

	// counterMu protects counters, which are updated by concurrent ingests.
	counterMu sync.Mutex

	state     State
	shotID    string
	metaJSON  string
	meta      map[string]any
	startedAt time.Time
	stoppedAt time.Time
	store     *storage.Storage
	dbPath    string
	dir       string
	size      int64
	sha256hex string
	counters  Counters
	lastErr   string

	// droppedOutsideShot counts records discarded because no shot was active.
	droppedOutsideShot int64
}

// New creates a Recorder that has not started a shot yet.
func New(cfg config.Config) *Recorder {
	return &Recorder{cfg: cfg, state: StateCreated, counters: newCounters()}
}

var safeID = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Start opens a new shot database. It is idempotent for the same shot_id and
// metadata while recording, and for an already-frozen identical shot. A
// different shot while recording conflicts.
func (r *Recorder) Start(ctx context.Context, req StartRequest) (Info, error) {
	if strings.TrimSpace(req.ShotID) == "" {
		return Info{}, ErrInvalidShotID
	}
	metaJSON := canonicalJSON(req.Metadata)

	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case StateRecording:
		if r.shotID == req.ShotID && r.metaJSON == metaJSON {
			return r.infoLocked(), nil
		}
		return Info{}, ErrAlreadyRecording
	case StateStopping:
		return Info{}, ErrStopping
	case StateFrozen:
		if r.shotID == req.ShotID && r.metaJSON == metaJSON {
			return r.infoLocked(), nil
		}
		return Info{}, ErrAlreadyRecording
	}

	dir := filepath.Join(r.cfg.DataDir, dirName(req.ShotID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.failLocked(fmt.Errorf("create data dir: %w", err))
		return Info{}, err
	}
	dbPath := filepath.Join(dir, ArtifactName)
	store, err := storage.Open(ctx, dbPath)
	if err != nil {
		r.failLocked(err)
		return Info{}, err
	}

	r.store = store
	r.shotID = req.ShotID
	r.metaJSON = metaJSON
	r.meta = req.Metadata
	r.startedAt = time.Now().UTC()
	r.stoppedAt = time.Time{}
	r.size = 0
	r.sha256hex = ""
	r.dbPath = dbPath
	r.dir = dir
	r.lastErr = ""
	r.counters = newCounters()
	r.state = StateRecording
	return r.infoLocked(), nil
}

// Ingest durably writes one batch for the active shot.
func (r *Recorder) Ingest(ctx context.Context, b *storage.Batch) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state != StateRecording {
		return ErrNotRecording
	}
	if b.ShotID != r.shotID {
		return ErrWrongShot
	}
	if r.store == nil {
		return ErrStorageUnavailable
	}
	if err := r.store.InsertBatch(ctx, b); err != nil {
		return err
	}
	r.addCounters(b)
	return nil
}

// Stop blocks new ingests, waits for in-flight writes, checkpoints and closes
// the database, then records size + SHA-256. It is idempotent once frozen.
// It never marks the shot frozen after an I/O error and never deletes the file.
func (r *Recorder) Stop(ctx context.Context, shotID string) (Result, error) {
	r.stopMu.Lock()
	defer r.stopMu.Unlock()

	r.mu.Lock()
	switch r.state {
	case StateFrozen:
		if shotID != "" && shotID != r.shotID {
			r.mu.Unlock()
			return Result{}, ErrAlreadyRecording
		}
		res := r.resultLocked()
		r.mu.Unlock()
		return res, nil
	case StateRecording:
		// fall through
	default:
		r.mu.Unlock()
		return Result{}, ErrNotStarted
	}

	// Acquiring the write lock waits for every in-flight Ingest (which holds
	// the read lock) to finish. Setting StateStopping under the lock prevents
	// any new ingest from entering.
	r.state = StateStopping
	store := r.store
	dbPath := r.dbPath
	r.mu.Unlock()

	if err := store.Checkpoint(ctx); err != nil {
		r.fail(fmt.Errorf("checkpoint: %w", err))
		return Result{}, err
	}
	if err := store.Close(); err != nil {
		r.fail(fmt.Errorf("close: %w", err))
		return Result{}, err
	}

	size, sum, err := fileSizeAndSHA256(dbPath)
	if err != nil {
		r.fail(err)
		return Result{}, err
	}
	// Best effort: mark the file read-only to reflect its immutable state.
	_ = os.Chmod(dbPath, 0o444)

	r.mu.Lock()
	r.size = size
	r.sha256hex = sum
	r.stoppedAt = time.Now().UTC()
	r.state = StateFrozen
	res := r.resultLocked()
	r.mu.Unlock()
	return res, nil
}

// Artifact returns metadata for the frozen file. It is disabled before Stop.
func (r *Recorder) Artifact() (ArtifactInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state != StateFrozen {
		return ArtifactInfo{}, ErrNotFrozen
	}
	return ArtifactInfo{Path: r.dbPath, Size: r.size, SHA256: r.sha256hex, Name: ArtifactName}, nil
}

// Metadata returns a point-in-time snapshot of the shot.
func (r *Recorder) Metadata() Metadata {
	r.mu.RLock()
	defer r.mu.RUnlock()

	counters := r.snapshotCountersLocked()
	return Metadata{
		ShotID:             r.shotID,
		State:              r.state,
		SchemaVersion:      storage.SchemaVersion,
		StartedAt:          r.startedAt,
		StoppedAt:          r.stoppedAt,
		Artifact:           artifactNameIfFrozen(r.state),
		SizeBytes:          r.size,
		SHA256:             r.sha256hex,
		Counters:           counters,
		Metadata:           r.meta,
		LastError:          r.lastErr,
		DroppedOutsideShot: r.snapshotDroppedOutsideShotLocked(),
	}
}

// Ready reports whether the process can accept traffic.
func (r *Recorder) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state != StateFailed
}

// ShotID returns the active shot id while recording.
func (r *Recorder) ShotID() (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state != StateRecording {
		return "", false
	}
	return r.shotID, true
}

// State returns the current lifecycle state.
func (r *Recorder) State() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// RecordDroppedOutsideShot counts records that arrived while no shot was
// active. Standalone ingest acknowledges and discards them (spec 022, Part C)
// so an exporter never enters a retry/backoff storm outside a shot window.
func (r *Recorder) RecordDroppedOutsideShot(records int64) {
	if records <= 0 {
		return
	}
	r.counterMu.Lock()
	defer r.counterMu.Unlock()
	r.droppedOutsideShot += records
}

func (r *Recorder) snapshotDroppedOutsideShotLocked() int64 {
	r.counterMu.Lock()
	defer r.counterMu.Unlock()
	return r.droppedOutsideShot
}

func (r *Recorder) infoLocked() Info {
	return Info{
		ShotID:    r.shotID,
		State:     r.state,
		StartedAt: r.startedAt,
		StoppedAt: r.stoppedAt,
		SizeBytes: r.size,
		SHA256:    r.sha256hex,
		Artifact:  artifactNameIfFrozen(r.state),
	}
}

func (r *Recorder) resultLocked() Result {
	return Result{
		ShotID:    r.shotID,
		State:     r.state,
		StartedAt: r.startedAt,
		StoppedAt: r.stoppedAt,
		SizeBytes: r.size,
		SHA256:    r.sha256hex,
		Artifact:  artifactNameIfFrozen(r.state),
	}
}

func (r *Recorder) addCounters(b *storage.Batch) {
	r.counterMu.Lock()
	defer r.counterMu.Unlock()
	sig := string(b.Signal)
	r.counters.Batches[sig]++
	r.counters.Records[sig] += b.RecordCount()
	switch b.Signal {
	case otlp.Traces:
		r.counters.Spans += int64(len(b.Spans))
	case otlp.Logs:
		r.counters.LogRecords += int64(len(b.LogRecords))
	case otlp.Metrics:
		r.counters.MetricPoints += int64(len(b.MetricPoints))
	}
}

func (r *Recorder) snapshotCountersLocked() Counters {
	r.counterMu.Lock()
	defer r.counterMu.Unlock()
	out := Counters{
		Batches:      map[string]int64{},
		Records:      map[string]int64{},
		Spans:        r.counters.Spans,
		LogRecords:   r.counters.LogRecords,
		MetricPoints: r.counters.MetricPoints,
	}
	for k, v := range r.counters.Batches {
		out.Batches[k] = v
	}
	for k, v := range r.counters.Records {
		out.Records[k] = v
	}
	return out
}

func (r *Recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failLocked(err)
}

func (r *Recorder) failLocked(err error) {
	r.state = StateFailed
	r.lastErr = err.Error()
}

func artifactNameIfFrozen(s State) string {
	if s == StateFrozen {
		return ArtifactName
	}
	return ""
}

func canonicalJSON(v map[string]any) string {
	if len(v) == 0 {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func dirName(shotID string) string {
	if safeID.MatchString(shotID) {
		return shotID
	}
	sum := sha256.Sum256([]byte(shotID))
	return "shot-" + hex.EncodeToString(sum[:8])
}

func fileSizeAndSHA256(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", fmt.Errorf("open frozen file: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return 0, "", fmt.Errorf("hash frozen file: %w", err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}
