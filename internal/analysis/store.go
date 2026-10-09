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
)

// ErrRestoreNotFound is returned when a restore id has not been restored.
var ErrRestoreNotFound = errors.New("analysis: restore not found")

// DefaultMaxArtifactBytes bounds a single restored artifact (8 GiB).
const DefaultMaxArtifactBytes = int64(8) << 30

var restoreIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// Store owns the set of restored frozen shots available to the analysis
// server. Restored engines live in an ephemeral directory; R2 is always the
// authoritative copy, so the container may be destroyed at any time.
type Store struct {
	dir      string
	policy   Policy
	maxBytes int64
	openOpts OpenOptions
	mu       sync.Mutex
	engines  map[string]*restoredEngine
	closed   bool
}

type restoredEngine struct {
	info   RestoreInfo
	engine *Engine
	path   string
}

// StoreOptions configures the restore store.
type StoreOptions struct {
	Policy          Policy
	MaxArtifactSize int64
	Open            OpenOptions
}

// NewStore creates a restore store rooted at dir.
func NewStore(dir string, opts StoreOptions) *Store {
	policy := opts.Policy
	if policy.MaxRows == 0 {
		policy = DefaultPolicy()
	}
	maxBytes := opts.MaxArtifactSize
	if maxBytes <= 0 {
		maxBytes = DefaultMaxArtifactBytes
	}
	return &Store{
		dir:      dir,
		policy:   policy,
		maxBytes: maxBytes,
		openOpts: opts.Open,
		engines:  map[string]*restoredEngine{},
	}
}

// Policy returns the query policy enforced by this store.
func (s *Store) Policy() Policy { return s.policy }

// EnsureDir creates the restore directory.
func (s *Store) EnsureDir() error {
	if s.dir == "" {
		return errors.New("analysis: restore directory is empty")
	}
	return os.MkdirAll(s.dir, 0o755)
}

// Restore streams a frozen artifact into a verified read-only engine.
// It is idempotent for an identical (id, sha256).
func (s *Store) Restore(ctx context.Context, id string, r io.Reader, size int64, sha string) (*RestoreInfo, error) {
	if !restoreIDPattern.MatchString(id) {
		return nil, policyErr("invalid_restore_id", "restore id must match [A-Za-z0-9._-]{1,128}")
	}
	sha = normalizeSHA(sha)
	if sha != "" && !isHexSHA256(sha) {
		return nil, policyErr("invalid_sha256", "sha256 must be 64 hex characters")
	}
	if size < 0 || size > s.maxBytes {
		return nil, policyErr("artifact_too_large", fmt.Sprintf("artifact size %d exceeds the %d byte limit", size, s.maxBytes))
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("analysis: store is closed")
	}
	if cur, ok := s.engines[id]; ok && sha != "" && cur.info.SHA256 == sha {
		info := cur.info
		info.Reused = true
		s.mu.Unlock()
		return &info, nil
	}
	s.mu.Unlock()

	if err := s.EnsureDir(); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(s.dir, "restore-")
	if err != nil {
		return nil, fmt.Errorf("analysis: temp dir: %w", err)
	}
	final := filepath.Join(dir, id+".duckdb")

	written, gotSHA, err := writeVerified(final, r, s.maxBytes)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if size > 0 && written != size {
		_ = os.RemoveAll(dir)
		return nil, policyErr("size_mismatch", fmt.Sprintf("artifact size %d does not match declared %d", written, size))
	}
	if sha != "" && gotSHA != sha {
		_ = os.RemoveAll(dir)
		return nil, policyErr("sha256_mismatch", "artifact sha256 does not match the declared checksum")
	}

	engine, err := OpenReadOnly(ctx, final, s.openOpts)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}

	info := RestoreInfo{
		RestoreID:     id,
		SizeBytes:     written,
		SHA256:        gotSHA,
		SchemaVersion: engine.SchemaVersion(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		_ = engine.Close()
		_ = os.RemoveAll(dir)
		return nil, errors.New("analysis: store is closed")
	}
	// Replace any previous engine for this id (different content).
	if prev, ok := s.engines[id]; ok {
		_ = prev.engine.Close()
		_ = os.RemoveAll(filepath.Dir(prev.path))
	}
	s.engines[id] = &restoredEngine{info: info, engine: engine, path: final}
	return &info, nil
}

// Engine returns the restored engine for id.
func (s *Store) Engine(id string) (*Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.engines[id]
	if !ok {
		return nil, ErrRestoreNotFound
	}
	return cur.engine, nil
}

// Release closes and deletes a restored engine.
func (s *Store) Release(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.engines[id]
	if !ok {
		return false
	}
	delete(s.engines, id)
	_ = cur.engine.Close()
	_ = os.RemoveAll(filepath.Dir(cur.path))
	return true
}

// Close releases every restored engine.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for id, cur := range s.engines {
		_ = cur.engine.Close()
		_ = os.RemoveAll(filepath.Dir(cur.path))
		delete(s.engines, id)
	}
	return nil
}

func writeVerified(path string, r io.Reader, maxBytes int64) (int64, string, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, "", fmt.Errorf("analysis: create restore file: %w", err)
	}
	hasher := sha256.New()
	limited := io.LimitReader(r, maxBytes+1)
	n, err := io.Copy(io.MultiWriter(f, hasher), limited)
	if err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("analysis: write restore file: %w", err)
	}
	if n > maxBytes {
		_ = f.Close()
		return 0, "", policyErr("artifact_too_large", "artifact exceeds the maximum allowed size")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, "", fmt.Errorf("analysis: sync restore file: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, "", fmt.Errorf("analysis: close restore file: %w", err)
	}
	return n, hex.EncodeToString(hasher.Sum(nil)), nil
}

func normalizeSHA(s string) string {
	out := ""
	for _, c := range s {
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		out += string(c)
	}
	return out
}

func isHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
