// Package compaction turns the durable raw OTLP journal (spec 014) into
// immutable, checksummed DuckDB segments stored in an R2-compatible object
// store, and publishes a durable manifest only after the stored object has been
// verified.
//
// The package is deliberately written against a small structural
// {@link ObjectStore} interface so the exact same compactor runs against:
//
//   - {@link FakeR2}, a filesystem-backed R2 stand-in used by the 60 s / 180 s
//     trials and the failure-injection tests, and
//   - a real bucket in production.
//
// Design invariants enforced here (spec 015):
//
//   - ACKed raw journal bytes are immutable and are never deleted until a
//     segment that covers them is verified in R2 and committed to the manifest.
//   - A manifest row is advertised only after the R2 object's size and SHA-256
//     have been re-verified; a partial upload is never advertised.
//   - Compaction is at-least-once but idempotent: the manifest is unique on the
//     segment key and on each covered journal key, so retries can never create
//     duplicate logical rows or duplicate coverage.
//   - A failed/missing segment is surfaced as an explicit gap / incomplete
//     condition, never silently skipped.
package compaction

import (
	"context"
	"errors"
	"time"
)

// ErrObjectNotFound is returned by Get/Head when an object does not exist.
var ErrObjectNotFound = errors.New("compaction: object not found")

// ErrFaultInjected is returned by the fake store when a test injects a fault.
var ErrFaultInjected = errors.New("compaction: fault injected")

// ObjectInfo describes one stored object.
type ObjectInfo struct {
	Key         string
	Size        int64
	SHA256      string
	ContentType string
	Metadata    map[string]string
	UploadedAt  time.Time
}

// PutOptions mirrors the subset of R2 PUT options the compactor needs.
type PutOptions struct {
	ContentType string
	// SHA256, when set, is the expected lowercase hex SHA-256 of data. The
	// store must reject a mismatch (as R2 does).
	SHA256   string
	Metadata map[string]string
}

// ObjectStore is the structural subset of an R2 bucket the compactor uses.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte, opts PutOptions) (ObjectInfo, error)
	Get(ctx context.Context, key string) ([]byte, ObjectInfo, error)
	Head(ctx context.Context, key string) (ObjectInfo, bool, error)
	// List returns up to limit objects whose key has the given prefix, using an
	// opaque pagination cursor. A non-empty next cursor means more pages exist.
	List(ctx context.Context, prefix, cursor string, limit int) ([]ObjectInfo, string, error)
	Delete(ctx context.Context, key string) error
}
