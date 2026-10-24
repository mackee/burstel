package analysis

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Spec 036 acceptance: a commit is refused when a selected part is not present
// locally, and the session is discarded so no rows can be served.
func TestCommitRefusesMissingSelectedPart(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "missing")
	dir := t.TempDir()
	present := buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 2, 0), true, false, false)
	absent := buildFixturePart(t, dir, "part-1", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 3, 10_000_000), true, false, false)
	catalog := []SegmentMeta{present.meta, absent.meta}

	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{})
	defer store.Close()

	if _, _, err := store.OpenWindowWithManifest(ctx, "s1", window, catalog, false, WindowManifest{Parts: 2, Spans: 5}); err != nil {
		t.Fatalf("open: %v", err)
	}
	raw, err := os.ReadFile(present.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddSegment(ctx, "s1", SegmentRef{Key: present.meta.Key, Instance: "web-1"}, bytes.NewReader(raw), present.meta.Size, present.meta.SHA256); err != nil {
		t.Fatalf("add: %v", err)
	}

	_, err = store.CommitWithManifest(ctx, "s1", WindowManifest{Parts: 2, Spans: 5})
	var incomplete *IncompleteError
	if !errors.As(err, &incomplete) {
		t.Fatalf("commit error = %v, want *IncompleteError", err)
	}
	if incomplete.Code() != "analysis_incomplete" {
		t.Fatalf("code = %q", incomplete.Code())
	}
	if len(incomplete.MissingParts) != 1 || incomplete.MissingParts[0] != absent.meta.Key {
		t.Fatalf("missing parts = %v, want [%s]", incomplete.MissingParts, absent.meta.Key)
	}
	if incomplete.AttachedParts != 1 || incomplete.ExpectedParts != 2 {
		t.Fatalf("attached/expected parts = %d/%d", incomplete.AttachedParts, incomplete.ExpectedParts)
	}
	// The session must be discarded: a refused commit never serves rows.
	if _, err := store.Scoped("s1"); !errors.Is(err, ErrWindowSessionNotFound) {
		t.Fatalf("session survived a refused commit: %v", err)
	}
	if _, err := store.Query(ctx, "s1", QueryRequest{SQL: "SELECT count(*) FROM spans"}); !errors.Is(err, ErrWindowSessionNotFound) {
		t.Fatalf("query after refused commit = %v, want session-not-found", err)
	}
}

func TestCommitSelfCheckDetectsSpanCountMismatch(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "selfcheck")
	dir := t.TempDir()
	part := buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 4, 0), true, false, false)
	raw, err := os.ReadFile(part.path)
	if err != nil {
		t.Fatal(err)
	}
	fetcher := &fakeFetcher{data: map[string][]byte{part.meta.Key: raw}}
	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: fetcher})
	defer store.Close()

	// The manifest claims one more span than the part actually carries.
	if _, _, err := store.OpenWindowWithManifest(ctx, "s1", window, []SegmentMeta{part.meta}, true, WindowManifest{Parts: 1, Spans: 5}); err != nil {
		t.Fatalf("open: %v", err)
	}
	_, err = store.CommitWithManifest(ctx, "s1", WindowManifest{Parts: 1, Spans: 5})
	var incomplete *IncompleteError
	if !errors.As(err, &incomplete) {
		t.Fatalf("commit error = %v, want *IncompleteError", err)
	}
	if incomplete.AttachedSpans != 4 || incomplete.ExpectedSpans != 5 {
		t.Fatalf("spans = %d/%d", incomplete.AttachedSpans, incomplete.ExpectedSpans)
	}
	if _, err := store.Scoped("s1"); !errors.Is(err, ErrWindowSessionNotFound) {
		t.Fatalf("session survived a failed self-check: %v", err)
	}
}

// Spec 036 acceptance: the direct download inside open is synchronous. When the
// open returns every selected part is on disk, and commit's self-check reports
// per-part row counts and an exactly matching attached span total.
func TestDirectDownloadIsSynchronousAndSelfChecked(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "sync")
	dir := t.TempDir()
	parts := []partFixture{
		buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 2, 0), true, false, false),
		buildFixturePart(t, dir, "part-1", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 3, 10_000_000), true, false, false),
	}
	raw := map[string][]byte{}
	catalog := make([]SegmentMeta, 0, len(parts))
	for _, part := range parts {
		b, err := os.ReadFile(part.path)
		if err != nil {
			t.Fatal(err)
		}
		raw[part.meta.Key] = b
		catalog = append(catalog, part.meta)
	}
	fetcher := &fakeFetcher{data: raw, delay: 10 * time.Millisecond}
	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: fetcher, FetchParallelism: 2})
	defer store.Close()

	_, keys, err := store.OpenWindowWithManifest(ctx, "s1", window, catalog, true, WindowManifest{Parts: 2, Spans: 5})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("cached keys = %v, want both parts", keys)
	}
	// No background download: every fetch already completed when open returned.
	if got := fetcher.callCount(); got != 2 {
		t.Fatalf("downloads = %d, want 2 (synchronous)", got)
	}
	for _, part := range parts {
		if _, _, ok := store.cachedSegment(part.meta.Key, part.meta.Size, part.meta.SHA256); !ok {
			t.Fatalf("part %s not verified after open", part.meta.Key)
		}
	}

	commit, err := store.CommitWithManifest(ctx, "s1", WindowManifest{Parts: 2, Spans: 5})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if !commit.Verified || !commit.Complete {
		t.Fatalf("commit = %+v", commit)
	}
	if commit.AttachedParts != 2 || commit.ExpectedParts != 2 {
		t.Fatalf("attached parts = %d/%d", commit.AttachedParts, commit.ExpectedParts)
	}
	if commit.AttachedSpans != 5 || commit.ViewSpans != 5 || commit.ExpectedSpans != 5 {
		t.Fatalf("span self-check = attached %d, view %d, expected %d", commit.AttachedSpans, commit.ViewSpans, commit.ExpectedSpans)
	}
	var spanRows int64
	for _, row := range commit.PartRows {
		spanRows += row.Spans
	}
	if len(commit.PartRows) != 2 || spanRows != 5 {
		t.Fatalf("part rows = %+v", commit.PartRows)
	}
	res, err := store.Query(ctx, "s1", QueryRequest{SQL: "SELECT count(*) FROM spans"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if got := asInt64(res.Rows[0][0]); got != 5 {
		t.Fatalf("view span count = %d", got)
	}
}

// A missing object during the synchronous download fails the open outright; no
// partially attached session is left behind.
func TestDirectDownloadFailureDiscardsSession(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	window := NewServerWindow(now.Add(-3*time.Minute), now.Add(-time.Minute), []string{"web-1"}, "partial")
	dir := t.TempDir()
	part := buildFixturePart(t, dir, "part-0", now.Add(-2*time.Minute), spanProfiles(uint64(window.StartNS()), 2, 0), true, false, false)
	fetcher := &fakeFetcher{data: map[string][]byte{}} // object missing
	store := NewWindowStore(filepath.Join(dir, "windows"), WindowStoreOptions{Fetcher: fetcher})
	defer store.Close()

	if _, _, err := store.OpenWindowWithManifest(ctx, "s1", window, []SegmentMeta{part.meta}, true, WindowManifest{Parts: 1, Spans: 2}); err == nil {
		t.Fatal("expected the open to fail when a part cannot be downloaded")
	}
	if _, err := store.Scoped("s1"); !errors.Is(err, ErrWindowSessionNotFound) {
		t.Fatalf("failed download left a session behind: %v", err)
	}
}
