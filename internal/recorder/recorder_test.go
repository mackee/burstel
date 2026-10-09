package recorder_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mackee/burstel/internal/config"
	"github.com/mackee/burstel/internal/otlp"
	"github.com/mackee/burstel/internal/recorder"
	"github.com/mackee/burstel/internal/storage"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	return config.Config{
		DataDir:      t.TempDir(),
		MaxInflight:  2,
		WriteTimeout: 10 * time.Second,
	}
}

func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	rec := recorder.New(cfg)

	if got := rec.Metadata().State; got != recorder.StateCreated {
		t.Fatalf("initial state = %q", got)
	}
	if _, err := rec.Artifact(); !errors.Is(err, recorder.ErrNotFrozen) {
		t.Fatalf("artifact before start err = %v", err)
	}
	if _, err := rec.Stop(ctx, "s1"); !errors.Is(err, recorder.ErrNotStarted) {
		t.Fatalf("stop before start err = %v", err)
	}
	if _, err := rec.Start(ctx, recorder.StartRequest{}); !errors.Is(err, recorder.ErrInvalidShotID) {
		t.Fatalf("empty shot id err = %v", err)
	}

	if _, err := rec.Start(ctx, recorder.StartRequest{ShotID: "s1", Metadata: map[string]any{"scenario": "bench"}}); err != nil {
		t.Fatalf("start: %v", err)
	}
	// Idempotent for identical inputs.
	if _, err := rec.Start(ctx, recorder.StartRequest{ShotID: "s1", Metadata: map[string]any{"scenario": "bench"}}); err != nil {
		t.Fatalf("idempotent start: %v", err)
	}
	// Conflict for a different shot, or a different metadata payload.
	if _, err := rec.Start(ctx, recorder.StartRequest{ShotID: "s2"}); !errors.Is(err, recorder.ErrAlreadyRecording) {
		t.Fatalf("conflict start err = %v", err)
	}
	if _, err := rec.Start(ctx, recorder.StartRequest{ShotID: "s1", Metadata: map[string]any{"scenario": "other"}}); !errors.Is(err, recorder.ErrAlreadyRecording) {
		t.Fatalf("metadata mismatch err = %v", err)
	}

	if err := rec.Ingest(ctx, &storage.Batch{ShotID: "other", Signal: otlp.Traces, Payload: []byte("x")}); !errors.Is(err, recorder.ErrWrongShot) {
		t.Fatalf("wrong shot err = %v", err)
	}
	batch := &storage.Batch{
		ShotID:  "s1",
		Signal:  otlp.Traces,
		Payload: []byte("payload"),
		Spans:   []otlp.Span{{Name: "op"}},
	}
	if err := rec.Ingest(ctx, batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	meta := rec.Metadata()
	if meta.State != recorder.StateRecording || meta.Counters.Batches["traces"] != 1 || meta.Counters.Spans != 1 {
		t.Fatalf("metadata = %+v", meta)
	}
	if meta.SchemaVersion != "1" {
		t.Fatalf("schema version = %q", meta.SchemaVersion)
	}

	res, err := rec.Stop(ctx, "s1")
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if res.State != recorder.StateFrozen || res.SizeBytes <= 0 || res.SHA256 == "" {
		t.Fatalf("stop result = %+v", res)
	}
	// Idempotent stop.
	res2, err := rec.Stop(ctx, "s1")
	if err != nil || res2.SHA256 != res.SHA256 || res2.SizeBytes != res.SizeBytes {
		t.Fatalf("second stop = %+v err=%v", res2, err)
	}
	// Ingest after stop is refused.
	if err := rec.Ingest(ctx, batch); !errors.Is(err, recorder.ErrNotRecording) {
		t.Fatalf("ingest after stop err = %v", err)
	}

	info, err := rec.Artifact()
	if err != nil {
		t.Fatalf("artifact: %v", err)
	}
	if info.Name != recorder.ArtifactName || info.SHA256 != res.SHA256 {
		t.Fatalf("artifact info = %+v", info)
	}
	// The frozen file exists at the documented location.
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "s1", recorder.ArtifactName)); err != nil {
		t.Fatalf("frozen file: %v", err)
	}
}
