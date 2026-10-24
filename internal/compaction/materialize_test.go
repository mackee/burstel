package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync"
	"testing"
)

type memoryRemote struct {
	mu      sync.Mutex
	objects map[string][]byte
	gets    int
	puts    int
}

func newMemoryRemote() *memoryRemote {
	return &memoryRemote{objects: map[string][]byte{}}
}

func (m *memoryRemote) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	data, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}
	return append([]byte(nil), data...), nil
}

func (m *memoryRemote) Put(_ context.Context, key string, data []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func objectInfo(key, signal string, receivedAt int64, wire []byte) MaterializeObject {
	sum := sha256.Sum256(wire)
	return MaterializeObject{
		Key:             key,
		Signal:          signal,
		ReceivedAt:      receivedAt,
		Size:            int64(len(wire)),
		SHA256:          hex.EncodeToString(sum[:]),
		InstanceID:      "inst-1",
		Instance:        "node-1",
		ContentEncoding: "",
		ContentType:     "application/x-protobuf",
	}
}

func TestMaterializeBuildsVerifiedPartAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	const at = int64(1_700_000_000_000)
	traces := spanBatch("svc", 5, uint64(at)*1_000_000)
	logsWire := logBatch("svc", 3, uint64(at)*1_000_000)
	traceKey := "journal/inst-1/traces/1700000000000-a.bin"
	logKey := "journal/inst-1/logs/1700000000001-b.bin"
	remote.objects[traceKey] = traces
	remote.objects[logKey] = logsWire

	svc := NewServiceWithRemote(DefaultServiceConfig(), remote, 4)
	svc.cfg.WorkDir = t.TempDir()
	req := MaterializeRequest{
		ShotID:    "shot-031",
		PartIndex: 0,
		PartTotal: 2,
		Objects: []MaterializeObject{
			objectInfo(traceKey, "traces", at, traces),
			objectInfo(logKey, "logs", at+1, logsWire),
		},
	}
	first, err := svc.Materialize(ctx, req)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if first.PartKey != "shots/shot-031/part-0.duckdb" {
		t.Fatalf("unexpected part key %q", first.PartKey)
	}
	if first.SpanCount != 5 || first.LogCount != 3 || first.RequestCount != 2 {
		t.Fatalf("unexpected counts: %+v", first)
	}
	if first.SizeBytes <= 0 || first.SHA256 == "" {
		t.Fatalf("missing checksum/size: %+v", first)
	}
	if first.Timings.Objects != 2 || first.Timings.TotalMS < 0 || first.Timings.PutMS < 0 {
		t.Fatalf("missing per-phase timings: %+v", first.Timings)
	}

	part := remote.objects[first.PartKey]
	if int64(len(part)) != first.SizeBytes || sha256Hex(part) != first.SHA256 {
		t.Fatal("stored part does not match the manifest")
	}
	path := t.TempDir() + "/part.duckdb"
	if err := os.WriteFile(path, part, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenSegmentReadOnly(ctx, path, first.SHA256)
	if err != nil {
		t.Fatalf("open part: %v", err)
	}
	defer reader.Close()
	spans, logs, metrics, batches, err := reader.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if spans != 5 || logs != 3 || metrics != 0 || batches != 2 {
		t.Fatalf("unexpected stored counts %d/%d/%d/%d", spans, logs, metrics, batches)
	}

	second, err := svc.Materialize(ctx, req)
	if err != nil {
		t.Fatalf("re-materialize: %v", err)
	}
	if second.PartKey != first.PartKey || second.SpanCount != first.SpanCount || second.LogCount != first.LogCount {
		t.Fatalf("re-materialization is not idempotent: %+v vs %+v", first, second)
	}
	if int64(len(remote.objects[first.PartKey])) != second.SizeBytes {
		t.Fatal("re-materialization did not replace the deterministic part key")
	}
}

func TestMaterializeHonoursExclusions(t *testing.T) {
	ctx := context.Background()
	remote := newMemoryRemote()
	const at = int64(1_700_000_000_000)
	traces := spanBatch("svc", 4, uint64(at)*1_000_000)
	key := "journal/inst-1/traces/1700000000000-a.bin"
	remote.objects[key] = traces

	svc := NewServiceWithRemote(DefaultServiceConfig(), remote, 2)
	svc.cfg.WorkDir = t.TempDir()
	resp, err := svc.Materialize(ctx, MaterializeRequest{
		ShotID:    "shot-excl",
		PartIndex: 0,
		PartTotal: 1,
		Objects:   []MaterializeObject{objectInfo(key, "traces", at, traces)},
		Excluded:  []string{key},
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	if resp.ObjectCount != 0 || resp.SpanCount != 0 {
		t.Fatalf("excluded object leaked into the part: %+v", resp)
	}
	if remote.gets != 0 {
		t.Fatalf("excluded object was downloaded (%d GETs)", remote.gets)
	}
}
