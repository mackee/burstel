package compaction

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mackee/burstel/internal/otlp"
)

// RemoteStore is the bucket-scoped R2 surface the materializer needs. The
// production implementation is internal/r2s3; tests inject an in-memory fake.
type RemoteStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte, contentType string) error
}

// MaterializeObject is one journal object to fold into a materialization part.
// It carries the trusted listing metadata so the container can verify the
// downloaded bytes without another coordinator round-trip.
type MaterializeObject struct {
	Key             string `json:"key"`
	Signal          string `json:"signal"`
	ReceivedAt      int64  `json:"received_at"`
	ContentEncoding string `json:"content_encoding"`
	ContentType     string `json:"content_type"`
	Size            int64  `json:"size"`
	SHA256          string `json:"sha256"`
	InstanceID      string `json:"instance_id"`
	Instance        string `json:"instance"`
}

// MaterializeRequest asks one materializer container to build one shot part.
// Part keys are deterministic (`shots/<shot_id>/part-<index>.duckdb`), so a
// retried request replaces the part atomically instead of duplicating data.
type MaterializeRequest struct {
	ShotID    string              `json:"shot_id"`
	PartIndex int                 `json:"part_index"`
	PartTotal int                 `json:"part_total"`
	Objects   []MaterializeObject `json:"objects"`
	// Excluded journal keys are honoured even if they were listed: revoked
	// tokens and late-write rejections must never enter a part (spec 023/025).
	Excluded []string `json:"excluded"`
}

// MaterializeResponse describes one uploaded shot part.
type MaterializeResponse struct {
	ShotID          string `json:"shot_id"`
	PartIndex       int    `json:"part_index"`
	PartKey         string `json:"part_key"`
	SizeBytes       int64  `json:"size_bytes"`
	SHA256          string `json:"sha256"`
	ObjectCount     int64  `json:"object_count"`
	RequestCount    int64  `json:"request_count"`
	RecordCount     int64  `json:"record_count"`
	SpanCount       int64  `json:"span_count"`
	LogCount        int64  `json:"log_count"`
	MetricCount     int64  `json:"metric_count"`
	FirstReceivedAt int64  `json:"first_received_at"`
	LastReceivedAt  int64  `json:"last_received_at"`
}

var shotIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Materialize downloads one chunk of journal objects directly from R2, builds a
// part DuckDB file and PUTs it back under `shots/`. Successful parts are
// byte-for-byte reproducible for the same inputs (deterministic key, sorted
// rows), which makes a re-run idempotent.
func (s *Service) Materialize(ctx context.Context, req MaterializeRequest) (MaterializeResponse, error) {
	if s.remote == nil {
		return MaterializeResponse{}, badReq("materialization requires bucket-scoped R2 credentials")
	}
	if !shotIDPattern.MatchString(req.ShotID) {
		return MaterializeResponse{}, badReq("invalid shot_id")
	}
	if req.PartTotal <= 0 || req.PartTotal > 64 {
		return MaterializeResponse{}, badReq("part_total must be in 1..64")
	}
	if req.PartIndex < 0 || req.PartIndex >= req.PartTotal {
		return MaterializeResponse{}, badReq("part_index must be in [0, part_total)")
	}
	if len(req.Objects) > s.cfg.MaxBatchObjects*1024 {
		return MaterializeResponse{}, badReq("materialization chunk exceeds the object budget")
	}
	excluded := make(map[string]struct{}, len(req.Excluded))
	for _, key := range req.Excluded {
		excluded[key] = struct{}{}
	}

	records, err := s.fetchMaterializationRecords(ctx, req.Objects, excluded)
	if err != nil {
		return MaterializeResponse{}, err
	}
	sort.SliceStable(records, func(i, j int) bool {
		a, b := records[i].Object, records[j].Object
		if a.ReceivedAtMS != b.ReceivedAtMS {
			return a.ReceivedAtMS < b.ReceivedAtMS
		}
		return a.Key < b.Key
	})

	partKey := MaterializationPartKey(req.ShotID, req.PartIndex)
	scratch, cleanup, err := NewScratchSegment(s.cfg.WorkDir, partKey)
	if err != nil {
		return MaterializeResponse{}, err
	}
	defer cleanup()
	info, err := BuildPart(ctx, scratch, partKey, "", "", records)
	if err != nil {
		return MaterializeResponse{}, err
	}
	if info.Size > s.cfg.MaxSegmentBytes {
		return MaterializeResponse{}, fmt.Errorf("materialization part %s exceeds the %d-byte limit", partKey, s.cfg.MaxSegmentBytes)
	}
	data, err := os.ReadFile(scratch)
	if err != nil {
		return MaterializeResponse{}, err
	}
	if err := s.remote.Put(ctx, partKey, data, "application/vnd.duckdb"); err != nil {
		return MaterializeResponse{}, fmt.Errorf("upload part %s: %w", partKey, err)
	}
	return MaterializeResponse{
		ShotID:          req.ShotID,
		PartIndex:       req.PartIndex,
		PartKey:         partKey,
		SizeBytes:       info.Size,
		SHA256:          info.SHA256,
		ObjectCount:     int64(len(records)),
		RequestCount:    info.RequestCount,
		RecordCount:     info.RecordCount,
		SpanCount:       info.SpanCount,
		LogCount:        info.LogCount,
		MetricCount:     info.MetricCount,
		FirstReceivedAt: info.FirstReceivedAt,
		LastReceivedAt:  info.LastReceivedAt,
	}, nil
}

// MaterializationPartKey is the deterministic object key of one shot part.
func MaterializationPartKey(shotID string, index int) string {
	return fmt.Sprintf("shots/%s/part-%d.duckdb", shotID, index)
}

func (s *Service) fetchMaterializationRecords(
	ctx context.Context,
	objects []MaterializeObject,
	excluded map[string]struct{},
) ([]JournalRecord, error) {
	records := make([]JournalRecord, 0, len(objects))
	var mu sync.Mutex
	var firstErr error
	sem := make(chan struct{}, s.materializeConcurrency)
	var wg sync.WaitGroup

	for _, obj := range objects {
		if _, skip := excluded[obj.Key]; skip {
			continue
		}
		if !strings.HasPrefix(obj.Key, "journal/") {
			return nil, badReq(fmt.Sprintf("object %s is outside the journal/ prefix", obj.Key))
		}
		obj := obj
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			rec, err := fetchMaterializationRecord(ctx, s.remote, obj)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return records, nil
}

func fetchMaterializationRecord(ctx context.Context, store RemoteStore, obj MaterializeObject) (JournalRecord, error) {
	signal := otlp.Signal(obj.Signal)
	if !signal.Valid() {
		return JournalRecord{}, badReq(fmt.Sprintf("object %s: invalid signal %q", obj.Key, obj.Signal))
	}
	if obj.ReceivedAt <= 0 {
		return JournalRecord{}, badReq(fmt.Sprintf("object %s: invalid received_at", obj.Key))
	}
	wire, err := store.Get(ctx, obj.Key)
	if err != nil {
		return JournalRecord{}, fmt.Errorf("fetch %s: %w", obj.Key, err)
	}
	if obj.Size > 0 && int64(len(wire)) != obj.Size {
		return JournalRecord{}, fmt.Errorf("object %s: size mismatch", obj.Key)
	}
	if obj.SHA256 != "" && !strings.EqualFold(sha256Hex(wire), obj.SHA256) {
		return JournalRecord{}, fmt.Errorf("object %s: sha256 mismatch", obj.Key)
	}
	payload := wire
	if strings.EqualFold(obj.ContentEncoding, "gzip") {
		payload, err = gunzipBytes(wire)
		if err != nil {
			return JournalRecord{}, fmt.Errorf("object %s: %w", obj.Key, err)
		}
	}
	decoded, err := otlp.Decode(signal, payload)
	if err != nil {
		return JournalRecord{}, fmt.Errorf("object %s: decode: %w", obj.Key, err)
	}
	return JournalRecord{
		Object: JournalObject{
			Key:             obj.Key,
			InstanceID:      obj.InstanceID,
			Instance:        obj.Instance,
			Signal:          signal,
			ReceivedAt:      time.UnixMilli(obj.ReceivedAt).UTC(),
			ReceivedAtMS:    obj.ReceivedAt,
			ContentEncoding: strings.ToLower(obj.ContentEncoding),
			ContentType:     obj.ContentType,
			SHA256:          obj.SHA256,
			Size:            int64(len(wire)),
			RequestID:       requestIDFromKey(obj.Key),
		},
		Wire:    wire,
		Payload: payload,
		Decoded: decoded,
	}, nil
}
