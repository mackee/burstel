package compaction

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	ShotID          string             `json:"shot_id"`
	PartIndex       int                `json:"part_index"`
	PartKey         string             `json:"part_key"`
	SizeBytes       int64              `json:"size_bytes"`
	SHA256          string             `json:"sha256"`
	ObjectCount     int64              `json:"object_count"`
	RequestCount    int64              `json:"request_count"`
	RecordCount     int64              `json:"record_count"`
	SpanCount       int64              `json:"span_count"`
	LogCount        int64              `json:"log_count"`
	MetricCount     int64              `json:"metric_count"`
	FirstReceivedAt int64              `json:"first_received_at"`
	LastReceivedAt  int64              `json:"last_received_at"`
	Timings         MaterializeTimings `json:"timings"`
}

// MaterializeTimings is the per-phase latency breakdown of one part build
// (spec 033 requirement 5). GET/decode are summed over the part's concurrent
// object fetches; append/index/checkpoint/hash/put are wall-clock phases.
type MaterializeTimings struct {
	Objects      int   `json:"objects"`
	GetMS        int64 `json:"get_ms"`
	DecodeMS     int64 `json:"decode_ms"`
	AppendMS     int64 `json:"append_ms"`
	IndexMS      int64 `json:"index_ms"`
	CheckpointMS int64 `json:"checkpoint_ms"`
	HashMS       int64 `json:"hash_ms"`
	PutMS        int64 `json:"put_ms"`
	TotalMS      int64 `json:"total_ms"`
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

	started := time.Now()
	records, getMS, decodeMS, err := s.fetchMaterializationRecords(ctx, req.Objects, excluded)
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
	info, profile, err := BuildPartProfile(ctx, scratch, partKey, "", "", records)
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
	putStart := time.Now()
	if err := s.remote.Put(ctx, partKey, data, "application/vnd.duckdb"); err != nil {
		return MaterializeResponse{}, fmt.Errorf("upload part %s: %w", partKey, err)
	}
	timings := MaterializeTimings{
		Objects:      len(records),
		GetMS:        getMS,
		DecodeMS:     decodeMS,
		AppendMS:     profile.AppendMS,
		IndexMS:      profile.IndexMS,
		CheckpointMS: profile.CheckpointMS,
		HashMS:       profile.HashMS,
		PutMS:        time.Since(putStart).Milliseconds(),
		TotalMS:      time.Since(started).Milliseconds(),
	}
	fmt.Printf("burstel-materialize: part=%s objects=%d get_ms=%d decode_ms=%d append_ms=%d checkpoint_ms=%d put_ms=%d total_ms=%d\n",
		partKey, timings.Objects, timings.GetMS, timings.DecodeMS, timings.AppendMS, timings.CheckpointMS, timings.PutMS, timings.TotalMS)
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
		Timings:         timings,
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
) ([]JournalRecord, int64, int64, error) {
	records := make([]JournalRecord, 0, len(objects))
	var mu sync.Mutex
	var firstErr error
	var getMS, decodeMS int64
	sem := make(chan struct{}, s.materializeConcurrency)
	var wg sync.WaitGroup

	for _, obj := range objects {
		if _, skip := excluded[obj.Key]; skip {
			continue
		}
		if !strings.HasPrefix(obj.Key, "journal/") {
			return nil, 0, 0, badReq(fmt.Sprintf("object %s is outside the journal/ prefix", obj.Key))
		}
		obj := obj
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			rec, getDuration, decodeDuration, err := fetchMaterializationRecord(ctx, s.remote, obj)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			atomic.AddInt64(&getMS, getDuration)
			atomic.AddInt64(&decodeMS, decodeDuration)
			mu.Lock()
			records = append(records, rec)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, 0, 0, firstErr
	}
	return records, getMS, decodeMS, nil
}

func fetchMaterializationRecord(ctx context.Context, store RemoteStore, obj MaterializeObject) (JournalRecord, int64, int64, error) {
	signal := otlp.Signal(obj.Signal)
	if !signal.Valid() {
		return JournalRecord{}, 0, 0, badReq(fmt.Sprintf("object %s: invalid signal %q", obj.Key, obj.Signal))
	}
	if obj.ReceivedAt <= 0 {
		return JournalRecord{}, 0, 0, badReq(fmt.Sprintf("object %s: invalid received_at", obj.Key))
	}
	getStart := time.Now()
	wire, err := store.Get(ctx, obj.Key)
	if err != nil {
		return JournalRecord{}, 0, 0, fmt.Errorf("fetch %s: %w", obj.Key, err)
	}
	getMS := time.Since(getStart).Milliseconds()
	if obj.Size > 0 && int64(len(wire)) != obj.Size {
		return JournalRecord{}, 0, 0, fmt.Errorf("object %s: size mismatch", obj.Key)
	}
	if obj.SHA256 != "" && !strings.EqualFold(sha256Hex(wire), obj.SHA256) {
		return JournalRecord{}, 0, 0, fmt.Errorf("object %s: sha256 mismatch", obj.Key)
	}
	decodeStart := time.Now()
	payload := wire
	if strings.EqualFold(obj.ContentEncoding, "gzip") {
		payload, err = gunzipBytes(wire)
		if err != nil {
			return JournalRecord{}, 0, 0, fmt.Errorf("object %s: %w", obj.Key, err)
		}
	}
	decoded, err := otlp.Decode(signal, payload)
	if err != nil {
		return JournalRecord{}, 0, 0, fmt.Errorf("object %s: decode: %w", obj.Key, err)
	}
	decodeMS := time.Since(decodeStart).Milliseconds()
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
	}, getMS, decodeMS, nil
}
