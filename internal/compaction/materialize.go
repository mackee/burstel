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
// object fetches; append/checkpoint/hash/put are wall-clock phases.
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
// projection-only part DuckDB file and PUTs it back under `shots/`. Successful
// parts are byte-for-byte reproducible for the same inputs (deterministic key,
// stable row order), which makes a re-run idempotent.
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
	objects := make([]MaterializeObject, 0, len(req.Objects))
	for _, obj := range req.Objects {
		if _, skip := excluded[obj.Key]; skip {
			continue
		}
		if !strings.HasPrefix(obj.Key, "journal/") {
			return MaterializeResponse{}, badReq(fmt.Sprintf("object %s is outside the journal/ prefix", obj.Key))
		}
		objects = append(objects, obj)
	}
	// The planner already sorts by receive time; sorting the (small) metadata
	// slice here keeps a part byte-for-byte deterministic for any caller.
	sort.SliceStable(objects, func(i, j int) bool {
		if objects[i].ReceivedAt != objects[j].ReceivedAt {
			return objects[i].ReceivedAt < objects[j].ReceivedAt
		}
		return objects[i].Key < objects[j].Key
	})

	started := time.Now()
	partKey := MaterializationPartKey(req.ShotID, req.PartIndex)
	scratch, cleanup, err := NewScratchSegment(s.cfg.WorkDir, partKey)
	if err != nil {
		return MaterializeResponse{}, err
	}
	defer cleanup()

	var getMS, decodeMS int64
	var profile BuildProfile
	info, err := buildPartStream(ctx, scratch, partKey, "", "", &profile, s.appendShards, func(emit func(JournalRecord) error) error {
		var err error
		getMS, decodeMS, err = s.runMaterializePipeline(ctx, objects, emit)
		return err
	})
	if err != nil {
		return MaterializeResponse{}, err
	}
	if info.Size > s.maxPartBytes() {
		return MaterializeResponse{}, fmt.Errorf("materialization part %s exceeds the %d-byte limit", partKey, s.maxPartBytes())
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
		Objects:      len(objects),
		GetMS:        getMS,
		DecodeMS:     decodeMS,
		AppendMS:     profile.AppendMS,
		IndexMS:      profile.IndexMS,
		CheckpointMS: profile.CheckpointMS,
		HashMS:       profile.HashMS,
		PutMS:        time.Since(putStart).Milliseconds(),
		TotalMS:      time.Since(started).Milliseconds(),
	}
	fmt.Printf("burstel-materialize: part=%s objects=%d size_bytes=%d limit_bytes=%d get_ms=%d decode_ms=%d append_ms=%d checkpoint_ms=%d put_ms=%d total_ms=%d\n",
		partKey, timings.Objects, info.Size, s.maxPartBytes(), timings.GetMS, timings.DecodeMS, timings.AppendMS, timings.CheckpointMS, timings.PutMS, timings.TotalMS)
	return MaterializeResponse{
		ShotID:          req.ShotID,
		PartIndex:       req.PartIndex,
		PartKey:         partKey,
		SizeBytes:       info.Size,
		SHA256:          info.SHA256,
		ObjectCount:     int64(len(objects)),
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

type fetchedMaterializeObject struct {
	index int
	obj   MaterializeObject
	wire  []byte
	getMS int64
}

type decodedMaterializeObject struct {
	index    int
	rec      JournalRecord
	getMS    int64
	decodeMS int64
	err      error
}

// runMaterializePipeline fetches journal objects with bounded GET concurrency,
// decodes them with a worker pool and emits fully decoded records in input
// order. Bounded concurrency plus an in-order reorder buffer keeps memory
// proportional to the in-flight window instead of the whole part (spec 034).
func (s *Service) runMaterializePipeline(
	ctx context.Context,
	objects []MaterializeObject,
	emit func(JournalRecord) error,
) (int64, int64, error) {
	getWorkers := s.materializeConcurrency
	if getWorkers <= 0 {
		getWorkers = 32
	}
	decodeWorkers := s.decodeWorkers
	if decodeWorkers <= 0 {
		decodeWorkers = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var errOnce sync.Once
	var firstErr error
	fail := func(err error) {
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	fetched := make(chan fetchedMaterializeObject, getWorkers)
	decodedCh := make(chan fetchedMaterializeObject, getWorkers)

	go func() {
		defer close(fetched)
		for i, obj := range objects {
			select {
			case fetched <- fetchedMaterializeObject{index: i, obj: obj}:
			case <-ctx.Done():
				return
			}
		}
	}()

	var getWG sync.WaitGroup
	for w := 0; w < getWorkers; w++ {
		getWG.Add(1)
		go func() {
			defer getWG.Done()
			for job := range fetched {
				wire, getMS, err := fetchMaterializeWire(ctx, s.remote, job.obj)
				if err != nil {
					fail(err)
					return
				}
				select {
				case decodedCh <- fetchedMaterializeObject{index: job.index, obj: job.obj, wire: wire, getMS: getMS}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		getWG.Wait()
		close(decodedCh)
	}()

	var decodeWG sync.WaitGroup
	results := make(chan decodedMaterializeObject, decodeWorkers*2)
	for w := 0; w < decodeWorkers; w++ {
		decodeWG.Add(1)
		go func() {
			defer decodeWG.Done()
			for job := range decodedCh {
				rec, decodeMS, err := decodeMaterializeObject(job.obj, job.wire)
				result := decodedMaterializeObject{index: job.index, rec: rec, getMS: job.getMS, decodeMS: decodeMS, err: err}
				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		decodeWG.Wait()
		close(results)
	}()

	// Reorder the concurrent decodes back into input order and hand each record
	// to the appenders exactly once.
	var getMS, decodeMS int64
	next := 0
	pending := make(map[int]JournalRecord, decodeWorkers+getWorkers)
	emitErr := error(nil)
	for result := range results {
		if result.err != nil {
			fail(result.err)
			continue
		}
		getMS += result.getMS
		decodeMS += result.decodeMS
		pending[result.index] = result.rec
		for {
			rec, ok := pending[next]
			if !ok {
				break
			}
			if emitErr == nil && ctx.Err() == nil {
				if err := emit(rec); err != nil {
					emitErr = err
					fail(err)
				}
			}
			delete(pending, next)
			next++
		}
	}
	if firstErr != nil {
		return getMS, decodeMS, firstErr
	}
	if emitErr != nil {
		return getMS, decodeMS, emitErr
	}
	return getMS, decodeMS, nil
}

// fetchMaterializeWire downloads and verifies one journal object's stored
// bytes. Verification happens before decode so a corrupt object never reaches
// DuckDB.
func fetchMaterializeWire(ctx context.Context, store RemoteStore, obj MaterializeObject) ([]byte, int64, error) {
	signal := otlp.Signal(obj.Signal)
	if !signal.Valid() {
		return nil, 0, badReq(fmt.Sprintf("object %s: invalid signal %q", obj.Key, obj.Signal))
	}
	if obj.ReceivedAt <= 0 {
		return nil, 0, badReq(fmt.Sprintf("object %s: invalid received_at", obj.Key))
	}
	getStart := time.Now()
	wire, err := store.Get(ctx, obj.Key)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch %s: %w", obj.Key, err)
	}
	getMS := time.Since(getStart).Milliseconds()
	if obj.Size > 0 && int64(len(wire)) != obj.Size {
		return nil, 0, fmt.Errorf("object %s: size mismatch", obj.Key)
	}
	if obj.SHA256 != "" && !strings.EqualFold(sha256Hex(wire), obj.SHA256) {
		return nil, 0, fmt.Errorf("object %s: sha256 mismatch", obj.Key)
	}
	return wire, getMS, nil
}

// decodeMaterializeObject gunzips (when needed) and decodes one object. The raw
// bytes are hashed and dropped: a projection-only part keeps the checksums, not
// the blobs (spec 034, R1).
func decodeMaterializeObject(obj MaterializeObject, wire []byte) (JournalRecord, int64, error) {
	signal := otlp.Signal(obj.Signal)
	decodeStart := time.Now()
	payload := wire
	var err error
	if strings.EqualFold(obj.ContentEncoding, "gzip") {
		payload, err = gunzipBytes(wire)
		if err != nil {
			return JournalRecord{}, 0, fmt.Errorf("object %s: %w", obj.Key, err)
		}
	}
	decoded, err := otlp.Decode(signal, payload)
	if err != nil {
		return JournalRecord{}, 0, fmt.Errorf("object %s: decode: %w", obj.Key, err)
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
		WireSHA256:    sha256Hex(wire),
		PayloadSHA256: sha256Hex(payload),
		PayloadLength: int64(len(payload)),
		Decoded:       decoded,
	}, decodeMS, nil
}
