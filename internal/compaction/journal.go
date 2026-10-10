package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/mackee/burstel/internal/otlp"
)

// JournalKeyFor builds the canonical raw journal key for one accepted export:
//
//	journal/{instance_id}/{signal}/{received_at_ms}-{id}.bin
//
// The id makes equal payloads distinct: a retried request is a new intake
// event unless the upstream supplies an explicit idempotency key.
func JournalKeyFor(instanceID string, signal otlp.Signal, receivedAt time.Time, id string) string {
	return fmt.Sprintf("%s/%s/%s/%d-%s.bin", JournalPrefix, instanceID, signal, receivedAt.UTC().UnixMilli(), id)
}

// WriteJournal durably writes one raw OTLP export into the journal, matching
// the spec 014 intake contract (original bytes preserved byte-for-byte). It is
// used by the field trial harness and the compactor integration tests.
func WriteJournal(
	ctx context.Context,
	store ObjectStore,
	instanceID, instanceName string,
	signal otlp.Signal,
	receivedAt time.Time,
	id string,
	wire []byte,
	contentEncoding string,
) (JournalObject, error) {
	if !signal.Valid() {
		return JournalObject{}, fmt.Errorf("compaction: invalid signal %q", signal)
	}
	sum := sha256.Sum256(wire)
	key := JournalKeyFor(instanceID, signal, receivedAt, id)
	at := receivedAt.UTC().UnixMilli()
	contentType := "application/x-protobuf"
	obj, err := store.Put(ctx, key, wire, PutOptions{
		ContentType: contentType,
		SHA256:      hex.EncodeToString(sum[:]),
		Metadata: map[string]string{
			"instance_id":      instanceID,
			"instance":         instanceName,
			"signal":           string(signal),
			"received_at":      fmt.Sprint(at),
			"sha256":           hex.EncodeToString(sum[:]),
			"content_encoding": contentEncoding,
			"content_type":     contentType,
		},
	})
	if err != nil {
		return JournalObject{}, err
	}
	return JournalObject{
		Key:             key,
		InstanceID:      instanceID,
		Instance:        instanceName,
		Signal:          signal,
		ReceivedAt:      receivedAt.UTC(),
		ReceivedAtMS:    at,
		ContentEncoding: contentEncoding,
		ContentType:     contentType,
		SHA256:          obj.SHA256,
		Size:            obj.Size,
		RequestID:       id,
	}, nil
}
