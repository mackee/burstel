package compaction

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// StoreCalls counts object-store operations. It is the basis for the R2 PUT
// cost surfaced by the trials and benchmarks.
type StoreCalls struct {
	Put      int64
	PutBytes int64
	Get      int64
	GetBytes int64
	Head     int64
	List     int64
	Delete   int64
}

// StoreFaults injects deterministic failures. Each counter is a countdown:
// while > 0 the operation fails and the counter is decremented. OnDelete is
// invoked (before the object is removed) so a mid-GC crash can be modelled.
type StoreFaults struct {
	FailPuts   int
	FailGets   int
	FailHeads  int
	FailLists  int
	FailDel    int
	OnPut      func(key string, n int) error
	OnDelete   func(key string, n int) error
	deleteHits int
}

// FakeR2 is a filesystem-backed, durable stand-in for an R2 bucket. Objects and
// their metadata survive process exits, which is what lets the trials prove
// recovery "after all writer processes exit".
type FakeR2 struct {
	root string

	mu      sync.Mutex
	calls   StoreCalls
	faultMu sync.Mutex
	faults  StoreFaults

	// putHook, when set, runs after each successful PUT. Returning an error
	// simulates a crash immediately after the object became durable.
	putHook func(info ObjectInfo) error
	// delHook, when set, runs after each successful DELETE.
	delHook func(key string) error
}

// NewFakeR2 creates (or reopens) a fake bucket rooted at dir.
func NewFakeR2(dir string) (*FakeR2, error) {
	if err := os.MkdirAll(filepath.Join(dir, "objects"), 0o755); err != nil {
		return nil, fmt.Errorf("fake r2: mkdir: %w", err)
	}
	return &FakeR2{root: dir}, nil
}

// Root returns the filesystem root of the fake bucket.
func (f *FakeR2) Root() string { return f.root }

// Calls returns a snapshot of the operation counters.
func (f *FakeR2) Calls() StoreCalls {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Faults exposes the fault controller.
func (f *FakeR2) Faults() *StoreFaults { return &f.faults }

// SetPutHook installs a post-PUT hook (used for "crash after upload").
func (f *FakeR2) SetPutHook(h func(ObjectInfo) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putHook = h
}

// SetDeleteHook installs a post-DELETE hook (used for "crash mid-GC").
func (f *FakeR2) SetDeleteHook(h func(key string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delHook = h
}

func (f *FakeR2) objectPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(f.root, "objects", hex.EncodeToString(sum[:])+".bin")
}

func (f *FakeR2) metaPath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(f.root, "objects", hex.EncodeToString(sum[:])+".json")
}

type fakeMeta struct {
	Key         string            `json:"key"`
	Size        int64             `json:"size"`
	SHA256      string            `json:"sha256"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	UploadedAt  time.Time         `json:"uploaded_at"`
}

// List implements ObjectStore.
func (f *FakeR2) List(ctx context.Context, prefix, cursor string, limit int) ([]ObjectInfo, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	f.mu.Lock()
	f.calls.List++
	f.mu.Unlock()
	if f.listsfail() {
		return nil, "", ErrFaultInjected
	}
	if limit <= 0 {
		limit = 1000
	}

	entries, err := os.ReadDir(filepath.Join(f.root, "objects"))
	if err != nil {
		return nil, "", fmt.Errorf("fake r2: list: %w", err)
	}
	var infos []ObjectInfo
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		m, err := f.readMetaByFile(filepath.Join(f.root, "objects", e.Name()))
		if err != nil {
			continue
		}
		if !strings.HasPrefix(m.Key, prefix) {
			continue
		}
		if cursor != "" && m.Key <= cursor {
			continue
		}
		infos = append(infos, infoFromMeta(m))
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Key < infos[j].Key })
	next := ""
	if len(infos) > limit {
		infos = infos[:limit]
		next = infos[len(infos)-1].Key
	}
	return infos, next, nil
}

func (f *FakeR2) listsfail() bool {
	f.faultMu.Lock()
	defer f.faultMu.Unlock()
	// Reuse the Head fault budget as a coarse "list unavailable" switch: tests
	// set FailHeads to model a transient listing outage.
	if f.faults.FailLists > 0 {
		f.faults.FailLists--
		return true
	}
	return false
}

// Put implements ObjectStore.
func (f *FakeR2) Put(ctx context.Context, key string, data []byte, opts PutOptions) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	got := sha256Hex(data)
	if opts.SHA256 != "" && opts.SHA256 != got {
		return ObjectInfo{}, fmt.Errorf("fake r2: sha256 mismatch for %s", key)
	}
	f.faultMu.Lock()
	fail := false
	onPut := f.faults.OnPut
	if f.faults.FailPuts > 0 {
		f.faults.FailPuts--
		fail = true
	}
	f.faultMu.Unlock()
	if fail {
		f.mu.Lock()
		f.calls.Put++
		f.mu.Unlock()
		return ObjectInfo{}, ErrFaultInjected
	}
	if onPut != nil {
		if err := onPut(key, 0); err != nil {
			return ObjectInfo{}, err
		}
	}

	tmp := f.objectPath(key) + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		return ObjectInfo{}, fmt.Errorf("fake r2: put data: %w", err)
	}
	if err := os.Rename(tmp, f.objectPath(key)); err != nil {
		return ObjectInfo{}, fmt.Errorf("fake r2: rename: %w", err)
	}
	m := fakeMeta{
		Key:         key,
		Size:        int64(len(data)),
		SHA256:      got,
		ContentType: opts.ContentType,
		Metadata:    opts.Metadata,
		UploadedAt:  time.Now().UTC(),
	}
	if err := f.writeMeta(m); err != nil {
		return ObjectInfo{}, err
	}
	info := infoFromMeta(m)

	f.mu.Lock()
	f.calls.Put++
	f.calls.PutBytes += int64(len(data))
	hook := f.putHook
	f.mu.Unlock()
	if hook != nil {
		if err := hook(info); err != nil {
			return info, err
		}
	}
	return info, nil
}

// Get implements ObjectStore.
func (f *FakeR2) Get(ctx context.Context, key string) ([]byte, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, err
	}
	f.mu.Lock()
	f.calls.Get++
	f.mu.Unlock()
	f.faultMu.Lock()
	if f.faults.FailGets > 0 {
		f.faults.FailGets--
		f.faultMu.Unlock()
		return nil, ObjectInfo{}, ErrFaultInjected
	}
	f.faultMu.Unlock()

	data, err := os.ReadFile(f.objectPath(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ObjectInfo{}, ErrObjectNotFound
		}
		return nil, ObjectInfo{}, fmt.Errorf("fake r2: get %s: %w", key, err)
	}
	m, err := f.readMeta(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	f.mu.Lock()
	f.calls.GetBytes += int64(len(data))
	f.mu.Unlock()
	return data, infoFromMeta(m), nil
}

// Head implements ObjectStore.
func (f *FakeR2) Head(ctx context.Context, key string) (ObjectInfo, bool, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, false, err
	}
	f.mu.Lock()
	f.calls.Head++
	f.mu.Unlock()
	f.faultMu.Lock()
	if f.faults.FailHeads > 0 {
		f.faults.FailHeads--
		f.faultMu.Unlock()
		return ObjectInfo{}, false, ErrFaultInjected
	}
	f.faultMu.Unlock()

	m, err := f.readMeta(key)
	if err != nil {
		if os.IsNotExist(err) {
			return ObjectInfo{}, false, nil
		}
		return ObjectInfo{}, false, err
	}
	return infoFromMeta(m), true, nil
}

// Delete implements ObjectStore.
func (f *FakeR2) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	f.calls.Delete++
	hook := f.delHook
	f.mu.Unlock()
	f.faultMu.Lock()
	if f.faults.FailDel > 0 {
		f.faults.FailDel--
		f.faultMu.Unlock()
		return ErrFaultInjected
	}
	onDel := f.faults.OnDelete
	hit := f.faults.deleteHits
	f.faults.deleteHits++
	f.faultMu.Unlock()
	if onDel != nil {
		if err := onDel(key, hit); err != nil {
			return err
		}
	}

	if err := os.Remove(f.objectPath(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fake r2: delete %s: %w", key, err)
	}
	if err := os.Remove(f.metaPath(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fake r2: delete meta %s: %w", key, err)
	}
	if hook != nil {
		if err := hook(key); err != nil {
			return err
		}
	}
	return nil
}

func (f *FakeR2) readMeta(key string) (fakeMeta, error) {
	return f.readMetaByFile(f.metaPath(key))
}

func (f *FakeR2) readMetaByFile(path string) (fakeMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fakeMeta{}, err
	}
	var m fakeMeta
	if err := json.Unmarshal(raw, &m); err != nil {
		return fakeMeta{}, err
	}
	return m, nil
}

func (f *FakeR2) writeMeta(m fakeMeta) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := f.metaPath(m.Key) + ".tmp"
	if err := writeFileSync(tmp, raw); err != nil {
		return fmt.Errorf("fake r2: write meta: %w", err)
	}
	return os.Rename(tmp, f.metaPath(m.Key))
}

func infoFromMeta(m fakeMeta) ObjectInfo {
	return ObjectInfo{
		Key:         m.Key,
		Size:        m.Size,
		SHA256:      m.SHA256,
		ContentType: m.ContentType,
		Metadata:    m.Metadata,
		UploadedAt:  m.UploadedAt,
	}
}

func writeFileSync(path string, data []byte) error {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
