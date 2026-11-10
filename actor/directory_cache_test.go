package actor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type cacheTestDirectory struct {
	Directory
	events  chan DirectoryEvent
	resume  chan struct{}
	watches atomic.Int32
	reads   atomic.Int32
}

func (d *cacheTestDirectory) Watch(ctx context.Context, prefix string) (<-chan DirectoryEvent, error) {
	if d.watches.Add(1) == 1 {
		return d.events, nil
	}
	select {
	case <-d.resume:
		return d.Directory.Watch(ctx, prefix)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (d *cacheTestDirectory) Lookup(ctx context.Context, path string) (RouteRecord, error) {
	d.reads.Add(1)
	return d.Directory.Lookup(ctx, path)
}

func waitCache(t *testing.T, cache *DirectoryCache, check func(bool, int64) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cache.mu.RLock()
		done := check(cache.ready, cache.revision)
		cache.mu.RUnlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("cache did not reach expected state")
}

func TestDirectoryCacheInvalidationAndResnapshot(t *testing.T) {
	d := &cacheTestDirectory{Directory: NewMemoryDirectory(), events: make(chan DirectoryEvent, 8), resume: make(chan struct{})}
	ctx := t.Context()
	a, err := d.Claim(ctx, RouteRecord{Path: "counter", Owner: "a", BootID: [16]byte{1}, ProcessID: 1})
	if err != nil {
		t.Fatal(err)
	}
	d.events <- DirectoryEvent{Snapshot: true, Revision: a.Revision, Routes: []RouteRecord{a}}
	cache, err := NewDirectoryCache(ctx, d, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	waitCache(t, cache, func(ready bool, rev int64) bool { return ready && rev == a.Revision })
	if found, err := cache.Lookup(ctx, a.Path); err != nil || found != a || d.reads.Load() != 0 {
		t.Fatalf("cache hit: %+v %v reads=%d", found, err, d.reads.Load())
	}
	b := a
	b.ProcessID = 2
	b, err = d.SwapInstance(ctx, a, b)
	if err != nil {
		t.Fatal(err)
	}
	d.events <- DirectoryEvent{Err: ErrWatchOverflow}
	close(d.events)
	waitCache(t, cache, func(ready bool, _ int64) bool { return !ready })
	if found, err := cache.Lookup(ctx, a.Path); err != nil || found != b || d.reads.Load() != 1 {
		t.Fatalf("invalid cache must read backend: %+v %v reads=%d", found, err, d.reads.Load())
	}
	close(d.resume)
	waitCache(t, cache, func(ready bool, rev int64) bool { return ready && rev == b.Revision })
	if found, _ := cache.Lookup(ctx, a.Path); found != b || d.reads.Load() != 1 {
		t.Fatalf("resnapshot: %+v reads=%d", found, d.reads.Load())
	}
	cache.Invalidate(a.Path)
	if found, _ := cache.Lookup(ctx, a.Path); found != b || d.reads.Load() != 2 {
		t.Fatalf("explicit invalidation: %+v reads=%d", found, d.reads.Load())
	}
}

func TestDirectoryCacheTombstoneRejectsOldWatchEvent(t *testing.T) {
	d := &cacheTestDirectory{Directory: NewMemoryDirectory(), events: make(chan DirectoryEvent, 8), resume: make(chan struct{})}
	ctx := t.Context()
	a, err := d.Claim(ctx, RouteRecord{Path: "counter", Owner: "a", BootID: [16]byte{1}, ProcessID: 1})
	if err != nil {
		t.Fatal(err)
	}
	dead, err := d.Release(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	d.events <- DirectoryEvent{Snapshot: true, Revision: dead.Revision, Routes: []RouteRecord{dead}}
	d.events <- DirectoryEvent{Revision: a.Revision, Routes: []RouteRecord{a}}
	cache, err := NewDirectoryCache(ctx, d, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	waitCache(t, cache, func(ready bool, rev int64) bool { return ready && rev == dead.Revision })
	if record, err := cache.Lookup(ctx, a.Path); !errors.Is(err, ErrDirectoryNotFound) || record != dead || d.reads.Load() != 0 {
		t.Fatalf("tombstone: %+v %v", record, err)
	}
}
