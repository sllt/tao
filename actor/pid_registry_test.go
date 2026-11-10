package actor

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
)

func TestConcurrentChildCreationKeepsNodeCapacityAndUniquePIDs(t *testing.T) {
	n := testNode(t, NodeOptions{MaxActors: 16})
	f := factory(func(*testBehavior, any) error { return nil })
	var parents []*process
	for range 8 {
		pid := spawn(t, n, f, WithRestart(Temporary))
		p, _, err := n.resolve(pid)
		if err != nil {
			t.Fatal(err)
		}
		parents = append(parents, p)
	}
	created := make(chan PID, 64)
	var group sync.WaitGroup
	for i := range 64 {
		group.Go(func() {
			pid, err := n.spawn(parents[i%len(parents)], t.Context(), f, WithRestart(Temporary))
			if err == nil {
				created <- pid
			} else if !errors.Is(err, ErrCapacity) {
				t.Error(err)
			}
		})
	}
	group.Wait()
	close(created)
	seen := make(map[PID]bool)
	for pid := range created {
		if seen[pid] {
			t.Fatalf("duplicate PID: %v", pid)
		}
		seen[pid] = true
	}
	if len(seen) != 8 || n.Stats().Actors != 16 {
		t.Fatalf("capacity commit: children=%d actors=%d", len(seen), n.Stats().Actors)
	}
}

func TestPIDExhaustionDoesNotWrap(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.nextPID.Store(math.MaxUint64 - 1)
	f := factory(func(*testBehavior, any) error { return nil })
	pid := spawn(t, n, f, WithRestart(Temporary))
	if pid.ID != math.MaxUint64 {
		t.Fatal(pid)
	}
	if _, err := n.Spawn(f); !errors.Is(err, ErrCapacity) || n.nextPID.Load() != math.MaxUint64 {
		t.Fatalf("exhausted PID allocation: %v", err)
	}
	if p, err := n.resolvePID(pid); err != nil || p.pid != pid {
		t.Fatalf("last PID not resolvable: %v %v", p, err)
	}
	if err := n.StopActor(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	if _, ok := n.pidIndex.Load(pid.ID); ok {
		t.Fatal("last PID survived retirement")
	}
	checkPIDRegistryPages(t, &n.pidIndex, 0)
}

func TestPIDRegistrySparseBoundariesAndReplacement(t *testing.T) {
	var registry pidRegistry
	ids := []uint64{0, pidPageSize - 1, pidPageSize, 1 << 32, 1 << 63, math.MaxUint64}
	want := make(map[uint64]*process)
	for _, id := range ids {
		if p, ok := registry.Load(id); ok || p != nil {
			t.Fatalf("zero registry contains %d", id)
		}
		registry.Delete(id)
		p := &process{pid: PID{ID: id}}
		registry.Store(id, p)
		want[id] = p
	}
	for _, id := range ids {
		if p, ok := registry.Load(id); !ok || p != want[id] {
			t.Fatalf("PID %d: got %p, %t; want %p", id, p, ok, want[id])
		}
	}
	replacement := &process{pid: PID{ID: pidPageSize - 1}}
	registry.Store(pidPageSize-1, replacement)
	if p, ok := registry.Load(pidPageSize - 1); !ok || p != replacement {
		t.Fatal("replacement was not published")
	}
	for _, id := range ids {
		registry.Delete(id)
		registry.Delete(id)
		if p, ok := registry.Load(id); ok || p != nil {
			t.Fatalf("PID %d survived deletion", id)
		}
	}
	checkPIDRegistryPages(t, &registry, 0)
}

func TestPIDRegistryRetainedPageKeepsOldReadersSafe(t *testing.T) {
	var registry pidRegistry
	const id = uint64(7)
	first := &process{pid: PID{ID: id}}
	registry.Store(id, first)
	oldDirectory := registry.shards[0].directory.Load()
	oldPage := oldDirectory.pages[0]

	// A second page in the same directory shard must not mutate the snapshot.
	otherID := uint64(len(registry.shards)) * pidPageSize
	other := &process{pid: PID{ID: otherID}}
	registry.Store(otherID, other)
	if len(oldDirectory.pages) != 1 || oldDirectory.pages[otherID>>pidPageBits] != nil {
		t.Fatal("page publication mutated an old directory")
	}
	registry.Delete(id)
	if oldDirectory.pages[0] != oldPage || oldPage.processes[id].Load() != nil {
		t.Fatal("last slot was not cleared in the retained page")
	}
	if registry.shards[0].directory.Load().pages[0] != oldPage {
		t.Fatal("empty page was not retained for its original ID range")
	}

	// Reusing the same page number is safe for old readers: each slot still
	// addresses exactly its original ID, including after the page became empty.
	next := &process{pid: PID{ID: id + 1}}
	registry.Store(id+1, next)
	currentPage := registry.shards[0].directory.Load().pages[0]
	if currentPage != oldPage || oldPage.processes[id+1].Load() != next || oldPage.processes[id].Load() != nil {
		t.Fatal("same-range reuse changed an old reader's slot identity")
	}
	if p, ok := registry.Load(id + 1); !ok || p != next {
		t.Fatal("new page is not readable")
	}
	registry.Delete(otherID)
	if p, ok := registry.Load(id + 1); !ok || p != next {
		t.Fatal("retiring a neighboring page removed a live PID")
	}
	registry.Delete(id + 1)
	if registry.shards[0].directory.Load().pages[otherID>>pidPageBits] != nil {
		t.Fatal("an older empty page remains in the current directory")
	}
	checkPIDRegistryPages(t, &registry, 0)

	// Crossing to another page in this shard evicts the old empty page. Its
	// array must never be reassigned to the new range, even at the same offset.
	thirdID := 2*otherID + id + 1
	third := &process{pid: PID{ID: thirdID}}
	registry.Store(thirdID, third)
	if registry.shards[0].directory.Load().pages[0] != nil || oldPage.processes[id+1].Load() != nil {
		t.Fatal("cross-range publication reassigned or retained the old empty page")
	}
	if len(oldDirectory.pages) != 1 || oldDirectory.pages[0] != oldPage {
		t.Fatal("cross-range publication mutated an old directory")
	}
	// Publication can complete out of allocation order. Returning to the old
	// range after eviction allocates a new page; there is no historical pool.
	fourth := &process{pid: PID{ID: id + 2}}
	registry.Store(id+2, fourth)
	if registry.shards[0].directory.Load().pages[0] == oldPage || oldPage.processes[id+2].Load() != nil {
		t.Fatal("evicted page was reused behind an old reader")
	}
	if p, ok := registry.Load(thirdID); !ok || p != third {
		t.Fatal("returning to an old range removed a live neighboring PID")
	}
	registry.Delete(thirdID)
	registry.Delete(id + 2)
	checkPIDRegistryPages(t, &registry, 0)
}

func TestPIDRegistryReusesEmptyPageAndDirectory(t *testing.T) {
	var registry pidRegistry
	registry.Store(0, &process{pid: PID{ID: 0}})
	directory := registry.shards[0].directory.Load()
	page := directory.pages[0]
	registry.Delete(0)

	// Retain the initial snapshot while publication reuses its original page.
	// These readers never reload the directory, including across its eviction.
	done := make(chan struct{})
	ready := make(chan struct{}, 2)
	var readers sync.WaitGroup
	for range 2 {
		readers.Go(func() {
			ready <- struct{}{}
			for {
				select {
				case <-done:
					return
				default:
				}
				for id := range uint64(pidPageSize) {
					if p := directory.pages[0].processes[id].Load(); p != nil && p.pid.ID != id {
						t.Errorf("old directory slot %d resolved PID %d", id, p.pid.ID)
						return
					}
				}
				runtime.Gosched()
			}
		})
	}
	defer func() {
		close(done)
		readers.Wait()
	}()
	for range 2 {
		<-ready
	}
	for id := uint64(1); id < pidPageSize; id++ {
		registry.Store(id, &process{pid: PID{ID: id}})
		replacement := &process{pid: PID{ID: id}}
		registry.Store(id, replacement)
		if p, ok := registry.Load(id); !ok || p != replacement || page.used != 1 {
			t.Fatalf("same-ID replacement changed slot occupancy at %d", id)
		}
		registry.Delete(id)
		if p, ok := registry.Load(id); ok || p != nil {
			t.Fatalf("retired PID %d remains readable", id)
		}
		if current := registry.shards[0].directory.Load(); current != directory || current.pages[0] != page {
			t.Fatal("same-range churn replaced its empty page or directory")
		}
	}
	if empty := checkPIDRegistryPages(t, &registry, 0); empty != 1 {
		t.Fatalf("same-range churn retained %d empty pages", empty)
	}
	crossID := uint64(len(registry.shards))*pidPageSize + 1
	registry.Store(crossID, &process{pid: PID{ID: crossID}})
	if registry.shards[0].directory.Load() == directory || page.processes[1].Load() != nil {
		t.Fatal("cross-range publication reused an old reader's page")
	}
	registry.Delete(crossID)
	checkPIDRegistryPages(t, &registry, 0)
}

func TestPIDRegistryKeepsAtMostOneEmptyPagePerShard(t *testing.T) {
	var registry pidRegistry
	for round := range 4 {
		for shard := range registry.shards {
			id := uint64(round*len(registry.shards)+shard)*pidPageSize + 7
			registry.Store(id, &process{pid: PID{ID: id}})
			registry.Delete(id)
			checkPIDRegistryPages(t, &registry, 0)
		}
	}
	if empty := checkPIDRegistryPages(t, &registry, 0); empty != len(registry.shards) {
		t.Fatalf("quiet registry retains %d empty pages, want %d", empty, len(registry.shards))
	}
	for i := range registry.shards {
		directory := registry.shards[i].directory.Load()
		for _, page := range directory.pages {
			for slot := range page.processes {
				if p := page.processes[slot].Load(); p != nil {
					t.Fatalf("cached page in shard %d retains a process in slot %d", i, slot)
				}
			}
		}
	}
}

func TestPIDRegistryConcurrentPublicationAndRetirement(t *testing.T) {
	const writers, rounds = 32, 128
	var registry pidRegistry
	start := make(chan struct{})
	done := make(chan struct{})
	var readers sync.WaitGroup
	for reader := range 4 {
		readers.Go(func() {
			<-start
			for round := 0; ; round++ {
				select {
				case <-done:
					return
				default:
				}
				id := uint64(round%rounds)*pidPageSize + uint64(reader)
				if p, ok := registry.Load(id); ok && (p == nil || p.pid.ID != id) {
					t.Errorf("PID %d resolved to %v", id, p)
					return
				}
				runtime.Gosched()
			}
		})
	}
	var group sync.WaitGroup
	for writer := range writers {
		group.Go(func() {
			<-start
			for round := range rounds {
				// Unique IDs share pages and directory shards. Fast writers may
				// retire a page while another writer first publishes into it.
				id := uint64(round)*pidPageSize + uint64(writer)
				p := &process{pid: PID{ID: id}}
				registry.Store(id, p)
				if got, ok := registry.Load(id); !ok || got != p {
					t.Errorf("published PID %d disappeared", id)
					return
				}
				registry.Delete(id)
				if got, ok := registry.Load(id); ok || got != nil {
					t.Errorf("retired PID %d remains readable", id)
					return
				}
			}
		})
	}
	close(start)
	group.Wait()
	close(done)
	readers.Wait()
	checkPIDRegistryPages(t, &registry, 0)
}

func TestPIDRegistryChurnRetainsOnlyLiveAndBoundedEmptyPages(t *testing.T) {
	var registry pidRegistry
	anchors := []uint64{1, math.MaxUint64}
	for _, id := range anchors {
		registry.Store(id, &process{pid: PID{ID: id}})
	}
	// Keep both old and very high IDs alive while touching many sparse pages.
	// Unbounded high-water slices or historical empty page keys cannot pass this.
	for page := uint64(1); page <= 4096; page++ {
		id := page << (pidPageBits + 12)
		registry.Store(id, &process{pid: PID{ID: id}})
		registry.Delete(id)
		if empty := checkPIDRegistryPages(t, &registry, len(anchors)); empty != 1 {
			t.Fatalf("after historical page %d: retained %d empty pages in one shard", page, empty)
		}
	}
	for _, id := range anchors {
		if p, ok := registry.Load(id); !ok || p.pid.ID != id {
			t.Fatalf("long-lived PID %d disappeared", id)
		}
		registry.Delete(id)
	}
	checkPIDRegistryPages(t, &registry, 0)
}

func TestPIDRegistryStoppedActorsReleaseSparsePages(t *testing.T) {
	n := testNode(t, NodeOptions{})
	f := factory(func(*testBehavior, any) error { return nil })
	anchor := spawn(t, n, f, WithRestart(Temporary))
	for round := range 256 {
		// PID allocation may leave gaps after rejected reservations. Move the
		// counter forward to exercise a fresh page on every real Spawn/Stop.
		n.nextPID.Store(uint64(round+1) * pidPageSize)
		pid := spawn(t, n, f, WithRestart(Temporary))
		if _, err := n.resolvePID(pid); err != nil {
			t.Fatal(err)
		}
		if err := n.StopActor(t.Context(), pid); err != nil {
			t.Fatal(err)
		}
		if _, err := n.resolvePID(pid); !errors.Is(err, ErrNoProcess) {
			t.Fatalf("stopped PID %d resolves: %v", pid.ID, err)
		}
		checkPIDRegistryPages(t, &n.pidIndex, 1)
	}
	if _, err := n.resolvePID(anchor); err != nil {
		t.Fatalf("old actor disappeared during churn: %v", err)
	}
	if err := n.StopActor(t.Context(), anchor); err != nil {
		t.Fatal(err)
	}
	checkPIDRegistryPages(t, &n.pidIndex, 0)
}

// Count actual directory entries, not just live slots. A shard may keep one
// empty page, and its cached pointer must identify that page and no live page.
func checkPIDRegistryPages(t *testing.T, registry *pidRegistry, wantLive int) int {
	t.Helper()
	live, empty := 0, 0
	for i := range registry.shards {
		shard := &registry.shards[i]
		shard.mu.Lock()
		shardEmpty := 0
		if directory := shard.directory.Load(); directory != nil {
			for _, page := range directory.pages {
				if page.used == 0 {
					empty++
					shardEmpty++
					if page != shard.empty {
						t.Errorf("shard %d retains an untracked empty page", i)
					}
				} else {
					live++
					if page == shard.empty {
						t.Errorf("shard %d caches a live page", i)
					}
				}
			}
		}
		if shardEmpty > 1 || (shardEmpty == 0 && shard.empty != nil) {
			t.Errorf("shard %d retains %d empty directory entries with cache %p", i, shardEmpty, shard.empty)
		}
		shard.mu.Unlock()
	}
	if live != wantLive || empty > len(registry.shards) {
		t.Fatalf("registry has %d live, %d empty pages; want %d live and at most %d empty", live, empty, wantLive, len(registry.shards))
	}
	return empty
}
