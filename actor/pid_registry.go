package actor

import (
	"sync"
	"sync/atomic"
)

const pidPageBits = 8
const pidPageSize = 1 << pidPageBits

// Readers use an immutable sparse directory and atomic page slots. Each shard
// retains at most one empty page for publication into the same ID range, so
// sequential Spawn/Stop does not allocate a new page and directory per actor.
// Other empty pages leave the directory even when old actors remain alive:
// current directories contain live pages plus at most 32 empty pages.
type pidRegistry struct {
	shards [32]pidShard
}

type pidShard struct {
	mu        sync.Mutex
	directory atomic.Pointer[pidDirectory]
	empty     *pidPage // mu; remains in the directory under its original page number
}

type pidDirectory struct {
	pages map[uint64]*pidPage // immutable after publication
}

type pidPage struct {
	processes [pidPageSize]atomic.Pointer[process]
	used      int // shard.mu
}

func (r *pidRegistry) Load(id uint64) (*process, bool) {
	number := id >> pidPageBits
	directory := r.shards[number%uint64(len(r.shards))].directory.Load()
	if directory == nil {
		return nil, false
	}
	page := directory.pages[number]
	if page == nil {
		return nil, false
	}
	p := page.processes[id&(pidPageSize-1)].Load()
	return p, p != nil
}

// Store receives a non-nil process from publishPIDLocked. Writers synchronize
// here because initial publication holds process.mu, not necessarily node.mu.
func (r *pidRegistry) Store(id uint64, p *process) {
	number := id >> pidPageBits
	shard := &r.shards[number%uint64(len(r.shards))]
	shard.mu.Lock()
	var page *pidPage
	if directory := shard.directory.Load(); directory != nil {
		page = directory.pages[number]
	}
	if page == nil {
		page = &pidPage{used: 1}
		page.processes[id&(pidPageSize-1)].Store(p)
		shard.publishPageLocked(number, page)
	} else {
		slot := &page.processes[id&(pidPageSize-1)]
		if slot.Load() == nil {
			if page.used == 0 {
				shard.empty = nil
			}
			page.used++
		}
		slot.Store(p)
	}
	shard.mu.Unlock()
}

func (r *pidRegistry) Delete(id uint64) {
	number := id >> pidPageBits
	shard := &r.shards[number%uint64(len(r.shards))]
	shard.mu.Lock()
	if directory := shard.directory.Load(); directory != nil {
		if page := directory.pages[number]; page != nil {
			slot := &page.processes[id&(pidPageSize-1)]
			if slot.Load() != nil {
				slot.Store(nil)
				page.used--
				if page.used == 0 {
					shard.retainEmptyPageLocked(page)
				}
			}
		}
	}
	shard.mu.Unlock()
}

// Requires s.mu and an absent page number. Replace the cached empty page, if
// any, together with the insertion in one directory publication. Removed pages
// are never reassigned to another page number: old readers can still address
// their cleared slots until Go's GC reclaims the directory and page.
func (s *pidShard) publishPageLocked(number uint64, page *pidPage) {
	previous := s.directory.Load()
	count := 1
	if previous != nil {
		count += len(previous.pages)
	}
	if s.empty != nil {
		count--
	}
	pages := make(map[uint64]*pidPage, count)
	if previous != nil {
		for key, existing := range previous.pages {
			if existing != s.empty {
				pages[key] = existing
			}
		}
	}
	pages[number] = page
	s.empty = nil
	s.directory.Store(&pidDirectory{pages: pages})
}

// Requires s.mu. The page's last slot has already been cleared. Keeping it in
// the directory allows only its original ID range to reuse the storage; a later
// empty page replaces it, so a quiet shard retains at most one cleared page.
func (s *pidShard) retainEmptyPageLocked(page *pidPage) {
	if s.empty != nil {
		previous := s.directory.Load()
		pages := make(map[uint64]*pidPage, len(previous.pages)-1)
		for key, existing := range previous.pages {
			if existing != s.empty {
				pages[key] = existing
			}
		}
		s.directory.Store(&pidDirectory{pages: pages})
	}
	s.empty = page
}
