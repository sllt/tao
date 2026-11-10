package actor

import (
	"sync"
	"sync/atomic"
	"time"
)

// One worker owns publication and local taking; the timer and Close may take
// the same slot under mu. No request/Actor state is run while mu is held.
// At most one pending callback is armed during a short handoff chain. It is
// runnable immediately: an opaque handler wait does not pay a timer interval.
// There is no polling or periodic scan of idle Actors or all active workers.
type executorTurn struct {
	cache                       *executorCache
	mu                          sync.Mutex
	next                        *process
	pending                     atomic.Bool // hint only; mu transfers the execution claim
	timer                       *time.Timer
	watched, registered, closed bool      // mu
	target                      *process  // worker only; cache only after a second Send
	multiSend                   bool      // worker only; reset after Receive
	offeredInReceive            bool      // worker only; this Receive successfully published next
	budgetStart                 time.Time // worker only; last sampled execution budget start
}

func (turn *executorTurn) beginSend() {
	// A pure batch may inherit next from an earlier Receive. Only this
	// Receive's own offer makes a later Send release the pending claim.
	if turn.offeredInReceive && turn.pending.Load() {
		turn.multiSend = true
		turn.flush()
	}
}

func (turn *executorTurn) offer(p *process) bool {
	turn.mu.Lock()
	defer turn.mu.Unlock()
	if turn.closed || turn.next != nil {
		return false
	}
	if !turn.registered {
		// Close copies the set under cache.mu and releases it before locking
		// any turn. Late workers cannot publish into an already-closed cache.
		turn.cache.mu.Lock()
		if turn.cache.closed {
			turn.cache.mu.Unlock()
			turn.closed = true
			return false
		}
		if turn.cache.turns == nil {
			turn.cache.turns = make(map[*executorTurn]struct{})
		}
		turn.cache.turns[turn] = struct{}{}
		turn.registered = true
		turn.cache.mu.Unlock()
	}
	turn.next = p
	turn.offeredInReceive = true
	turn.pending.Store(true)
	if !turn.watched {
		turn.watched = true
		if turn.timer == nil {
			turn.timer = time.AfterFunc(0, turn.watch)
		} else {
			turn.timer.Reset(0)
		}
	}
	return true
}

func (turn *executorTurn) take() *process {
	if !turn.pending.Load() {
		return nil
	}
	turn.mu.Lock()
	p := turn.next
	turn.next = nil
	turn.pending.Store(false)
	turn.mu.Unlock()
	return p
}

func (turn *executorTurn) flush() {
	if !turn.pending.Load() {
		return
	}
	turn.mu.Lock()
	p := turn.next
	turn.next = nil
	turn.pending.Store(false)
	// A started callback still owns this notification and can claim a later
	// offer. Only a successful Stop lets the next offer arm another callback.
	if turn.watched && turn.timer.Stop() {
		turn.watched = false
	}
	turn.mu.Unlock()
	if p != nil {
		p.schedule()
	}
}

func (turn *executorTurn) watch() {
	turn.mu.Lock()
	if turn.closed || turn.next == nil {
		turn.watched = false
		turn.mu.Unlock()
		return
	}
	p := turn.next
	turn.next = nil
	turn.pending.Store(false)
	turn.watched = false
	turn.mu.Unlock()
	// This is the original unbounded-busy fallback, not a fixed worker pool:
	// progress does not depend on some other Actor returning from Receive.
	p.schedule()
}

// The turn belongs to one worker for its whole lifetime, never a pooled slot.
// A running callback may still submit its claimed task after Stop returns;
// it cannot touch a new publication or the closed slot afterward.
func (turn *executorTurn) close() bool {
	turn.mu.Lock()
	registered := turn.registered
	turn.closed = true
	p := turn.next
	turn.next = nil
	turn.pending.Store(false)
	turn.watched = false
	if turn.timer != nil {
		turn.timer.Stop()
	}
	turn.mu.Unlock()
	if p != nil {
		p.schedule()
	}
	return registered
}

func (p *process) releaseNext() {
	if turn := p.executor.Load(); turn != nil {
		turn.flush()
	}
}
