package actor

import (
	"runtime"
	"sync"
	"time"
)

// Retain only a bounded number of idle executors, including their grown
// stacks. Awake workers drain their shard's ready queue between Actors. A
// zero-delay callback independently dispatches queued work even when every
// existing worker is blocked inside a handler.
type executorPool struct {
	shards []executorCache
}

type executorCache struct {
	mu           sync.Mutex
	idle         []chan *process
	ready        executorReady
	busy         int // includes waking and blocked workers; never limits dispatch
	readyTimer   *time.Timer
	readyWatched bool // one independent queued-work callback owns the notification
	maxIdle      int
	closed       bool
	turns        map[*executorTurn]struct{} // only workers that have used run-next; close drains their slots
	_            [64]byte
}

func (e *executorPool) init(maxIdle, count int) {
	// Keep retention independent of shard count: a wider idle cache should
	// not also change which actors contend for the same handoff lock.
	e.shards = make([]executorCache, count)
	for i := range e.shards {
		e.shards[i].maxIdle = maxIdle / count
		if i < maxIdle%count {
			e.shards[i].maxIdle++
		}
	}
}

func (p *process) schedule() { p.node.executors.submit(p) }

// New processes already own their initial execution claim. Their creator
// immediately waits for Started, so dispatch it without a ready callback.
func (p *process) scheduleStart() { p.node.executors.submitStart(p) }

func (e *executorPool) submit(p *process) {
	// Independent actors do not need one shared idle-stack lock. A worker
	// returns to the same cache, keeping its stack reusable for this shard.
	e.shards[p.pid.ID%uint64(len(e.shards))].submit(p)
}

func (e *executorPool) submitStart(p *process) {
	e.shards[p.pid.ID%uint64(len(e.shards))].submitStart(p)
}

func (e *executorCache) submitStart(p *process) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		go p.run()
		return
	}
	jobs := e.claimWorkerLocked()
	e.mu.Unlock()
	e.dispatch(p, jobs)
}

func (e *executorCache) submit(p *process) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		go p.run()
		return
	}
	if e.busy != 0 {
		e.ready.push(p)
		e.watchReadyLocked()
		e.mu.Unlock()
		return
	}
	jobs := e.claimWorkerLocked()
	e.mu.Unlock()
	e.dispatch(p, jobs)
}

// Removal from idle transfers exclusive ownership of the handoff. Close
// cannot close a channel already handed to a submitter or the ready callback.
func (e *executorCache) claimWorkerLocked() chan *process {
	e.busy++
	if count := len(e.idle); count > 0 {
		jobs := e.idle[count-1]
		e.idle[count-1] = nil
		e.idle = e.idle[:count-1]
		return jobs
	}
	return nil
}

func (e *executorCache) dispatch(p *process, jobs chan *process) {
	if jobs != nil {
		jobs <- p
	} else {
		go e.run(p)
	}
}

func (e *executorCache) watchReadyLocked() {
	if e.readyWatched {
		return
	}
	e.readyWatched = true
	if e.readyTimer == nil {
		e.readyTimer = time.AfterFunc(0, e.watchReady)
	} else {
		e.readyTimer.Reset(0)
	}
}

func (e *executorCache) watchReady() {
	var handoffs [4]struct {
		jobs chan *process
		p    *process
	}
	count := 0
	e.mu.Lock()
	e.readyWatched = false
	p := e.ready.pop()
	if p == nil {
		e.mu.Unlock()
		return
	}
	jobs := e.claimWorkerLocked()
	if jobs != nil {
		handoffs[0].jobs, handoffs[0].p = jobs, p
		count = 1
		p = nil
		// One notification can release a short burst to already-idle workers.
		// Only the first claim may need a new worker: additional claims never
		// create goroutines, wait for arrivals or wait for handlers to finish.
		for count < len(handoffs) && len(e.idle) != 0 && e.ready.size != 0 {
			handoffs[count].p = e.ready.pop()
			handoffs[count].jobs = e.claimWorkerLocked()
			count++
		}
	}
	if e.ready.size != 0 {
		// Transfer responsibility for the remaining queue before dispatching:
		// this task may immediately block in arbitrary user code. A busy count
		// is not evidence that any worker can come back to drain the queue.
		e.watchReadyLocked()
	}
	e.mu.Unlock()
	// Each channel was removed from idle under mu, so Close cannot close it.
	// Release every transferred claim before this callback can run user code.
	for i := range count {
		handoffs[i].jobs <- handoffs[i].p
	}
	if p != nil {
		// AfterFunc already gives this callback an independent goroutine.
		// The remaining queue has its own notification before any handler runs.
		e.run(p)
	}
}

func (e *executorCache) run(p *process) {
	var jobs chan *process
	var batch *mailboxBatch
	turn := &executorTurn{cache: e, budgetStart: time.Now()}
	defer func() {
		if turn.close() {
			e.mu.Lock()
			delete(e.turns, turn)
			e.mu.Unlock()
		}
	}()
	chain := 0
	for {
		p.executor.Store(turn)
		next := p.runWithBatch(&batch)
		// Idle publication can let another worker start this Actor before the
		// old run returns. Never clear that newer worker's execution context.
		p.executor.CompareAndSwap(turn, nil)
		if next != nil {
			// Requeue the previous Actor's retained execution claim before any
			// target handler can block. A new worker must see its own context;
			// submit also preserves progress when the cache has already closed.
			e.submit(p)
		} else {
			next = turn.take()
		}
		chain++
		if chain == 64 {
			// A run-next chain cannot keep the shard's FIFO waiting forever.
			// Keep an uncontended claim local, including across PID shards.
			// With queued work, rotate it behind the FIFO under the same lock.
			if next != nil {
				e.mu.Lock()
				if !e.closed && e.ready.size != 0 {
					head := e.ready.pop()
					e.ready.push(next)
					next = head
					// The selected Actor can block. The remaining claims must
					// keep an independent owner even while this worker is busy.
					e.watchReadyLocked()
				}
				e.mu.Unlock()
			}
			chain = 0
			runtime.Gosched()
		}
		if next != nil {
			p = next
			continue
		}
		p = nil
		e.mu.Lock()
		if p = e.ready.pop(); p != nil {
			e.mu.Unlock()
			continue
		}
		e.busy--
		if e.closed || len(e.idle) >= e.maxIdle {
			e.mu.Unlock()
			return
		}
		chain = 0
		if jobs == nil {
			jobs = make(chan *process, 1)
		}
		e.idle = append(e.idle, jobs)
		e.mu.Unlock()
		var ok bool
		p, ok = <-jobs
		if !ok {
			return
		}
	}
}

func (e *executorPool) close() {
	for i := range e.shards {
		e.shards[i].close()
	}
}

func (e *executorCache) close() {
	e.mu.Lock()
	e.closed = true
	ready := e.ready
	e.ready = executorReady{}
	e.readyWatched = false
	if e.readyTimer != nil {
		e.readyTimer.Stop()
	}
	var turns []*executorTurn
	for turn := range e.turns {
		turns = append(turns, turn)
	}
	e.turns = nil
	for _, jobs := range e.idle {
		close(jobs)
	}
	e.idle = nil
	e.mu.Unlock()
	for p := ready.pop(); p != nil; p = ready.pop() {
		go p.run()
	}
	for _, turn := range turns {
		turn.close()
	}
}
