package actor

import (
	"context"
	"errors"
	"math"
)

// RequestID identifies an asynchronous operation owned by one Actor instance.
// It is a local correlation token, not the wire CallID or a transferable ticket.
type RequestID struct {
	Caller PID
	ID     uint64
}

// CallResult is delivered once through the requesting Actor's system mailbox.
// Sender is nil; ID identifies the request. Unclaimed results are dropped on Stop.
type CallResult struct {
	ID    RequestID
	Value any
	Err   error
}

var ErrAsyncPending = errors.New("actor: asynchronous requests or results are still pending")

type asyncRequestContextKey struct{}

// The async marker describes one outgoing operation, not its descendants.
// A handler may use Context() for a synchronous callback; do not classify that
// callback as asynchronous merely because its parent was started with Request.
type requestHandlerContext struct{ context.Context }

func (ctx requestHandlerContext) Value(key any) any {
	if _, internal := key.(asyncRequestContextKey); internal {
		return nil
	}
	return ctx.Context.Value(key)
}

// All mutable fields belong to the caller's process.mu. The worker only calls
// the existing synchronous request path; it never invokes the caller's behavior.
type asyncRequest struct {
	id        RequestID
	cancel    context.CancelFunc
	completed bool
	result    CallResult
}

// Request starts a bounded asynchronous Call. A successful return reserves one
// completion until Receive handles CallResult, even if the business mailbox is
// full. Admission errors are returned here; routing/reply errors arrive in the
// result. The deadline starts now, not when the worker is scheduled.
//
// Like other Actor operations, call this from Receive. ctx controls the operation;
// Stop also cancels it. Use a.Context() to tie it to the current incoming Call, or
// a separate context when the work should outlive that Call. Messages retain the
// ordinary local Send/Call ownership rules and must not be mutated after Request.
func (a *Actor) Request(ctx context.Context, to Addr, message any) (RequestID, error) {
	p, n := a.proc, a.proc.node
	p.releaseNext()
	if err := p.preparationError(); err != nil {
		return RequestID{}, err
	}
	if err := ctx.Err(); err != nil {
		return RequestID{}, err
	}
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); ok {
		ctx, cancel = context.WithCancel(ctx)
	} else {
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
	}
	// This ordering also makes worker registration precede Node.Stop's Wait.
	n.mu.Lock()
	p.mu.Lock()
	var err error
	switch {
	case n.stopping.Load():
		err = ErrClosed
	case p.stopRequested || p.finished || p.unresponsive:
		err = ErrStopped
	case p.change != nil || p.handoff != nil:
		err = ErrMigrating
	case len(p.asyncRequests) >= p.mailbox.limit || n.asyncPending.Load() >= int64(n.opts.MaxAsyncRequests) || n.nextAsync == math.MaxUint64:
		err = ErrCapacity
	}
	if err != nil {
		p.mu.Unlock()
		n.mu.Unlock()
		cancel()
		return RequestID{}, err
	}
	n.nextAsync++
	id := RequestID{Caller: p.pid, ID: n.nextAsync}
	r := &asyncRequest{id: id, cancel: cancel}
	if p.asyncRequests == nil {
		p.asyncRequests = make(map[RequestID]*asyncRequest)
	}
	p.asyncRequests[id] = r
	n.asyncPending.Add(1)
	n.asyncWorkers.Add(1)
	p.mu.Unlock()
	n.mu.Unlock()
	ctx = context.WithValue(ctx, asyncRequestContextKey{}, id)
	go func() {
		defer n.asyncWorkers.Done()
		value, err := n.call(ctx, to, message, p)
		cancel()
		p.completeAsync(r, value, err)
	}()
	return id, nil
}

// CancelRequest asks an unfinished operation owned by this instance to cancel.
// A reply may already have won the race; cancellation does not retract effects
// or change an already queued result. The eventual CallResult remains observable.
func (a *Actor) CancelRequest(id RequestID) bool {
	p := a.proc
	p.mu.Lock()
	r := p.asyncRequests[id]
	if r == nil || r.completed {
		p.mu.Unlock()
		return false
	}
	cancel := r.cancel
	p.mu.Unlock()
	cancel()
	return true
}

func (p *process) completeAsync(r *asyncRequest, value any, err error) {
	p.mu.Lock()
	if p.stopRequested || p.finished || p.unresponsive || p.node.stopping.Load() {
		delete(p.asyncRequests, r.id)
		p.node.asyncPending.Add(-1)
		p.mu.Unlock()
		return
	}
	r.completed = true
	r.result = CallResult{ID: r.id, Value: value, Err: err}
	p.system = append(p.system, r)
	p.interruptBatchLocked()
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	if start {
		p.schedule()
	}
}

func (p *process) takeAsyncResult(r *asyncRequest) (CallResult, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.asyncRequests[r.id] != r {
		return CallResult{}, false // Stop discarded this queued notification.
	}
	delete(p.asyncRequests, r.id)
	p.node.asyncPending.Add(-1)
	result := r.result
	r.result = CallResult{}
	return result, true
}

// Stop drops queued results immediately. Running operations retain their slot
// until their worker exits, so repeated restarts cannot accumulate uncounted work.
func (p *process) cancelAsyncRequestsLocked() {
	for id, r := range p.asyncRequests {
		r.cancel()
		if r.completed {
			delete(p.asyncRequests, id)
			r.result = CallResult{}
			p.node.asyncPending.Add(-1)
		}
	}
}

func (n *Node) waitAsyncWorkers(ctx context.Context) error {
	n.mu.Lock()
	used := n.nextAsync != 0
	n.mu.Unlock()
	if !used {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, n.opts.ShutdownTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() { n.asyncWorkers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Join(ErrUnresponsive, ctx.Err())
	}
}

func (r *request) asynchronous() bool {
	ctx := r.ctx
	if scope, ok := ctx.(*requestScope); ok {
		ctx = scope.parent // observation must not materialize an unused context/timer
	}
	return ctx.Value(asyncRequestContextKey{}) != nil
}
