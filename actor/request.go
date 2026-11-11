package actor

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	inlineActive uint32 = iota
	inlineReplied
	inlineExpired
	inlinePromoted
)

type request struct {
	trace                        *requestTrace // owning registry lock; allocated only with diagnostics enabled
	remote                       *requestKey   // immutable after admission; nil for local and outgoing requests
	forwarded                    atomic.Bool   // set under node.mu; completion reads without registry lock
	relocated                    bool          // node.mu; local caller now waits for a remote owner
	inline                       bool          // immutable; lock-free local PID call until promoted by Defer/waitDone
	inlineState                  atomic.Uint32 // inlineActive, inlineReplied, inlineExpired, inlinePromoted
	node                         *Node
	id                           uint64
	owner                        PID
	ctx                          context.Context
	cancel                       context.CancelFunc
	done                         chan struct{} // owning registry lock; lazily attached broadcast waiters
	callerDone                   chan struct{} // private cached result signal; immutable
	value                        any
	err                          error
	kind                         requestKind      // immutable completion domain
	origin                       CompletionOrigin // owning registry lock or inline CAS winner
	completed, expired, deferred bool             // owning registry lock or inline CAS winner
	settled                      atomic.Bool      // publishes the result to completion hooks and the executor
	completion                   atomic.Pointer[requestCompletion]
}

func (r *request) remoteKey() requestKey {
	if r.remote == nil {
		return requestKey{}
	}
	return *r.remote
}

func (r *request) setRemoteKey(key requestKey) {
	if key == (requestKey{}) && r.remote == nil {
		return
	}
	if r.remote == nil {
		r.remote = new(requestKey)
	}
	*r.remote = key
}

type requestKind uint8

const (
	requestNode requestKind = iota
	requestRemotePID
	requestActorPID
)

func (r *request) actorRequests() *actorRequests {
	if r.kind != requestActorPID {
		return nil
	}
	if r.inline && r.inlineState.Load() != inlinePromoted {
		return nil
	}
	return r.ctx.(*requestScope).actorOwner.Load()
}

func (r *request) context() context.Context {
	ctx := r.ctx
	if scope, ok := r.ctx.(*requestScope); ok {
		ctx = scope.context()
	}
	if ctx.Value(asyncRequestContextKey{}) != nil {
		return requestHandlerContext{ctx}
	}
	return ctx
}

func (r *request) contextErr() error {
	return r.ctx.Err()
}

func (r *request) deadline() (time.Time, bool) {
	return r.ctx.Deadline()
}

func (r *request) signalDoneLocked() {
	r.settled.Store(true)
	if r.done != nil {
		close(r.done)
	}
	if r.callerDone != nil {
		r.callerDone <- struct{}{}
	}
}

var completedRequestSignal = func() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}()

// Only broadcast waiters use this method. Ordinary incoming Calls need no
// channel; Defer/Replace/Handoff can attach one under the completion lock.
// The caller keeps an independently owned reusable result signal.
func (r *request) waitDone() <-chan struct{} {
	if r.inline && r.inlineState.Load() != inlinePromoted {
		if owner := r.promoteInline(false); owner != nil {
			owner.mu.Lock()
			defer owner.mu.Unlock()
			return r.waitDoneLocked()
		}
		for !r.settled.Load() {
			runtime.Gosched()
		}
		return completedRequestSignal
	}
	if owner := r.actorRequests(); owner != nil {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return r.waitDoneLocked()
	}
	if r.kind == requestRemotePID {
		r.node.network.mu.Lock()
		defer r.node.network.mu.Unlock()
		return r.waitDoneLocked()
	}
	r.node.mu.Lock()
	defer r.node.mu.Unlock()
	return r.waitDoneLocked()
}

func (r *request) waitDoneLocked() <-chan struct{} {
	if r.completed {
		return completedRequestSignal
	}
	if r.done == nil {
		r.done = make(chan struct{})
	}
	return r.done
}

type requestTrace struct {
	caller  PID
	target  string
	started time.Time
}

func (r *request) enableTraceLocked() *requestTrace {
	if r.trace == nil {
		r.trace = &requestTrace{}
	}
	return r.trace
}

type requestCompletion struct{ run func(*request) }

// Bind one immutable hook during mailbox admission.
// The hook can be shared by requests; callback ownership stays on each request.
// No node.mu is taken under the mailbox lock. settled publishes the result, and
// true means the registrar must invoke hook.run after releasing its lock.
func (r *request) bindCompletion(hook *requestCompletion) bool {
	r.completion.Store(hook)
	if r.settled.Load() {
		return r.completion.CompareAndSwap(hook, nil)
	}
	return false
}

func (r *request) finish() {
	if r.kind == requestActorPID && r.done != nil {
		// Only the successful commit reaches finish. waitDoneLocked never
		// attaches a new broadcast channel after completed is published.
		close(r.done)
	}
	if r.completion.Load() != nil {
		if hook := r.completion.Swap(nil); hook != nil {
			hook.run(r)
		}
	}
	r.cancelContext()
	if r.kind == requestActorPID && r.callerDone != nil {
		r.callerDone <- struct{}{}
	}
}

func (r *request) cancelContext() {
	if scope, ok := r.ctx.(*requestScope); ok {
		scope.finish()
	} else {
		r.cancel()
	}
}

func (n *Node) call(ctx context.Context, to Addr, msg any, sender *process) (any, error) {
	plainCtx := ctx == context.Background() || ctx == context.TODO()
	if !plainCtx {
		_, deadline := ctx.Deadline()
		plainCtx = !deadline && ctx.Done() == nil
	}
	if n.network != nil && !n.isGlobal(to) && remoteNode(to, n.Name()) != "" {
		if plainCtx {
			var from PID
			if sender != nil {
				from = sender.pid
			}
			return n.network.callDefault(ctx, to, msg, from)
		}
	}
	// Actor.Call supplies its own cancellable context. Only ordinary Node.Call
	// without caller cancellation/deadline enters the lightweight local path.
	if plainCtx {
		local := false
		switch address := to.(type) {
		case PID:
			local = address.Node == n.Name()
			if local && sender == nil {
				if ctx != context.Background() && ctx != context.TODO() {
					if err := ctx.Err(); err != nil {
						return nil, err
					}
				}
				return n.callLocalPID(ctx, address, msg)
			}
		case Ref:
			local = !n.isGlobal(to) && (address.Node == "" || address.Node == n.Name())
		}
		if local {
			if ctx != context.Background() && ctx != context.TODO() {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			return n.callLocal(ctx, nil, to, msg, sender, true)
		}
	}
	var cancel context.CancelFunc
	if _, ok := ctx.Deadline(); !ok {
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n.isGlobal(to) {
		return n.callGlobal(ctx, to.(Ref), msg, sender)
	}
	if peer := remoteNode(to, n.opts.Name); peer != "" && n.network != nil {
		var from PID
		if sender != nil {
			from = sender.pid
		}
		if cancel != nil {
			return n.network.callOwned(ctx, cancel, to, msg, from, nil)
		}
		return n.network.call(ctx, to, msg, from)
	}
	// The default timeout already gives this request its own cancellation.
	// Only derive another context when the deadline belongs to the caller.
	callCtx := ctx
	if cancel == nil {
		callCtx, cancel = context.WithCancel(ctx)
	}
	return n.callLocal(callCtx, cancel, to, msg, sender, false)
}

type localCallState struct {
	request request
	scope   requestScope
}

// A physical local PID cannot transfer its requests to another process. Use
// lock-free inline completion until Defer/waitDone promotes to actorRequests.
func (n *Node) callLocalPID(ctx context.Context, to PID, msg any) (any, error) {
	deadline := time.Now().Add(n.opts.CallTimeout)
	p, err := n.resolvePID(to)
	if err != nil {
		return nil, err
	}
	if n.diagnostics.Load() {
		state := &localCallState{scope: requestScope{parent: ctx, deadline: deadline, hasDeadline: true, callerTimer: true}}
		r, scope := &state.request, &state.scope
		done := n.takeCallSignal()
		defer n.recycleCallSignal(done)
		r.callerDone = done
		if err := p.registerActorRequest(r, scope, nil); err != nil {
			scope.finish()
			return nil, err
		}
		scope.start()
		if err := p.enqueue(envelope{message: msg, request: r}); err != nil {
			_ = n.reply(r, p.pid, nil, err, true)
		}
		<-done
		scope.releaseCallerTimer()
		return r.value, r.err
	}
	if p.incomingRequests.Load() == closedActorRequests || n.stopping.Load() {
		return nil, ErrStopped
	}
	if err := n.reservePendingCall(); err != nil {
		return nil, err
	}
	if !p.inlineCallsUsed.Load() {
		p.inlineCallsUsed.Store(true)
	}
	done := n.takeCallSignal()
	defer n.recycleCallSignal(done)
	state := &localCallState{
		request: request{
			inline:     true,
			node:       n,
			owner:      p.pid,
			callerDone: done,
			kind:       requestActorPID,
		},
		scope: requestScope{
			parent:      ctx,
			deadline:    deadline,
			hasDeadline: true,
			callerTimer: true,
		},
	}
	r, scope := &state.request, &state.scope
	r.ctx = scope
	scope.request = r
	scope.actorProc.Store(p)
	scope.start()
	if err := p.enqueue(envelope{message: msg, request: r}); err != nil {
		_ = r.replyInline(p.pid, nil, err, true, Reply)
	}
	<-done
	scope.releaseCallerTimer()
	return r.value, r.err
}

func (n *Node) callLocal(ctx context.Context, cancel context.CancelFunc, to Addr, msg any, sender *process, managed bool) (any, error) {
	var r *request
	var scope *requestScope
	var done chan struct{}
	if managed {
		state := &localCallState{scope: requestScope{parent: ctx, deadline: time.Now().Add(n.opts.CallTimeout), hasDeadline: true}}
		r, scope = &state.request, &state.scope
		done = n.takeCallSignal()
		defer n.recycleCallSignal(done)
		*r = request{node: n, ctx: scope, callerDone: done}
		scope.request = r
	} else {
		done = make(chan struct{})
		r = &request{node: n, ctx: ctx, cancel: cancel, done: done}
	}
	n.mu.Lock()
	p, toRef, err := n.resolveLocked(to)
	if err != nil || sender == p && !r.asynchronous() {
		n.mu.Unlock()
		r.cancelContext()
		if err == nil {
			return nil, ErrSelfCall
		}
		if globalRecord(to) != nil {
			err = deliveryError(err, Rejected)
		}
		return nil, err
	}
	id, err := n.reserveRequest()
	if err != nil {
		n.mu.Unlock()
		r.cancelContext()
		return nil, err
	}
	r.id, r.owner = id, p.pid
	if n.diagnostics.Load() {
		r.trace = &requestTrace{started: time.Now(), target: diagnosticTarget(to)}
		if sender != nil {
			r.trace.caller = sender.pid
		}
	}
	n.pending[r.id] = r
	p.callsUsed.Store(true)
	n.mu.Unlock()
	if managed {
		scope.start()
	}
	var from Addr
	if sender != nil {
		from = sender.selfAddr
	}
	if err = p.enqueue(envelope{message: msg, sender: from, request: r, toRef: toRef, global: globalRecord(to)}); err != nil {
		if globalRecord(to) != nil {
			err = deliveryError(err, Rejected)
		}
		_ = n.reply(r, p.pid, nil, err, true)
	}
	if managed {
		<-done
	} else {
		select {
		case <-done:
		case <-ctx.Done():
			n.expireRequest(r, ctx.Err())
			<-done
		}
	}
	return r.value, r.err
}

func (n *Node) noReply(r *request, owner PID) error {
	if r.inline {
		return r.replyInline(owner, nil, ErrNoReply, true, NoReply)
	}
	if domain := r.actorRequests(); domain != nil {
		return domain.replyWithOrigin(r, owner, nil, ErrNoReply, true, NoReply)
	}
	return n.reply(r, owner, nil, ErrNoReply, true)
}

func (n *Node) reply(r *request, owner PID, value any, err error, expired bool) error {
	if r.inline {
		return r.replyInline(owner, value, err, expired, Reply)
	}
	if domain := r.actorRequests(); domain != nil {
		return domain.reply(r, owner, value, err, expired)
	}
	if r.kind == requestRemotePID {
		return n.network.replyRemote(r, owner, value, err, expired)
	}
	n.mu.Lock()
	if r.completed {
		wasExpired := r.expired
		n.mu.Unlock()
		if wasExpired {
			return ErrReplyExpired
		}
		return ErrAlreadyReplied
	}
	if r.owner != owner {
		n.mu.Unlock()
		return ErrReplyExpired
	}
	late := false
	if cause := r.contextErr(); cause != nil {
		late = !expired
		expired = true
		value = nil
		err = cause
	}
	r.completed = true
	r.expired = expired
	r.value = value
	r.err = err
	delete(n.pending, r.id)
	n.pendingCalls.Add(-1)
	r.signalDoneLocked()
	n.mu.Unlock()
	r.finish()
	if late {
		return ErrReplyExpired
	}
	return nil
}

func (n *Node) failRequests(p *process, reason error) {
	n.failRequestsExcept(p, reason, nil)
}

func (n *Node) failRequestsExcept(p *process, reason error, keep map[*request]bool) {
	var failed []*request
	if p.inlineCallsUsed.Load() {
		p.mu.Lock()
		failed = p.failInlineRequestsLocked(reason)
		p.mu.Unlock()
	}
	// Send-only actors have no request to fail. Avoid joining the global
	// request/registry lock queue on every normal stop in a fan-out workload.
	// Ownership registration/transfer sets this before publishing the request.
	if !p.callsUsed.Load() {
		failed = append(failed, p.closeActorRequests(reason)...)
		for _, r := range failed {
			r.finish()
		}
		return
	}
	n.mu.Lock()
	failed = append(failed, n.failRequestsLocked(p, reason, keep)...)
	n.mu.Unlock()
	for _, r := range failed {
		r.finish()
	}
}

func (n *Node) failRequestsLocked(p *process, reason error, keep map[*request]bool) []*request {
	failed := p.closeActorRequests(reason)
	for _, r := range n.pending {
		if r.owner == p.pid && !r.relocated && !keep[r] {
			r.completed = true
			r.expired = true
			r.err = reason
			if err := r.contextErr(); err != nil {
				r.err = err
			}
			delete(n.pending, r.id)
			n.pendingCalls.Add(-1)
			r.signalDoneLocked()
			failed = append(failed, r)
		}
	}
	return failed
}
