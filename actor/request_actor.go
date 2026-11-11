package actor

import (
	"context"
	"sync"
	"time"
)

// Ordinary local/default and incoming PID requests belong here. A completed DeferredReply
// retains this small completion domain, not the retired Actor and its behavior.
// This lock never acquires node.mu, process.mu or network.mu; finish callbacks
// run after it is released. The network still owns its cancellation index.
type actorRequests struct {
	mu      sync.Mutex
	closed  bool
	pending map[uint64]*request
}

// Seal a never-used owner with CAS, so a concurrent first request cannot
// publish a new completion domain after Stop has already scanned it.
var closedActorRequests = &actorRequests{closed: true}

func (p *process) requestOwner() (*actorRequests, error) {
	owner := p.incomingRequests.Load()
	if owner == nil {
		candidate := &actorRequests{pending: make(map[uint64]*request)}
		if p.incomingRequests.CompareAndSwap(nil, candidate) {
			owner = candidate
		} else {
			owner = p.incomingRequests.Load()
		}
	}
	if owner == closedActorRequests {
		return nil, ErrStopped
	}
	return owner, nil
}

func (w *network) registerActorRequest(r *request, p *process, scope *requestScope, key requestKey, sender *PID) (*request, error) {
	r.setRemoteKey(key)
	if err := p.registerActorRequest(r, scope, sender); err != nil {
		return nil, err
	}
	return r, nil
}

func (p *process) registerActorRequest(r *request, scope *requestScope, sender *PID) error {
	r.node, r.owner, r.kind, r.ctx = p.node, p.pid, requestActorPID, scope
	scope.request = r
	owner, err := p.requestOwner()
	if err != nil {
		return err
	}
	scope.actorOwner.Store(owner)
	owner.mu.Lock()
	if owner.closed || p.node.stopping.Load() {
		owner.mu.Unlock()
		return ErrStopped
	}
	r.id, err = p.node.reserveRequest()
	if err != nil {
		owner.mu.Unlock()
		return err
	}
	if p.node.diagnostics.Load() {
		r.trace = &requestTrace{target: p.pid.String(), started: time.Now()}
		if sender != nil {
			r.trace.caller = *sender
		}
	}
	owner.pending[r.id] = r
	owner.mu.Unlock()
	return nil
}

func (owner *actorRequests) commitLocked(r *request, value any, err error, expired bool, origin CompletionOrigin) {
	r.value, r.err, r.completed, r.expired, r.origin = value, err, true, expired, origin
	delete(owner.pending, r.id)
	r.node.pendingCalls.Add(-1)
	// Publish the value under the owner lock, but wake the caller afterwards.
	// Its next Call must not immediately contend with the winning publisher.
	r.settled.Store(true)
}

func (owner *actorRequests) reply(r *request, pid PID, value any, err error, expired bool) error {
	return owner.replyWithOrigin(r, pid, value, err, expired, Reply)
}

func (owner *actorRequests) replyWithOrigin(r *request, pid PID, value any, err error, expired bool, origin CompletionOrigin) error {
	owner.mu.Lock()
	if r.completed {
		wasExpired := r.expired
		owner.mu.Unlock()
		if wasExpired {
			return ErrReplyExpired
		}
		return ErrAlreadyReplied
	}
	if r.owner != pid {
		owner.mu.Unlock()
		return ErrReplyExpired
	}
	late := false
	if cause := r.contextErr(); cause != nil {
		late, expired, value, err, origin = !expired, true, nil, cause, Canceled
	}
	owner.commitLocked(r, value, err, expired, origin)
	owner.mu.Unlock()
	r.finish()
	if late {
		return ErrReplyExpired
	}
	return nil
}

func (owner *actorRequests) expire(r *request, err error) bool {
	owner.mu.Lock()
	if r.completed {
		owner.mu.Unlock()
		return false
	}
	owner.commitLocked(r, nil, err, true, Canceled)
	owner.mu.Unlock()
	r.finish()
	return true
}

func (owner *actorRequests) cancel(r *request, scope *requestScope) bool {
	owner.mu.Lock()
	if r.completed {
		owner.mu.Unlock()
		return false
	}
	err := scope.err()
	if err == nil {
		err = context.Canceled
	}
	owner.commitLocked(r, nil, err, true, Canceled)
	owner.mu.Unlock()
	scope.finish()
	r.finish()
	return true
}

// A published domain stays reachable for all concurrent closers. Only nil
// transitions to the sentinel; a late first registrar must then reject.
func (p *process) closeActorRequests(reason error) []*request {
	owner := p.incomingRequests.Load()
	if owner == nil {
		if p.incomingRequests.CompareAndSwap(nil, closedActorRequests) {
			return nil
		}
		owner = p.incomingRequests.Load()
	}
	if owner == closedActorRequests {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.closed = true
	var failed []*request
	for _, r := range owner.pending {
		origin := TargetExit
		err := reason
		if cause := r.contextErr(); cause != nil {
			err = cause
			origin = Canceled
		}
		owner.commitLocked(r, nil, err, true, origin)
		failed = append(failed, r)
	}
	return failed
}

func (r *request) promoteInline(deferring bool) *actorRequests {
	scope := r.ctx.(*requestScope)
	for {
		switch r.inlineState.Load() {
		case inlinePromoted:
			owner := scope.actorOwner.Load()
			if deferring {
				owner.mu.Lock()
				r.deferred = true
				owner.mu.Unlock()
			}
			return owner
		case inlineReplied, inlineExpired:
			return nil
		}
		proc := scope.actorProc.Load()
		if proc == nil {
			continue
		}
		owner, err := proc.requestOwner()
		if err != nil {
			r.expireInline(ErrStopped, TargetExit)
			scope.actorProc.Store(nil)
			return nil
		}
		owner.mu.Lock()
		if owner.closed {
			owner.mu.Unlock()
			r.expireInline(ErrStopped, TargetExit)
			scope.actorProc.Store(nil)
			return nil
		}
		if r.inlineState.Load() == inlinePromoted {
			if deferring {
				r.deferred = true
			}
			owner.mu.Unlock()
			scope.actorProc.Store(nil)
			return owner
		}
		id, err := r.node.allocateRequestID()
		if err != nil {
			owner.mu.Unlock()
			r.expireInline(err, Canceled)
			scope.actorProc.Store(nil)
			return nil
		}
		scope.actorOwner.Store(owner)
		if !r.inlineState.CompareAndSwap(inlineActive, inlinePromoted) {
			if r.inlineState.Load() != inlinePromoted {
				scope.actorOwner.Store(nil)
			}
			owner.mu.Unlock()
			scope.actorProc.Store(nil)
			continue
		}
		r.id = id
		if deferring {
			r.deferred = true
		}
		owner.pending[id] = r
		owner.mu.Unlock()
		scope.actorProc.Store(nil)
		return owner
	}
}

func (r *request) replyInline(pid PID, value any, err error, expired bool, origin CompletionOrigin) error {
	switch r.inlineState.Load() {
	case inlineReplied:
		return ErrAlreadyReplied
	case inlineExpired:
		return ErrReplyExpired
	case inlinePromoted:
		return r.ctx.(*requestScope).actorOwner.Load().replyWithOrigin(r, pid, value, err, expired, origin)
	}
	if r.owner != pid {
		return ErrReplyExpired
	}
	commitVal, commitErr, commitExpired, commitOrigin := value, err, expired, origin
	late := false
	if cause := r.contextErr(); cause != nil {
		late, commitExpired, commitVal, commitErr, commitOrigin = !expired, true, nil, cause, Canceled
	}
	target := inlineReplied
	if commitExpired {
		target = inlineExpired
	}
	if r.inlineState.CompareAndSwap(inlineActive, target) {
		r.value, r.err, r.completed, r.expired, r.origin = commitVal, commitErr, true, commitExpired, commitOrigin
		r.node.pendingCalls.Add(-1)
		r.settled.Store(true)
		r.finish()
		if late {
			return ErrReplyExpired
		}
		return nil
	}
	switch r.inlineState.Load() {
	case inlineReplied:
		return ErrAlreadyReplied
	case inlinePromoted:
		return r.ctx.(*requestScope).actorOwner.Load().replyWithOrigin(r, pid, value, err, expired, origin)
	default:
		return ErrReplyExpired
	}
}

func (r *request) expireInline(err error, origin CompletionOrigin) bool {
	switch r.inlineState.Load() {
	case inlineReplied, inlineExpired:
		return false
	case inlinePromoted:
		return r.ctx.(*requestScope).actorOwner.Load().expire(r, err)
	}
	if r.inlineState.CompareAndSwap(inlineActive, inlineExpired) {
		r.value, r.err, r.completed, r.expired, r.origin = nil, err, true, true, origin
		r.node.pendingCalls.Add(-1)
		r.settled.Store(true)
		r.finish()
		return true
	}
	if r.inlineState.Load() == inlinePromoted {
		return r.ctx.(*requestScope).actorOwner.Load().expire(r, err)
	}
	return false
}

func (p *process) failInlineRequestsLocked(reason error) []*request {
	var failed []*request
	failOne := func(r *request) {
		if r == nil || !r.inline || r.inlineState.Load() != inlineActive {
			return
		}
		origin := TargetExit
		err := reason
		if cause := r.contextErr(); cause != nil {
			err = cause
			origin = Canceled
		}
		if r.inlineState.CompareAndSwap(inlineActive, inlineExpired) {
			r.value, r.err, r.completed, r.expired, r.origin = nil, err, true, true, origin
			r.node.pendingCalls.Add(-1)
			r.settled.Store(true)
			failed = append(failed, r)
		}
	}
	if p.inFlightCall != nil {
		failOne(p.inFlightCall)
		p.inFlightCall = nil
	}
	for i := 0; i < p.mailbox.size; i++ {
		failOne(p.mailbox.at(i).request)
	}
	return failed
}

func (r *request) deferReply() {
	if r.inline {
		r.promoteInline(true)
		return
	}
	if owner := r.actorRequests(); owner != nil {
		owner.mu.Lock()
		r.deferred = true
		owner.mu.Unlock()
		return
	}
	r.node.mu.Lock()
	r.deferred = true
	r.node.mu.Unlock()
}

func (r *request) missingReply() bool {
	if r.inline {
		state := r.inlineState.Load()
		if state != inlinePromoted {
			return state == inlineActive
		}
	}
	if owner := r.actorRequests(); owner != nil {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return !r.completed && !r.deferred
	}
	r.node.mu.Lock()
	defer r.node.mu.Unlock()
	return !r.completed && !r.deferred
}

func (p *process) deferredRequests() []*request {
	var pending []*request
	p.node.mu.Lock()
	for _, r := range p.node.pending {
		if r.owner == p.pid && r.deferred {
			pending = append(pending, r)
		}
	}
	p.node.mu.Unlock()
	if owner := p.incomingRequests.Load(); owner != nil && owner != closedActorRequests {
		owner.mu.Lock()
		for _, r := range owner.pending {
			if r.deferred {
				pending = append(pending, r)
			}
		}
		owner.mu.Unlock()
	}
	return pending
}
