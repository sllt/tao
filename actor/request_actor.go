package actor

import (
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
	r.remoteKey = key
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
	scope.actorOwner = owner
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

func (owner *actorRequests) commitLocked(r *request, value any, err error, expired bool) {
	r.value, r.err, r.completed, r.expired = value, err, true, expired
	delete(owner.pending, r.id)
	r.node.pendingCalls.Add(-1)
	r.signalDoneLocked()
}

func (owner *actorRequests) reply(r *request, pid PID, value any, err error, expired bool) error {
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
		late, expired, value, err = !expired, true, nil, cause
	}
	owner.commitLocked(r, value, err, expired)
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
	owner.commitLocked(r, nil, err, true)
	owner.mu.Unlock()
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
		err := reason
		if cause := r.contextErr(); cause != nil {
			err = cause
		}
		owner.commitLocked(r, nil, err, true)
		failed = append(failed, r)
	}
	return failed
}

func (r *request) deferReply() {
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
