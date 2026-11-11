package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Calls managed by this scope own a deadline but need not expose a Go context.
// Keep only the completion timer on that path. Context() materializes
// a standard context and transfers deadline notification to its AfterFunc, so
// children, values, Cause and cancellation retain standard-library behavior.
type requestScope struct {
	actorOwner   atomic.Pointer[actorRequests] // ordinary PID completion domain; node is available through request
	actorProc    atomic.Pointer[process]       // target process for lazy inline promotion; cleared on promotion or finish
	request      *request
	outgoing     *outgoingCall // immutable before start; nil for local/incoming Calls
	parent       context.Context
	deadline     time.Time
	hasDeadline  bool
	callerTimer  bool // synchronous local PID caller reclaims the timer after receiving its result
	mu           sync.Mutex
	reason       atomic.Uint32 // 0 active, 1 canceled, 2 deadline exceeded
	materialized atomic.Pointer[requestScopeCallbacks]
	timer        *requestTimer
	callbacks    *requestScopeCallbacks // mu; allocated only for a public Context, watched parent, or custom cause
	canceled     chan struct{}          // mu; private transport waiter, independent of public Context
}

type requestScopeCallbacks struct {
	ctx           context.Context // immutable after materialized publishes this object
	cause         error           // mu; non-standard completion cause
	cancelContext context.CancelFunc
	cancelCause   context.CancelCauseFunc
	stopParent    func() bool
	stopContext   func() bool
}

func (s *requestScope) callbacksLocked() *requestScopeCallbacks {
	if s.callbacks == nil {
		s.callbacks = &requestScopeCallbacks{}
	}
	return s.callbacks
}

func newRequestScope(parent context.Context, deadline time.Time) *requestScope {
	if earlier, ok := parent.Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	return &requestScope{parent: parent, deadline: deadline, hasDeadline: true}
}

func newContextRequestScope(parent context.Context) requestScope {
	deadline, hasDeadline := parent.Deadline()
	return requestScope{parent: parent, deadline: deadline, hasDeadline: hasDeadline}
}

// The private request slot can hold a scope without growing every local and
// outgoing request. Public Actor.Context returns context(), a standard context.
func (s *requestScope) Deadline() (time.Time, bool) {
	if !s.hasDeadline {
		return time.Time{}, false
	}
	return s.deadline, true
}
func (s *requestScope) Done() <-chan struct{} { return s.context().Done() }
func (s *requestScope) Err() error            { return s.err() }
func (s *requestScope) Value(key any) any     { return s.context().Value(key) }

// Transport waiters need cancellation, not the full public context tree. Only
// callers using the scope's own timer use this signal; caller-supplied contexts
// continue to propagate through their standard Done channel.
func (s *requestScope) waitCanceled() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reason.Load() != 0 {
		return completedRequestSignal
	}
	if s.canceled == nil {
		s.canceled = make(chan struct{})
	}
	return s.canceled
}

func (s *requestScope) err() error {
	switch s.reason.Load() {
	case 1:
		return context.Canceled
	case 2:
		return context.DeadlineExceeded
	}
	if ctx := s.materialized.Load(); ctx != nil {
		return ctx.ctx.Err()
	}
	if s.parent != context.Background() && s.parent != context.TODO() {
		if err := s.parent.Err(); err != nil {
			return err
		}
	}
	if s.hasDeadline && time.Until(s.deadline) <= 0 {
		return context.DeadlineExceeded
	}
	return nil
}

func (s *requestScope) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hasDeadline && s.reason.Load() == 0 && s.materialized.Load() == nil {
		s.timer = s.request.node.startRequestTimer(s, time.Until(s.deadline))
	}
}

func (s *requestScope) startWatched() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reason.Load() != 0 || s.materialized.Load() != nil {
		return
	}
	if s.hasDeadline {
		s.timer = s.request.node.startRequestTimer(s, time.Until(s.deadline))
	}
	if s.parent.Done() != nil {
		s.callbacksLocked().stopParent = context.AfterFunc(s.parent, s.parentDone)
	}
}

func (s *requestScope) complete(reason error, timerSource bool) bool {
	s.mu.Lock()
	if s.reason.Load() != 0 || timerSource && (!s.hasDeadline || s.materialized.Load() != nil) {
		s.mu.Unlock()
		return false
	}
	cause := reason
	if ctx := s.materialized.Load(); ctx != nil {
		if err := ctx.ctx.Err(); err != nil {
			reason = err
			cause = context.Cause(ctx.ctx)
		}
	} else if s.parent != context.Background() && s.parent != context.TODO() {
		if err := s.parent.Err(); err != nil {
			reason = err
			cause = context.Cause(s.parent)
		}
	}
	state := uint32(1)
	if reason == context.DeadlineExceeded {
		state = 2
	}
	s.reason.Store(state)
	if (cause != context.Canceled && cause != context.DeadlineExceeded) || s.callbacks != nil {
		s.callbacksLocked().cause = cause
	}
	if s.canceled != nil {
		close(s.canceled)
	}
	if s.timer != nil && !s.callerTimer {
		s.request.node.stopRequestTimer(s.timer)
		s.timer = nil
	}
	if s.callbacks == nil {
		s.mu.Unlock()
		return true
	}
	callbacks := *s.callbacks
	s.callbacks.stopParent, s.callbacks.stopContext = nil, nil
	s.callbacks.cancelContext, s.callbacks.cancelCause = nil, nil
	s.mu.Unlock()
	if callbacks.stopParent != nil {
		callbacks.stopParent()
	}
	if callbacks.stopContext != nil {
		callbacks.stopContext()
	}
	if callbacks.cancelCause != nil {
		callbacks.cancelCause(cause)
	}
	if callbacks.cancelContext != nil {
		callbacks.cancelContext()
	}
	return true
}

// The caller owns this cleanup even when Reply/Stop is still returning from
// finish. Public Context may already have taken over the deadline under mu.
func (s *requestScope) releaseCallerTimer() {
	s.mu.Lock()
	if s.timer != nil {
		s.request.node.stopRequestTimer(s.timer)
		s.timer = nil
	}
	s.mu.Unlock()
}

func (s *requestScope) timeout() {
	if s.complete(context.DeadlineExceeded, true) {
		s.expire()
	}
}

func (s *requestScope) parentDone() {
	if s.complete(s.parent.Err(), false) {
		s.expire()
	}
}

func (s *requestScope) contextDone() {
	ctx := s.materialized.Load()
	if s.complete(ctx.ctx.Err(), false) {
		s.expire()
	}
}

func (s *requestScope) expire() {
	if s.outgoing != nil {
		// Keep the outgoing record itself: completion may already have
		// removed its map entry before cancellation routing runs.
		node := s.request.node
		key := requestKey{node.Name(), node.BootID(), s.request.id}
		_, _ = node.network.cancelCall(s, s.outgoing, key)
		return
	}
	s.request.node.expireRequest(s.request, s.err())
}

func (s *requestScope) causeLocked(state uint32) error {
	if s.callbacks != nil && s.callbacks.cause != nil {
		return s.callbacks.cause
	}
	switch state {
	case 1:
		return context.Canceled
	case 2:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (s *requestScope) finish() {
	if s.actorProc.Load() != nil {
		s.actorProc.Store(nil)
	}
	reason := context.Canceled
	if r := s.request; r == nil || !r.settled.Load() || r.expired {
		if err := s.err(); err != nil {
			reason = err
		}
	}
	s.complete(reason, false)
}

func (s *requestScope) cancel() {
	reason := s.err()
	if reason == nil {
		reason = context.Canceled
	}
	if s.complete(reason, false) {
		s.expire()
	}
}

func (s *requestScope) context() context.Context {
	if ctx := s.materialized.Load(); ctx != nil {
		return ctx.ctx
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx := s.materialized.Load(); ctx != nil {
		return ctx.ctx
	}
	parent := s.parent
	state := s.reason.Load()
	cause := s.causeLocked(state)
	callbacks := s.callbacksLocked()
	var ctx context.Context
	if !s.hasDeadline {
		if state != 0 {
			parent = context.WithoutCancel(parent)
			var cancelCause context.CancelCauseFunc
			ctx, cancelCause = context.WithCancelCause(parent)
			cancelCause(cause)
		} else {
			ctx, callbacks.cancelCause = context.WithCancelCause(parent)
		}
	} else {
		if state != 0 {
			// A later node cancellation must not replace this request's already
			// committed reason. Values remain available after completion.
			parent = context.WithoutCancel(parent)
			if state == 1 {
				var cancel context.CancelCauseFunc
				parent, cancel = context.WithCancelCause(parent)
				cancel(cause)
			}
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadlineCause(parent, s.deadline, cause)
		callbacks.cancelContext = cancel
	}
	callbacks.ctx = ctx
	s.materialized.Store(callbacks)
	if s.timer != nil {
		s.request.node.stopRequestTimer(s.timer)
		s.timer = nil
	}
	if callbacks.stopParent != nil {
		callbacks.stopParent()
		callbacks.stopParent = nil
	}
	if state == 0 {
		callbacks.stopContext = context.AfterFunc(ctx, s.contextDone)
	}
	return ctx
}
