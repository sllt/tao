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
	actorOwner    *actorRequests // ordinary PID completion domain; node is available through request
	request       *request
	outgoing      *outgoingCall // immutable before start; nil for local/incoming Calls
	parent        context.Context
	deadline      time.Time
	mu            sync.Mutex
	reason        atomic.Uint32 // 0 active, 1 canceled, 2 deadline exceeded
	materialized  atomic.Pointer[context.Context]
	timer         *requestTimer
	cancelContext context.CancelFunc
	stopContext   func() bool
	canceled      chan struct{} // mu; private transport waiter, independent of public Context
	cause         error         // mu; first completion's context cause
}

func newRequestScope(parent context.Context, deadline time.Time) *requestScope {
	if earlier, ok := parent.Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	return &requestScope{parent: parent, deadline: deadline}
}

// The private request slot can hold a scope without growing every local and
// outgoing request. Public Actor.Context returns context(), a standard context.
func (s *requestScope) Deadline() (time.Time, bool) { return s.deadline, true }
func (s *requestScope) Done() <-chan struct{}       { return s.context().Done() }
func (s *requestScope) Err() error                  { return s.err() }
func (s *requestScope) Value(key any) any           { return s.context().Value(key) }

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
		return (*ctx).Err()
	}
	if err := s.parent.Err(); err != nil {
		return err
	}
	if time.Until(s.deadline) <= 0 {
		return context.DeadlineExceeded
	}
	return nil
}

func (s *requestScope) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reason.Load() == 0 && s.materialized.Load() == nil {
		s.timer = s.request.node.startRequestTimer(s, time.Until(s.deadline))
	}
}

func (s *requestScope) complete(reason error, timerSource bool) bool {
	s.mu.Lock()
	if s.reason.Load() != 0 || timerSource && s.materialized.Load() != nil {
		s.mu.Unlock()
		return false
	}
	cause := reason
	if ctx := s.materialized.Load(); ctx != nil {
		if err := (*ctx).Err(); err != nil {
			reason = err
			cause = context.Cause(*ctx)
		}
	} else if err := s.parent.Err(); err != nil {
		reason = err
		cause = context.Cause(s.parent)
	}
	state := uint32(1)
	if reason == context.DeadlineExceeded {
		state = 2
	}
	s.reason.Store(state)
	s.cause = cause
	if s.canceled != nil {
		close(s.canceled)
	}
	if s.timer != nil {
		s.request.node.stopRequestTimer(s.timer)
		s.timer = nil
	}
	stop, cancel := s.stopContext, s.cancelContext
	s.stopContext = nil
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	if cancel != nil {
		cancel()
	}
	return true
}

func (s *requestScope) timeout() {
	if s.complete(context.DeadlineExceeded, true) {
		s.expire()
	}
}

func (s *requestScope) contextDone() {
	ctx := s.materialized.Load()
	if s.complete((*ctx).Err(), false) {
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

func (s *requestScope) finish() {
	reason := s.err()
	if reason == nil {
		reason = context.Canceled
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
		return *ctx
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx := s.materialized.Load(); ctx != nil {
		return *ctx
	}
	parent := s.parent
	state := s.reason.Load()
	if state != 0 {
		// A later node cancellation must not replace this request's already
		// committed reason. Values remain available after completion.
		parent = context.WithoutCancel(parent)
		if state == 1 {
			var cancel context.CancelCauseFunc
			parent, cancel = context.WithCancelCause(parent)
			cancel(s.cause)
		}
	}
	ctx, cancel := context.WithDeadlineCause(parent, s.deadline, s.cause)
	s.cancelContext = cancel
	s.materialized.Store(&ctx)
	if s.timer != nil {
		s.request.node.stopRequestTimer(s.timer)
		s.timer = nil
	}
	if state == 0 {
		s.stopContext = context.AfterFunc(ctx, s.contextDone)
	}
	return ctx
}
