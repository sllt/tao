package actor

import "context"

// CompletionOrigin identifies how a BeginCall request settled.
type CompletionOrigin uint8

const (
	Reply CompletionOrigin = iota + 1
	NoReply
	Canceled
	TargetExit
)

func (o CompletionOrigin) String() string {
	switch o {
	case Reply:
		return "reply"
	case NoReply:
		return "no_reply"
	case Canceled:
		return "canceled"
	case TargetExit:
		return "target_exit"
	default:
		return "unknown"
	}
}

// CallCompletion carries the terminal result of a BeginCall request.
type CallCompletion struct {
	Origin CompletionOrigin
	Value  any
	Err    error
}

// CallHandle represents an admitted asynchronous local Call started via BeginCall.
type CallHandle struct {
	request *request
}

// Cancel competes to fail the request with Canceled. It returns true only if
// this cancellation won the single completion state of the request.
func (h *CallHandle) Cancel() bool {
	if h == nil || h.request == nil {
		return false
	}
	scope, ok := h.request.ctx.(*requestScope)
	if !ok || scope.actorOwner == nil {
		return false
	}
	return scope.actorOwner.cancel(h.request, scope)
}

type beginCallState struct {
	request   request
	scope     requestScope
	callbacks requestScopeCallbacks
	hook      requestCompletion
	handle    CallHandle
}

// BeginCall synchronously admits a Call into a local PID's mailbox without
// blocking for the handler's reply. If admission returns an error, the target
// did not receive the message and complete is never called. If BeginCall
// returns a non-nil handle, complete is invoked at most and at least once
// outside runtime locks—potentially before BeginCall itself returns.
func (n *Node) BeginCall(
	ctx context.Context,
	target PID,
	message any,
	complete func(CallCompletion),
) (*CallHandle, error) {
	if ctx == nil || complete == nil {
		return nil, ErrInvalidMessage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := n.resolvePID(target)
	if err != nil {
		return nil, err
	}
	state := &beginCallState{
		scope: newContextRequestScope(ctx),
	}
	r, scope := &state.request, &state.scope
	scope.callbacks = &state.callbacks
	state.handle.request = r
	state.hook.run = func(done *request) {
		complete(CallCompletion{
			Origin: done.origin,
			Value:  done.value,
			Err:    done.err,
		})
	}
	if err := p.registerActorRequest(r, scope, nil); err != nil {
		scope.finish()
		return nil, err
	}
	scope.startWatched()
	completedBeforeAdmission := false
	err = p.enqueueAdmitted(envelope{message: message, request: r}, func() error {
		completedBeforeAdmission = r.bindCompletion(&state.hook)
		return nil
	})
	if err != nil {
		n.expireRequest(r, err)
		scope.finish()
		return nil, err
	}
	if completedBeforeAdmission {
		state.hook.run(r)
	}
	return &state.handle, nil
}
