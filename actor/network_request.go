package actor

import (
	"context"
	"time"

	"github.com/sllt/tao/taonet"
)

// A direct remote PID has no local owner or relocation rights. Its request,
// result and cancellation route live only in outgoing under network.mu.
func (w *network) registerPIDRequest(state *outgoingRequestState, ctx context.Context, cancel context.CancelFunc, owner, sender PID) (*outgoingCall, error) {
	n := w.node
	r, out := &state.request, &state.outgoing
	signal := n.takeCallSignal()
	*r = request{node: n, owner: owner, ctx: ctx, cancel: cancel, callerDone: signal, kind: requestRemotePID}
	*out = outgoingCall{request: r, peer: owner.Node}
	if scope, ok := ctx.(*requestScope); ok {
		scope.request, scope.outgoing = r, out
	}
	if n.diagnostics.Load() {
		r.trace = &requestTrace{caller: sender, target: owner.String(), started: time.Now()}
	}
	w.mu.Lock()
	var err error
	if w.closed || n.stopping.Load() {
		err = ErrClosed
	} else {
		r.id, err = n.reserveRequest()
	}
	if err == nil {
		if r.trace == nil && n.diagnostics.Load() {
			r.trace = &requestTrace{caller: sender, target: owner.String(), started: time.Now()}
		}
		w.outgoing[r.id] = out
	}
	w.mu.Unlock()
	if err != nil {
		r.cancelContext()
		n.recycleCallSignal(signal)
		return nil, err
	}
	return out, nil
}

func (w *network) callPIDOwned(ctx context.Context, cancel context.CancelFunc, target PID, message any, sender PID, state *defaultCallState) (any, error) {
	var storage *outgoingRequestState
	if state != nil {
		storage = &state.outgoingRequestState
	} else {
		storage = new(outgoingRequestState)
	}
	out, err := w.registerPIDRequest(storage, ctx, cancel, target, sender)
	if err != nil {
		return nil, deliveryError(err, NotSent)
	}
	r := out.request
	defer w.node.recycleCallSignal(r.callerDone)
	scope, managed := ctx.(*requestScope)
	if managed {
		scope.start()
	}
	packet := actorPacket{Kind: "call", Target: wireAddress{Kind: "pid", PID: &r.owner}, Key: requestKey{w.node.Name(), w.node.BootID(), r.id}}
	if err := w.queueMessage(target.Node, taonet.ClassInteractive, packet, message, sender, out, nil); err != nil {
		// Expiration may already have won while send admission was blocked.
		w.expireRemote(r, deliveryError(err, NotSent))
		return r.value, r.err
	}
	if managed {
		<-r.callerDone
	} else {
		select {
		case <-r.callerDone:
		case <-ctx.Done():
			return w.cancelRemoteCall(ctx, out, packet.Key)
		}
	}
	return r.value, r.err
}

// Caller holds network.mu and has won completion of an active remotePID
// request. Publish the outcome, retire its one index/quota, then signal once.
func (w *network) commitRemoteLocked(r *request, value any, err error, expired bool) {
	if err != nil {
		outcome := NotSent
		if w.outgoing[r.id].issued {
			outcome = Unknown
		}
		err = deliveryError(err, outcome)
		value = nil
	}
	r.value, r.err, r.completed, r.expired = value, err, true, expired
	delete(w.outgoing, r.id)
	w.node.pendingCalls.Add(-1)
	r.signalDoneLocked()
}

func (w *network) replyRemote(r *request, owner PID, value any, err error, expired bool) error {
	w.mu.Lock()
	if r.completed {
		wasExpired := r.expired
		w.mu.Unlock()
		if wasExpired {
			return ErrReplyExpired
		}
		return ErrAlreadyReplied
	}
	if r.owner != owner {
		w.mu.Unlock()
		return ErrReplyExpired
	}
	late := false
	if cause := r.contextErr(); cause != nil {
		late, expired, value, err = !expired, true, nil, cause
	}
	w.commitRemoteLocked(r, value, err, expired)
	w.mu.Unlock()
	r.cancelContext() // direct PID requests never bind a local mailbox completion hook
	if late {
		return ErrReplyExpired
	}
	return nil
}

func (w *network) expireRemote(r *request, err error) bool {
	w.mu.Lock()
	if r.completed {
		w.mu.Unlock()
		return false
	}
	w.commitRemoteLocked(r, nil, err, true)
	w.mu.Unlock()
	r.cancelContext()
	return true
}

func (w *network) cancelRemoteCall(ctx context.Context, out *outgoingCall, key requestKey) (any, error) {
	r := out.request
	w.mu.Lock()
	if r.completed {
		w.mu.Unlock()
		return r.value, r.err
	}
	out.canceled = true
	issued, scope, boot, sequence, peer := out.issued, out.scope, out.boot, out.sequence, out.peer
	w.commitRemoteLocked(r, nil, ctx.Err(), true)
	w.mu.Unlock()
	r.cancelContext()
	if issued && scope.Channel != 0 {
		w.queueControl(peer, actorPacket{Kind: "cancel", Key: key, Scope: scope, Sequence: sequence}, boot)
	}
	return r.value, r.err
}
