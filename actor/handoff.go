package actor

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/sllt/tao/taonet"
)

var ErrHandedOff = errors.New("actor: instance handed off")

type HandoffOptions struct {
	TargetNode string
	Version    string
}
type HandoffResult struct {
	MoveID         MoveID
	Source, Target RouteRecord
	State          MoveState
	Unknown        bool
}
type HandoffError struct {
	Result HandoffResult
	Cause  error
}

func (err *HandoffError) Error() string {
	return fmt.Sprintf("actor handoff %s to %s (state=%s unknown=%t): %v", err.Result.Source.Path, err.Result.Target.Owner, err.Result.State, err.Result.Unknown, err.Cause)
}
func (err *HandoffError) Unwrap() error { return err.Cause }

type handoffResolution struct {
	ctx    context.Context
	result chan handoffCompletion
}
type handoffCompletion struct {
	result HandoffResult
	err    error
}
type handoffOperation struct {
	ctx              context.Context
	options          HandoffOptions
	source           *process
	result           HandoffResult // source.mu
	err              error
	phase            replacePhase
	hookActive       bool
	done, uncertain  chan struct{}
	resolve          chan handoffResolution
	localCalls       map[uint64]*outgoingCall // coordinator only
	prepareAttempted bool
}

func (n *Node) Handoff(ctx context.Context, target Addr, options HandoffOptions) (HandoffResult, error) {
	if err := ctx.Err(); err != nil {
		return HandoffResult{}, err
	}
	if n.network == nil || n.opts.Directory == nil {
		return HandoffResult{}, ErrHandoffUnsupported
	}
	if !validSegment(options.TargetNode) || options.TargetNode == n.Name() || len(options.Version) > 128 {
		return HandoffResult{}, ErrInvalidMessage
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
		defer cancel()
	}
	p, err := n.replacementTarget(ctx, target)
	if err != nil {
		return HandoffResult{}, err
	}
	if !p.opts.handoff || p.opts.kind == "" || p.opts.version == "" {
		return HandoffResult{}, ErrHandoffUnsupported
	}
	p.childrenMu.Lock()
	leaf := len(p.children) == 0
	p.childrenMu.Unlock()
	n.mu.Lock()
	observations := n.watchCounts[p.pid]
	n.mu.Unlock()
	if !leaf || observations != 0 {
		return HandoffResult{}, ErrHandoffUnsupported
	}
	id, err := NewMoveID()
	if err != nil {
		return HandoffResult{}, err
	}
	op := &handoffOperation{ctx: ctx, options: options, source: p, result: HandoffResult{MoveID: id, Target: RouteRecord{Owner: options.TargetNode}}, done: make(chan struct{}), uncertain: make(chan struct{}), resolve: make(chan handoffResolution), localCalls: make(map[uint64]*outgoingCall)}
	w := n.network
	w.mu.Lock()
	p.mu.Lock()
	if p.change != nil || p.handoff != nil {
		p.mu.Unlock()
		w.mu.Unlock()
		return HandoffResult{}, ErrMigrating
	}
	if p.stopRequested || p.finished || w.closed {
		p.mu.Unlock()
		w.mu.Unlock()
		return HandoffResult{}, ErrStopped
	}
	if len(p.asyncRequests) != 0 {
		p.mu.Unlock()
		w.mu.Unlock()
		return HandoffResult{}, ErrAsyncPending
	}
	op.result.Source = p.ownership
	p.handoff = op
	p.interruptBatchLocked()
	w.outgoingHandoffs[id] = op
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	w.mu.Unlock()
	if start {
		p.schedule()
	}
	select {
	case <-op.done:
		return op.result, op.err
	case <-op.uncertain:
		p.mu.Lock()
		result, cause := op.result, op.err
		p.mu.Unlock()
		return result, &HandoffError{Result: result, Cause: errors.Join(ErrOutcomeUnknown, cause)}
	case <-ctx.Done():
		p.mu.Lock()
		result, phase := op.result, op.phase
		blocked := phase == replaceSnapshot && op.hookActive
		if blocked {
			p.stopRequested, p.suppressRestart, p.stopReason = true, true, ctx.Err()
			p.interruptBatchLocked()
		}
		p.mu.Unlock()
		select {
		case <-op.done:
			return op.result, op.err
		default:
		}
		cause := ctx.Err()
		if blocked {
			p.cancel(cause)
			n.failRequests(p, cause)
			n.markUnresponsive(p)
			cause = errors.Join(ErrUnresponsive, cause)
		}
		if phase == replaceCommitting || phase == replaceUnknown {
			result.Unknown = true
			cause = errors.Join(ErrOutcomeUnknown, cause)
		}
		return result, &HandoffError{Result: result, Cause: cause}
	}
}

func (n *Node) ResolveHandoff(ctx context.Context, id MoveID) (HandoffResult, error) {
	if n.opts.Directory == nil || n.network == nil {
		return HandoffResult{}, ErrHandoffUnsupported
	}
	w := n.network
	w.mu.Lock()
	op := w.outgoingHandoffs[id]
	w.mu.Unlock()
	if op != nil {
		request := handoffResolution{ctx: ctx, result: make(chan handoffCompletion, 1)}
		select {
		case op.resolve <- request:
		case <-op.done:
			return op.result, op.err
		case <-ctx.Done():
			return HandoffResult{MoveID: id, Unknown: true}, ctx.Err()
		}
		select {
		case result := <-request.result:
			return result.result, result.err
		case <-ctx.Done():
			return HandoffResult{MoveID: id, Unknown: true}, ctx.Err()
		}
	}
	move, err := n.opts.Directory.ReadMove(ctx, id)
	if err != nil {
		return HandoffResult{MoveID: id, Unknown: true}, err
	}
	result := HandoffResult{MoveID: id, Source: move.Source, Target: move.Target, State: move.State}
	if move.State == MoveCommitted {
		packet := handoffControl{Kind: "activate", Move: id, Source: move.Source}
		if move.Target.Owner == n.Name() && move.Target.BootID == n.BootID() {
			_, err = w.activateHandoff(ctx, packet)
		} else if move.Source.Owner == n.Name() && move.Source.BootID == n.BootID() {
			_, err = w.handoffRequest(ctx, move.Target.Owner, packet)
		} else {
			err = ErrHandoffUnsupported
		}
	}
	return result, err
}

func (p *process) finishHandoff(op *handoffOperation, err error) {
	p.mu.Lock()
	if err != nil {
		err = &HandoffError{Result: op.result, Cause: err}
	}
	op.err, op.phase = err, replaceDone
	p.handoff, p.sealed = nil, false
	close(op.done)
	p.mu.Unlock()
	w := p.node.network
	w.mu.Lock()
	delete(w.outgoingHandoffs, op.result.MoveID)
	w.mu.Unlock()
}
func (p *process) handoffPhase(op *handoffOperation, phase replacePhase) {
	p.mu.Lock()
	op.phase = phase
	p.mu.Unlock()
}

func (p *process) runHandoff(op *handoffOperation) bool {
	ctx, cancel := context.WithCancel(op.ctx)
	defer cancel()
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()
	w, d := p.node.network, p.node.opts.Directory
	p.childrenMu.Lock()
	leaf := len(p.children) == 0
	p.childrenMu.Unlock()
	p.node.mu.Lock()
	observations := p.node.watchCounts[p.pid]
	p.node.mu.Unlock()
	if !leaf || observations != 0 {
		p.finishHandoff(op, ErrHandoffUnsupported)
		return false
	}
	packet := handoffControl{Kind: "reserve", Move: op.result.MoveID, Source: op.result.Source, Version: op.options.Version, Mailbox: p.opts.mailbox, Restart: p.opts.restart, MailPolicy: p.opts.mailPolicy}
	reserved, err := w.handoffRequest(ctx, op.options.TargetNode, packet)
	if err == nil {
		if reserved.Target.Validate() != nil || reserved.Target.Path != p.path || reserved.Target.Owner != op.options.TargetNode {
			err = ErrInvalidMessage
		} else {
			p.mu.Lock()
			op.result.Target = reserved.Target
			p.mu.Unlock()
		}
	}
	if err == nil {
		var move MoveRecord
		op.prepareAttempted = true
		move, err = d.PrepareMove(ctx, op.result.Source, op.result.Target, op.result.MoveID)
		if err == nil {
			p.mu.Lock()
			op.result.State, op.result.Target = move.State, move.Target
			p.mu.Unlock()
		}
	}
	if err == nil {
		err = p.waitHandoffDeferred(ctx)
	}
	var snapshot []byte
	if err == nil {
		p.mu.Lock()
		op.phase, op.hookActive = replaceSnapshot, true
		p.mu.Unlock()
		p.snapshot = &snapshotCapture{}
		err = p.invoke(envelope{message: Snapshotting{}})
		capture := p.snapshot
		p.snapshot = nil
		p.mu.Lock()
		op.hookActive = false
		p.mu.Unlock()
		if err == nil {
			err = capture.err
		}
		if err == nil && !capture.called {
			err = ErrNoReply
		}
		snapshot = capture.data
	}
	if err == nil {
		p.handoffPhase(op, replacePreparing)
		err = p.transferHandoff(ctx, op, snapshot)
	}
	if err == nil {
		p.handoffPhase(op, replaceCommitting)
		var move MoveRecord
		move, err = d.CommitMove(ctx, op.result.MoveID)
		if err == nil {
			p.mu.Lock()
			op.result.State, op.result.Target = move.State, move.Target
			p.mu.Unlock()
		}
	}
	completionCtx := op.ctx
	var resolution *handoffResolution
	if err != nil {
		cause := err
		settleCtx, settleCancel := context.WithTimeout(context.Background(), p.node.opts.CallTimeout)
		known, settleErr := p.settleHandoff(settleCtx, op)
		settleCancel()
		for !known {
			p.mu.Lock()
			op.phase, op.result.Unknown, op.err = replaceUnknown, true, errors.Join(cause, settleErr)
			select {
			case <-op.uncertain:
			default:
				close(op.uncertain)
			}
			p.mu.Unlock()
			select {
			case request := <-op.resolve:
				known, settleErr = p.settleHandoff(request.ctx, op)
				if known {
					resolution = &request
					completionCtx = request.ctx
				} else {
					p.mu.Lock()
					result := op.result
					p.mu.Unlock()
					request.result <- handoffCompletion{result, &HandoffError{Result: result, Cause: errors.Join(ErrOutcomeUnknown, settleErr)}}
				}
			case <-p.ctx.Done():
				p.node.markUnresponsive(p)
				p.finishHandoff(op, errors.Join(ErrOutcomeUnknown, p.ctx.Err()))
				return false
			}
		}
		if op.result.State != MoveCommitted {
			p.removeHandoffCalls(op)
			cleanupCtx, cleanupCancel := context.WithTimeout(p.ctx, p.node.opts.CallTimeout)
			packet.Kind = "abort"
			_, cleanupErr := w.handoffRequest(cleanupCtx, op.options.TargetNode, packet)
			cleanupCancel()
			p.finishHandoff(op, errors.Join(cause, cleanupErr))
			if resolution != nil {
				resolution.result <- handoffCompletion{op.result, op.err}
			}
			return false
		}
	}
	p.mu.Lock()
	op.phase, op.result.Unknown = replaceCommitted, false
	p.mu.Unlock()
	p.commitHandoffMailbox(op)
	activation := make(chan error, 1)
	packet.Kind = "activate"
	go func() { _, err := w.handoffRequest(completionCtx, op.options.TargetNode, packet); activation <- err }()
	stop()
	p.requestStop(ErrHandedOff, true)
	p.finish()
	select {
	case err = <-activation:
	case <-completionCtx.Done():
		err = completionCtx.Err()
	}
	p.finishHandoff(op, err)
	if resolution != nil {
		resolution.result <- handoffCompletion{op.result, op.err}
	}
	return true
}

func (p *process) waitHandoffDeferred(ctx context.Context) error {
	for _, r := range p.deferredRequests() {
		select {
		case <-r.waitDone():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return ctx.Err()
}

// Settle a failed/uncertain operation by winning an Abort CAS or observing a
// terminal receipt. Reading Preparing (or absence) alone is not a rollback fence.
func (p *process) settleHandoff(ctx context.Context, op *handoffOperation) (bool, error) {
	d := p.node.opts.Directory
	move, err := d.ReadMove(ctx, op.result.MoveID)
	if errors.Is(err, ErrDirectoryNotFound) {
		if !op.prepareAttempted {
			p.mu.Lock()
			op.result.State, op.result.Unknown = MoveAborted, false
			p.mu.Unlock()
			return true, nil
		}
		current, readErr := d.Lookup(ctx, p.path)
		if readErr != nil {
			return false, readErr
		}
		if current == op.result.Source {
			current, err = d.SwapInstance(ctx, current, current)
			if err != nil {
				return false, err
			}
		}
		if current.PID() != p.pid || current.State != RouteActive {
			return false, ErrDirectoryConflict
		}
		p.mu.Lock()
		p.ownership = current
		op.result.State, op.result.Unknown = MoveAborted, false
		p.mu.Unlock()
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if move.Source != op.result.Source {
		return false, ErrDirectoryConflict
	}
	if move.State == MovePreparing {
		move, err = d.AbortMove(ctx, move.ID)
		if err != nil && !errors.Is(err, ErrMoveCommitted) {
			return false, err
		}
	}
	if move.State == MoveAborted {
		current, err := d.Lookup(ctx, p.path)
		if err != nil {
			return false, err
		}
		if current.PID() != p.pid || current.State != RouteActive {
			return false, ErrDirectoryConflict
		}
		p.mu.Lock()
		p.ownership = current
		p.mu.Unlock()
	}
	if move.State != MoveCommitted && move.State != MoveAborted {
		return false, ErrDirectoryConflict
	}
	p.mu.Lock()
	op.result.State, op.result.Target, op.result.Unknown = move.State, move.Target, false
	p.mu.Unlock()
	return true, nil
}

func (p *process) copyGlobalMailbox() []envelope {
	p.mu.Lock()
	defer p.mu.Unlock()
	var messages []envelope
	for i := 0; i < p.mailbox.size; i++ {
		env := p.mailbox.at(i)
		if env.global != nil {
			messages = append(messages, env)
		}
	}
	return messages
}

func (p *process) transferHandoff(ctx context.Context, op *handoffOperation, snapshot []byte) error {
	w := p.node.network
	session, err := w.connection(ctx, op.options.TargetNode, taonet.ClassBulk)
	if err != nil {
		return err
	}
	if [16]byte(session.Info().Remote.Instance) != op.result.Target.BootID {
		return ErrStalePID
	}
	stream, err := session.OpenStream(ctx, handoffBulkRoute, nil)
	if err != nil {
		return err
	}
	defer stream.Close()
	digest := sha256.New()
	payload, err := writeHandoffFrame(stream, handoffFrame{Kind: "begin", Move: op.result.MoveID, Snapshot: snapshot})
	if err != nil {
		return err
	}
	_, _ = digest.Write(payload)
	var sequence uint64
	send := func(messages []envelope) error {
		for _, env := range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			sequence++
			frame, err := p.handoffMessage(ctx, op, env, sequence)
			if err != nil {
				return err
			}
			payload, err := writeHandoffFrame(stream, frame)
			if err != nil {
				return err
			}
			_, _ = digest.Write(payload)
		}
		return nil
	}
	initial := p.copyGlobalMailbox()
	if err := send(initial); err != nil {
		return err
	}
	p.mu.Lock()
	p.sealed = true
	p.mu.Unlock()
	all := p.copyGlobalMailbox()
	if err := send(all[len(initial):]); err != nil {
		return err
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	if _, err := writeHandoffFrame(stream, handoffFrame{Kind: "barrier", Sequence: sequence, Digest: sum}); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	ack, _, err := readHandoffFrame(stream, 8192)
	if err != nil {
		return err
	}
	if ack.Kind != "prepared" || ack.Move != op.result.MoveID || ack.Sequence != sequence || ack.Digest != sum {
		return ErrInvalidMessage
	}
	_, err = io.Copy(io.Discard, stream)
	return err
}

func (p *process) handoffMessage(ctx context.Context, op *handoffOperation, env envelope, sequence uint64) (handoffFrame, error) {
	frame := handoffFrame{Kind: "message", Sequence: sequence}
	message := handoffEnvelope{}
	if env.sender != nil {
		sender, ok := env.sender.(PID)
		if !ok {
			return frame, ErrInvalidMessage
		}
		message.Sender = &sender
	}
	if r := env.request; r != nil {
		p.node.mu.Lock()
		completed := r.completed
		key := r.remoteKey()
		p.node.mu.Unlock()
		if completed || r.contextErr() != nil {
			return handoffFrame{Kind: "skip", Sequence: sequence}, nil
		}
		deadline, _ := r.deadline()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return handoffFrame{Kind: "skip", Sequence: sequence}, nil
		}
		message.BudgetMS = uint64(remaining / time.Millisecond)
		if remaining%time.Millisecond != 0 {
			message.BudgetMS++
		}
		if key.ID == 0 {
			key = requestKey{p.node.Name(), p.node.BootID(), r.id}
			if err := p.installHandoffCall(op, r); err != nil {
				return frame, err
			}
		}
		message.Call = key
		if err := p.connectHandoffCaller(ctx, op, key); err != nil {
			return frame, err
		}
	}
	encoded, err := p.node.network.registry.Encode(env.message)
	if err != nil {
		return frame, err
	}
	message.Message = encoded
	frame.Envelope = &message
	return frame, nil
}

func (p *process) installHandoffCall(op *handoffOperation, r *request) error {
	w := p.node.network
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if _, exists := op.localCalls[r.id]; exists {
		return nil
	}
	out := &outgoingCall{request: r, peer: p.node.Name(), boot: p.node.BootID(), issued: true, local: true, routing: &outgoingRoute{route: op.result.Source, move: op.result.MoveID, moveTarget: op.result.Target}}
	w.outgoing[r.id] = out
	op.localCalls[r.id] = out
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		<-r.waitDone()
		if r.expired {
			w.cancelRelocated(out, requestKey{p.node.Name(), p.node.BootID(), r.id})
		}
		w.mu.Lock()
		if w.outgoing[r.id] == out {
			delete(w.outgoing, r.id)
		}
		w.mu.Unlock()
	}()
	return nil
}
func (p *process) removeHandoffCalls(op *handoffOperation) {
	w := p.node.network
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, out := range op.localCalls {
		if w.outgoing[id] == out {
			delete(w.outgoing, id)
		}
	}
}

func (p *process) commitHandoffMailbox(op *handoffOperation) {
	w, n := p.node.network, p.node
	w.mu.Lock()
	n.mu.Lock()
	p.mu.Lock()
	for _, out := range op.localCalls {
		out.peer, out.boot, out.local = op.result.Target.Owner, op.result.Target.BootID, false
		out.request.relocated = true
	}
	var forwarded []*request
	var releases []func()
	remaining := mailbox{limit: p.mailbox.limit}
	for p.mailbox.size > 0 {
		env := p.mailbox.pop()
		if env.global == nil {
			_ = remaining.push(env)
			continue
		}
		if env.release != nil {
			releases = append(releases, env.release)
		}
		if env.request != nil && env.request.remoteKey().ID != 0 {
			env.request.forwarded.Store(true)
			forwarded = append(forwarded, env.request)
		}
	}
	p.mailbox = remaining
	p.ownership = RouteRecord{}
	n.publishPIDLocked(p, false)
	n.routes[p.path] = route{p, ErrMigrating}
	p.mu.Unlock()
	n.mu.Unlock()
	w.mu.Unlock()
	for _, release := range releases {
		release()
	}
	for _, r := range forwarded {
		n.expireRequest(r, ErrHandedOff)
	}
}
