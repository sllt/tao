package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrMigrating          = errors.New("actor: state transfer in progress")
	ErrReplaced           = errors.New("actor: instance replaced")
	ErrHandoffUnsupported = errors.New("actor: state transfer unsupported for this actor or target kind")
	ErrOutcomeUnknown     = errors.New("actor: ownership outcome unknown")
	ErrRestoring          = errors.New("actor: external actions are unavailable during state preparation")
)

type Snapshotting struct{}
type Upgraded struct {
	FromVersion string
	rawSnapshot []byte
}

func (message Upgraded) Restore(target any) error { return json.Unmarshal(message.rawSnapshot, target) }

type Activated struct{}

// ReplaceError identifies the side of the commit boundary. Unknown requires
// ResolveReplace; neither instance is allowed to execute until it is settled.
type ReplaceError struct {
	PID                PID
	Committed, Unknown bool
	Cause              error
}

func (err *ReplaceError) Error() string {
	return fmt.Sprintf("actor replace %s (committed=%t unknown=%t): %v", err.PID, err.Committed, err.Unknown, err.Cause)
}
func (err *ReplaceError) Unwrap() error { return err.Cause }

type replacePhase uint8

const (
	replaceWaiting replacePhase = iota
	replaceSnapshot
	replacePreparing
	replaceCommitting
	replaceUnknown
	replaceCommitted
	replaceDone
)

type replacementResolution struct {
	ctx    context.Context
	result chan error
}
type replaceOperation struct {
	ctx        context.Context
	kind       KindSpec
	phase      replacePhase // source.mu, as are next, committed, err
	next       *process
	committed  bool
	hookActive bool
	uncertain  chan struct{}
	err        error
	done       chan struct{}
	resolve    chan replacementResolution
}
type snapshotCapture struct {
	called bool
	data   []byte
	err    error
}

func (p *process) preparationError() error {
	if p.preparing || p.snapshot != nil {
		return ErrRestoring
	}
	return nil
}
func (p *process) snapshotReply(value any, cause error) error {
	snapshot := p.snapshot
	if snapshot.called {
		return ErrAlreadyReplied
	}
	snapshot.called = true
	if cause != nil {
		snapshot.err = cause
		return nil
	}
	snapshot.data, snapshot.err = json.Marshal(value)
	if len(snapshot.data) > p.node.opts.MaxSnapshotBytes {
		snapshot.data = nil
		snapshot.err = ErrMessageTooLarge
	}
	return snapshot.err
}

func (n *Node) replacementTarget(ctx context.Context, target Addr) (*process, error) {
	if n.isGlobal(target) {
		route, err := n.lookupGlobal(ctx, target.(Ref).Path, true)
		if err != nil {
			return nil, err
		}
		target = route
		if err := n.checkGlobal(ctx, route.record); err != nil {
			return nil, err
		}
	}
	p, _, err := n.resolve(target)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (n *Node) Replace(ctx context.Context, target Addr, kind KindSpec) error {
	if err := kind.validate(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p, err := n.replacementTarget(ctx, target)
	if err != nil {
		return err
	}
	p.childrenMu.Lock()
	leaf := len(p.children) == 0
	p.childrenMu.Unlock()
	if !leaf {
		return ErrHandoffUnsupported
	}
	op := &replaceOperation{ctx: ctx, kind: kind, done: make(chan struct{}), uncertain: make(chan struct{}), resolve: make(chan replacementResolution)}
	p.mu.Lock()
	if p.change != nil || p.handoff != nil {
		p.mu.Unlock()
		return ErrMigrating
	}
	if p.stopRequested || p.finished {
		p.mu.Unlock()
		return ErrStopped
	}
	if len(p.asyncRequests) != 0 {
		p.mu.Unlock()
		return ErrAsyncPending
	}
	p.change = op
	p.interruptBatchLocked()
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	if start {
		p.schedule()
	}
	select {
	case <-op.done:
		return op.err
	case <-op.uncertain:
		p.mu.Lock()
		pid, cause := op.next.pid, op.err
		p.mu.Unlock()
		return &ReplaceError{PID: pid, Unknown: true, Cause: errors.Join(ErrOutcomeUnknown, cause)}
	case <-ctx.Done():
		p.mu.Lock()
		phase, committed, next := op.phase, op.committed, op.next
		blocked := phase == replaceSnapshot && op.hookActive
		if blocked {
			p.stopRequested, p.suppressRestart, p.stopReason = true, true, ctx.Err()
			p.interruptBatchLocked()
		}
		p.mu.Unlock()
		select {
		case <-op.done:
			return op.err
		default:
		}
		cause := ctx.Err()
		if blocked {
			p.cancel(cause)
			n.failRequests(p, cause)
			n.markUnresponsive(p)
			cause = errors.Join(ErrUnresponsive, cause)
		}
		unknown := phase == replaceCommitting || phase == replaceUnknown
		if unknown {
			cause = errors.Join(ErrOutcomeUnknown, cause)
		}
		pid := p.pid
		if next != nil {
			pid = next.pid
		}
		return &ReplaceError{PID: pid, Committed: committed, Unknown: unknown, Cause: cause}
	}
}

// ResolveReplace retries only the ownership decision for a frozen replacement.
// The old PID remains the handle even if the Directory already points to new PID.
func (n *Node) ResolveReplace(ctx context.Context, old PID) error {
	if old.Node != n.Name() || old.BootID != n.BootID() {
		return ErrNoProcess
	}
	n.mu.Lock()
	p := n.processes[old.ID]
	n.mu.Unlock()
	if p == nil {
		return ErrNoProcess
	}
	p.mu.Lock()
	op := p.change
	p.mu.Unlock()
	if op == nil {
		return ErrNoProcess
	}
	request := replacementResolution{ctx: ctx, result: make(chan error, 1)}
	select {
	case op.resolve <- request:
	case <-op.done:
		return op.err
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *process) phase(op *replaceOperation, phase replacePhase) {
	p.mu.Lock()
	op.phase = phase
	p.mu.Unlock()
}
func (p *process) finishReplacement(op *replaceOperation, err error) {
	p.mu.Lock()
	if err != nil {
		pid := p.pid
		if op.next != nil {
			pid = op.next.pid
		}
		err = &ReplaceError{PID: pid, Committed: op.committed, Unknown: errors.Is(err, ErrOutcomeUnknown), Cause: err}
	}
	op.phase, op.err = replaceDone, err
	p.change, p.sealed = nil, false
	close(op.done)
	p.mu.Unlock()
}

func (p *process) replace(op *replaceOperation) bool {
	ctx, cancel := context.WithCancel(op.ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer cancel()
	defer stop()
	rollback := func(err error) bool { p.finishReplacement(op, err); return false }
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	p.childrenMu.Lock()
	leaf := len(p.children) == 0
	p.childrenMu.Unlock()
	if !leaf {
		return rollback(ErrHandoffUnsupported)
	}
	for _, r := range p.deferredRequests() {
		select {
		case <-r.waitDone():
		case <-ctx.Done():
			return rollback(ctx.Err())
		}
	}
	p.mu.Lock()
	op.phase, op.hookActive = replaceSnapshot, true
	p.mu.Unlock()
	p.snapshot = &snapshotCapture{}
	err := p.invoke(envelope{message: Snapshotting{}})
	p.mu.Lock()
	op.hookActive = false
	p.mu.Unlock()
	snapshot := p.snapshot
	p.snapshot = nil
	if err == nil {
		err = snapshot.err
	}
	if err == nil && !snapshot.called {
		err = ErrNoReply
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return rollback(err)
	}
	p.phase(op, replacePreparing)
	q, err := p.allocatePrepared(op.kind)
	if err != nil {
		return rollback(err)
	}
	p.mu.Lock()
	op.next = q
	p.mu.Unlock()
	ready := make(chan error, 1)
	activate := make(chan struct{})
	activated := make(chan error, 1)
	go q.prepareReplacement(snapshot.data, p.opts.version, ready, activate, activated)
	select {
	case err = <-ready:
	case <-ctx.Done():
		err = ctx.Err()
		q.requestStop(err, true)
		p.node.markUnresponsive(q)
	}
	if err != nil {
		q.requestStop(err, true)
		return rollback(err)
	}
	p.mu.Lock()
	p.sealed = true
	p.mu.Unlock()
	if err = p.checkReplacementCapacity(q); err != nil {
		q.requestStop(err, true)
		return rollback(err)
	}
	p.phase(op, replaceCommitting)
	committed, known, err := p.commitReplacement(ctx, q, false)
	var resolution *replacementResolution
	for !known {
		p.mu.Lock()
		op.phase, op.err = replaceUnknown, err
		select {
		case <-op.uncertain:
		default:
			close(op.uncertain)
		}
		p.mu.Unlock()
		// The original caller can leave; retain both frozen instances and
		// answer explicit resolution requests without guessing a rollback.
		select {
		case request := <-op.resolve:
			committed, known, err = p.commitReplacement(request.ctx, q, true)
			if !known {
				request.result <- &ReplaceError{PID: q.pid, Unknown: true, Cause: errors.Join(ErrOutcomeUnknown, err)}
			} else {
				resolution = &request
			}
		case <-p.ctx.Done():
			q.requestStop(ErrOutcomeUnknown, true)
			p.node.markUnresponsive(p)
			p.finishReplacement(op, errors.Join(ErrOutcomeUnknown, p.ctx.Err()))
			return false
		}
	}
	if !committed {
		q.requestStop(err, true)
		rollback(err)
		if resolution != nil {
			resolution.result <- op.err
		}
		return false
	}
	p.mu.Lock()
	op.committed, op.phase = true, replaceCommitted
	p.mu.Unlock()
	close(activate)
	stop() // Retiring the source must not cancel the already committed operation.
	p.requestStop(ErrReplaced, true)
	p.finish()
	completionCtx := op.ctx
	if resolution != nil {
		completionCtx = resolution.ctx
	}
	select {
	case err = <-activated:
	case <-completionCtx.Done():
		select {
		case err = <-activated:
		default:
			err = errors.Join(ErrUnresponsive, completionCtx.Err())
			q.requestStop(err, true)
			p.node.markUnresponsive(q)
		}
	}
	p.finishReplacement(op, err)
	if resolution != nil {
		resolution.result <- op.err
	}
	return true
}

func (p *process) allocatePrepared(kind KindSpec) (*process, error) {
	n := p.node
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping.Load() {
		return nil, ErrClosed
	}
	if len(n.processes) >= n.opts.MaxActors {
		return nil, ErrCapacity
	}
	pid, err := n.allocatePID()
	if err != nil {
		return nil, err
	}
	opts := p.opts
	opts.kind, opts.version = kind.Name, kind.Version
	q := newProcess(n, p.parent, p.path, pid, kind.Factory, opts)
	q.preparing, q.booted = true, true
	q.spec = &childSpec{factory: kind.Factory, opts: opts, path: p.path, process: q}
	n.processes[pid.ID] = q
	return q, nil
}
func (q *process) prepareReplacement(snapshot []byte, version string, ready chan<- error, activate <-chan struct{}, activated chan<- error) {
	behavior, err := construct(q.factory, q)
	q.behavior = behavior
	if err == nil {
		err = q.invoke(envelope{message: Started{Restoring: true}})
	}
	if err == nil {
		err = q.invoke(envelope{message: Upgraded{FromVersion: version, rawSnapshot: snapshot}})
	}
	ready <- err
	if err == nil {
		select {
		case <-activate:
		case <-q.ctx.Done():
			err = context.Cause(q.ctx)
		}
	}
	q.mu.Lock()
	stopping := q.stopRequested
	q.mu.Unlock()
	if err == nil && stopping {
		err = ErrStopped
	}
	if err != nil {
		q.requestStop(err, true)
		q.finish()
		close(q.started)
		activated <- err
		return
	}
	q.mu.Lock()
	q.preparing = false
	q.mu.Unlock()
	err = q.invoke(envelope{message: Activated{}})
	if err != nil {
		q.requestStop(&CrashError{PID: q.pid, Cause: err}, false)
	}
	close(q.started)
	activated <- err
	q.run()
}

func (p *process) checkReplacementCapacity(q *process) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.stopRequested || p.unresponsive {
		return ErrStopped
	}
	count := q.mailbox.size
	for i := 0; i < p.mailbox.size; i++ {
		if p.mailbox.at(i).toRef {
			count++
		}
	}
	if count > q.mailbox.limit {
		return ErrMailboxFull
	}
	return nil
}

// commitReplacement is implemented separately from preparation: after a CAS
// succeeds, publication must complete even if cancellation races the response.
func (p *process) commitReplacement(ctx context.Context, q *process, resolve bool) (committed, known bool, err error) {
	p.mu.Lock()
	old := p.ownership
	p.mu.Unlock()
	next := RouteRecord{Path: q.path, Owner: q.pid.Node, BootID: q.pid.BootID, ProcessID: q.pid.ID, Kind: q.opts.kind, Version: q.opts.version}
	if p.node.opts.Directory != nil {
		var record RouteRecord
		if !resolve {
			record, err = p.node.opts.Directory.SwapInstance(ctx, old, next)
		} else {
			err = &DirectoryError{Unknown: true, Cause: ErrOutcomeUnknown}
		}
		var storage *DirectoryError
		if errors.As(err, &storage) && storage.Unknown {
			var committed bool
			resolveCtx := ctx
			var cancel context.CancelFunc
			if !resolve && ctx.Err() != nil {
				resolveCtx, cancel = context.WithTimeout(p.ctx, p.node.opts.CallTimeout)
				defer cancel()
			}
			record, committed, err = p.resolveSwap(resolveCtx, old, next)
			if err != nil {
				return false, false, err
			}
			if !committed {
				p.mu.Lock()
				p.ownership = record
				p.mu.Unlock()
				return false, true, ErrMigrating
			}
		}
		if err != nil {
			return false, true, err
		}
		next = record
	} else if err := ctx.Err(); err != nil {
		return false, true, err
	}
	p.publishReplacement(q, next)
	return true, true, nil
}

func (p *process) resolveSwap(ctx context.Context, old, next RouteRecord) (RouteRecord, bool, error) {
	d := p.node.opts.Directory
	current, err := d.Lookup(ctx, old.Path)
	if err != nil {
		return RouteRecord{}, false, err
	}
	if current.PID() == next.PID() && current.Epoch == old.Epoch+1 {
		return current, true, nil
	}
	if current.PID() == old.PID() && current.Epoch == old.Epoch+1 {
		return current, false, nil
	}
	if current != old {
		return RouteRecord{}, false, ErrDirectoryConflict
	}
	// A read of the old record alone cannot rule out a delayed original CAS.
	// Advancing its epoch with a self-swap fences that CAS before rollback.
	fenced, err := d.SwapInstance(ctx, old, old)
	if err == nil {
		return fenced, false, nil
	}
	current, readErr := d.Lookup(ctx, old.Path)
	if readErr != nil {
		return RouteRecord{}, false, errors.Join(err, readErr)
	}
	if current.PID() == next.PID() && current.Epoch == old.Epoch+1 {
		return current, true, nil
	}
	if current.PID() == old.PID() && current.Epoch == old.Epoch+1 {
		return current, false, nil
	}
	return RouteRecord{}, false, err
}

func (p *process) publishReplacement(q *process, record RouteRecord) {
	parent, n := p.parent, p.node
	parent.childrenMu.Lock()
	n.mu.Lock()
	p.mu.Lock()
	q.mu.Lock()
	oldPIDMessages := mailbox{limit: p.mailbox.limit}
	transferred := mailbox{limit: q.mailbox.limit}
	for p.mailbox.size > 0 {
		env := p.mailbox.pop()
		if env.toRef {
			if env.request != nil {
				env.request.owner = q.pid
				q.callsUsed.Store(true)
			}
			if env.global != nil {
				env.global = &record
			}
			_ = transferred.push(env)
		} else {
			_ = oldPIDMessages.push(env)
		}
	}
	for q.mailbox.size > 0 {
		_ = transferred.push(q.mailbox.pop())
	}
	p.mailbox, q.mailbox = oldPIDMessages, transferred
	q.ownership, p.ownership = record, RouteRecord{}
	n.publishPIDLocked(p, false)
	n.publishPIDLocked(q, true)
	n.routes[p.path] = route{process: q}
	for i, spec := range parent.children {
		if spec == p.spec {
			parent.children[i] = q.spec
			p.spec.removed = true
			break
		}
	}
	if n.stopping.Load() || p.stopRequested || parent.ctx.Err() != nil {
		q.stopRequested, q.suppressRestart, q.stopReason = true, true, ErrStopped
		q.interruptBatchLocked()
	}
	q.mu.Unlock()
	p.mu.Unlock()
	n.mu.Unlock()
	parent.childrenMu.Unlock()
}
