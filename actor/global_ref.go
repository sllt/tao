package actor

import (
	"context"
	"errors"
)

var ErrRedirect = errors.New("actor: route changed before mailbox admission")
var ErrDirectoryUnavailable = errors.New("actor: global Ref requires a Directory")

type routedRef struct{ record RouteRecord }

func (r routedRef) String() string { return r.record.Path }
func (routedRef) isAddr()          {}
func globalRecord(to Addr) *RouteRecord {
	if r, ok := to.(routedRef); ok {
		// Allocate only for a global route. Taking the assertion variable's
		// address makes it escape even when a local PID/Ref fails the assertion.
		record := r.record
		return &record
	}
	return nil
}
func (n *Node) isGlobal(to Addr) bool {
	r, ok := to.(Ref)
	return ok && r.Node == "" && n.opts.Directory != nil
}

func (n *Node) lookupGlobal(ctx context.Context, path string, refresh bool) (routedRef, error) {
	if err := ValidateDirectoryPath(path); err != nil {
		return routedRef{}, err
	}
	var record RouteRecord
	var err error
	if refresh {
		n.directoryCache.Invalidate(path)
		record, err = n.opts.Directory.Lookup(ctx, path)
	} else {
		record, err = n.directoryCache.Lookup(ctx, path)
	}
	if errors.Is(err, ErrDirectoryNotFound) {
		return routedRef{}, ErrNoProcess
	}
	if err != nil {
		return routedRef{}, err
	}
	return routedRef{record}, nil
}

// Each rejected attempt is known not to have entered a mailbox. Unknown or a
// business reply is never retried. Network rerouting keeps the original CallID.
func safeRedirect(err error) bool {
	if !errors.Is(err, ErrRedirect) {
		return false
	}
	var remote *RemoteError
	return !errors.As(err, &remote) || remote.Outcome == Rejected || remote.Outcome == NotSent
}
func (n *Node) sendGlobal(ctx context.Context, to Ref, message any, sender Addr, confirmed bool) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
		defer cancel()
	}
	for attempt := 0; attempt < 3; attempt++ {
		target, err := n.lookupGlobal(ctx, to.Path, attempt != 0)
		if err != nil {
			return err
		}
		if target.record.Owner != n.Name() {
			if n.network == nil {
				return ErrRemoteUnavailable
			}
			var from PID
			if sender != nil {
				from, _ = sender.(PID)
			}
			return n.network.send(ctx, target, message, from, confirmed)
		}
		if err = n.checkGlobal(ctx, target.record); err == nil {
			err = n.send(target, message, sender, nil)
		}
		if !safeRedirect(err) || attempt == 2 {
			return err
		}
	}
	return ErrRedirect
}
func (n *Node) callGlobal(ctx context.Context, to Ref, message any, sender *process) (any, error) {
	for attempt := 0; attempt < 3; attempt++ {
		target, err := n.lookupGlobal(ctx, to.Path, attempt != 0)
		if err != nil {
			return nil, err
		}
		if target.record.Owner != n.Name() {
			if n.network == nil {
				return nil, ErrRemoteUnavailable
			}
			var from PID
			if sender != nil {
				from = sender.pid
			}
			return n.network.call(ctx, target, message, from)
		}
		if err = n.checkGlobal(ctx, target.record); err == nil {
			value, callErr := n.call(ctx, target, message, sender)
			var rejected *RemoteError
			if !errors.As(callErr, &rejected) || rejected.Outcome != Rejected || !errors.Is(callErr, ErrRedirect) {
				return value, callErr
			}
			err = callErr
		}
		if !safeRedirect(err) || attempt == 2 {
			return nil, err
		}
	}
	return nil, ErrRedirect
}

func (n *Node) checkGlobal(ctx context.Context, expected RouteRecord) error {
	if n.opts.Directory == nil {
		return ErrDirectoryUnavailable
	}
	current, err := n.opts.Directory.Lookup(ctx, expected.Path)
	if errors.Is(err, ErrDirectoryNotFound) {
		return ErrRedirect
	}
	if err != nil {
		return errors.Join(ErrDirectoryUnavailable, err)
	}
	if current.Owner != n.Name() || current.PID() != expected.PID() || current.Epoch != expected.Epoch {
		return ErrRedirect
	}
	return nil
}

func (p *process) registerGlobal() error {
	d := p.node.opts.Directory
	if d == nil || p.internal {
		return nil
	}
	ctx, cancel := context.WithTimeout(p.ctx, p.node.opts.StartupTimeout)
	defer cancel()
	wanted := RouteRecord{Path: p.path, Owner: p.pid.Node, BootID: p.pid.BootID, ProcessID: p.pid.ID, Kind: p.opts.kind, Version: p.opts.version}
	p.mu.Lock()
	previous := p.ownership
	p.mu.Unlock()
	var record RouteRecord
	var err error
	if previous.Epoch != 0 {
		record, err = d.SwapInstance(ctx, previous, wanted)
	} else {
		record, err = d.Claim(ctx, wanted)
	}
	var failure *DirectoryError
	if errors.As(err, &failure) && failure.Unknown {
		// A successful authoritative read can settle a lost Claim/Swap reply.
		resolveCtx, stop := context.WithTimeout(context.Background(), p.node.opts.StartupTimeout)
		found, readErr := d.Lookup(resolveCtx, p.path)
		stop()
		if readErr == nil && found.PID() == wanted.PID() && found.State == RouteActive {
			record, err = found, nil
		}
	}
	if errors.Is(err, ErrDirectoryConflict) {
		return errors.Join(ErrNameTaken, err)
	}
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.ownership = record
	p.mu.Unlock()
	return nil
}
func (p *process) releaseGlobal(ctx context.Context) {
	p.mu.Lock()
	record, unresponsive := p.ownership, p.unresponsive
	p.mu.Unlock()
	if record.Epoch == 0 || unresponsive {
		return
	}
	directory := p.node.opts.Directory
	_, err := directory.Release(ctx, record)
	if errors.Is(err, ErrDirectoryConflict) {
		current, readErr := directory.Lookup(ctx, record.Path)
		if errors.Is(readErr, ErrDirectoryNotFound) || readErr == nil && current.PID() != p.pid {
			return
		}
		// An aborted preparation changes revision without replacing the process.
		// Retiring this exact PID may release that newer active record by CAS.
		if readErr == nil && current.State == RouteActive {
			_, err = directory.Release(ctx, current)
		}
	}
	if err != nil {
		p.logger().Error("Directory release failed; ownership remains reserved", "error", err)
	}
}

func (w *network) redirectJob(ctx context.Context, job *networkJob, err error) (bool, error) {
	if job.packet.Target.Kind != "global" || job.redirects >= 2 || !safeRedirect(err) {
		return false, nil
	}
	if out := job.call; out != nil {
		w.mu.Lock()
		out.issued = false // The peer explicitly rejected admission.
		out.scope, out.sequence = wireScope{}, 0
		canceled := out.canceled
		w.mu.Unlock()
		if canceled {
			return false, deliveryError(context.Canceled, NotSent)
		}
	}
	target, err := w.node.lookupGlobal(ctx, job.packet.Target.Route.Path, true)
	if err != nil {
		return false, deliveryError(err, NotSent)
	}
	job.peer = target.record.Owner
	job.packet.Target = wireAddress{Kind: "global", Route: &target.record}
	// A new owner may change variable-length address metadata. Rebuild that
	// rare submission from Message.Data, retaining its original buffer until
	// the last redirect finishes; never encode over an aliased input body.
	job.raw = nil
	job.redirects++
	if out := job.call; out != nil {
		w.mu.Lock()
		out.peer, out.boot, out.local = job.peer, [16]byte{}, false
		out.routing.route = target.record
		w.mu.Unlock()
	}
	if err := w.enqueue(job); err != nil {
		return false, deliveryError(err, NotSent)
	}
	return true, nil
}

// A redirected global request can return to its caller's own node. Keep its
// original outgoing request/key, and use a separate local request for the actor.
func (w *network) writeLocalJob(ctx context.Context, job *networkJob) error {
	if err := w.node.checkGlobal(ctx, *job.packet.Target.Route); err != nil {
		return deliveryError(err, Rejected)
	}
	p, _, err := w.node.resolve(routedRef{record: *job.packet.Target.Route})
	if err != nil {
		return deliveryError(err, Rejected)
	}
	message, err := w.registry.Decode(job.packet.Message)
	if err != nil {
		return deliveryError(err, Rejected)
	}
	var sender Addr
	if job.packet.Sender != nil {
		sender = *job.packet.Sender
	}
	env := envelope{message: message, sender: sender, toRef: true, global: job.packet.Target.Route}
	if job.call == nil {
		if err := p.enqueue(env); err != nil {
			return deliveryError(err, Rejected)
		}
		return nil
	}
	out := job.call
	r, err := w.coreRequest(out.request.context(), p.pid)
	if err != nil {
		return deliveryError(err, Rejected)
	}
	env.request = r
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.node.expireRequest(r, ErrClosed)
		return deliveryError(ErrClosed, NotSent)
	}
	w.wg.Add(1)
	out.local, out.issued = true, true
	w.mu.Unlock()
	stop := context.AfterFunc(out.request.context(), func() { w.node.expireRequest(r, out.request.contextErr()) })
	if err := p.enqueue(env); err != nil {
		stop()
		w.node.expireRequest(r, err)
		w.wg.Done()
		return deliveryError(err, Rejected)
	}
	go func() {
		defer w.wg.Done()
		defer stop()
		<-r.waitDone()
		var response error
		if r.err != nil {
			response = &RemoteError{Code: codeForError(r.err), Message: r.err.Error(), Outcome: Responded, Cause: r.err}
		}
		_ = w.node.reply(out.request, out.request.owner, r.value, response, false)
	}()
	return nil
}
