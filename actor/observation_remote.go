package actor

import (
	"context"
	"crypto/rand"
	"math"
)

type LinkID [16]byte
type remoteMonitor struct {
	ref     MonitorRef
	target  PID
	scope   wireScope
	token   *controlReservation
	pending bool
}

func (w *network) monitor(observer *process, target PID) (MonitorRef, error) {
	n := w.node
	n.mu.Lock()
	if n.stopping.Load() || observer.observationsEnded {
		n.mu.Unlock()
		return MonitorRef{}, ErrStopped
	}
	if n.nextMonitor == math.MaxUint64 {
		n.mu.Unlock()
		return MonitorRef{}, ErrCapacity
	}
	if err := n.reserveObservationLocked(observer.pid); err != nil {
		n.mu.Unlock()
		return MonitorRef{}, err
	}
	n.nextMonitor++
	ref := MonitorRef{observer.pid, n.nextMonitor}
	record := &monitorRecord{ref: ref, observer: observer, target: target, remote: true}
	n.monitors[ref] = record
	n.mu.Unlock()
	_, err := w.controlJob(target.Node, actorPacket{Kind: "monitor", Monitor: &ref, Target: wireAddress{Kind: "pid", PID: &target}}, target.BootID, nil,
		func(err error) {
			if err != nil {
				_ = n.deliverRemoteDown(ref, target, err)
			}
			n.mu.Lock()
			active := n.monitors[ref] == record && !record.queued
			n.mu.Unlock()
			if !active {
				w.sendObservationCleanup(target, actorPacket{Kind: "demonitor", Monitor: &ref})
			}
		}, func(scope wireScope) {
			n.mu.Lock()
			if n.monitors[ref] == record {
				record.scope = scope
			}
			n.mu.Unlock()
		})
	if err != nil {
		n.mu.Lock()
		if n.monitors[ref] == record {
			delete(n.monitors, ref)
			n.releaseObservationLocked(observer.pid)
		}
		n.mu.Unlock()
		return MonitorRef{}, err
	}
	return ref, nil
}
func (w *network) demonitor(observer *process, ref MonitorRef) bool {
	n := w.node
	n.mu.Lock()
	record := n.monitors[ref]
	if record == nil || !record.remote || ref.Observer != observer.pid {
		n.mu.Unlock()
		return false
	}
	observer.removeNotification(func(message any) bool { notice, ok := message.(monitorNotice); return ok && notice.ref == ref })
	delete(n.monitors, ref)
	n.releaseObservationLocked(observer.pid)
	n.mu.Unlock()
	w.sendObservationCleanup(record.target, actorPacket{Kind: "demonitor", Monitor: &ref})
	return true
}
func (n *Node) deliverRemoteDown(ref MonitorRef, target PID, reason error) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	record := n.monitors[ref]
	if record == nil {
		return nil
	}
	if !record.remote || record.target != target {
		return ErrInvalidMessage
	}
	if record.queued {
		return nil
	}
	record.queued = true
	record.reason = reason
	if !record.observer.enqueueSystem(monitorNotice{ref}) {
		delete(n.monitors, ref)
		n.releaseObservationLocked(ref.Observer)
	}
	return nil
}
func (w *network) registerRemoteMonitor(ref MonitorRef, target PID, scope wireScope) error {
	token, err := w.reserveControl(true)
	if err != nil {
		return err
	}
	n := w.node
	n.mu.Lock()
	if n.stopping.Load() {
		n.mu.Unlock()
		token.release()
		return ErrClosed
	}
	if existing := n.remoteMonitors[ref]; existing != nil {
		n.mu.Unlock()
		token.release()
		if existing.target != target {
			return ErrInvalidMessage
		}
		return nil
	}
	if err = n.reserveObservationLocked(ref.Observer); err != nil {
		n.mu.Unlock()
		token.release()
		return err
	}
	if n.remoteMonitors == nil {
		n.remoteMonitors = make(map[MonitorRef]*remoteMonitor)
	}
	record := &remoteMonitor{ref: ref, target: target, scope: scope, token: token}
	n.remoteMonitors[ref] = record
	_, missing := n.localProcessLocked(target)
	if missing != nil {
		record.pending = true
	}
	n.mu.Unlock()
	if missing != nil {
		w.sendRemoteDown(record, missing)
	}
	return nil
}
func (w *network) sendRemoteDown(record *remoteMonitor, reason error) {
	packet := actorPacket{Kind: "down", Monitor: &record.ref, Target: wireAddress{Kind: "pid", PID: &record.target}, Error: actorWireError(reason)}
	w.sendReservedObservation(record.token, record.ref.Observer, packet, func() {
		n := w.node
		n.mu.Lock()
		if n.remoteMonitors[record.ref] == record {
			delete(n.remoteMonitors, record.ref)
			n.releaseObservationLocked(record.ref.Observer)
		}
		n.mu.Unlock()
	})
}
func (w *network) removeRemoteMonitor(ref MonitorRef) {
	n := w.node
	n.mu.Lock()
	record := n.remoteMonitors[ref]
	if record != nil {
		delete(n.remoteMonitors, ref)
		n.releaseObservationLocked(ref.Observer)
	}
	pending := record != nil && record.pending
	n.mu.Unlock()
	if record != nil && !pending {
		record.token.release()
	}
}
func (n *Node) endRemoteMonitorsLocked(p *process, reason error) []fatalExit {
	var actions []fatalExit
	for _, record := range n.remoteMonitors {
		if record.target == p.pid && !record.pending {
			record.pending = true
			actions = append(actions, fatalExit{network: func() { n.network.sendRemoteDown(record, reason) }})
		}
	}
	return actions
}

func (w *network) link(ctx context.Context, observer *process, target PID) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.node.opts.CallTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	token, err := w.reserveControl(true)
	if err != nil {
		return err
	}
	var id LinkID
	if _, err = rand.Read(id[:]); err != nil {
		token.release()
		return err
	}
	n := w.node
	n.mu.Lock()
	if n.stopping.Load() || observer.observationsEnded {
		n.mu.Unlock()
		token.release()
		return ErrStopped
	}
	if existing := n.links[observer.pid][target]; existing != nil {
		established := existing.established
		n.mu.Unlock()
		token.release()
		if established {
			return nil
		}
		return ErrLinkConflict
	}
	if err = n.reserveObservationLocked(observer.pid); err != nil {
		n.mu.Unlock()
		token.release()
		return err
	}
	record := &linkRecord{a: observer.pid, b: target, id: id, remote: true, token: token}
	if n.links[observer.pid] == nil {
		n.links[observer.pid] = make(map[PID]*linkRecord)
	}
	n.links[observer.pid][target] = record
	n.mu.Unlock()
	packet := actorPacket{Kind: "link", From: &observer.pid, Target: wireAddress{Kind: "pid", PID: &target}, Link: id}
	job, err := w.controlJob(target.Node, packet, target.BootID, nil, nil, func(scope wireScope) {
		n.mu.Lock()
		if n.links[observer.pid][target] == record {
			record.scope = scope
		}
		n.mu.Unlock()
	})
	if err == nil {
		select {
		case err = <-job.result:
		case <-ctx.Done():
			err = deliveryError(ctx.Err(), Unknown)
		}
	}
	n.mu.Lock()
	current := n.links[observer.pid][target] == record
	if err == nil && current {
		record.established = true
	}
	var actions []fatalExit
	if err == nil && current && record.exitReceived {
		actions = n.applyRemoteExitLocked(observer, target, record, actorWireReason(record.pendingExit))
	}
	if err != nil && current {
		n.deleteLinkLocked(observer.pid, target)
		n.releaseObservationLocked(observer.pid)
	}
	n.mu.Unlock()
	for _, action := range actions {
		action.run()
	}
	if err != nil && current {
		packet.Kind = "unlink"
		w.sendReservedObservation(token, target, packet, nil)
	}
	if !current {
		packet.Kind = "unlink"
		w.sendObservationCleanup(target, packet)
		if err == nil {
			return ErrStopped
		}
	}
	return err
}
func (w *network) unlink(observer *process, target PID) {
	n := w.node
	n.mu.Lock()
	record := n.links[observer.pid][target]
	if record == nil || !record.remote {
		n.mu.Unlock()
		return
	}
	observer.removeNotification(func(message any) bool { notice, ok := message.(linkNotice); return ok && notice.record == record })
	n.deleteLinkLocked(observer.pid, target)
	n.releaseObservationLocked(observer.pid)
	pending := record.pendingFor == observer.pid
	n.mu.Unlock()
	if !pending {
		w.sendReservedObservation(record.token, target, actorPacket{Kind: "unlink", From: &observer.pid, Target: wireAddress{Kind: "pid", PID: &target}, Link: record.id}, nil)
	}
}
func (w *network) registerRemoteLink(from, target PID, id LinkID, scope wireScope) error {
	token, err := w.reserveControl(true)
	if err != nil {
		return err
	}
	n := w.node
	n.mu.Lock()
	if n.stopping.Load() {
		n.mu.Unlock()
		token.release()
		return ErrClosed
	}
	if _, err = n.localProcessLocked(target); err != nil {
		n.mu.Unlock()
		token.release()
		return err
	}
	if existing := n.links[target][from]; existing != nil {
		same := existing.id == id
		n.mu.Unlock()
		token.release()
		if same {
			return nil
		}
		return ErrLinkConflict
	}
	if err = n.reserveObservationLocked(target); err != nil {
		n.mu.Unlock()
		token.release()
		return err
	}
	if n.links[target] == nil {
		n.links[target] = make(map[PID]*linkRecord)
	}
	n.links[target][from] = &linkRecord{a: target, b: from, id: id, remote: true, established: true, token: token, scope: scope}
	n.mu.Unlock()
	return nil
}
func (w *network) removeRemoteLink(owner, target PID, id LinkID) {
	n := w.node
	n.mu.Lock()
	record := n.links[owner][target]
	if record == nil || !record.remote || record.id != id {
		n.mu.Unlock()
		return
	}
	if p := n.processes[owner.ID]; p != nil {
		p.removeNotification(func(message any) bool { notice, ok := message.(linkNotice); return ok && notice.record == record })
	}
	n.deleteLinkLocked(owner, target)
	n.releaseObservationLocked(owner)
	pending := record.pendingFor == owner
	n.mu.Unlock()
	if !pending {
		record.token.release()
	}
}

func (n *Node) deliverRemoteExit(owner, target PID, id LinkID, reason *wireActorError) error {
	n.mu.Lock()
	record := n.links[owner][target]
	if record == nil {
		n.mu.Unlock()
		return nil
	}
	if !record.remote || record.id != id {
		n.mu.Unlock()
		return nil
	}
	if record.pendingFor == owner {
		n.mu.Unlock()
		return nil
	}
	if !record.established {
		record.pendingExit = reason
		record.exitReceived = true
		n.mu.Unlock()
		return nil
	}
	p := n.processes[owner.ID]
	var actions []fatalExit
	if p != nil {
		actions = n.applyRemoteExitLocked(p, target, record, actorWireReason(reason))
	}
	n.mu.Unlock()
	for _, action := range actions {
		action.run()
	}
	return nil
}
func (n *Node) applyRemoteExitLocked(observer *process, target PID, record *linkRecord, reason error) []fatalExit {
	actions := []fatalExit{{network: record.token.release}}
	n.deleteLinkLocked(observer.pid, target)
	observer.mu.Lock()
	trap, stopping := observer.trapExit, observer.stopRequested
	observer.mu.Unlock()
	if trap && !stopping {
		record.pendingFor = observer.pid
		if n.links[observer.pid] == nil {
			n.links[observer.pid] = make(map[PID]*linkRecord)
		}
		n.links[observer.pid][target] = record
		if !observer.enqueueSystem(linkNotice{record, observer.pid, target, reason}) {
			n.deleteLinkLocked(observer.pid, target)
			n.releaseObservationLocked(observer.pid)
		}
	} else {
		n.releaseObservationLocked(observer.pid)
		if reason != nil && !stopping {
			exit := &ExitError{PID: target, Cause: reason}
			observer.mu.Lock()
			mark := !observer.stopRequested
			if mark {
				observer.stopRequested = true
				observer.interruptBatchLocked()
				observer.stopReason = exit
			}
			observer.mu.Unlock()
			if mark {
				actions = append(actions, fatalExit{process: observer, reason: exit})
			}
		}
	}
	return actions
}

func (n *Node) remoteObservationLost(peer string, boot [16]byte, scope *wireScope) {
	n.mu.Lock()
	var actions []fatalExit
	for ref, record := range n.monitors {
		if record.remote && record.target.Node == peer && record.target.BootID == boot && !record.queued && (scope == nil && record.scope != (wireScope{}) || scope != nil && record.scope == *scope) {
			record.queued = true
			record.reason = ErrNodeUnreachable
			if !record.observer.enqueueSystem(monitorNotice{ref}) {
				delete(n.monitors, ref)
				n.releaseObservationLocked(ref.Observer)
			}
		}
	}
	for owner, links := range n.links {
		for target, record := range links {
			if record.remote && target.Node == peer && target.BootID == boot && record.pendingFor == (PID{}) && (scope == nil && record.scope != (wireScope{}) || scope != nil && record.scope == *scope) {
				if !record.established {
					record.pendingExit = actorWireError(ErrNodeUnreachable)
					record.exitReceived = true
					continue
				}
				if p := n.processes[owner.ID]; p != nil {
					actions = append(actions, n.applyRemoteExitLocked(p, target, record, ErrNodeUnreachable)...)
				}
			}
		}
	}
	n.mu.Unlock()
	for _, action := range actions {
		action.run()
	}
}
func (w *network) dropInboundScope(peer string, boot [16]byte, scope wireScope) {
	n := w.node
	n.mu.Lock()
	var tokens []*controlReservation
	for ref, record := range n.remoteMonitors {
		if record.scope == scope {
			delete(n.remoteMonitors, ref)
			n.releaseObservationLocked(ref.Observer)
			if !record.pending {
				tokens = append(tokens, record.token)
			}
		}
	}
	n.mu.Unlock()
	for _, token := range tokens {
		token.release()
	}
	n.remoteObservationLost(peer, boot, &scope)
}
