package actor

import (
	"context"
	"math"
)

type monitorRecord struct {
	remote   bool
	scope    wireScope
	ref      MonitorRef
	observer *process
	target   PID
	queued   bool
	reason   error
}
type monitorNotice struct{ ref MonitorRef }
type linkRecord struct {
	id                  LinkID
	remote, established bool
	token               *controlReservation
	scope               wireScope
	pendingExit         *wireActorError
	exitReceived        bool
	a, b                PID
	pendingFor          PID
}
type linkNotice struct {
	record           *linkRecord
	observer, target PID
	reason           error
}
type fatalExit struct {
	network func()
	process *process
	reason  error
}

func (action fatalExit) run() {
	if action.network != nil {
		action.network()
	} else {
		action.process.requestStop(action.reason, false)
	}
}

func (n *Node) reserveObservationLocked(owners ...PID) error {
	if len(owners) > n.opts.MaxObservations-n.observations {
		return ErrCapacity
	}
	for _, owner := range owners {
		if n.watchCounts[owner] >= n.opts.MaxObservationsPerActor {
			return ErrCapacity
		}
	}
	n.observations += len(owners)
	for _, owner := range owners {
		n.watchCounts[owner]++
	}
	return nil
}
func (n *Node) releaseObservationLocked(owner PID) {
	n.observations--
	n.watchCounts[owner]--
	if n.watchCounts[owner] == 0 {
		delete(n.watchCounts, owner)
	}
}
func (n *Node) localProcessLocked(pid PID) (*process, error) {
	if pid.Node != n.opts.Name {
		return nil, ErrRemoteUnavailable
	}
	if pid.BootID != n.bootID {
		return nil, ErrStalePID
	}
	p := n.processes[pid.ID]
	if p == nil {
		return nil, ErrNoProcess
	}
	p.mu.Lock()
	dead := p.finished || p.unresponsive
	p.mu.Unlock()
	if dead {
		return nil, ErrNoProcess
	}
	return p, nil
}

func (n *Node) monitor(observer *process, target PID) (MonitorRef, error) {
	if target.Node != n.Name() && n.network != nil {
		return n.network.monitor(observer, target)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if observer.observationsEnded {
		return MonitorRef{}, ErrStopped
	}
	if target.Node != n.opts.Name {
		return MonitorRef{}, ErrRemoteUnavailable
	}
	if n.nextMonitor == math.MaxUint64 {
		return MonitorRef{}, ErrCapacity
	}
	if err := n.reserveObservationLocked(observer.pid); err != nil {
		return MonitorRef{}, err
	}
	n.nextMonitor++
	ref := MonitorRef{observer.pid, n.nextMonitor}
	record := &monitorRecord{ref: ref, observer: observer, target: target}
	n.monitors[ref] = record
	if _, err := n.localProcessLocked(target); err != nil {
		record.queued = true
		record.reason = err
		if !observer.enqueueSystem(monitorNotice{ref}) {
			delete(n.monitors, ref)
			n.releaseObservationLocked(observer.pid)
			return MonitorRef{}, ErrStopped
		}
	}
	return ref, nil
}

func (n *Node) demonitor(observer *process, ref MonitorRef) {
	if n.network != nil && n.network.demonitor(observer, ref) {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	record := n.monitors[ref]
	if record == nil || ref.Observer != observer.pid {
		return
	}
	observer.removeNotification(func(message any) bool { notice, ok := message.(monitorNotice); return ok && notice.ref == ref })
	delete(n.monitors, ref)
	n.releaseObservationLocked(observer.pid)
}

func (p *process) removeNotification(match func(any) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.system[:0]
	for _, message := range p.system {
		if !match(message) {
			kept = append(kept, message)
		}
	}
	clear(p.system[len(kept):])
	p.system = kept
}

func (n *Node) link(ctx context.Context, observer *process, target PID) error {
	if target.Node != n.Name() && n.network != nil {
		return n.network.link(ctx, observer, target)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if target == observer.pid {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if observer.observationsEnded {
		return ErrStopped
	}
	if _, err := n.localProcessLocked(target); err != nil {
		return err
	}
	if n.links[observer.pid][target] != nil {
		return nil
	}
	if err := n.reserveObservationLocked(observer.pid, target); err != nil {
		return err
	}
	record := &linkRecord{a: observer.pid, b: target}
	if n.links[observer.pid] == nil {
		n.links[observer.pid] = make(map[PID]*linkRecord)
	}
	if n.links[target] == nil {
		n.links[target] = make(map[PID]*linkRecord)
	}
	n.links[observer.pid][target] = record
	n.links[target][observer.pid] = record
	return nil
}
func (n *Node) deleteLinkLocked(owner, target PID) {
	delete(n.links[owner], target)
	if len(n.links[owner]) == 0 {
		delete(n.links, owner)
	}
}
func (n *Node) unlink(observer *process, target PID) {
	if target.Node != n.Name() && n.network != nil {
		n.network.unlink(observer, target)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	record := n.links[observer.pid][target]
	if record == nil {
		return
	}
	if record.pendingFor != (PID{}) {
		observer.removeNotification(func(message any) bool { notice, ok := message.(linkNotice); return ok && notice.record == record })
		n.deleteLinkLocked(observer.pid, target)
		n.releaseObservationLocked(observer.pid)
		return
	}
	n.deleteLinkLocked(observer.pid, target)
	n.deleteLinkLocked(target, observer.pid)
	n.releaseObservationLocked(observer.pid)
	n.releaseObservationLocked(target)
}

// Called with node.mu after the terminating process's own mutex is released.
func (n *Node) endObservationsLocked(p *process, reason error) []fatalExit {
	if p.observationsEnded {
		return nil
	}
	p.observationsEnded = true
	var fatal []fatalExit
	for ref, record := range n.monitors {
		if record.observer == p {
			if record.remote {
				fatal = append(fatal, fatalExit{network: func() {
					n.network.sendObservationCleanup(record.target, actorPacket{Kind: "demonitor", Monitor: &record.ref})
				}})
			}
			delete(n.monitors, ref)
			n.releaseObservationLocked(p.pid)
			continue
		}
		if record.target == p.pid && !record.queued {
			record.queued = true
			record.reason = reason
			if !record.observer.enqueueSystem(monitorNotice{ref}) {
				delete(n.monitors, ref)
				n.releaseObservationLocked(record.observer.pid)
			}
		}
	}
	fatal = append(fatal, n.endRemoteMonitorsLocked(p, reason)...)
	for target, record := range n.links[p.pid] {
		n.deleteLinkLocked(p.pid, target)
		n.releaseObservationLocked(p.pid)
		if record.pendingFor == p.pid {
			continue
		}
		if record.remote {
			fatal = append(fatal, fatalExit{network: func() {
				kind := "exit"
				if !record.established {
					kind = "unlink"
				}
				packet := actorPacket{Kind: kind, From: &p.pid, Target: wireAddress{Kind: "pid", PID: &target}, Link: record.id, Error: actorWireError(reason)}
				n.network.sendReservedObservation(record.token, target, packet, nil)
			}})
			continue
		}
		n.deleteLinkLocked(target, p.pid)
		other := n.processes[target.ID]
		if other == nil {
			n.releaseObservationLocked(target)
			continue
		}
		other.mu.Lock()
		trap, stopping := other.trapExit, other.stopRequested
		other.mu.Unlock()
		if trap && !stopping {
			record.pendingFor = target
			if n.links[target] == nil {
				n.links[target] = make(map[PID]*linkRecord)
			}
			n.links[target][p.pid] = record
			if !other.enqueueSystem(linkNotice{record, target, p.pid, reason}) {
				n.deleteLinkLocked(target, p.pid)
				n.releaseObservationLocked(target)
			}
		} else {
			n.releaseObservationLocked(target)
			if reason != nil && !stopping {
				exit := &ExitError{PID: p.pid, Cause: reason}
				other.mu.Lock()
				mark := !other.stopRequested
				if mark {
					other.stopRequested = true
					other.interruptBatchLocked()
					other.stopReason = exit
				}
				other.mu.Unlock()
				if mark {
					fatal = append(fatal, fatalExit{process: other, reason: exit})
				}
			}
		}
	}
	return fatal
}

func (n *Node) consumeNotification(message any) (any, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch notice := message.(type) {
	case monitorNotice:
		record := n.monitors[notice.ref]
		if record == nil {
			return nil, false
		}
		delete(n.monitors, notice.ref)
		n.releaseObservationLocked(notice.ref.Observer)
		return Down{Ref: notice.ref, PID: record.target, Reason: record.reason}, true
	case linkNotice:
		if n.links[notice.observer][notice.target] != notice.record {
			return nil, false
		}
		n.deleteLinkLocked(notice.observer, notice.target)
		n.releaseObservationLocked(notice.observer)
		return Exit{PID: notice.target, Reason: notice.reason}, true
	default:
		return message, true
	}
}
