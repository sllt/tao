package actor

import (
	"errors"
	"time"
)

// A child spec survives its process and stays in creation order.
// All spec fields and the child list are protected by parent.childrenMu.
type childSpec struct {
	factory Factory
	opts    spawnOptions
	path    string
	process *process
	saved   []envelope
	removed bool // parent.childrenMu; avoids rescanning an already detached child
}
type childFailure struct {
	spec *childSpec
	old  *process
}

func (p *process) enqueueSystem(message any) bool {
	p.mu.Lock()
	if p.stopRequested || p.finished {
		p.mu.Unlock()
		return false
	}
	p.system = append(p.system, message)
	p.interruptBatchLocked()
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	if start {
		p.schedule()
	}
	return true
}
func (p *process) restartEligibleLocked() bool {
	return p.opts.restart == Permanent || p.opts.restart == Transient && p.stopReason != nil
}

func (p *process) retire(reason, cleanup error) (bool, bool) {
	if p.parent != nil {
		p.parent.childrenMu.Lock()
	}
	n := p.node
	n.mu.Lock()
	p.mu.Lock()
	if errors.Is(cleanup, ErrUnresponsive) {
		p.unresponsive = true
	}
	restart := p.parent != nil && !p.unresponsive && (p.restartRound || !p.suppressRestart && p.restartEligibleLocked()) && !n.stopping.Load()
	if restart {
		p.parent.mu.Lock()
		restart = !p.parent.stopRequested
		p.parent.mu.Unlock()
	}
	failure := reason
	if failure == nil {
		failure = ErrStopped
	}
	var inlineFailed []*request
	if p.inlineCallsUsed.Load() {
		inlineFailed = p.failInlineRequestsLocked(failure)
	}
	var saved []envelope
	var released []func()
	keep := make(map[*request]bool)
	for p.mailbox.size > 0 {
		env := p.mailbox.pop()
		if restart && p.opts.mailPolicy == PreserveRef && env.toRef && (env.request == nil || !env.request.completed) {
			saved = append(saved, env)
			if env.request != nil {
				keep[env.request] = true
			}
		} else if env.release != nil {
			released = append(released, env.release)
		}
	}
	if restart {
		p.spec.saved = saved
		n.routes[p.path] = route{p, ErrRestarting}
	} else if r := n.routes[p.path]; r.process == p {
		if p.unresponsive {
			n.routes[p.path] = route{p, ErrUnresponsive}
		} else {
			delete(n.routes, p.path)
		}
	}
	failed := append(inlineFailed, n.failRequestsLocked(p, failure, keep)...)
	n.publishPIDLocked(p, false)
	delete(n.processes, p.pid.ID)
	p.mailbox = mailbox{limit: p.mailbox.limit}
	p.system = nil
	p.cancelAsyncRequestsLocked()
	if p.inspection != nil {
		p.inspection.done <- inspectionResult{err: failure}
		p.inspection = nil
	}
	p.finished = true
	p.running = false
	p.stopErr = cleanup
	round := p.restartRound
	p.mu.Unlock()
	observationReason := reason
	if p.unresponsive {
		observationReason = ErrUnresponsive
	}
	fatal := n.endObservationsLocked(p, observationReason)
	n.mu.Unlock()
	if p.parent != nil {
		if !restart {
			p.parent.removeSpecLocked(p.spec)
		}
		p.parent.childrenMu.Unlock()
	}
	for _, r := range failed {
		r.finish()
	}
	for _, release := range released {
		release()
	}
	for _, action := range fatal {
		action.run()
	}
	return restart, round
}

func (p *process) removeSpecLocked(spec *childSpec) {
	if spec.removed {
		return
	}
	// Shutdown visits children in reverse creation order. Search from that end
	// so retiring a wide, idle supervisor tree does not take quadratic time.
	for i := len(p.children) - 1; i >= 0; i-- {
		if p.children[i] == spec {
			copy(p.children[i:], p.children[i+1:])
			p.children[len(p.children)-1] = nil
			p.children = p.children[:len(p.children)-1]
			spec.removed = true
			return
		}
	}
}
func (p *process) discardChild(spec *childSpec, old *process) {
	p.childrenMu.Lock()
	if spec.process != old {
		p.childrenMu.Unlock()
		return
	}
	p.removeSpecLocked(spec)
	spec.saved = nil
	p.node.mu.Lock()
	r := p.node.routes[spec.path]
	if r.process == old && !errors.Is(r.err, ErrUnresponsive) {
		delete(p.node.routes, spec.path)
	}
	var inlineFailed []*request
	if old.inlineCallsUsed.Load() {
		old.mu.Lock()
		inlineFailed = old.failInlineRequestsLocked(ErrStopped)
		old.mu.Unlock()
	}
	failed := append(inlineFailed, p.node.failRequestsLocked(old, ErrStopped, nil)...)
	p.node.mu.Unlock()
	p.childrenMu.Unlock()
	for _, r := range failed {
		r.finish()
	}
}

func (p *process) restartChild(fault childFailure) {
	p.childrenMu.Lock()
	index := -1
	for i, spec := range p.children {
		if spec == fault.spec && spec.process == fault.old {
			index = i
			break
		}
	}
	if index < 0 {
		p.childrenMu.Unlock()
		return
	}
	var affected []*childSpec
	switch p.strategy {
	case OneForOne:
		affected = []*childSpec{fault.spec}
	case OneForAll:
		affected = append(affected, p.children...)
	case RestForOne:
		affected = append(affected, p.children[index:]...)
	}
	p.childrenMu.Unlock()
	for {
		if p.ctx.Err() != nil {
			return
		}
		now := time.Now()
		first := 0
		for first < len(p.restartTimes) && now.Sub(p.restartTimes[first]) >= p.restartWindow {
			first++
		}
		p.restartTimes = append(p.restartTimes[:0], p.restartTimes[first:]...)
		if len(p.restartTimes) >= p.maxRestarts {
			p.requestStop(ErrMaxRestartsExceeded, false)
			return
		}
		p.restartTimes = append(p.restartTimes, now)
		for i := len(affected) - 1; i >= 0; i-- {
			spec := affected[i]
			p.childrenMu.Lock()
			old := spec.process
			p.childrenMu.Unlock()
			old.mu.Lock()
			if !old.finished && spec.opts.restart != Temporary {
				old.restartRound = true
			}
			old.mu.Unlock()
			if err := p.node.stopProcess(p.ctx, old, ErrRestarting, true); err != nil {
				p.requestStop(err, false)
				return
			}
		}
		retry := false
		for _, spec := range affected {
			if spec.opts.restart == Temporary {
				continue
			}
			if err := p.startReplacement(spec); err != nil {
				if errors.Is(err, ErrUnresponsive) || p.ctx.Err() != nil {
					p.requestStop(err, false)
					return
				}
				retry = true
				break
			}
		}
		if !retry {
			return
		}
	}
}

func (parent *process) startReplacement(spec *childSpec) error {
	parent.childrenMu.Lock()
	n := parent.node
	n.mu.Lock()
	if n.stopping.Load() || parent.ctx.Err() != nil {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		return ErrStopped
	}
	if len(n.processes) >= n.opts.MaxActors {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		return ErrCapacity
	}
	pid, err := n.allocatePID()
	if err != nil {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		return err
	}
	p := newProcess(n, parent, spec.path, pid, spec.factory, spec.opts)
	p.restarts = spec.process.restarts + 1
	p.ownership = spec.process.ownership // old executor retired before replacement starts
	p.spec = spec
	p.restartRound = true
	var releases []func()
	for _, env := range spec.saved {
		if env.request != nil {
			if env.request.completed {
				if env.release != nil {
					releases = append(releases, env.release)
				}
				continue
			}
			env.request.owner = pid
			p.callsUsed.Store(true)
		}
		_ = p.mailbox.push(env)
	}
	spec.saved = nil
	spec.process = p
	n.processes[pid.ID] = p
	n.routes[spec.path] = route{p, ErrRestarting}
	n.mu.Unlock()
	parent.childrenMu.Unlock()
	for _, release := range releases {
		release()
	}
	p.scheduleStart()
	if err := n.waitStarted(parent.ctx, p); err != nil {
		return err
	}
	return p.startErr
}
