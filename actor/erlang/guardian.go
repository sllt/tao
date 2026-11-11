package erlangactor

import (
	"context"
	"errors"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

const desiredGuardianGroupSize = 128

type guardianWatchCmd struct {
	target actor.PID
}

type guardianWatchAck struct {
	ref    actor.MonitorRef
	ticket actor.DeferredReply
}

type guardianUnwatchCmd struct {
	ref actor.MonitorRef
}

type guardianGroup struct {
	pid       actor.PID
	count     int
	saturated bool
}

type targetWatch struct {
	target   actor.PID
	group    *guardianGroup
	ref      actor.MonitorRef
	down     bool
	reason   error
	bindings map[*Binding]struct{}
}

func (b *Bridge) spawnGuardian() (*guardianGroup, error) {
	pid, err := b.taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case actor.Started, actor.Stopped:
			return nil
		case guardianWatchCmd:
			ref, err := a.Monitor(m.target)
			if err != nil {
				_ = a.ReplyError(err)
				return nil
			}
			// If m.target was already dead, Node.monitor already queued monitorNotice into
			// this guardian's system queue, which runs strictly before mailbox messages.
			// Deferring the reply behind one self-mailbox turn guarantees early Down is
			// observed before guardianWatch returns (§9.3).
			ticket := a.Defer()
			if sendErr := a.Send(a.PID(), guardianWatchAck{ref: ref, ticket: ticket}); sendErr != nil {
				_ = ticket.Reply(ref)
			}
			return nil
		case guardianWatchAck:
			_ = m.ticket.Reply(m.ref)
			return nil
		case guardianUnwatchCmd:
			a.Demonitor(m.ref)
			_ = a.Reply(struct{}{})
			return nil
		case actor.Down:
			b.onTargetDown(m.Ref, m.PID, m.Reason)
			return nil
		default:
			return nil
		}
	}))
	if err != nil {
		return nil, err
	}
	return &guardianGroup{pid: pid}, nil
}

func (b *Bridge) guardianWatch(ctx context.Context, g *guardianGroup, target actor.PID) (actor.MonitorRef, error) {
	return b.taoNode.Call[actor.MonitorRef](ctx, g.pid, guardianWatchCmd{target: target})
}

func (b *Bridge) guardianUnwatch(g *guardianGroup, ref actor.MonitorRef) {
	select {
	case <-b.taoNode.Done():
		return
	default:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _ = b.taoNode.Call[struct{}](ctx, g.pid, guardianUnwatchCmd{ref: ref})
}

func (b *Bridge) stopGuardian(g *guardianGroup) {
	if g == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = b.taoNode.StopActor(ctx, g.pid)
}

// attachWatch installs or shares a guardian Monitor on localPID (§9.3).
// If localPID already exited before registration finished, alreadyDown is true with downErr.
func (b *Bridge) attachWatch(localPID actor.PID, binding *Binding) (alreadyDown bool, downErr error, err error) {
	// Fast path: share an existing live watch for the same localPID.
	b.watchMu.Lock()
	if b.closed.Load() {
		b.watchMu.Unlock()
		return false, nil, ErrBridgeClosed
	}
	if tw, ok := b.watches[localPID]; ok && !tw.down {
		tw.bindings[binding] = struct{}{}
		b.watchMu.Unlock()
		return false, nil, nil
	}
	b.watchMu.Unlock()

	b.guardianOpMu.Lock()
	defer b.guardianOpMu.Unlock()

	b.watchMu.Lock()
	if b.closed.Load() {
		b.watchMu.Unlock()
		return false, nil, ErrBridgeClosed
	}
	if tw, ok := b.watches[localPID]; ok && !tw.down {
		tw.bindings[binding] = struct{}{}
		b.watchMu.Unlock()
		return false, nil, nil
	}
	var chosen *guardianGroup
	for _, g := range b.guardians {
		if !g.saturated && g.count < desiredGuardianGroupSize {
			chosen = g
			break
		}
	}
	chosenWasEmpty := chosen != nil && chosen.count == 0
	b.watchMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var usedGroup *guardianGroup
	var monRef actor.MonitorRef

	if chosen != nil {
		ref, watchErr := b.guardianWatch(ctx, chosen, localPID)
		if watchErr == nil {
			usedGroup = chosen
			monRef = ref
		} else if errors.Is(watchErr, actor.ErrCapacity) && !chosenWasEmpty {
			// Non-empty guardian hit MaxObservationsPerActor (< 128): mark saturated and try
			// at most ONE new candidate guardian (§9.3).
			b.watchMu.Lock()
			chosen.saturated = true
			b.watchMu.Unlock()

			cand, spawnErr := b.spawnGuardian()
			if spawnErr != nil {
				return false, nil, spawnErr
			}
			candRef, candErr := b.guardianWatch(ctx, cand, localPID)
			if candErr != nil {
				b.stopGuardian(cand)
				return false, nil, candErr
			}
			b.watchMu.Lock()
			if b.closed.Load() {
				b.watchMu.Unlock()
				b.stopGuardian(cand)
				return false, nil, ErrBridgeClosed
			}
			b.guardians = append(b.guardians, cand)
			b.watchMu.Unlock()
			usedGroup = cand
			monRef = candRef
		} else {
			return false, nil, watchErr
		}
	} else {
		cand, spawnErr := b.spawnGuardian()
		if spawnErr != nil {
			return false, nil, spawnErr
		}
		candRef, candErr := b.guardianWatch(ctx, cand, localPID)
		if candErr != nil {
			// Empty candidate guardian failed on its first Monitor: recycle it (§9.3).
			b.stopGuardian(cand)
			return false, nil, candErr
		}
		b.watchMu.Lock()
		if b.closed.Load() {
			b.watchMu.Unlock()
			b.stopGuardian(cand)
			return false, nil, ErrBridgeClosed
		}
		b.guardians = append(b.guardians, cand)
		b.watchMu.Unlock()
		usedGroup = cand
		monRef = candRef
	}

	b.watchMu.Lock()
	if reason, earlyDown := b.earlyDowns[monRef]; earlyDown {
		delete(b.earlyDowns, monRef)
		b.watchMu.Unlock()
		if reason == nil {
			reason = actor.ErrNoProcess
		}
		return true, reason, nil
	}
	if b.closed.Load() {
		b.watchMu.Unlock()
		b.guardianUnwatch(usedGroup, monRef)
		return false, nil, ErrBridgeClosed
	}
	usedGroup.count++
	tw := &targetWatch{
		target: localPID,
		group:  usedGroup,
		ref:    monRef,
		bindings: map[*Binding]struct{}{
			binding: {},
		},
	}
	b.watches[localPID] = tw
	b.refToTarget[monRef] = localPID
	b.watchMu.Unlock()
	return false, nil, nil
}

func (b *Bridge) detachBindingWatch(binding *Binding) {
	var unwatchGroup *guardianGroup
	var unwatchRef actor.MonitorRef

	b.watchMu.Lock()
	tw, ok := b.watches[binding.localPID]
	if ok {
		delete(tw.bindings, binding)
		if len(tw.bindings) == 0 && !tw.down {
			delete(b.watches, binding.localPID)
			delete(b.refToTarget, tw.ref)
			if tw.group != nil && tw.group.count > 0 {
				tw.group.count--
				tw.group.saturated = false
			}
			unwatchGroup = tw.group
			unwatchRef = tw.ref
		}
	}
	b.watchMu.Unlock()

	if unwatchGroup != nil {
		b.guardianUnwatch(unwatchGroup, unwatchRef)
	}
}

func (b *Bridge) onTargetDown(ref actor.MonitorRef, pid actor.PID, reason error) {
	var affected []*Binding

	b.watchMu.Lock()
	targetPID, mapped := b.refToTarget[ref]
	if !mapped {
		// Down arrived before attachWatch recorded refToTarget (e.g. target did not exist).
		b.earlyDowns[ref] = reason
		b.watchMu.Unlock()
		return
	}
	delete(b.refToTarget, ref)
	if tw, ok := b.watches[targetPID]; ok && tw.ref == ref {
		tw.down = true
		tw.reason = reason
		if tw.group != nil && tw.group.count > 0 {
			tw.group.count--
			tw.group.saturated = false
		}
		delete(b.watches, targetPID)
		affected = make([]*Binding, 0, len(tw.bindings))
		for bd := range tw.bindings {
			affected = append(affected, bd)
		}
		tw.bindings = nil
	}
	b.watchMu.Unlock()

	erlReason := mapDownReason(reason)
	for _, bd := range affected {
		bd.requestTerminate(erlReason, false)
	}
}

// mapDownReason translates a Tao Actor stop/crash/migration error into the §10.4 / §11.2
// Erlang reason vocabulary without dynamically creating atoms from error text.
func mapDownReason(err error) etf.Term {
	if err == nil || errors.Is(err, actor.ErrStopped) {
		return etf.Atom("normal")
	}
	if errors.Is(err, actor.ErrNoProcess) || errors.Is(err, actor.ErrStalePID) {
		return etf.Atom("noproc")
	}
	if errors.Is(err, actor.ErrClosed) {
		return etf.Atom("shutdown")
	}
	if errors.Is(err, actor.ErrHandedOff) {
		return erlang.TaoReason("handed_off", err.Error())
	}
	if errors.Is(err, actor.ErrReplaced) {
		return erlang.TaoReason("replaced", err.Error())
	}
	if errors.Is(err, actor.ErrMigrating) {
		return erlang.TaoReason("migrating", err.Error())
	}
	if errors.Is(err, actor.ErrProcessCrashed) {
		return erlang.TaoReason("crashed", err.Error())
	}
	return erlang.TaoReason("stopped", err.Error())
}
