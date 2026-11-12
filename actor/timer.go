package actor

import (
	"context"
	"sync/atomic"
	"time"
)

// TimerRef tracks one scheduled send to a physical Actor instance. Done closes
// after admission or cancellation, not after Receive. The zero value is inert:
// Cancel returns false, Done returns nil, and Err returns nil.
type TimerRef struct{ timer *actorTimer }

type actorTimer struct {
	owner   atomic.Pointer[process]
	timer   *time.Timer // owner.mu, until the callback claims it
	message any         // owner.mu, until the callback claims it
	done    chan struct{}
	err     error // published by closing done
}

// SendAfter schedules message for this instance's mailbox. Non-positive delays
// are immediately eligible. Stop/restart cancels pending timers; Replace and
// Handoff do not transfer them. Message ownership follows ordinary local Send.
// Use TimerRef.Err after Done to observe refusal, including ErrMailboxFull.
func (a *Actor) SendAfter(delay time.Duration, message any) (TimerRef, error) {
	p := a.proc
	p.releaseNext()
	if err := p.preparationError(); err != nil {
		return TimerRef{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// Node.Stop visits siblings in reverse order. Until this instance is asked
	// to stop, its current handler may still schedule work just like local Send.
	if p.stopRequested || p.finished || p.unresponsive {
		return TimerRef{}, ErrStopped
	}
	if p.sealed {
		return TimerRef{}, ErrMigrating
	}
	t := &actorTimer{message: message, done: make(chan struct{})}
	t.owner.Store(p)
	if p.timers == nil {
		p.timers = make(map[*actorTimer]struct{})
	}
	p.timers[t] = struct{}{}
	t.timer = time.AfterFunc(delay, t.fire)
	return TimerRef{timer: t}, nil
}

// Cancel returns true only if it prevented the callback from claiming the send.
// It cannot retract a send already claimed or a message already in the mailbox.
func (ref TimerRef) Cancel() bool {
	if ref.timer == nil {
		return false
	}
	t := ref.timer
	p := t.owner.Load()
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if t.owner.Load() != p {
		return false
	}
	t.cancelLocked(p, context.Canceled)
	return true
}

func (ref TimerRef) Done() <-chan struct{} {
	if ref.timer == nil {
		return nil
	}
	return ref.timer.done
}

// Err is nil while pending, and after successful mailbox admission. Otherwise
// it reports explicit cancellation, owner termination, or the admission error.
func (ref TimerRef) Err() error {
	if ref.timer != nil {
		select {
		case <-ref.timer.done:
			return ref.timer.err
		default:
		}
	}
	return nil
}

func (t *actorTimer) fire() {
	p := t.owner.Load()
	if p == nil {
		return
	}
	p.mu.Lock()
	if t.owner.Load() != p {
		p.mu.Unlock()
		return
	}
	message := t.message
	delete(p.timers, t)
	t.message, t.timer = nil, nil
	t.owner.Store(nil)
	p.mu.Unlock()
	// A concurrent Stop still rejects this send through normal mailbox admission.
	t.err = p.enqueue(envelope{message: message, sender: p.selfAddr})
	close(t.done)
}

func (t *actorTimer) cancelLocked(p *process, err error) {
	delete(p.timers, t)
	t.timer.Stop()
	t.message, t.timer = nil, nil
	t.owner.Store(nil)
	t.err = err
	close(t.done)
}

func (p *process) cancelTimersLocked() {
	for timer := range p.timers {
		timer.cancelLocked(p, ErrStopped)
	}
	p.timers = nil
}
