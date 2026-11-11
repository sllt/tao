package actor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type process struct {
	batchState                                             atomic.Uint32 // count of unclaimed pure messages plus control interrupt
	incomingRequests                                       atomic.Pointer[actorRequests]
	asyncRequests                                          map[RequestID]*asyncRequest // mu; allocated only for Request users
	executor                                               atomic.Pointer[executorTurn]
	created                                                time.Time
	restarts                                               uint64
	metrics                                                atomic.Pointer[ReceiveMetrics]
	callsUsed                                              atomic.Bool        // monotonic: this instance has owned a Call
	inspection                                             *inspectionRequest // mu
	handoff                                                *handoffOperation  // mu
	change                                                 *replaceOperation  // mu
	sealed                                                 bool               // mu
	preparing                                              bool               // executor only
	snapshot                                               *snapshotCapture   // executor only
	ownership                                              RouteRecord        // mu; registration/retirement by executor
	node                                                   *Node
	parent                                                 *process
	pid                                                    PID
	selfAddr                                               Addr // immutable PID boxed once, shared by outgoing envelopes
	path                                                   string
	factory                                                Factory
	opts                                                   spawnOptions
	behavior                                               Behavior
	log                                                    atomic.Pointer[slog.Logger]
	ctx                                                    context.Context
	cancel                                                 context.CancelCauseFunc
	mu                                                     sync.Mutex
	mailbox                                                mailbox
	running, booted, internal                              bool
	stopRequested, suppressRestart, finished, unresponsive bool
	stopReason                                             error
	started                                                chan struct{}
	startErr                                               error
	done                                                   chan struct{}
	stopErr                                                error
	childrenMu                                             sync.Mutex
	children                                               []*childSpec
	spec                                                   *childSpec
	system                                                 []any
	restartRound                                           bool
	strategy                                               Strategy
	maxRestarts                                            int
	restartWindow                                          time.Duration
	restartTimes                                           []time.Time
	trapExit                                               bool
	observationsEnded                                      bool        // node.mu
	published                                              atomic.Bool // stores hold mu; indexes precede true
	current                                                envelope    // executor only; zero between Receive calls
	inFlightCall                                           *request    // mu; popped inline PID Call until its turn finishes
	inlineCallsUsed                                        atomic.Bool // monotonic: this instance has owned an inline PID Call
}

func newProcess(node *Node, parent *process, path string, pid PID, factory Factory, opts spawnOptions) *process {
	ctx, cancel := context.WithCancelCause(context.Background())
	p := &process{node: node, parent: parent, pid: pid, path: path, factory: factory, opts: opts, ctx: ctx, cancel: cancel, running: true, mailbox: mailbox{limit: opts.mailbox}, started: make(chan struct{}), done: make(chan struct{}), maxRestarts: 5, restartWindow: 10 * time.Second, created: time.Now()}
	p.selfAddr = pid
	if node.diagnostics.Load() {
		p.metrics.Store(&ReceiveMetrics{Since: p.created})
	}
	return p
}

func (p *process) logger() *slog.Logger {
	if logger := p.log.Load(); logger != nil {
		return logger
	}
	logger := p.node.opts.Logger.With("pid", p.pid.String(), "path", p.path)
	if p.log.CompareAndSwap(nil, logger) {
		return logger
	}
	return p.log.Load()
}

func (p *process) enqueue(message envelope) error {
	return p.enqueueAdmitted(message, nil)
}

const batchInterrupted uint32 = 1 << 31

// mu protects the physical mailbox; the executor alone claims the batch.
func (p *process) queuedLocked() int {
	return p.mailbox.size + int(p.batchState.Load()&^batchInterrupted)
}

// Publish control state under mu. Claim and interrupt share one CAS order,
// so a message either starts before the control operation or is restored.
func (p *process) interruptBatchLocked() {
	// A new batch can only start under mu. Without held messages the ordinary
	// loop observes the control fields directly; no interrupt publication is needed.
	if p.batchState.Load()&^batchInterrupted != 0 {
		p.batchState.Or(batchInterrupted)
	}
}

func (p *process) enqueueAdmitted(message envelope, accepted func() error) error {
	return p.enqueueFrom(message, accepted, nil)
}

func (p *process) enqueueFrom(message envelope, accepted func() error, source *process) error {
	p.mu.Lock()
	if p.sealed {
		p.mu.Unlock()
		return ErrMigrating
	}
	if message.global != nil && (p.ownership.PID() != message.global.PID() || p.ownership.Epoch != message.global.Epoch || p.ownership.Path != message.global.Path) {
		p.mu.Unlock()
		return ErrRedirect
	}
	if p.stopRequested || p.finished {
		p.mu.Unlock()
		return ErrStopped
	}
	if message.request != nil && (p.unresponsive || (message.request.inline && p.node.stopping.Load())) {
		p.mu.Unlock()
		return ErrStopped
	}
	// A producer holds mu, so no new batch can be detached concurrently.
	// With more than a full batch of spare slots, even the largest possible
	// unclaimed suffix leaves room for this message. Equality needs the count.
	held := 0
	if p.mailbox.limit-p.mailbox.size <= len(mailboxBatch{}) {
		held = int(p.batchState.Load() &^ batchInterrupted)
	}
	if err := p.mailbox.pushReserved(message, held); err != nil {
		if metrics := p.metrics.Load(); metrics != nil {
			metrics.MailboxRejected++
		}
		p.mu.Unlock()
		return err
	}
	if metrics := p.metrics.Load(); metrics != nil {
		metrics.MailboxHighWater = max(metrics.MailboxHighWater, p.queuedLocked())
	}
	// Queue the transport acceptance before the executor can release delivery
	// credit. An ACK failure cannot retract a message already in the mailbox.
	if accepted != nil {
		_ = accepted()
	}
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	if start {
		if source != nil {
			if turn := source.executor.Load(); turn != nil && !turn.multiSend && turn.offer(p) {
				return nil
			}
		}
		if message.request != nil {
			p.scheduleCall()
		} else {
			p.schedule()
		}
	}
	return nil
}

func (p *process) requestStop(reason error, suppress bool) {
	p.releaseNext()
	p.mu.Lock()
	if p.finished {
		p.mu.Unlock()
		return
	}
	if !p.stopRequested {
		p.stopRequested = true
		p.stopReason = reason
	}
	p.suppressRestart = p.suppressRestart || suppress
	p.cancelAsyncRequestsLocked()
	p.interruptBatchLocked()
	var keep map[*request]bool
	if p.opts.mailPolicy == PreserveRef && (p.restartRound || !p.suppressRestart && p.restartEligibleLocked()) {
		keep = make(map[*request]bool)
		for i := 0; i < p.mailbox.size; i++ {
			env := p.mailbox.at(i)
			if env.toRef && env.request != nil {
				keep[env.request] = true
			}
		}
	}
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	p.cancel(reason)
	failure := reason
	if failure == nil {
		failure = ErrStopped
	}
	p.node.failRequestsExcept(p, failure, keep)
	if p.internal {
		p.node.mu.Lock()
		p.node.stopping.Store(true)
		p.node.mu.Unlock()
		p.node.executors.close()
	}
	if start {
		p.schedule()
	}
}

func (p *process) run() {
	p.executor.Store(nil)
	var batch *mailboxBatch
	p.runWithBatch(&batch)
}

type mailboxBatch [16]mailboxMessage

// A cached executor reuses this storage across runs. Single-message and Call
// turns do not allocate or zero a batch; every consumed/restored slot is cleared.
// A non-nil result owns a taken next claim. The executor must clear this
// process's old context and requeue its still-running claim before using next.
func (p *process) runWithBatch(storage **mailboxBatch) *process {
	turn := p.executor.Load()
	if !p.booted {
		p.mu.Lock()
		stopping := p.stopRequested
		p.mu.Unlock()
		var err error
		if stopping {
			err = ErrStopped
		} else {
			p.behavior, err = construct(p.factory, p)
			if err == nil {
				err = p.invoke(envelope{message: Started{}})
				p.releaseNext()
			}
		}
		p.booted = true
		p.mu.Lock()
		stopping = p.stopRequested
		p.mu.Unlock()
		if err == nil && stopping {
			err = ErrStopped
		}
		if err == nil {
			err = p.registerGlobal()
		}
		if err != nil {
			p.startErr = err
			p.requestStop(err, true)
			p.finish()
			close(p.started)
			return nil
		}
		p.mu.Lock()
		if p.stopRequested || p.unresponsive || p.node.stopping.Load() {
			p.mu.Unlock()
			p.startErr = ErrStopped
			p.requestStop(ErrStopped, true)
			p.finish()
			close(p.started)
			return nil
		}
		p.restartRound = false
		p.node.publishPIDLocked(p, true)
		p.mu.Unlock()
		close(p.started)
	}
	count, systemBurst := 0, 0
	var batchStart time.Time
	if turn != nil {
		// An older worker timestamp can cause an early yield, never omit the
		// beginning of this turn. Single-message turns need no clock read.
		batchStart = turn.budgetStart
	} else {
		batchStart = time.Now()
	}
	batch := *storage
	var restore []mailboxMessage
	freshBudget := turn != nil
	for {
		p.mu.Lock()
		p.inFlightCall = nil
		afterPureBatch := restore != nil
		if restore != nil {
			if len(restore) > 0 {
				p.mailbox.prependPure(restore)
				clear(restore)
			}
			p.batchState.Store(0)
			restore = nil
		}
		if afterPureBatch && turn != nil && p.mailbox.size > 0 && len(p.system) == 0 &&
			!p.stopRequested && p.change == nil && p.handoff == nil {
			if next := turn.take(); next != nil {
				// The private suffix is restored and cleared. Keep running=true:
				// this claim transfers to ready, never through the idle state.
				p.mu.Unlock()
				return next
			}
		}
		if turn != nil && turn.pending.Load() && (p.mailbox.size > 0 || len(p.system) > 0 || p.stopRequested || p.change != nil || p.handoff != nil) {
			p.mu.Unlock()
			turn.flush()
			continue
		}
		if p.stopRequested {
			change, moving := p.change, p.handoff
			p.mu.Unlock()
			if change != nil {
				p.finishReplacement(change, ErrStopped)
			}
			if moving != nil {
				p.finishHandoff(moving, ErrStopped)
			}
			p.finish()
			return nil
		}
		if change := p.change; change != nil {
			p.mu.Unlock()
			if p.replace(change) {
				return nil
			}
			continue
		}
		if moving := p.handoff; moving != nil {
			p.mu.Unlock()
			if p.runHandoff(moving) {
				return nil
			}
			continue
		}
		if freshBudget {
			// A queued turn should get its own slice rather than repeatedly
			// yielding against time spent idle or on another Actor. A lone
			// message can borrow the earlier timestamp without a clock read.
			if len(p.system)+p.mailbox.size > 1 {
				batchStart = time.Now()
				turn.budgetStart = batchStart
			}
			freshBudget = false
		}
		// Time starts before the first message. Sampling every eight handled
		// messages trades up to seven handler durations for fewer clock reads;
		// control state is still checked at each message boundary.
		if count > 0 && (len(p.system) > 0 || p.mailbox.size > 0) &&
			(count == 64 || count&7 == 0 && time.Since(batchStart) >= time.Millisecond) {
			p.mu.Unlock()
			runtime.Gosched()
			count, batchStart = 0, time.Now()
			if turn != nil {
				turn.budgetStart = batchStart
			}
			continue
		}
		if len(p.system) > 0 && (systemBurst < 32 || p.mailbox.size == 0) {
			message := p.system[0]
			p.system[0] = nil
			p.system = p.system[1:]
			p.mu.Unlock()
			systemBurst++
			if inspection, ok := message.(*inspectionRequest); ok {
				p.runInspection(inspection)
			} else if request, ok := message.(*asyncRequest); ok {
				if result, deliver := p.takeAsyncResult(request); deliver {
					if err := p.invoke(envelope{message: result}); err != nil {
						p.requestStop(&CrashError{PID: p.pid, Cause: err}, false)
					}
				}
			} else if fault, ok := message.(childFailure); ok {
				p.restartChild(fault)
			} else if delivered, ok := p.node.consumeNotification(message); ok {
				if err := p.invoke(envelope{message: delivered}); err != nil {
					p.requestStop(&CrashError{PID: p.pid, Cause: err}, false)
				}
			}
			count++
			continue
		}
		if p.mailbox.size == 0 {
			p.running = false
			p.mu.Unlock()
			return nil
		}
		if metrics := p.metrics.Load(); metrics != nil {
			metrics.MailboxHighWater = max(metrics.MailboxHighWater, p.mailbox.size)
		}
		systemBurst = 0
		if len(p.system) == 0 && p.mailbox.size > 1 {
			if batch == nil && p.mailbox.purePrefix(2) == 2 {
				batch = new(mailboxBatch)
				*storage = batch
			}
			if batch != nil {
				if n := p.mailbox.takePure(batch[:]); n > 0 {
					p.batchState.Store(uint32(n))
					p.mu.Unlock()
					restore = p.runPureBatch(batch[:n], &count, &batchStart, turn)
					continue
				}
			}
		}
		message := p.mailbox.pop()
		if message.request != nil && message.request.inline {
			p.inFlightCall = message.request
		}
		p.mu.Unlock()
		if message.request != nil {
			if err := message.request.contextErr(); err != nil {
				if message.release != nil {
					message.release()
				}
				p.node.expireRequest(message.request, err)
				continue
			}
		}
		if err := p.invoke(message); err != nil {
			p.requestStop(&CrashError{PID: p.pid, Cause: err}, false)
		}
		count++
	}
}

func (p *process) runPureBatch(batch []mailboxMessage, count *int, began *time.Time, turn *executorTurn) []mailboxMessage {
	index := 0
	for index < len(batch) {
		state := p.batchState.Load()
		if state&batchInterrupted != 0 {
			break
		}
		if !p.batchState.CompareAndSwap(state, state-1) {
			continue
		}
		value := batch[index]
		batch[index] = mailboxMessage{}
		index++
		if err := p.invoke(envelope{message: value.message, sender: value.sender}); err != nil {
			p.requestStop(&CrashError{PID: p.pid, Cause: err}, false)
		}
		// A held target keeps its independent watch across Receives. Explicit
		// Actor operations and the outer batch boundary still release it.
		*count++
		if index < len(batch) && (*count == 64 || *count&7 == 0 && time.Since(*began) >= time.Millisecond) {
			runtime.Gosched()
			*count = 0
			*began = time.Now()
			if turn != nil {
				turn.budgetStart = *began
			}
		}
	}
	// The outer loop restores the suffix and clears even a late interrupt
	// under its next mailbox lock, before observing control state or going idle.
	return batch[index:]
}

func construct(factory Factory, p *process) (behavior Behavior, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("factory panic: %v", value)
			behavior = nil
		}
	}()
	behavior = factory()
	if behavior == nil {
		return nil, fmt.Errorf("actor: factory returned nil")
	}
	if err = behavior.bindProcess(p); err != nil {
		return nil, err
	}
	return behavior, nil
}

func (p *process) invoke(message envelope) (err error) {
	turn := p.executor.Load()
	metrics := p.metrics.Load()
	if metrics != nil {
		p.beginReceive(metrics, message.message)
	}
	p.current = message
	defer func() {
		if turn != nil {
			if turn.offeredInReceive {
				turn.offeredInReceive = false
			}
			if turn.multiSend {
				turn.target, turn.multiSend = nil, false
			}
		}
		if value := recover(); value != nil {
			err = fmt.Errorf("Receive panic: %v", value)
		}
		if metrics != nil {
			p.endReceive(metrics, err)
		}
		p.current = envelope{}
		if message.release != nil {
			message.release()
		}
		if r := message.request; r != nil && err == nil {
			if r.settled.Load() {
				// Reply already published completion, including incoming Calls
				// that never needed a waiting channel.
				return
			}
			missing := r.missingReply()
			if missing {
				p.releaseNext()
				_ = p.node.noReply(r, p.pid)
			}
		}
	}()
	return p.behavior.Receive(message.message)
}

func (p *process) finish() {
	deadline := time.Now().Add(p.opts.shutdown)
	watch := time.AfterFunc(time.Until(deadline), func() { p.node.markUnresponsive(p) })
	defer watch.Stop()
	p.childrenMu.Lock()
	children := make([]*process, 0, len(p.children))
	for _, spec := range p.children {
		children = append(children, spec.process)
	}
	p.childrenMu.Unlock()
	ctx := context.Background()
	// A leaf without a Directory has no operation consuming this context.
	// Its Stopped callback still has exactly the same shutdown watchdog.
	if len(children) > 0 || p.node.opts.Directory != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	var cleanup error
	for i := len(children) - 1; i >= 0; i-- {
		if err := p.node.stopProcess(ctx, children[i], nil, true); err != nil {
			cleanup = errors.Join(cleanup, err)
		}
		p.discardChild(children[i].spec, children[i])
	}
	p.mu.Lock()
	reason := p.stopReason
	p.mu.Unlock()
	if !p.internal && p.behavior != nil {
		if err := p.invoke(envelope{message: Stopped{Reason: reason}}); err != nil {
			p.logger().Error("Stopped callback failed", "error", err)
		}
	}
	restart, round := p.retire(reason, cleanup)
	if !restart {
		p.releaseGlobal(ctx)
	}
	p.childrenMu.Lock()
	p.children = nil
	p.childrenMu.Unlock()
	close(p.done)
	if restart && !round && !p.parent.enqueueSystem(childFailure{p.spec, p}) {
		p.parent.discardChild(p.spec, p)
	}
}
