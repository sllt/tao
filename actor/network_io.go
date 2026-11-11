package actor

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/sllt/tao/taonet"
)

func (w *network) reserve(charge int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reserveLocked(charge)
}

func (w *network) reserveLocked(charge int) error {
	if w.closed || w.draining {
		return ErrClosed
	}
	if charge > w.opts.SendBytes-w.bytes || w.messages >= w.opts.SendMessages {
		return ErrCapacity
	}
	w.bytes += charge
	w.messages++
	return nil
}

// Calls already have a bounded lifetime and a pending-call slot. Await send
// admission within that budget instead of rejecting a small message merely
// because other encoders temporarily reserved their maximum output size.
// No input is encoded or submitted until this reservation succeeds.
func (w *network) reserveCall(ctx context.Context, charge int) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.mu.Lock()
		err := w.reserveLocked(charge)
		if err != ErrCapacity {
			w.mu.Unlock()
			return err
		}
		if w.sendChanged == nil {
			w.sendChanged = make(chan struct{})
		}
		changed := w.sendChanged
		w.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		case <-w.ctx.Done():
			return ErrClosed
		}
	}
}

func (w *network) wakeSendersLocked() {
	if w.sendChanged != nil {
		close(w.sendChanged)
		w.sendChanged = nil
	}
}

func (w *network) release(charge int) {
	w.mu.Lock()
	w.bytes -= charge
	w.messages--
	w.wakeSendersLocked()
	w.mu.Unlock()
}

func (w *network) queueMessage(peer string, class taonet.TrafficClass, packet actorPacket, value any, sender PID, out *outgoingCall, result chan error) error {
	charge := 2 * (w.opts.MaxMessageBytes + 4096)
	if class == taonet.ClassReply && value == nil {
		// Error/rejection packets have bounded metadata and no business body.
		// Do not require a maximum payload reservation to report congestion.
		charge = 4096
	}
	var reserveErr error
	if out != nil {
		reserveErr = out.request.contextErr()
		if reserveErr == nil {
			reserveErr = w.reserve(charge)
			if reserveErr == ErrCapacity {
				reserveErr = w.reserveCall(out.request.context(), charge)
			}
		}
	} else {
		reserveErr = w.reserve(charge)
	}
	if reserveErr != nil {
		return reserveErr
	}
	return w.queueReservedMessage(peer, class, packet, value, sender, out, result, charge, nil)
}

func (w *network) queueReservedMessage(peer string, class taonet.TrafficClass, packet actorPacket, value any, sender PID, out *outgoingCall, result chan error, charge int, incoming *request) error {
	if sender.ID != 0 {
		from := sender
		packet.Sender = &from
	}
	// These final fixed-width fields are patched at actual submission time.
	if class == taonet.ClassInteractive {
		packet.Sequence = 1
	}
	if out != nil {
		packet.BudgetMS = 1
	}
	buffer := w.takePacketBuffer()
	if value == nil {
		// A cached growth array must not turn a small rejection/error packet's
		// 4KiB reservation into a large-message admission requirement.
		buffer.data = buffer.storage[:0]
	}
	payload, encoded, err := w.registry.encodePacket(buffer.data, packet, value)
	if err != nil {
		w.putPacketBuffer(buffer)
		w.release(charge)
		return err
	}
	buffer.data = payload
	// Owned Channel and reply Stream writes borrow this same array. Only
	// global redirects retain an original while Channel owns a separate copy.
	actual := buffer.retainedBytes()
	if cap(payload) > packetBufferRetainLimit && packet.Target.Kind != "global" {
		// Oversized configured messages remain valid, but their growth arrays
		// are not retained. Channel can own that array without this wrapper.
		w.putPacketBuffer(buffer)
		buffer = nil
		actual = cap(payload)
	}
	if packet.Target.Kind == "global" {
		actual += len(payload)
	}
	packet.Message = encoded
	job := w.takeJob()
	*job = networkJob{packet: packet, buffer: buffer, raw: payload, peer: peer, class: class, charge: actual, result: result, call: out, deadline: time.Now().Add(w.opts.SendTimeout)}
	if packet.Kind == "reply" || packet.Kind == "reject" {
		job.expectedBoot = packet.Key.Boot
	}
	w.mu.Lock()
	if actual-charge > w.opts.SendBytes-w.bytes {
		err = ErrCapacity
	} else {
		w.bytes -= charge - actual
		if charge > actual {
			w.wakeSendersLocked()
		}
		charge = actual
		err = w.enqueueLocked(job)
		if err == nil && incoming != nil {
			// Business completion already won. Keep cancellation/capacity state
			// until the reply is queued, then retire it in this same transaction.
			delete(w.incoming, incoming.remoteKey)
		}
	}
	w.mu.Unlock()
	if err != nil {
		w.releaseJobBuffer(job)
		w.release(charge)
		w.recycleJob(job)
		return err
	}
	return nil
}
func (w *network) queueControl(peer string, packet actorPacket, boot [16]byte) {
	if _, err := w.controlJob(peer, packet, boot, nil, nil, nil); err != nil {
		w.event(NetworkEvent{Peer: peer, Kind: "control_failed", RequestID: packet.Key.ID, Err: err})
	}
}
func (w *network) enqueue(job *networkJob) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enqueueLocked(job)
}

func (w *network) enqueueLocked(job *networkJob) error {
	if w.closed {
		return ErrClosed
	}
	key := pipeKey{job.peer, job.class}
	pipe := w.pipes[key]
	if pipe == nil {
		pipe = &actorPipe{key: key, wake: make(chan struct{}, 1)}
		w.pipes[key] = pipe
		w.wg.Add(1)
		go w.writePipe(pipe)
	}
	pipe.appendJob(job)
	if job.token != nil {
		w.controlJobs++
	}
	select {
	case pipe.wake <- struct{}{}:
	default:
	}
	return nil
}

const actorPipeWindow = 32

type pendingWrite struct {
	job     *networkJob
	ctx     context.Context
	cancel  context.CancelFunc
	receipt sendCompletion
	err     error
}

// Accept-confirmed sends and unconfirmed Calls share only completion waiting;
// the latter obtain their admission/business outcome from the Actor reply.
type sendCompletion interface {
	Done() <-chan struct{}
	IsDone() bool
	Released() <-chan struct{}
	Wait(context.Context) error
}

func (p *pendingWrite) ready() bool {
	if p.receipt == nil {
		return true
	}
	return p.receipt.IsDone() || p.ctx.Err() != nil
}
func (w *network) finishPending(p *pendingWrite) {
	err := p.err
	if p.receipt != nil {
		if waitErr := p.receipt.Wait(p.ctx); waitErr != nil {
			err = deliveryError(waitErr, Unknown)
		}
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.receipt != nil && p.job.buffer != nil {
		// Delivery settlement remains prompt even when cancellation returned
		// before physical I/O stopped reading the pooled input.
		w.settleJob(p.job, err)
		<-p.receipt.Released()
		w.releaseJob(p.job)
		w.recycleJob(p.job)
	} else {
		w.finishJob(p.job, err)
	}
	*p = pendingWrite{}
}
func sendBarrier(job *networkJob) bool {
	// Global redirects must remain ordered across ownership changes. Control
	// callbacks also retain their existing serial completion order.
	return job.packet.Target.Kind == "global" || job.class == taonet.ClassControl
}

func (w *network) writePipe(pipe *actorPipe) {
	defer w.wg.Done()
	if pipe.key.class == taonet.ClassReply {
		w.writeReplies(pipe)
		return
	}
	var pending [actorPipeWindow]pendingWrite
	var jobs [actorPipeWindow]*networkJob
	head, count := 0, 0
	defer func() {
		for count > 0 {
			p := &pending[head]
			if p.cancel != nil {
				p.cancel()
			}
			w.finishPending(p)
			head, count = (head+1)%len(pending), count-1
		}
		if pipe.channel != nil {
			_ = pipe.channel.Close()
		}
	}()
	for {
		for count > 0 && pending[head].ready() {
			w.finishPending(&pending[head])
			head, count = (head+1)%len(pending), count-1
		}
		w.mu.Lock()
		if w.closed {
			queued, head, size := pipe.queue, pipe.queueHead, pipe.queueSize
			pipe.queue = nil
			pipe.queueHead, pipe.queueSize = 0, 0
			w.mu.Unlock()
			for i := range size {
				job := queued[(head+i)&(len(queued)-1)]
				w.finishJob(job, deliveryError(ErrNodeUnreachable, NotSent))
			}
			return
		}
		var channelDone <-chan struct{}
		if pipe.channel != nil {
			channelDone = pipe.channel.Done()
		}
		if count == 0 && pipe.queued() == 0 {
			retire := channelDone == nil
			select {
			case <-channelDone:
				retire = true
			default:
			}
			if retire {
				// Enqueue uses the same lock, so the next send creates a new
				// writer rather than appending to one that is about to exit.
				delete(w.pipes, pipe.key)
				w.mu.Unlock()
				return
			}
		}
		n := 0
		// Drain only already-queued work into the unchanged in-flight window.
		// Global/control barriers still wait for all earlier receipts and run
		// alone. This avoids rechecking the head receipt and queue lock for
		// every message while a burst is being submitted.
		for n < pipe.queued() && count+n < len(pending) {
			candidate := pipe.jobAt(n)
			if sendBarrier(candidate) {
				if n == 0 && count == 0 {
					jobs[0], n = candidate, 1
				}
				break
			}
			jobs[n] = candidate
			n++
		}
		if n > 0 {
			pipe.dropJobs(n)
		}
		w.mu.Unlock()
		if n == 0 {
			var settled, expired <-chan struct{}
			if count > 0 {
				settled = pending[head].receipt.Done()
				if scope, ok := pending[head].ctx.(*requestScope); ok && scope.outgoing != nil {
					expired = scope.waitCanceled()
				} else {
					expired = pending[head].ctx.Done()
				}
			}
			select {
			case <-pipe.wake:
			case <-w.ctx.Done():
			case <-channelDone:
			case <-settled:
			case <-expired:
			}
			continue
		}
		for i := range n {
			job := jobs[i]
			jobs[i] = nil
			ctx, cancel := w.sendContext(job)
			if !sendBarrier(job) {
				receipt, err := w.submitJob(ctx, pipe, job)
				pending[(head+count)%len(pending)] = pendingWrite{job: job, ctx: ctx, cancel: cancel, receipt: receipt, err: err}
				count++
				continue
			}
			err := w.writeJob(ctx, pipe, job)
			rerouted, redirectErr := w.redirectJob(ctx, job, err)
			if redirectErr != nil {
				err = redirectErr
			}
			if cancel != nil {
				cancel()
			}
			if !rerouted {
				w.finishJob(job, err)
			}
		}
	}
}

func (w *network) sendContext(job *networkJob) (context.Context, context.CancelFunc) {
	if job.call != nil {
		ctx := job.call.request.ctx
		if deadline, ok := ctx.Deadline(); ok && !job.deadline.Before(deadline) {
			// The Call already owns the earlier deadline and cancellation.
			// Receipt settlement must not cancel this borrowed context.
			return ctx, nil
		}
		return context.WithDeadline(ctx, job.deadline)
	}
	return context.WithDeadline(w.ctx, job.deadline)
}
func (w *network) writeJob(ctx context.Context, pipe *actorPipe, job *networkJob) error {
	receipt, err := w.submitJob(ctx, pipe, job)
	if err != nil || receipt == nil {
		return err
	}
	if err := receipt.Wait(ctx); err != nil {
		return deliveryError(err, Unknown)
	}
	return nil
}
func (w *network) submitJob(ctx context.Context, pipe *actorPipe, job *networkJob) (sendCompletion, error) {
	if err := ctx.Err(); err != nil {
		return nil, deliveryError(err, NotSent)
	}
	if job.peer == w.node.Name() && job.packet.Target.Kind == "global" {
		return nil, w.writeLocalJob(ctx, job)
	}
	if pipe.channel != nil {
		w.mu.Lock()
		boot := w.currentBoot[job.peer]
		w.mu.Unlock()
		if boot != ([16]byte{}) && boot != [16]byte(pipe.info.Remote.Instance) {
			_ = pipe.channel.Close()
			pipe.channel = nil
		}
	}
	if pipe.channel != nil {
		select {
		case <-pipe.channel.Done():
			pipe.channel = nil
		default:
		}
	}
	if pipe.channel == nil {
		// A stale cache can point at a source that already shut down. Refresh
		// ownership before dialing so an unsent Call need not wait on that node.
		if route := job.packet.Target.Route; route != nil && w.node.opts.Directory != nil {
			current, err := w.node.opts.Directory.Lookup(ctx, route.Path)
			if errors.Is(err, ErrDirectoryNotFound) || err == nil && (current.PID() != route.PID() || current.Epoch != route.Epoch) {
				return nil, deliveryError(ErrRedirect, NotSent)
			}
			if err != nil {
				return nil, deliveryError(errors.Join(ErrDirectoryUnavailable, err), NotSent)
			}
		}
		session, err := w.connection(ctx, job.peer, job.class)
		if err != nil {
			return nil, deliveryError(err, NotSent)
		}
		route := inboxRoute
		if job.class == taonet.ClassControl {
			route = controlRoute
		}
		pipe.sequence = 0
		pipe.channel, err = session.OpenChannel(w.ctx, route, nil)
		if err != nil {
			return nil, deliveryError(err, NotSent)
		}
		pipe.info = pipe.channel.Session().Info()
		if job.class == taonet.ClassControl {
			w.watchControlChannel(pipe.channel)
		} else {
			w.watchInboxWriter(pipe.channel)
		}
	}
	info := pipe.info
	if job.expectedBoot != ([16]byte{}) && job.expectedBoot != [16]byte(info.Remote.Instance) {
		return nil, deliveryError(ErrStalePID, NotSent)
	}
	if job.class == taonet.ClassInteractive && job.packet.Target.PID != nil && job.packet.Target.PID.BootID != [16]byte(info.Remote.Instance) {
		return nil, deliveryError(ErrStalePID, NotSent)
	}
	if job.class == taonet.ClassInteractive && job.packet.Target.Route != nil && job.packet.Target.Route.BootID != [16]byte(info.Remote.Instance) {
		return nil, deliveryError(ErrRedirect, NotSent)
	}
	packet := job.packet
	if job.class == taonet.ClassInteractive {
		if pipe.sequence == math.MaxUint64 {
			return nil, deliveryError(ErrCapacity, NotSent)
		}
		pipe.sequence++
		packet.Sequence = pipe.sequence
	}
	if out := job.call; out != nil {
		deadline, _ := out.request.deadline()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, deliveryError(context.DeadlineExceeded, NotSent)
		}
		packet.BudgetMS = uint64(remaining / time.Millisecond)
		if remaining%time.Millisecond != 0 {
			packet.BudgetMS++
		}
	}
	payload := job.raw
	if payload == nil {
		var err error
		payload, err = marshalPacket(packet)
		if err != nil {
			return nil, deliveryError(err, NotSent)
		}
	} else if job.class == taonet.ClassInteractive {
		// Patch the final HA/2 fields while this job still owns the packet.
		// Storage remains immutable after submission until Released returns it.
		end := len(payload)
		if job.call != nil {
			end -= 8
			binary.BigEndian.PutUint64(payload[end:], packet.BudgetMS)
		}
		binary.BigEndian.PutUint64(payload[end-8:end], packet.Sequence)
	}
	if len(payload) > w.opts.MaxMessageBytes+4096 {
		return nil, deliveryError(ErrMessageTooLarge, NotSent)
	}
	if out := job.call; out != nil {
		w.mu.Lock()
		if out.canceled || out.request.settled.Load() || out.request.contextErr() != nil {
			w.mu.Unlock()
			return nil, deliveryError(context.Canceled, NotSent)
		}
		out.boot = [16]byte(info.Remote.Instance)
		out.scope = wireScope{info.ID, pipe.channel.ID()}
		out.sequence = packet.Sequence
		out.issued = true
		w.mu.Unlock()
	}
	if job.onScope != nil {
		job.onScope(wireScope{info.ID, pipe.channel.ID()})
	}
	var receipt sendCompletion
	var err error
	if job.raw != nil && job.packet.Target.Kind == "global" {
		receipt, err = pipe.channel.SendWithReceipt(ctx, taonet.Message{Body: payload})
	} else if job.call != nil && job.packet.Target.Kind != "global" {
		receipt, err = pipe.channel.SendOwnedUnconfirmed(ctx, taonet.Message{Body: payload})
		if errors.Is(err, taonet.ErrUnsupported) {
			receipt, err = pipe.channel.SendOwnedWithReceipt(ctx, taonet.Message{Body: payload})
		}
	} else {
		receipt, err = pipe.channel.SendOwnedWithReceipt(ctx, taonet.Message{Body: payload})
	}
	if err != nil {
		return nil, deliveryError(err, NotSent)
	}
	if job.buffer == nil && job.packet.Target.Kind != "global" {
		job.raw, job.packet.Message.Data = nil, nil
	}
	return receipt, nil
}
func (w *network) finishJob(job *networkJob, err error) {
	w.releaseJob(job)
	w.settleJob(job, err)
	w.recycleJob(job)
}

func (w *network) releaseJob(job *networkJob) {
	w.releaseJobBuffer(job)
	if job.token != nil {
		job.token.release()
		w.mu.Lock()
		w.controlJobs--
		w.mu.Unlock()
		select {
		case w.controlWake <- struct{}{}:
		default:
		}
	} else {
		w.release(job.charge)
	}
}

func (w *network) settleJob(job *networkJob, err error) {
	if job.onComplete != nil {
		job.onComplete(err)
	}
	if job.result != nil {
		job.result <- err
	}
	if err == nil {
		return
	}
	if out := job.call; out != nil {
		if out.request.kind == requestRemotePID {
			// Commit before cancellation routing. The winning owner holds the
			// sole index, and a late writer cannot rewrite a published reply.
			if completeErr := w.replyRemote(out.request, out.request.owner, nil, err, true); completeErr != nil {
				return
			}
			var failure *RemoteError
			if errors.As(err, &failure) && failure.Outcome == Unknown {
				w.mu.Lock()
				scope, boot, sequence := out.scope, out.boot, out.sequence
				w.mu.Unlock()
				w.queueControl(job.peer, actorPacket{Kind: "cancel", Key: job.packet.Key, Scope: scope, Sequence: sequence}, boot)
			}
			return
		}
		w.node.mu.Lock()
		completed := out.request.completed
		w.node.mu.Unlock()
		if !completed {
			_ = w.node.reply(out.request, out.request.owner, nil, err, true)
			var failure *RemoteError
			if errors.As(err, &failure) && failure.Outcome == Unknown {
				w.mu.Lock()
				scope, boot, sequence := out.scope, out.boot, out.sequence
				w.mu.Unlock()
				w.queueControl(job.peer, actorPacket{Kind: "cancel", Key: job.packet.Key, Scope: scope, Sequence: sequence}, boot)
			}
		}
		return
	}
	w.event(NetworkEvent{Peer: job.peer, Kind: "send_failed", RequestID: job.packet.Key.ID, Err: err})
}

func (w *network) readPacket(delivery *taonet.Delivery, peer string, targetPID *PID) (actorPacket, error) {
	var packet actorPacket
	body := delivery.Message().Body
	if len(body) > w.opts.MaxMessageBytes+4096 {
		return packet, ErrMessageTooLarge
	}
	return unmarshalPacketInto(body, w.registry.wireNames, w.node.Name(), peer, targetPID)
}
func rejectDelivery(delivery *taonet.Delivery, err error) {
	_ = delivery.Reject(rejectCode(err))
	delivery.Release()
}

// One inbox reader owns sequence validation and the temporary decoded target.
// network.inboxProgress is the cancellation watermark, published separately
// when an incoming record protects the Call or rejection has settled it.
type inboxReader struct {
	sequence  uint64
	targetPID PID
}

func (w *network) serveInbox(ctx context.Context, channel *taonet.Channel) error {
	if err := w.checkClass(channel, taonet.ClassInteractive); err != nil {
		return err
	}
	info := channel.Session().Info()
	scope := wireScope{info.ID, channel.ID()}
	w.mu.Lock()
	w.inboxProgress[scope] = 0
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.inboxProgress, scope)
		var cancels []context.CancelFunc
		for key, record := range w.canceled {
			if record.scope == scope {
				delete(w.canceled, key)
			}
		}
		for _, call := range w.incoming {
			if call.scope == scope {
				cancels = append(cancels, call.cancel)
			}
		}
		w.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
	}()
	var reader inboxReader
	for {
		delivery, err := channel.Receive(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		w.admitInbox(ctx, info.Remote, scope, delivery, &reader)
	}
}

func (w *network) admitInbox(ctx context.Context, peer taonet.Peer, scope wireScope, delivery *taonet.Delivery, reader *inboxReader) {
	packet, err := w.readPacket(delivery, string(peer.ID), &reader.targetPID)
	if err == nil && (packet.Kind != "send" && packet.Kind != "call" || !w.validPeerPacket(peer, packet, packet.Kind == "call")) {
		err = ErrInvalidMessage
	}
	if err == nil {
		if packet.Sequence == 0 || packet.Sequence <= reader.sequence {
			err = ErrInvalidMessage
		}
	}
	if err != nil {
		rejectDelivery(delivery, err)
		return
	}
	// Before an incoming record exists, CANCEL must reserve a tombstone rather
	// than mistake an in-progress decode for a late cancellation.
	// Only authenticated Calls may suppress acceptance ACKs; every admission
	// failure must instead return a definite rejection on the reply stream.
	if delivery.Unconfirmed() && (packet.Kind != "call" || packet.Target.Kind == "global") {
		rejectDelivery(delivery, ErrInvalidMessage)
		return
	}
	reject := func(err error) {
		if delivery.Unconfirmed() {
			w.rejectInboxCall(packet.Key, err)
		}
		rejectDelivery(delivery, err)
	}
	reader.sequence = packet.Sequence
	settled := false
	defer func() {
		if settled {
			return
		}
		w.mu.Lock()
		w.inboxProgress[scope] = packet.Sequence
		if canceled, ok := w.canceled[packet.Key]; ok && canceled.scope == scope && canceled.sequence == packet.Sequence {
			if _, active := w.incoming[packet.Key]; !active {
				delete(w.canceled, packet.Key)
			}
		}
		w.mu.Unlock()
	}()
	var target Addr
	var message any
	direct := packet.Target.localPID(w.node.Name())
	if err == nil && direct == nil {
		target, err = packet.Target.decode(w.node.Name())
	}
	if err == nil {
		message, err = w.registry.Decode(packet.Message)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && packet.Target.Route != nil {
		admissionCtx, cancel := context.WithTimeout(ctx, w.node.opts.CallTimeout)
		err = w.node.checkGlobal(admissionCtx, *packet.Target.Route)
		cancel()
	}
	if err != nil {
		reject(err)
		return
	}
	var sender Addr
	if packet.Sender != nil {
		sender = *packet.Sender
	}
	var p *process
	var toRef bool
	if direct != nil {
		p, err = w.node.resolvePID(*direct)
	} else {
		p, toRef, err = w.node.resolve(target)
	}
	if err != nil {
		reject(err)
		return
	}
	if packet.Kind == "send" {
		err = p.enqueueAdmitted(envelope{message: message, sender: sender, toRef: toRef, global: globalRecord(target), release: delivery.Release}, delivery.Accept)
		if err != nil {
			reject(err)
		}
		return
	}
	if packet.BudgetMS == 0 || packet.BudgetMS > uint64(math.MaxInt64/int64(time.Millisecond)) {
		reject(ErrInvalidMessage)
		return
	}
	deadline := time.Now().Add(time.Duration(packet.BudgetMS) * time.Millisecond)
	// Prepare heap-owned scope/request/callback storage before the network
	// lock. Admission and CANCEL bookkeeping still commit together below.
	state := &incomingRequestState{scope: requestScope{parent: w.ctx, deadline: deadline, hasDeadline: true}}
	budget, prepared := &state.scope, &state.request
	incoming := incomingCall{budget: budget, scope: scope, sequence: packet.Sequence}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		reject(ErrClosed)
		return
	}
	if canceledScope, ok := w.canceled[packet.Key]; ok {
		delete(w.canceled, packet.Key)
		w.mu.Unlock()
		if canceledScope.scope != scope || canceledScope.sequence != packet.Sequence {
			reject(ErrInvalidMessage)
		} else {
			reject(context.Canceled)
		}
		return
	}
	if _, duplicate := w.incoming[packet.Key]; duplicate {
		w.mu.Unlock()
		reject(ErrInvalidMessage)
		return
	}
	// Completed replies can still be waiting for send admission. Keep those
	// entries bounded too, including DeferredReply callers outside executors.
	if len(w.incoming) >= w.node.opts.MaxPendingCalls {
		w.mu.Unlock()
		reject(ErrCapacity)
		return
	}
	var r *request
	if direct != nil {
		r, err = w.registerActorRequest(prepared, p, budget, packet.Key, packet.Sender)
	} else {
		r, err = w.registerRequestInto(prepared, w.ctx, nil, p.pid, false, budget)
	}
	if err != nil {
		w.mu.Unlock()
		reject(err)
		return
	}
	if r.kind != requestActorPID {
		r.remoteKey = packet.Key
	}
	if r.kind != requestActorPID && w.node.diagnostics.Load() {
		w.node.mu.Lock()
		trace := r.enableTraceLocked()
		if direct != nil {
			trace.target = p.pid.String()
		} else {
			trace.target = target.String()
		}
		if packet.Sender != nil {
			trace.caller = *packet.Sender
		}
		w.node.mu.Unlock()
	}
	w.incoming[packet.Key] = incoming
	// Publish under the admission lock: CANCEL either finds this record, or
	// sees this watermark after completion removes it. No per-Call cleanup
	// lock is then needed after mailbox admission, even for an immediate Reply.
	w.inboxProgress[scope] = packet.Sequence
	settled = true
	w.wg.Add(1)
	w.mu.Unlock()
	budget.start()
	completedBeforeAdmission := false
	err = p.enqueueAdmitted(envelope{message: message, sender: sender, request: r, toRef: toRef, global: globalRecord(target), release: delivery.Release}, func() error {
		err := delivery.Accept()
		completedBeforeAdmission = r.bindCompletion(&incomingRequestCompletion)
		return err
	})
	if err != nil {
		w.node.expireRequest(r, err)
		reject(err)
		w.finishIncoming(r, false)
	} else if completedBeforeAdmission {
		completeIncomingRequest(r)
	}
}

var incomingRequestCompletion = requestCompletion{run: completeIncomingRequest}

func completeIncomingRequest(r *request) {
	r.node.network.finishIncoming(r, true)
}

func (w *network) finishIncoming(r *request, accepted bool) {
	defer w.wg.Done()
	retired := false
	defer func() {
		if retired {
			return
		}
		w.mu.Lock()
		delete(w.incoming, r.remoteKey)
		w.mu.Unlock()
	}()
	if accepted && !r.forwarded.Load() {
		retired = w.sendReply(r.remoteKey, r.value, r.err, MoveID{}, r)
	}
}

func (w *network) replyMoved(key requestKey, value any, err error, move MoveID) {
	w.sendReply(key, value, err, move, nil)
}

// retired reports that successful queue admission also removed the incoming
// index. Failure, local replies and forwarding retain finishIncoming's cleanup.
func (w *network) sendReply(key requestKey, value any, err error, move MoveID, incoming *request) (retired bool) {
	if key.Caller == w.node.Name() && key.Boot == w.node.BootID() {
		w.mu.Lock()
		out := w.outgoing[key.ID]
		var origin RouteRecord
		if out != nil && out.routing != nil {
			origin = out.routing.route
		}
		w.mu.Unlock()
		if out == nil {
			return
		}
		ctx, cancel := context.WithTimeout(w.ctx, w.node.opts.CallTimeout)
		defer cancel()
		if proofErr := w.verifyMovedReply(ctx, move, origin, w.node.Name(), w.node.BootID()); proofErr != nil {
			w.node.expireRequest(out.request, proofErr)
			return
		}
		var response error
		if err != nil {
			response = &RemoteError{Code: codeForError(err), Message: wireErrorText(err), Outcome: Responded, Cause: err}
		}
		_ = w.node.reply(out.request, out.request.owner, value, response, false)
		return
	}
	packet := actorPacket{Kind: "reply", Key: key, Move: move}
	if err != nil {
		packet.Error = &wireActorError{codeForError(err), wireErrorText(err)}
		value = nil
	}
	var sendErr error
	if incoming != nil && value != nil {
		sendErr = w.queueIncomingReply(incoming, packet, value)
		retired = sendErr == nil
	} else {
		sendErr = w.queueMessage(key.Caller, taonet.ClassReply, packet, value, PID{}, nil, nil)
	}
	if sendErr != nil && err == nil {
		packet.Error = &wireActorError{codeForError(sendErr), wireErrorText(sendErr)}
		sendErr = w.queueMessage(key.Caller, taonet.ClassReply, packet, nil, PID{}, nil, nil)
	}
	if sendErr != nil {
		w.event(NetworkEvent{Peer: key.Caller, Kind: "reply_failed", RequestID: key.ID, Err: sendErr})
	}
	return
}
func wireErrorText(err error) string {
	message := strings.ToValidUTF8(err.Error(), "\uFFFD")
	if len(message) > 1024 {
		message = message[:1024]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return message
}

func (w *network) serveControl(ctx context.Context, channel *taonet.Channel) error {
	if err := w.checkClass(channel, taonet.ClassControl); err != nil {
		return err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	w.wg.Add(1)
	w.mu.Unlock()
	defer w.wg.Done()
	info := channel.Session().Info()
	scope := wireScope{info.ID, channel.ID()}
	defer w.dropInboundScope(string(info.Remote.ID), [16]byte(info.Remote.Instance), scope)
	for {
		delivery, err := channel.Receive(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		packet, err := w.readPacket(delivery, string(info.Remote.ID), nil)
		if err == nil {
			err = w.handleControl(channel.Session(), scope, packet)
		}
		if err != nil {
			rejectDelivery(delivery, err)
			continue
		}
		_ = delivery.Accept()
		delivery.Release()
	}
}
