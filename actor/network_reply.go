package actor

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/sllt/tao/taonet"
)

// One reply stream retains at most a default-size Actor payload plus its
// envelope. Larger configured messages remain valid but are not kept idle.
const replyBufferRetainLimit = (128 << 10) + 4096

// Backpressure a successful reply before encoding it. The common path only
// takes the ordinary reservation; contexts and stop callbacks are needed only
// when the send budget is full. Request completion and transport completion
// remain separate: a timeout here cannot undo business execution.
func (w *network) queueIncomingReply(r *request, packet actorPacket, value any) error {
	charge := 2 * (w.opts.MaxMessageBytes + 4096)
	err := w.reserve(charge)
	if err == ErrCapacity {
		w.node.mu.Lock()
		owner := w.node.processes[r.owner.ID]
		w.node.mu.Unlock()
		if owner == nil || owner.pid != r.owner {
			return ErrStopped
		}
		ctx, cancel := context.WithTimeout(r.context(), w.opts.SendTimeout)
		stop := context.AfterFunc(owner.ctx, cancel)
		if owner.ctx.Err() != nil {
			cancel()
		}
		err = w.reserveCall(ctx, charge)
		stop()
		cancel()
	}
	if err != nil {
		return err
	}
	return w.queueReservedMessage(packet.Key.Caller, taonet.ClassReply, packet, value, PID{}, nil, nil, charge, r)
}

// Replies complete a Call; they do not need another acceptance round trip.
// One writer owns the record headers. Native I/O borrows encoded job buffers;
// the returned release signal, including on error, controls their reuse.
func (w *network) writeReplies(pipe *actorPipe) {
	var stream *taonet.Stream
	defer func() {
		if stream != nil {
			_ = stream.Close()
		}
	}()
	var jobs [32]*networkJob
	var headers [32][4]byte
	var parts [64][]byte
	for {
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
		var streamDone <-chan struct{}
		if stream != nil {
			streamDone = stream.Done()
		}
		if pipe.queued() == 0 {
			retire := streamDone == nil
			select {
			case <-streamDone:
				retire = true
			default:
			}
			if retire {
				delete(w.pipes, pipe.key)
				w.mu.Unlock()
				return
			}
		}
		count, size := 0, 0
		for count < len(jobs) && count < pipe.queued() {
			job := pipe.jobAt(count)
			if count > 0 && (job.expectedBoot != jobs[0].expectedBoot || size+len(job.raw)+4 > 64<<10) {
				break
			}
			jobs[count] = job
			size += len(job.raw) + 4
			count++
		}
		pipe.dropJobs(count)
		w.mu.Unlock()
		if count == 0 {
			select {
			case <-pipe.wake:
			case <-w.ctx.Done():
			case <-streamDone:
			}
			continue
		}
		n := 0
		var deadline time.Time
		for i := 0; i < count; i++ {
			job := jobs[i]
			jobs[i] = nil
			if time.Until(job.deadline) < 0 {
				w.finishJob(job, deliveryError(context.DeadlineExceeded, NotSent))
				continue
			}
			if len(job.raw) > w.opts.MaxMessageBytes+4096 {
				w.finishJob(job, deliveryError(ErrMessageTooLarge, NotSent))
				continue
			}
			binary.BigEndian.PutUint32(headers[n][:], uint32(len(job.raw)))
			parts[2*n], parts[2*n+1] = headers[n][:], job.raw
			jobs[n] = job
			n++
			if deadline.IsZero() || job.deadline.Before(deadline) {
				deadline = job.deadline
			}
		}
		if n == 0 {
			continue
		}
		var err error
		var released <-chan struct{}
		stream, err = w.replyStream(stream, pipe, jobs[0].expectedBoot, deadline)
		if err == nil {
			_ = stream.SetWriteDeadline(deadline)
			var writeErr error
			_, released, writeErr = stream.WritevBorrowed(net.Buffers(parts[:2*n]))
			if writeErr != nil {
				err = deliveryError(writeErr, Unknown)
				_ = stream.Close()
				stream = nil
			}
		}
		for i := 0; i < n; i++ {
			w.settleJob(jobs[i], err)
		}
		if released != nil {
			<-released
		}
		clear(parts[:2*n])
		w.releaseReplyBatch(jobs[:n])
	}
}

// Reply jobs have no control reservation. Once the whole borrowed write has
// released its inputs, return buffers individually but publish the byte/message
// credit once, instead of waking admission waiters after every job in a batch.
func (w *network) releaseReplyBatch(jobs []*networkJob) {
	charge := 0
	for i, job := range jobs {
		charge += job.charge
		w.releaseJobBuffer(job)
		jobs[i] = nil
		w.recycleJob(job)
	}
	w.mu.Lock()
	w.bytes -= charge
	w.messages -= len(jobs)
	w.wakeSendersLocked()
	w.mu.Unlock()
}

func (w *network) replyStream(stream *taonet.Stream, pipe *actorPipe, boot [16]byte, deadline time.Time) (*taonet.Stream, error) {
	peer := pipe.key.peer
	if stream != nil {
		w.mu.Lock()
		current := w.currentBoot[peer]
		w.mu.Unlock()
		if current != ([16]byte{}) && current != [16]byte(pipe.info.Remote.Instance) {
			_ = stream.Close()
			stream = nil
		} else {
			select {
			case <-stream.Done():
				stream = nil
			default:
			}
		}
	}
	if stream == nil {
		ctx, cancel := context.WithDeadline(w.ctx, deadline)
		defer cancel()
		session, err := w.connection(ctx, peer, taonet.ClassReply)
		if err != nil {
			return nil, deliveryError(err, NotSent)
		}
		// The stream spans calls; its lifetime belongs to the network.
		stream, err = session.OpenStream(w.ctx, replyRoute, nil)
		if err != nil {
			return nil, deliveryError(err, NotSent)
		}
		pipe.info = stream.Session().Info()
	}
	if boot != [16]byte(pipe.info.Remote.Instance) {
		return stream, deliveryError(ErrStalePID, NotSent)
	}
	return stream, nil
}

type replyFrameReader struct {
	reader   *bufio.Reader
	large    []byte
	borrowed int
}

func newReplyFrameReader(reader io.Reader) *replyFrameReader {
	return &replyFrameReader{reader: bufio.NewReader(reader)}
}

// The returned frame is valid until the next call. Small frames borrow the
// reader's 4KiB storage, avoiding separate Stream reads for length and body.
// Large frames retain their own bounded scratch; a borrowed view never becomes
// a destination for reads that could overwrite unread frames in the reader.
func (r *replyFrameReader) next(limit int) ([]byte, error) {
	if r.borrowed > 0 {
		_, _ = r.reader.Discard(r.borrowed)
		r.borrowed = 0
	}
	header, err := r.reader.Peek(4)
	if err != nil {
		if err == io.EOF && len(header) != 0 {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	size := uint64(binary.BigEndian.Uint32(header))
	if size < 6 {
		return nil, ErrInvalidMessage
	}
	if size > uint64(limit) {
		return nil, ErrMessageTooLarge
	}
	_, _ = r.reader.Discard(4)
	if size <= uint64(r.reader.Size()) {
		frame, err := r.reader.Peek(int(size))
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			r.borrowed = int(size)
		}
		return frame, err
	}
	if uint64(cap(r.large)) < size {
		r.large = make([]byte, int(size))
	}
	frame := r.large[:int(size)]
	_, err = io.ReadFull(r.reader, frame)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if cap(r.large) > replyBufferRetainLimit {
		r.large = nil
	}
	return frame, err
}

func (w *network) serveReplies(ctx context.Context, stream *taonet.Stream) error {
	info := stream.Session().Info()
	if w.opts.Profile == IsolatedNetwork && info.Class != taonet.ClassReply {
		return ErrInvalidMessage
	}
	w.watchInbound(stream.Session())
	reader := newReplyFrameReader(stream)
	for {
		buffer, err := reader.next(w.opts.MaxMessageBytes + 4096)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		packet, err := unmarshalPacketNames(buffer, w.registry.wireNames, w.node.Name(), string(info.Remote.ID))
		if err == nil {
			err = w.receiveReply(ctx, info.Remote, packet)
		}
		if err != nil {
			w.event(NetworkEvent{Peer: string(info.Remote.ID), Kind: "reply_rejected", RequestID: packet.Key.ID, Err: err})
		}
	}
}

func (w *network) receiveReply(ctx context.Context, peer taonet.Peer, packet actorPacket) error {
	if packet.Kind != "reply" && packet.Kind != "reject" || packet.Key.Caller != w.node.Name() || packet.Key.Boot != w.node.BootID() || packet.Key.ID == 0 {
		return ErrInvalidMessage
	}
	w.mu.Lock()
	out := w.outgoing[packet.Key.ID]
	var origin RouteRecord
	needsProof := out != nil && (out.peer != string(peer.ID) || out.boot != [16]byte(peer.Instance))
	if needsProof && out.routing != nil {
		origin = out.routing.route
	}
	w.mu.Unlock()
	if out == nil { // Expired replies cannot complete another request.
		return nil
	}
	if needsProof {
		if packet.Kind == "reject" {
			return ErrInvalidMessage
		}
		proofCtx, cancel := context.WithTimeout(ctx, w.node.opts.CallTimeout)
		defer cancel()
		if err := w.verifyMovedReply(proofCtx, packet.Move, origin, string(peer.ID), [16]byte(peer.Instance)); err != nil {
			return err
		}
	}
	if packet.Error != nil && (len(packet.Error.Message) > 1024 || len(packet.Error.Code) > 64) {
		return ErrInvalidMessage
	}
	if packet.Kind == "reject" {
		if packet.Error == nil || packet.Message.Type != "" || len(packet.Message.Data) != 0 || packet.Move != (MoveID{}) {
			return ErrInvalidMessage
		}
		response := &RemoteError{Code: packet.Error.Code, Message: packet.Error.Message, Outcome: Rejected}
		_ = w.node.reply(out.request, out.request.owner, nil, response, false)
		return nil
	}
	value, err := w.registry.Decode(packet.Message)
	var response error
	if err != nil {
		response = &RemoteError{Code: codeForError(err), Message: wireErrorText(err), Outcome: Responded, Cause: err}
	} else if packet.Error != nil {
		response = &RemoteError{Code: packet.Error.Code, Message: packet.Error.Message, Outcome: Responded}
	}
	_ = w.node.reply(out.request, out.request.owner, value, response, false)
	return err
}

func (w *network) rejectInboxCall(key requestKey, err error) {
	packet := actorPacket{Kind: "reject", Key: key, Error: &wireActorError{Code: codeForError(err), Message: wireErrorText(err)}}
	if sendErr := w.queueMessage(key.Caller, taonet.ClassReply, packet, nil, PID{}, nil, nil); sendErr != nil {
		w.event(NetworkEvent{Peer: key.Caller, Kind: "reply_failed", RequestID: key.ID, Err: sendErr})
	}
}

// An unconfirmed write can finish before mailbox admission. A later reset of
// that inbox must end affected Calls even if the peer's control session lives.
func (w *network) watchInboxWriter(channel *taonet.Channel) {
	scope := wireScope{channel.Session().Info().ID, channel.ID()}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		select {
		case <-channel.Done():
		case <-w.ctx.Done():
			return
		}
		w.mu.Lock()
		var pending []*request
		for _, out := range w.outgoing {
			if out.issued && out.routing == nil && out.scope == scope {
				pending = append(pending, out.request)
			}
		}
		w.mu.Unlock()
		err := channel.Err()
		if err == nil {
			err = ErrNodeUnreachable
		}
		for _, r := range pending {
			_ = w.node.reply(r, r.owner, nil, deliveryError(err, Unknown), true)
		}
	}()
}
