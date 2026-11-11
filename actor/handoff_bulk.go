package actor

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/sllt/tao/taonet"
)

const handoffBulkRoute = "hive.actor/handoff-bulk.v2"

type handoffEnvelope struct {
	Message  EncodedMessage `json:"message"`
	Sender   *PID           `json:"sender,omitempty"`
	Call     requestKey     `json:"call"`
	BudgetMS uint64         `json:"budget_ms,omitempty"`
}
type handoffFrame struct {
	Kind     string           `json:"kind"`
	Move     MoveID           `json:"move,omitempty"`
	Snapshot []byte           `json:"snapshot,omitempty"`
	Sequence uint64           `json:"sequence,omitempty"`
	Envelope *handoffEnvelope `json:"envelope,omitempty"`
	Digest   [32]byte         `json:"digest,omitempty"`
}

func writeHandoffFrame(writer io.Writer, frame handoffFrame) ([]byte, error) {
	payload, err := marshalHandoffFrame(frame)
	if err != nil {
		return nil, err
	}
	// The encoder reserves the length prefix so a frame is one stream write.
	data := payload
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
		data = data[n:]
	}
	return payload[4:], nil // Digest covers the frame, excluding its length.
}
func readHandoffFrame(reader io.Reader, limit int) (handoffFrame, []byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return handoffFrame{}, nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || uint64(length) > uint64(limit) {
		return handoffFrame{}, nil, ErrMessageTooLarge
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(reader, payload); err != nil {
		return handoffFrame{}, nil, err
	}
	frame, err := unmarshalHandoffFrame(payload)
	return frame, payload, err
}

func (w *network) serveHandoffBulk(ctx context.Context, stream *taonet.Stream) error {
	if w.opts.Profile == IsolatedNetwork && stream.Session().Info().Class != taonet.ClassBulk {
		return ErrInvalidMessage
	}
	w.watchInbound(stream.Session())
	limit := max(w.node.opts.MaxSnapshotBytes, w.opts.MaxMessageBytes) + 4096
	begin, payload, err := readHandoffFrame(stream, limit)
	if err != nil {
		return err
	}
	if begin.Kind != "begin" || begin.Move == (MoveID{}) || len(begin.Snapshot) > w.node.opts.MaxSnapshotBytes {
		return ErrInvalidMessage
	}
	w.mu.Lock()
	reservation := w.handoffs[begin.Move]
	w.mu.Unlock()
	if reservation == nil {
		return ErrNoProcess
	}
	peer := stream.Session().Info().Remote
	if reservation.source.Owner != string(peer.ID) || reservation.source.BootID != [16]byte(peer.Instance) {
		return ErrInvalidMessage
	}
	move, err := w.node.opts.Directory.ReadMove(ctx, begin.Move)
	if err != nil {
		return err
	}
	if move.State != MovePreparing || move.Source != reservation.source || move.Target.PID() != reservation.target.PID() {
		return ErrDirectoryConflict
	}
	reservation.mu.Lock()
	if reservation.receiving || reservation.prepared {
		reservation.mu.Unlock()
		return ErrMigrating
	}
	reservation.receiving = true
	reservation.activate, reservation.activated = make(chan struct{}), make(chan struct{})
	reservation.mu.Unlock()
	q := reservation.process
	q.mu.Lock()
	if q.stopRequested || q.finished {
		q.mu.Unlock()
		return ErrStopped
	}
	q.running = true
	q.mu.Unlock()
	ready, activated := make(chan error, 1), make(chan error, 1)
	go q.prepareReplacement(begin.Snapshot, move.Source.Version, ready, reservation.activate, activated)
	go func() {
		var err error
		select {
		case err = <-activated:
		case <-w.ctx.Done():
			err = ErrClosed
		}
		reservation.mu.Lock()
		reservation.activationErr = err
		close(reservation.activated)
		reservation.mu.Unlock()
	}()
	select {
	case err = <-ready:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		return err
	}
	digest := sha256.New()
	_, _ = digest.Write(payload)
	var sequence uint64
	for {
		frame, payload, err := readHandoffFrame(stream, limit)
		if err != nil {
			return err
		}
		if frame.Kind == "barrier" {
			var sum [32]byte
			copy(sum[:], digest.Sum(nil))
			if frame.Sequence != sequence || frame.Digest != sum {
				return ErrInvalidMessage
			}
			var extra [1]byte
			if n, err := stream.Read(extra[:]); n != 0 || err != io.EOF {
				return ErrInvalidMessage
			}
			current, err := w.node.opts.Directory.ReadMove(ctx, begin.Move)
			if err != nil {
				return err
			}
			if current != move {
				return ErrDirectoryConflict
			}
			reservation.mu.Lock()
			reservation.prepared, reservation.last = true, sequence
			reservation.mu.Unlock()
			_, err = writeHandoffFrame(stream, handoffFrame{Kind: "prepared", Move: begin.Move, Sequence: sequence, Digest: sum})
			if err != nil {
				return err
			}
			return stream.CloseWrite()
		}
		if frame.Kind != "message" && frame.Kind != "skip" || sequence == math.MaxUint64 || frame.Sequence != sequence+1 {
			return ErrInvalidMessage
		}
		sequence++
		if sequence > uint64(q.opts.mailbox) {
			return ErrCapacity
		}
		if frame.Kind == "message" {
			if frame.Envelope == nil {
				return ErrInvalidMessage
			}
			if err := w.stageHandoffEnvelope(reservation, move, *frame.Envelope); err != nil {
				return err
			}
		}
		_, _ = digest.Write(payload)
	}
}

func (w *network) stageHandoffEnvelope(reservation *handoffReservation, move MoveRecord, message handoffEnvelope) error {
	value, err := w.registry.Decode(message.Message)
	if err != nil {
		return err
	}
	var sender Addr
	if message.Sender != nil {
		if !validSegment(message.Sender.Node) || message.Sender.BootID == ([16]byte{}) || message.Sender.ID == 0 {
			return ErrInvalidMessage
		}
		sender = *message.Sender
	}
	q := reservation.process
	env := envelope{message: value, sender: sender, toRef: true, global: &move.Target}
	if message.Call.ID == 0 {
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.stopRequested {
			return ErrStopped
		}
		return q.mailbox.push(env)
	}
	if !validSegment(message.Call.Caller) || message.Call.Boot == ([16]byte{}) || message.BudgetMS == 0 || message.BudgetMS > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return ErrInvalidMessage
	}
	ctx, cancel := context.WithTimeout(w.ctx, time.Duration(message.BudgetMS)*time.Millisecond)
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		cancel()
		return ErrClosed
	}
	if _, duplicate := w.incoming[message.Call]; duplicate {
		w.mu.Unlock()
		cancel()
		return ErrInvalidMessage
	}
	r, err := w.coreRequest(ctx, q.pid)
	if err != nil {
		w.mu.Unlock()
		cancel()
		return err
	}
	r.setRemoteKey(message.Call)
	if w.node.diagnostics.Load() {
		w.node.mu.Lock()
		trace := r.enableTraceLocked()
		trace.target = move.Target.Path
		if message.Sender != nil {
			trace.caller = *message.Sender
		}
		w.node.mu.Unlock()
	}
	w.incoming[message.Call] = incomingCall{cancelContext: cancel, move: move.ID}
	w.wg.Add(1)
	w.mu.Unlock()
	reservation.mu.Lock()
	canceled := reservation.canceled[message.Call]
	delete(reservation.canceled, message.Call)
	reservation.mu.Unlock()
	if canceled {
		cancel()
	}
	stop := context.AfterFunc(ctx, func() { w.node.expireRequest(r, ctx.Err()) })
	env.request = r
	q.mu.Lock()
	if q.stopRequested {
		err = ErrStopped
	} else {
		err = q.mailbox.push(env)
	}
	q.mu.Unlock()
	if err != nil {
		w.node.expireRequest(r, err)
	}
	go func() {
		defer w.wg.Done()
		defer stop()
		defer cancel()
		<-r.waitDone()
		w.mu.Lock()
		delete(w.incoming, message.Call)
		w.mu.Unlock()
		select {
		case <-reservation.decision:
		case <-w.ctx.Done():
			return
		}
		reservation.mu.Lock()
		committed := reservation.committed
		reservation.mu.Unlock()
		if committed && !r.forwarded.Load() {
			w.replyMoved(message.Call, r.value, r.err, move.ID)
		}
	}()
	return err
}
