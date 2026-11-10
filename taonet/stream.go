package taonet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"math"
	"os"
	"slices"
	"sync"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

// Stream carries bytes in both directions. One reader and one writer may run
// concurrently. A read deadline can be cleared without losing a partial record.
type Stream struct {
	Route         string
	Metadata      map[string]string
	session       *Session
	op            *operation
	raw           *native.Stream
	ctx           context.Context
	owner         bool
	once          sync.Once
	mu            sync.Mutex
	err           error
	done          chan struct{}
	readEOF       bool
	writeEOF      bool
	writeDeadline time.Time
	readMu        sync.Mutex
	header        [16]byte
	headerRead    int
	remaining     uint32
	readOffset    uint64
	endSeen       bool
	writeMu       sync.Mutex
	writeOffset   uint64
	borrowed      *streamWriteVector // writeMu; allocated only by borrowed writes
}

func (s *Session) OpenStream(ctx context.Context, route string, metadata map[string]string) (*Stream, error) {
	op, err := s.openDuplex(ctx, "stream", route, metadata)
	if err != nil {
		return nil, err
	}
	return newStream(s, op, op.ctx, route, maps.Clone(metadata), true), nil
}

func (s *Session) openDuplex(ctx context.Context, mode, route string, metadata map[string]string) (*operation, error) {
	if !slices.Contains(s.info.Features, mode) {
		return nil, operationError(ctx, &StatusError{Code: CodeNotFound, Message: "peer does not support " + mode}, NotSent, 0)
	}
	op, err := s.prepareUnary(ctx, route, Message{Metadata: metadata})
	if err != nil {
		return nil, operationError(ctx, err, NotSent, 0)
	}
	success := false
	defer func() {
		if !success {
			s.finish(op)
			if op.stream != nil {
				_ = op.stream.Close()
			}
		}
	}()
	var options *channelOptions
	if mode == "channel" {
		local := channelLimits(s.cfg.Limits)
		options = &local
		if err = s.reserve(op, int(local.Bytes), 0, false); err != nil {
			return nil, operationError(ctx, err, NotSent, 0)
		}
	}
	opening, cancel := context.WithTimeout(op.ctx, s.cfg.DefaultRequestTimeout)
	defer cancel()
	raw, err := s.raw.OpenStream(opening)
	if err != nil {
		return nil, operationError(opening, err, NotSent, 0)
	}
	op.stream = raw
	stop := context.AfterFunc(opening, func() { _ = raw.Close() })
	defer stop()
	deadline, _ := opening.Deadline()
	_ = raw.SetReadDeadline(deadline)
	_ = raw.SetWriteDeadline(deadline)
	var remainingMS uint64
	if lifetime, ok := ctx.Deadline(); ok {
		remaining := time.Until(lifetime)
		if remaining <= 0 {
			return nil, operationError(ctx, context.DeadlineExceeded, NotSent, raw.ID())
		}
		remainingMS = uint64(remaining / time.Millisecond)
		if remaining%time.Millisecond != 0 {
			remainingMS++
		}
	}
	if err = writeJSON(raw, recordOpen, wireOpen{ID: raw.ID(), Mode: mode, Route: route, Metadata: metadata, TimeoutMS: remainingMS, Channel: options}); err != nil {
		return nil, operationError(opening, err, admissionOutcome(err), raw.ID())
	}
	h, err := readRecordHeader(raw)
	if err != nil {
		return nil, operationError(opening, err, admissionOutcome(err), raw.ID())
	}
	if h.kind == recordOpenError {
		var status StatusError
		if err = readJSONBody(raw, h, &status); err == nil && validStatus(&status) {
			finishStream(raw)
			return nil, remoteError(&status, Rejected, raw.ID())
		}
		return nil, operationError(opening, protocolError("invalid OPEN_ERR"), Unknown, raw.ID())
	}
	if h.kind != recordOpenOK || h.tag != 0 || mode == "stream" && h.length != 0 {
		return nil, operationError(opening, protocolError("invalid OPEN_OK"), Unknown, raw.ID())
	}
	if mode == "channel" {
		var peer channelOptions
		if err = readJSONBody(raw, h, &peer); err != nil {
			return nil, operationError(opening, err, Unknown, raw.ID())
		}
		if peer != channelLimits(s.peerLimits) {
			return nil, operationError(opening, protocolError("invalid channel window"), Unknown, raw.ID())
		}
	}
	if err = opening.Err(); err != nil {
		return nil, operationError(opening, err, Unknown, raw.ID())
	}
	_ = raw.SetReadDeadline(time.Time{})
	success = true
	return op, nil
}

func newStream(session *Session, op *operation, ctx context.Context, route string, metadata map[string]string, owner bool) *Stream {
	st := &Stream{Route: route, Metadata: metadata, session: session, op: op, raw: op.stream, ctx: ctx, owner: owner, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			st.end(context.Cause(ctx))
		case <-st.raw.Done():
			if err := st.raw.Err(); err != nil {
				st.end(err)
			} else {
				select {
				case <-st.done:
				case <-ctx.Done():
					st.end(context.Cause(ctx))
				}
			}
		case <-st.done:
		}
	}()
	return st
}

func (st *Stream) Session() *Session     { return st.session }
func (st *Stream) Done() <-chan struct{} { return st.done }
func (st *Stream) Err() error            { st.mu.Lock(); defer st.mu.Unlock(); return st.err }

func (st *Stream) end(err error) {
	st.once.Do(func() {
		st.mu.Lock()
		st.err = err
		close(st.done)
		st.mu.Unlock()
		if err != nil {
			st.op.cancel(err)
			_ = st.raw.Close()
		}
		if st.owner {
			st.session.finish(st.op)
		}
	})
}

func (st *Stream) complete(read bool) {
	st.mu.Lock()
	if read {
		st.readEOF = true
	} else {
		st.writeEOF = true
	}
	done := st.readEOF && st.writeEOF
	st.mu.Unlock()
	if done {
		st.end(nil)
	}
}

func (st *Stream) Close() error                      { st.end(ErrClosed); return nil }
func (st *Stream) SetReadDeadline(t time.Time) error { return st.raw.SetReadDeadline(t) }
func (st *Stream) SetWriteDeadline(t time.Time) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.writeDeadline = t
	return st.raw.SetWriteDeadline(t)
}

func (st *Stream) prepareWrite() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.err != nil {
		return st.err
	}
	if st.writeEOF {
		return io.ErrClosedPipe
	}
	deadline := st.writeDeadline
	if deadline.IsZero() {
		deadline = time.Now().Add(st.session.cfg.DefaultWriteTimeout)
	}
	return st.raw.SetWriteDeadline(deadline)
}

func (st *Stream) Write(p []byte) (int, error) {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	if err := st.prepareWrite(); err != nil {
		return 0, err
	}
	written := 0
	for len(p) > 0 {
		n := min(len(p), maxDataRecord)
		if st.writeOffset > math.MaxUint64-uint64(n) {
			err := protocolError("stream offset overflow")
			st.end(err)
			return written, err
		}
		// One owned record replaces separate synchronous header/body writes.
		// Copy p here to preserve Write's ownership contract even on timeout.
		record := make([]byte, 16+n)
		record[0] = recordData
		binary.BigEndian.PutUint32(record[4:8], uint32(n))
		binary.BigEndian.PutUint64(record[8:16], st.writeOffset)
		copy(record[16:], p[:n])
		count, err := st.raw.WriteOwned(record)
		count = max(0, count-16)
		written += count
		st.writeOffset += uint64(count)
		if err != nil {
			st.end(err)
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

func (st *Stream) CloseWrite() error {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	st.mu.Lock()
	closed := st.writeEOF
	st.mu.Unlock()
	if closed {
		return st.Err()
	}
	if err := st.prepareWrite(); err != nil {
		return err
	}
	if err := writeRecord(st.raw, recordEnd, st.writeOffset, nil); err != nil {
		st.end(err)
		return err
	}
	if err := st.raw.CloseWrite(); err != nil {
		st.end(err)
		return err
	}
	st.complete(false)
	return nil
}

func (st *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	st.readMu.Lock()
	defer st.readMu.Unlock()
	st.mu.Lock()
	err, ended := st.err, st.readEOF
	st.mu.Unlock()
	if err != nil {
		return 0, err
	}
	if ended {
		return 0, io.EOF
	}
	for {
		if st.remaining > 0 {
			n, err := st.raw.Read(p[:min(len(p), int(st.remaining))])
			st.remaining -= uint32(n)
			st.readOffset += uint64(n)
			if n > 0 {
				st.session.touch()
			}
			return n, st.readError(err)
		}
		if st.endSeen {
			var extra [1]byte
			n, err := st.raw.Read(extra[:])
			if err == io.EOF {
				st.complete(true)
				return 0, io.EOF
			}
			if n != 0 {
				err = protocolError("data after stream END")
			}
			if err != nil {
				return 0, st.readError(err)
			}
			continue
		}
		for st.headerRead < len(st.header) {
			n, err := st.raw.Read(st.header[st.headerRead:])
			st.headerRead += n
			if err != nil {
				return 0, st.readError(err)
			}
		}
		h, err := decodeRecordHeader(st.header)
		st.headerRead = 0
		if err == nil && (h.tag != st.readOffset || (h.kind != recordData && h.kind != recordEnd) || st.readOffset > math.MaxUint64-uint64(h.length)) {
			err = protocolError("invalid stream record or offset")
		}
		if err != nil {
			return 0, st.readError(err)
		}
		if h.kind == recordEnd {
			st.endSeen = true
		} else {
			st.remaining = h.length
		}
	}
}

func (st *Stream) readError(err error) error {
	if err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		return err
	}
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	st.end(err)
	return err
}

func callStream(handler StreamHandler, ctx context.Context, st *Stream) (err error) {
	defer func() {
		if recover() != nil {
			err = &StatusError{Code: CodeInternal, Message: "stream handler panicked"}
		}
	}()
	return handler(ctx, st)
}
