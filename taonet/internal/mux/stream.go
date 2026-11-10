package mux

import (
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// Stream supports a reader and writer concurrently. Writes are serialized as
// complete calls. All fields other than writeMu are protected by s.mu.
type Stream struct {
	s                *Session
	id               uint64
	local            bool
	writeMu          sync.Mutex
	vectors          *vectorState // allocated only by vectored writes
	rx               ring
	window           uint64
	received         uint64
	consumed         uint64
	advertised       uint64
	peerWindow       uint64
	reserved         uint64
	issued           uint64
	peerConsumed     uint64
	openIssued       bool
	ackReceived      bool
	ackIssued        bool
	ackSent          bool
	openRequest      *writeRequest
	windowRequest    *writeRequest
	finRequest       *writeRequest
	queue            []*writeRequest
	pending          int
	scheduled        bool
	deficit          int
	writeClosed      bool
	finSent          bool
	remoteFin        bool
	localReset       bool
	remoteReset      bool
	resetSent        bool
	resetAckSent     bool
	resetAckReceived bool
	nacked           bool
	resetAt          time.Time
	readDeadline     time.Time
	readWake         chan struct{}
	writeDeadline    time.Time
	writeWake        chan struct{}
	done             chan struct{}
	ended            bool
	err              error
}

func (st *Stream) ID() uint64            { return st.id }
func (st *Stream) Done() <-chan struct{} { return st.done }
func (st *Stream) Err() error            { st.s.mu.Lock(); defer st.s.mu.Unlock(); return st.err }

func (st *Stream) endLocked(err error) {
	if st.ended {
		return
	}
	st.ended = true
	st.err = err
	close(st.done)
	st.notifyReadLocked()
	st.notifyWriteLocked()
	st.s.notifyLocked()
}

// Read supports one reader. Keep one coalesced notification for its lifetime
// instead of allocating a new broadcast channel after every DATA record.
func (st *Stream) readWakeLocked() <-chan struct{} {
	if st.readWake == nil {
		st.readWake = make(chan struct{}, 1)
	}
	return st.readWake
}

func (st *Stream) notifyReadLocked() {
	select {
	case st.readWake <- struct{}{}:
	default:
	}
}

// DATA writes have one waiter under writeMu. FIN can have multiple callers
// and continues to use the session broadcast in waitWrite.
func (st *Stream) writeWakeLocked() <-chan struct{} {
	if st.writeWake == nil {
		st.writeWake = make(chan struct{}, 1)
	}
	return st.writeWake
}

func (st *Stream) notifyWriteLocked() {
	select {
	case st.writeWake <- struct{}{}:
	default:
	}
}

func expired(t time.Time) bool { return !t.IsZero() && time.Until(t) <= 0 }

var waitTimers sync.Pool

func waitChange(ch <-chan struct{}, deadline time.Time) {
	if deadline.IsZero() {
		<-ch
		return
	}
	select {
	case <-ch:
		return
	default:
	}
	var timer *time.Timer
	if cached := waitTimers.Get(); cached != nil {
		timer = cached.(*time.Timer)
		timer.Reset(time.Until(deadline))
	} else {
		timer = time.NewTimer(time.Until(deadline))
	}
	select {
	case <-ch:
	case <-timer.C:
	}
	// Go 1.26+ guarantees that Stop/Reset cannot expose stale timer values.
	timer.Stop()
	waitTimers.Put(timer)
}

func (st *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	s := st.s
	for {
		s.mu.Lock()
		if st.err != nil {
			err := st.err
			s.mu.Unlock()
			return 0, err
		}
		if expired(st.readDeadline) {
			s.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		if st.rx.size > 0 {
			n := st.rx.read(p)
			st.consumed += uint64(n)
			var fatal error
			if st.consumed-st.advertised >= st.window/4 {
				fatal = s.queueWindowLocked(st)
			}
			s.reapLocked(st)
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return n, fatal
		}
		if st.remoteFin {
			s.mu.Unlock()
			return 0, io.EOF
		}
		ch, deadline := st.readWakeLocked(), st.readDeadline
		s.mu.Unlock()
		waitChange(ch, deadline)
	}
}

func (st *Stream) Write(p []byte) (int, error) {
	return st.write(p, false)
}

// WriteOwned consumes p, including on error. The caller must never modify or
// reuse its backing array after this call: cancellation can return while the
// connection writer still holds it. Write retains its normal copying contract.
func (st *Stream) WriteOwned(p []byte) (int, error) {
	return st.write(p, true)
}

func (st *Stream) write(p []byte, owned bool) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	s := st.s
	written := 0
	for len(p) > 0 {
		s.mu.Lock()
		if st.err != nil {
			err := st.err
			s.mu.Unlock()
			return written, err
		}
		if st.writeClosed {
			s.mu.Unlock()
			return written, io.ErrClosedPipe
		}
		if expired(st.writeDeadline) {
			var fatal error
			if written > 0 {
				fatal = s.resetLocked(st, DeadlineExceeded, os.ErrDeadlineExceeded)
			}
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return written, os.ErrDeadlineExceeded
		}
		available := st.peerWindow - (st.reserved - st.peerConsumed)
		space := s.cfg.SendBudget - s.dataBytes - headerSize
		if st.id == 1 {
			space = controlBudget - s.controlDataBytes - headerSize
		}
		if available == 0 || space <= 0 || (st.id != 1 && s.dataFrames >= 4096) {
			ch, deadline := s.changedLocked(), st.writeDeadline
			s.mu.Unlock()
			waitChange(ch, deadline)
			continue
		}
		n := min(len(p), int(s.maxData), int(available), space)
		if st.reserved > math.MaxUint64-uint64(n) {
			fatal := s.resetLocked(st, Internal, protocolError("send offset exhausted"))
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return written, protocolError("send offset exhausted")
		}
		payload := p[:n]
		if !owned {
			payload = append([]byte(nil), payload...)
		}
		q := s.enqueueDataLocked(st, header{kind: typeData, length: uint32(n), stream: st.id, value: st.reserved}, payload)
		st.reserved += uint64(n)
		s.mu.Unlock()
		if err := st.waitWrite(q, time.Time{}); err != nil {
			return written, err
		}
		written += n
		p = p[n:]
	}
	return written, nil
}

func (st *Stream) waitWrite(q *writeRequest, fallback time.Time) error {
	s := st.s
	for {
		s.mu.Lock()
		if q.finished {
			err := q.err
			s.mu.Unlock()
			return err
		}
		if st.err != nil {
			err := st.err
			s.mu.Unlock()
			return err
		}
		deadline := st.writeDeadline
		if deadline.IsZero() {
			deadline = fallback
		}
		if expired(deadline) {
			fatal := s.resetLocked(st, DeadlineExceeded, os.ErrDeadlineExceeded)
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return os.ErrDeadlineExceeded
		}
		var ch <-chan struct{}
		if q.h.kind == typeFin {
			ch = s.changedLocked()
		} else {
			ch = st.writeWakeLocked()
		}
		s.mu.Unlock()
		waitChange(ch, deadline)
	}
}

func (st *Stream) CloseWrite() error {
	s := st.s
	fallback := time.Now().Add(s.cfg.IOTimeout)
	for {
		s.mu.Lock()
		if st.err != nil {
			err := st.err
			s.mu.Unlock()
			return err
		}
		if st.finRequest != nil {
			q := st.finRequest
			s.mu.Unlock()
			return st.waitWrite(q, fallback)
		}
		deadline := st.writeDeadline
		if deadline.IsZero() {
			deadline = fallback
		}
		if expired(deadline) {
			fatal := s.resetLocked(st, DeadlineExceeded, os.ErrDeadlineExceeded)
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return os.ErrDeadlineExceeded
		}
		st.writeClosed = true
		full := s.dataBytes+headerSize > s.cfg.SendBudget || s.dataFrames >= 4096
		if st.id == 1 {
			full = s.controlDataBytes+headerSize > controlBudget
		}
		if full {
			ch := s.changedLocked()
			s.mu.Unlock()
			waitChange(ch, deadline)
			continue
		}
		q := s.enqueueDataLocked(st, header{kind: typeFin, stream: st.id, value: st.reserved}, nil)
		st.finRequest = q
		s.mu.Unlock()
		return st.waitWrite(q, fallback)
	}
}

func (st *Stream) Close() error {
	return st.Reset(Canceled)
}

// Reset aborts this stream with an explicit wire reason.
func (st *Stream) Reset(code Code) error {
	s := st.s
	s.mu.Lock()
	fatal := s.resetLocked(st, code, &StreamError{Code: code})
	s.mu.Unlock()
	if fatal != nil {
		s.stop(fatal)
	}
	return nil
}

func (st *Stream) SetReadDeadline(t time.Time) error {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if st.readDeadline == t {
		return nil
	}
	st.readDeadline = t
	st.notifyReadLocked()
	return nil
}
func (st *Stream) SetWriteDeadline(t time.Time) error {
	st.s.mu.Lock()
	defer st.s.mu.Unlock()
	if st.writeDeadline == t {
		return nil
	}
	st.writeDeadline = t
	st.notifyWriteLocked()
	st.s.notifyLocked()
	return nil
}
