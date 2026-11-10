package mux

import (
	"context"
	"math"
	"net"
	"sync"
	"time"
)

type Role uint8

const (
	Initiator Role = iota
	Acceptor
)

// Session owns conn. All stream and queue state is protected by mu; I/O runs
// outside it. The initial implementation deliberately uses one state lock.
type Session struct {
	conn             net.Conn
	cfg              Config
	peer             settings
	role             Role
	maxData          uint32
	mu               sync.Mutex
	changed          chan struct{}
	acceptChanged    chan struct{}
	writerWake       chan struct{}
	done             chan struct{}
	err              error
	wg               sync.WaitGroup
	ready            bool
	draining         bool
	peerDraining     bool
	nextID           uint64
	localHigh        uint64
	peerHigh         uint64
	acceptedHigh     uint64
	streams          map[uint64]*Stream
	accepts          []*Stream
	control          *Stream
	controlUsed      bool
	controlAccepted  bool
	localCount       int
	peerCount        int
	receiveReserved  uint64
	controls         []*writeRequest
	controlBytes     int
	controlDataBytes int
	dataBytes        int
	dataFrames       int
	runnable         []*Stream
	active           *Stream
	controlBurst     int
}

// New performs the native bootstrap and SETTINGS exchange, then starts I/O.
// ctx controls the handshake only. Call Close when the session is no longer used.
func New(ctx context.Context, conn net.Conn, role Role, config Config) (*Session, error) {
	cfg, err := config.normalized()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if role != Initiator && role != Acceptor {
		_ = conn.Close()
		return nil, protocolError("invalid role")
	}
	peer, err := handshake(ctx, conn, role, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	next := uint64(3)
	if role == Acceptor {
		next = 2
	}
	s := &Session{conn: conn, cfg: cfg, peer: peer, role: role,
		maxData: min(cfg.MaxDataPayload, peer.maxData), done: make(chan struct{}), writerWake: make(chan struct{}, 1),
		streams: make(map[uint64]*Stream), nextID: next}
	s.wg.Add(3)
	go s.readLoop()
	go s.writeLoop()
	go s.maintenance()
	return s, nil
}

// MarkReady is called by the HN/1 layer after its identity handshake succeeds.
func (s *Session) MarkReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ready = true
	s.notifyLocked()
}

func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) Err() error            { s.mu.Lock(); defer s.mu.Unlock(); return s.err }

func (s *Session) Close() error {
	s.stop(ErrClosed)
	s.wg.Wait()
	return nil
}

// Waiters check their predicate and subscribe under mu. The channel is shared
// by that generation of waiters; notifications without waiters allocate nothing.
func (s *Session) changedLocked() <-chan struct{} {
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}
func (s *Session) notifyLocked() {
	if s.changed != nil {
		close(s.changed)
		s.changed = nil
	}
}

func (s *Session) notifyWriterLocked() {
	select {
	case s.writerWake <- struct{}{}:
	default:
	}
}

func (s *Session) acceptChangedLocked() <-chan struct{} {
	if s.acceptChanged == nil {
		s.acceptChanged = make(chan struct{})
	}
	return s.acceptChanged
}

func (s *Session) notifyAcceptLocked() {
	if s.acceptChanged != nil {
		close(s.acceptChanged)
		s.acceptChanged = nil
	}
}

func (s *Session) stop(err error) {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	s.err = err
	close(s.done)
	for _, st := range s.streams {
		st.endLocked(err)
		st.rx = ring{}
		for _, q := range st.queue {
			s.finishLocked(q, err)
		}
		st.queue = nil
	}
	for _, q := range s.controls {
		s.finishLocked(q, err)
	}
	s.controls = nil
	s.runnable = nil
	s.active = nil
	s.accepts = nil
	s.streams = make(map[uint64]*Stream)
	s.receiveReserved = 0
	s.localCount, s.peerCount = 0, 0
	s.notifyAcceptLocked()
	s.notifyLocked()
	s.mu.Unlock()
	_ = s.conn.Close()
}

func (s *Session) OpenStream(ctx context.Context) (*Stream, error)  { return s.open(ctx, false) }
func (s *Session) OpenControl(ctx context.Context) (*Stream, error) { return s.open(ctx, true) }

func (s *Session) open(ctx context.Context, control bool) (*Stream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return nil, err
	}
	if s.draining || s.peerDraining {
		s.mu.Unlock()
		return nil, ErrDraining
	}
	if control {
		if s.role != Initiator || s.controlUsed || s.ready {
			s.mu.Unlock()
			return nil, ErrNotReady
		}
	} else {
		if !s.ready {
			s.mu.Unlock()
			return nil, ErrNotReady
		}
		if s.localCount >= int(s.peer.maxPeerStreams) || !s.capacityLocked() {
			s.mu.Unlock()
			return nil, ErrCapacity
		}
		if s.nextID > math.MaxUint64-2 {
			s.draining = true
			s.mu.Unlock()
			return nil, ErrDraining
		}
	}
	id, w := s.nextID, s.cfg.ReceiveWindow
	if control {
		id, w = 1, controlWindow
		s.controlUsed = true
	} else {
		s.nextID += 2
	}
	st := s.newStreamLocked(id, w, true)
	q, err := s.enqueueControlLocked(header{kind: typeOpen, stream: id, value: uint64(w)}, st)
	if err != nil {
		s.removeLocked(st)
		s.mu.Unlock()
		return nil, err
	}
	st.openRequest = q
	if control {
		s.control = st
	}
	s.mu.Unlock()
	for {
		s.mu.Lock()
		if st.err != nil {
			err := st.err
			s.mu.Unlock()
			return nil, err
		}
		if st.ackReceived {
			s.mu.Unlock()
			return st, nil
		}
		changed := s.changedLocked()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			s.mu.Lock()
			fatal := s.resetLocked(st, Canceled, ctx.Err())
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return nil, ctx.Err()
		}
	}
}

func (s *Session) AcceptStream(ctx context.Context) (*Stream, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return nil, err
		}
		if len(s.accepts) > 0 {
			st := s.accepts[0]
			copy(s.accepts, s.accepts[1:])
			s.accepts[len(s.accepts)-1] = nil
			s.accepts = s.accepts[:len(s.accepts)-1]
			s.mu.Unlock()
			return st, nil
		}
		changed := s.acceptChangedLocked()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Session) AcceptControl(ctx context.Context) (*Stream, error) {
	if s.role != Acceptor {
		return nil, ErrNotReady
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return nil, err
		}
		if s.controlAccepted {
			s.mu.Unlock()
			return nil, ErrNotReady
		}
		if s.control != nil {
			st := s.control
			s.controlAccepted = true
			s.mu.Unlock()
			return st, nil
		}
		changed := s.changedLocked()
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *Session) capacityLocked() bool {
	return s.localCount+s.peerCount < s.cfg.MaxStreamsTotal &&
		s.receiveReserved+uint64(s.cfg.ReceiveWindow) <= s.cfg.ReceiveBudget
}

func (s *Session) newStreamLocked(id uint64, w uint32, local bool) *Stream {
	st := &Stream{s: s, id: id, local: local, rx: ring{limit: int(w)}, window: uint64(w), done: make(chan struct{})}
	s.streams[id] = st
	if id != 1 {
		s.receiveReserved += uint64(w)
		if local {
			s.localCount++
		} else {
			s.peerCount++
		}
	}
	return st
}

func (s *Session) removeAcceptLocked(st *Stream) {
	for i, v := range s.accepts {
		if v == st {
			copy(s.accepts[i:], s.accepts[i+1:])
			s.accepts[len(s.accepts)-1] = nil
			s.accepts = s.accepts[:len(s.accepts)-1]
			break
		}
	}
}

func (s *Session) removeLocked(st *Stream) {
	if s.streams[st.id] != st {
		return
	}
	delete(s.streams, st.id)
	s.removeAcceptLocked(st)
	s.removeRunnableLocked(st)
	st.rx = ring{}
	if st.id != 1 {
		s.receiveReserved -= st.window
		if st.local {
			s.localCount--
		} else {
			s.peerCount--
		}
	}
	s.notifyLocked()
}

func (s *Session) reapLocked(st *Stream) {
	if st.pending != 0 {
		return
	}
	if st.err != nil {
		remoteSafe := st.remoteReset || st.resetAckReceived || st.nacked
		localSafe := (!st.localReset || st.resetSent) && (!st.remoteReset || st.resetAckSent)
		if remoteSafe && localSafe {
			s.removeLocked(st)
		}
	} else if st.finSent && st.remoteFin && st.rx.size == 0 {
		st.endLocked(nil)
		s.removeLocked(st)
	}
}

func (s *Session) cancelControlLocked(q *writeRequest) {
	for i, v := range s.controls {
		if v == q {
			copy(s.controls[i:], s.controls[i+1:])
			s.controls[len(s.controls)-1] = nil
			s.controls = s.controls[:len(s.controls)-1]
			s.finishLocked(q, ErrClosed)
			return
		}
	}
}

func (s *Session) discardDataLocked(st *Stream) {
	for _, q := range st.queue {
		s.finishLocked(q, st.err)
	}
	st.queue = nil
	s.removeRunnableLocked(st)
	if st.windowRequest != nil {
		s.cancelControlLocked(st.windowRequest)
		st.windowRequest = nil
	}
	s.removeAcceptLocked(st)
	st.rx.head, st.rx.size = 0, 0
	st.consumed = st.received
}

func (s *Session) resetLocked(st *Stream, code Code, cause error) error {
	if s.err != nil || st.ended {
		return nil
	}
	st.endLocked(cause)
	if st.local && !st.openIssued {
		s.cancelControlLocked(st.openRequest)
		s.removeLocked(st)
		return nil
	}
	st.localReset = true
	st.resetAt = time.Now()
	s.discardDataLocked(st)
	_, err := s.enqueueControlLocked(header{kind: typeReset, stream: st.id, value: uint64(code)}, st)
	return err
}

func (s *Session) queueWindowLocked(st *Stream) error {
	if st.err != nil || st.remoteFin || st.consumed <= st.advertised {
		return nil
	}
	if st.windowRequest != nil {
		st.windowRequest.h.value = st.consumed
		return nil
	}
	q, err := s.enqueueControlLocked(header{kind: typeWindow, stream: st.id, value: st.consumed}, st)
	if err == nil {
		st.windowRequest = q
	}
	return err
}

func (s *Session) maintenance() {
	defer s.wg.Done()
	ticker := time.NewTicker(windowDelay)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			var fatal error
			for _, st := range s.streams {
				if st.localReset && !st.remoteReset && !st.resetAckReceived && !st.nacked && now.Sub(st.resetAt) >= s.cfg.ResetTimeout {
					fatal = protocolError("RST_ACK timeout for stream %d", st.id)
					break
				}
				if fatal = s.queueWindowLocked(st); fatal != nil {
					break
				}
			}
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
				return
			}
		}
	}
}
