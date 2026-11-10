package taonet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

type operation struct {
	ctx           context.Context
	cancel        context.CancelCauseFunc
	stream        *native.Stream
	incoming      bool
	admitted      bool
	receivedBytes int
	sentBytes     int
	handler       bool
}

type Session struct {
	raw             *native.Session
	control         *native.Stream
	role            Role
	cfg             SessionConfig
	peerLimits      Limits
	info            SessionInfo
	routes          routeTable
	ctx             context.Context
	cancel          context.CancelCauseFunc
	once            sync.Once
	wg              sync.WaitGroup
	controlMu       sync.Mutex
	mu              sync.Mutex
	done            chan struct{}
	changed         chan struct{}
	err             error
	draining        bool
	peerDraining    bool
	operations      map[*operation]struct{}
	receivedBytes   int
	sentBytes       int
	handlers        int
	lastIncoming    uint64
	lastActivity    time.Time
	lastOperation   time.Time
	pingSequence    uint64
	pendingPing     uint64
	notifyQueue     chan notification
	events          chan Event
	droppedEvents   uint64
	requestFailures uint64
	unknownResults  uint64
	asyncFailures   uint64
}

func makeSession(raw *native.Session, control *native.Stream, role Role, cfg SessionConfig, info SessionInfo, peerLimits Limits) *Session {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &Session{raw: raw, control: control, role: role, cfg: cfg, peerLimits: peerLimits, info: info, routes: cfg.Mux.snapshot(),
		ctx: ctx, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{}), operations: make(map[*operation]struct{}), lastActivity: time.Now(), lastOperation: time.Now(),
		notifyQueue: make(chan notification, cfg.Limits.MaxHandlers), events: make(chan Event, 64)}
}

func (s *Session) start() {
	s.wg.Add(4)
	go s.acceptLoop()
	go s.controlLoop()
	go s.heartbeat()
	go s.dispatchNotifications()
}

func (s *Session) Info() SessionInfo {
	info := s.info
	info.Features = append([]string(nil), info.Features...)
	return info
}
func (s *Session) Done() <-chan struct{} { return s.done }
func (s *Session) Err() error            { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
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
func (s *Session) touch() { s.mu.Lock(); s.lastActivity = time.Now(); s.mu.Unlock() }

func (s *Session) stop(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.err = err
		s.draining = true
		close(s.done)
		close(s.events)
		s.notifyLocked()
		ops := make([]*operation, 0, len(s.operations))
		for op := range s.operations {
			ops = append(ops, op)
		}
		s.mu.Unlock()
		s.cancel(err)
		for _, op := range ops {
			op.cancel(err)
		}
		_ = s.raw.Close()
	})
}

// Close stops network activity. Application callbacks must cooperate with
// cancellation; Close does not wait indefinitely for a stuck user callback.
func (s *Session) Close() error { s.stop(ErrClosed); s.wg.Wait(); return nil }

func (s *Session) begin(parent context.Context, incoming bool) (*operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if s.draining || !incoming && s.peerDraining {
		return nil, ErrDraining
	}
	if len(s.operations) >= s.cfg.Limits.MaxOperations {
		return nil, &StatusError{Code: CodeResourceExhausted, Message: "operation limit reached"}
	}
	ctx, cancel := context.WithCancelCause(parent)
	op := &operation{ctx: ctx, cancel: cancel, incoming: incoming}
	s.operations[op] = struct{}{}
	s.lastOperation = time.Now()
	s.notifyLocked()
	return op, nil
}

func (s *Session) reserve(op *operation, receive, send int, handler bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.draining && !op.admitted {
		return ErrDraining
	}
	if receive > s.cfg.Limits.ReceiveBytes-s.receivedBytes || send > s.cfg.Limits.SendBytes-muxSendBudget-s.sentBytes || handler && s.handlers >= s.cfg.Limits.MaxHandlers {
		return &StatusError{Code: CodeResourceExhausted, Message: "message or handler budget exhausted"}
	}
	s.receivedBytes += receive
	op.receivedBytes += receive
	s.sentBytes += send
	op.sentBytes += send
	if handler {
		s.handlers++
		op.handler = true
	}
	op.admitted = true
	if op.incoming {
		s.lastIncoming = max(s.lastIncoming, op.stream.ID())
	}
	return nil
}

func (s *Session) finish(op *operation) {
	s.mu.Lock()
	delete(s.operations, op)
	s.lastOperation = time.Now()
	s.receivedBytes -= op.receivedBytes
	s.sentBytes -= op.sentBytes
	if op.handler {
		s.handlers--
	}
	s.notifyLocked()
	s.mu.Unlock()
	op.cancel(context.Canceled)
}

func (s *Session) releaseSend(op *operation, n int) {
	s.mu.Lock()
	s.sentBytes -= n
	op.sentBytes -= n
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *Session) acceptLoop() {
	defer s.wg.Done()
	for {
		stream, err := s.raw.AcceptStream(s.ctx)
		if err != nil {
			if s.ctx.Err() == nil {
				s.stop(err)
			}
			return
		}
		op, err := s.begin(s.ctx, true)
		if err != nil {
			code := native.ResourceExhausted
			if errors.Is(err, ErrDraining) {
				code = native.Draining
			}
			_ = stream.Reset(code)
			continue
		}
		op.stream = stream
		go s.serveOperation(op)
	}
}

type goAway struct {
	Last uint64 `json:"last"`
}

func (s *Session) sendControl(kind byte, tag uint64, payload []byte) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	_ = s.control.SetWriteDeadline(time.Now().Add(s.cfg.DefaultWriteTimeout))
	return writeRecord(s.control, kind, tag, payload)
}

func (s *Session) controlLoop() {
	defer s.wg.Done()
	for {
		h, err := readRecordHeader(s.control)
		if err != nil {
			s.stop(err)
			return
		}
		s.touch()
		switch h.kind {
		case recordPing:
			if err = s.sendControl(recordPong, h.tag, nil); err != nil {
				s.stop(err)
				return
			}
		case recordPong:
			s.mu.Lock()
			valid := s.pendingPing != 0 && s.pendingPing == h.tag
			if valid {
				s.pendingPing = 0
			}
			s.mu.Unlock()
			if !valid {
				s.stop(protocolError("unexpected PONG"))
				return
			}
		case recordGoAway:
			var msg goAway
			if err = readJSONBody(s.control, h, &msg); err != nil {
				s.stop(err)
				return
			}
			s.mu.Lock()
			s.peerDraining = true
			s.notifyLocked()
			s.mu.Unlock()
		default:
			s.stop(protocolError("unexpected control record %d", h.kind))
			return
		}
	}
}

func (s *Session) heartbeat() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.KeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			s.mu.Lock()
			idle := now.Sub(s.lastActivity)
			var ping uint64
			if idle >= s.cfg.KeepAliveInterval && s.pendingPing == 0 {
				s.pingSequence++
				ping = s.pingSequence
				s.pendingPing = ping
			}
			s.mu.Unlock()
			if idle >= s.cfg.IdleTimeout {
				s.stop(fmt.Errorf("taonet: heartbeat timeout"))
				return
			}
			if ping != 0 {
				if err := s.sendControl(recordPing, ping, nil); err != nil {
					s.stop(err)
					return
				}
			}
		}
	}
}

func (s *Session) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return s.Close()
	}
	first := !s.draining
	s.draining = true
	last := s.lastIncoming
	s.mu.Unlock()
	if first {
		payload, _ := json.Marshal(goAway{Last: last})
		stop := context.AfterFunc(ctx, func() { s.stop(ctx.Err()) })
		err := s.sendControl(recordGoAway, 0, payload)
		stop()
		if err != nil {
			s.stop(err)
			_ = s.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if err = s.raw.Drain(); err != nil {
			s.stop(err)
			return err
		}
	}
	for {
		s.mu.Lock()
		remaining := len(s.operations)
		changed := s.changedLocked()
		s.mu.Unlock()
		if remaining == 0 {
			return s.Close()
		}
		select {
		case <-changed:
		case <-ctx.Done():
			_ = s.Close()
			return ctx.Err()
		}
	}
}
