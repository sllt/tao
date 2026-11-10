package taonet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"
)

type Target struct {
	Peer    PeerID
	Address string
}
type EndpointConfig struct {
	Profile         Profile
	Session         SessionConfig
	MaxSessions     int
	ManagedBytes    int
	ConnectTimeout  time.Duration
	IdleTimeout     time.Duration
	MaxDialAttempts int
}
type EndpointEventKind string

const (
	SessionReady        EndpointEventKind = "session_ready"
	SessionClosed       EndpointEventKind = "session_closed"
	PeerInstanceChanged EndpointEventKind = "peer_instance_changed"
	DialFailed          EndpointEventKind = "dial_failed"
)

type EndpointEvent struct {
	Kind             EndpointEventKind
	Peer             PeerID
	Session          SessionInfo
	PreviousInstance InstanceID
	Err              error
}
type dialFlight struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	session *Session
	err     error
}

type dialTargetKey struct {
	Target
	Class TrafficClass
	Group SessionID
}

// Endpoint owns listeners and sessions. Compact uses one preferred session per
// peer; existing operations stay attached to the session that created them.
type Endpoint struct {
	cfg                                               EndpointConfig
	ctx                                               context.Context
	cancel                                            context.CancelFunc
	mu                                                sync.Mutex
	wg                                                sync.WaitGroup
	closeOnce                                         sync.Once
	done                                              chan struct{}
	changed                                           chan struct{}
	events                                            chan EndpointEvent
	eventsClosed                                      bool
	draining, closed                                  bool
	listeners                                         map[net.Listener]struct{}
	sessions                                          map[*Session]Target
	instances                                         map[PeerID]InstanceID
	flights                                           map[dialTargetKey]*dialFlight
	groups                                            map[SessionID]*connectionGroup
	pending                                           int
	reservedBytes                                     int
	sessionCost                                       int
	dialAttempts, dialFailures, reused, droppedEvents uint64
}

func NewEndpoint(cfg EndpointConfig) (*Endpoint, error) {
	var err error
	cfg.Session, err = cfg.Session.normalized()
	if err != nil {
		return nil, err
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = 8
	}
	if cfg.ManagedBytes == 0 {
		cfg.ManagedBytes = 256 << 20
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = 15 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = time.Minute
	}
	if cfg.MaxDialAttempts == 0 {
		cfg.MaxDialAttempts = 3
	}
	// Conservative capacity reservations, not a measurement of total Go heap.
	const base = 4<<20 + 256<<10
	if cfg.Profile > Isolated || cfg.MaxSessions < 1 || cfg.ManagedBytes < base || cfg.Session.Limits.ReceiveBytes > cfg.ManagedBytes-base || cfg.Session.Limits.SendBytes > cfg.ManagedBytes-base-cfg.Session.Limits.ReceiveBytes || cfg.ConnectTimeout < 0 || cfg.IdleTimeout < 0 || cfg.MaxDialAttempts < 1 || cfg.MaxDialAttempts > 16 {
		return nil, fmt.Errorf("taonet: invalid endpoint limits")
	}
	cost := base + cfg.Session.Limits.ReceiveBytes + cfg.Session.Limits.SendBytes
	routes := cfg.Session.Mux.snapshot()
	cfg.Session.Mux = &Mux{requests: routes.requests, notifies: routes.notifies, channels: routes.channels, streams: routes.streams}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Endpoint{cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{}), changed: make(chan struct{}), events: make(chan EndpointEvent, 64),
		listeners: make(map[net.Listener]struct{}), sessions: make(map[*Session]Target), instances: make(map[PeerID]InstanceID), flights: make(map[dialTargetKey]*dialFlight), groups: make(map[SessionID]*connectionGroup), sessionCost: int(cost)}
	e.cfg.Session.connection.Isolated = cfg.Profile == Isolated
	e.cfg.Session.acceptConnection = e.acceptConnection
	if cfg.Profile == Isolated {
		if err = e.cfg.Session.Mux.HandleRequest(addConnectionRoute, e.addConnection); err != nil {
			cancel()
			return nil, err
		}
	}
	e.cfg.Session.Mux.snapshot()
	e.wg.Add(1)
	go e.maintain()
	return e, nil
}

func (e *Endpoint) Done() <-chan struct{}        { return e.done }
func (e *Endpoint) Events() <-chan EndpointEvent { return e.events }
func (e *Endpoint) notifyLocked()                { close(e.changed); e.changed = make(chan struct{}) }
func (e *Endpoint) emitLocked(event EndpointEvent) {
	if e.eventsClosed {
		e.droppedEvents++
		return
	}
	select {
	case e.events <- event:
	default:
		e.droppedEvents++
	}
}

func (e *Endpoint) reserveLocked() error {
	if e.closed {
		return ErrClosed
	}
	if e.draining {
		return ErrDraining
	}
	if len(e.sessions)+e.pending >= e.cfg.MaxSessions || e.sessionCost > e.cfg.ManagedBytes-e.reservedBytes {
		return &StatusError{Code: CodeResourceExhausted, Message: "endpoint connection budget exhausted"}
	}
	e.pending++
	e.reservedBytes += e.sessionCost
	e.notifyLocked()
	return nil
}
func (e *Endpoint) releasePending() {
	e.mu.Lock()
	e.pending--
	e.reservedBytes -= e.sessionCost
	e.notifyLocked()
	e.mu.Unlock()
}

func (e *Endpoint) Serve(ctx context.Context, listener net.Listener) error {
	e.mu.Lock()
	if e.closed || e.draining {
		e.mu.Unlock()
		_ = listener.Close()
		return ErrClosed
	}
	e.listeners[listener] = struct{}{}
	e.wg.Add(1)
	e.mu.Unlock()
	defer e.wg.Done()
	defer func() { _ = listener.Close(); e.mu.Lock(); delete(e.listeners, listener); e.mu.Unlock() }()
	stopCaller := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopCaller()
	stopEndpoint := context.AfterFunc(e.ctx, func() { _ = listener.Close() })
	defer stopEndpoint()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if e.ctx.Err() != nil {
				return ErrClosed
			}
			return err
		}
		e.mu.Lock()
		err = e.reserveLocked()
		if err == nil {
			e.wg.Add(1)
		}
		e.mu.Unlock()
		if err != nil {
			_ = conn.Close()
			continue
		}
		go func() {
			defer e.wg.Done()
			handshake, cancel := context.WithTimeout(ctx, e.cfg.Session.HandshakeTimeout)
			defer cancel()
			stop := context.AfterFunc(e.ctx, cancel)
			defer stop()
			session, err := newSession(handshake, conn, Acceptor, e.cfg.Session)
			if err == nil && e.register(handshake, session, Target{Peer: session.info.Remote.ID}) {
				return
			}
			if session != nil {
				_ = session.Close()
			}
			e.releasePending()
		}()
	}
}

func (e *Endpoint) register(ctx context.Context, session *Session, target Target) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || e.draining || ctx.Err() != nil || session.Err() != nil {
		return false
	}
	peer := session.info.Remote
	if session.info.Class != ClassControl {
		group := e.groups[session.info.Group]
		if group == nil || group.remote != peer || group.main.Err() != nil {
			return false
		}
	} else {
		if previous, ok := e.instances[peer.ID]; ok && previous != peer.Instance {
			e.emitLocked(EndpointEvent{Kind: PeerInstanceChanged, Peer: peer.ID, Session: session.Info(), PreviousInstance: previous})
		}
		e.instances[peer.ID] = peer.Instance
		if session.info.Group != (ID{}) {
			e.groups[session.info.Group] = &connectionGroup{main: session, remote: peer}
		}
	}
	e.pending--
	e.sessions[session] = target
	e.notifyLocked()
	e.emitLocked(EndpointEvent{Kind: SessionReady, Peer: peer.ID, Session: session.Info()})
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		<-session.Done()
		_ = session.Close()
		e.mu.Lock()
		delete(e.sessions, session)
		if session.info.Class == ClassControl {
			delete(e.groups, session.info.Group)
		}
		e.reservedBytes -= e.sessionCost
		e.notifyLocked()
		e.emitLocked(EndpointEvent{Kind: SessionClosed, Peer: peer.ID, Session: session.Info(), Err: session.Err()})
		e.mu.Unlock()
	}()
	return true
}

func preferred(a, b *Session) int {
	initiator := func(s *Session) PeerID {
		if s.role == Initiator {
			return s.info.Local.ID
		}
		return s.info.Remote.ID
	}
	left, right := initiator(a), initiator(b)
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return bytes.Compare(a.info.ID[:], b.info.ID[:])
}
func (e *Endpoint) preferredLocked(peer PeerID, class TrafficClass, group SessionID) *Session {
	var chosen *Session
	for session := range e.sessions {
		if session.info.Remote.ID != peer || session.info.Remote.Instance != e.instances[peer] || session.info.Class != class || group != (ID{}) && session.info.Group != group {
			continue
		}
		session.mu.Lock()
		available := session.err == nil && !session.draining && !session.peerDraining
		session.mu.Unlock()
		if available && (chosen == nil || preferred(session, chosen) < 0) {
			chosen = session
		}
	}
	return chosen
}

func (e *Endpoint) Sessions(peer PeerID) []*Session {
	e.mu.Lock()
	defer e.mu.Unlock()
	var sessions []*Session
	for session := range e.sessions {
		if peer == "" || session.info.Remote.ID == peer {
			sessions = append(sessions, session)
		}
	}
	slices.SortFunc(sessions, preferred)
	return sessions
}

func (e *Endpoint) Connect(ctx context.Context, target Target) (*Session, error) {
	return e.connect(ctx, dialTargetKey{Target: target})
}

func (e *Endpoint) connect(ctx context.Context, key dialTargetKey) (*Session, error) {
	target := key.Target
	ctx, cancel := withDefaultTimeout(ctx, e.cfg.ConnectTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validName(string(target.Peer)) {
		return nil, fmt.Errorf("taonet: target peer is required")
	}
	e.mu.Lock()
	if e.closed || e.draining {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if session := e.preferredLocked(target.Peer, key.Class, key.Group); session != nil {
		e.reused++
		e.mu.Unlock()
		return session, nil
	}
	if target.Address == "" {
		e.mu.Unlock()
		return nil, fmt.Errorf("taonet: address required for unconnected peer %q", target.Peer)
	}
	flight := e.flights[key]
	if flight == nil {
		if err := e.reserveLocked(); err != nil {
			e.mu.Unlock()
			return nil, err
		}
		dialCtx, stop := context.WithTimeout(e.ctx, e.cfg.ConnectTimeout)
		flight = &dialFlight{done: make(chan struct{}), cancel: stop}
		e.flights[key] = flight
		e.wg.Add(1)
		go e.dialTarget(dialCtx, key, flight)
	}
	flight.waiters++
	e.notifyLocked()
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		flight.waiters--
		if flight.waiters == 0 {
			if e.flights[key] == flight {
				delete(e.flights, key)
			}
			flight.cancel()
		}
		e.notifyLocked()
		e.mu.Unlock()
	}()
	select {
	case <-flight.done:
		return flight.session, flight.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ctx.Done():
		return nil, ErrClosed
	}
}

func (e *Endpoint) dialTarget(ctx context.Context, key dialTargetKey, flight *dialFlight) {
	defer e.wg.Done()
	defer flight.cancel()
	var session *Session
	var err error
	target := key.Target
	cfg := e.cfg.Session
	cfg.Transport.ExpectedPeer = target.Peer
	if key.Class != ClassControl {
		e.mu.Lock()
		group := e.groups[key.Group]
		if group == nil || group.remote.ID != target.Peer {
			err = ErrClosed
		} else {
			cfg.connection = connectionBinding{Isolated: true, Class: key.Class, Group: key.Group, RemoteInstance: group.remote.Instance}
		}
		e.mu.Unlock()
	}
	for attempt := 0; attempt < e.cfg.MaxDialAttempts; attempt++ {
		if err != nil && attempt == 0 {
			break
		}
		e.mu.Lock()
		e.dialAttempts++
		e.mu.Unlock()
		session, err = dial(ctx, target.Address, cfg)
		if err == nil {
			break
		}
		e.mu.Lock()
		e.dialFailures++
		e.mu.Unlock()
		var network net.Error
		if attempt+1 == e.cfg.MaxDialAttempts || ctx.Err() != nil || !errors.As(err, &network) {
			break
		}
		cap := min(100*time.Millisecond<<attempt, 10*time.Second)
		timer := time.NewTimer(time.Duration(rand.Int64N(int64(cap))))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			err = ctx.Err()
		}
		if ctx.Err() != nil {
			break
		}
	}
	registered := false
	if err == nil && ctx.Err() == nil {
		registered = e.register(ctx, session, target)
	}
	if !registered {
		if session != nil {
			_ = session.Close()
			session = nil
		}
		if err == nil {
			err = ctx.Err()
			if err == nil {
				err = ErrClosed
			}
		}
		e.releasePending()
	}
	e.mu.Lock()
	flight.session, flight.err = session, err
	if e.flights[key] == flight {
		delete(e.flights, key)
	}
	if err != nil {
		e.emitLocked(EndpointEvent{Kind: DialFailed, Peer: target.Peer, Err: err})
	}
	close(flight.done)
	e.notifyLocked()
	e.mu.Unlock()
}

func (e *Endpoint) maintain() {
	defer e.wg.Done()
	ticker := time.NewTicker(max(time.Millisecond, min(e.cfg.IdleTimeout/2, time.Second)))
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case now := <-ticker.C:
			e.retireIdle(now)
		}
	}
}

func (e *Endpoint) retireIdle(now time.Time) {
	var idle []*Session
	e.mu.Lock()
	for session := range e.sessions {
		keepPrimary := false
		if session.info.Class == ClassControl && session.info.Group != (ID{}) {
			for member := range e.sessions {
				if member == session || member.info.Group != session.info.Group {
					continue
				}
				member.mu.Lock()
				busy := len(member.operations) > 0 || now.Sub(member.lastOperation) < e.cfg.IdleTimeout
				member.mu.Unlock()
				if busy {
					keepPrimary = true
					break
				}
			}
		}
		if keepPrimary {
			continue
		}
		session.mu.Lock()
		if session.err == nil && !session.draining && len(session.operations) == 0 && now.Sub(session.lastOperation) >= e.cfg.IdleTimeout {
			session.draining = true
			idle = append(idle, session)
		}
		session.mu.Unlock()
	}
	e.mu.Unlock()
	for _, session := range idle {
		_ = session.Close()
	}
}

func (e *Endpoint) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	e.draining = true
	listeners := make([]net.Listener, 0, len(e.listeners))
	for listener := range e.listeners {
		listeners = append(listeners, listener)
	}
	sessions := make([]*Session, 0, len(e.sessions))
	for session := range e.sessions {
		sessions = append(sessions, session)
	}
	e.mu.Unlock()
	e.cancel()
	for _, listener := range listeners {
		_ = listener.Close()
	}
	var wg sync.WaitGroup
	failures := make(chan error, len(sessions))
	for _, session := range sessions {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := session.Shutdown(ctx); err != nil {
				failures <- err
			}
		}()
	}
	wg.Wait()
	close(failures)
	_ = e.Close()
	for err := range failures {
		return err
	}
	return ctx.Err()
}

func (e *Endpoint) Close() error {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		e.draining = true
		listeners := make([]net.Listener, 0, len(e.listeners))
		for listener := range e.listeners {
			listeners = append(listeners, listener)
		}
		sessions := make([]*Session, 0, len(e.sessions))
		for session := range e.sessions {
			sessions = append(sessions, session)
		}
		e.mu.Unlock()
		e.cancel()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		for _, session := range sessions {
			_ = session.Close()
		}
		e.wg.Wait()
		e.mu.Lock()
		e.eventsClosed = true
		close(e.events)
		close(e.done)
		e.notifyLocked()
		e.mu.Unlock()
	})
	return nil
}
