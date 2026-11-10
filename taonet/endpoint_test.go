package taonet

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func endpoint(t *testing.T, cfg EndpointConfig) *Endpoint {
	t.Helper()
	e, err := NewEndpoint(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}
func serveEndpoint(t *testing.T, e *Endpoint) (string, context.CancelFunc, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- e.Serve(ctx, listener) }()
	t.Cleanup(func() { cancel(); _ = listener.Close() })
	return listener.Addr().String(), cancel, result
}
func waitEndpoint(t *testing.T, e *Endpoint, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		e.mu.Lock()
		ready := condition()
		changed := e.changed
		e.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("endpoint state did not advance")
		}
	}
}

func TestEndpointReusesInboundAndListenerCancellationKeepsSession(t *testing.T) {
	m := NewMux()
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	a := endpoint(t, EndpointConfig{Session: localConfig("client", m)})
	b := endpoint(t, EndpointConfig{Session: localConfig("server", nil)})
	address, cancelServe, served := serveEndpoint(t, b)
	s, err := a.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, b, func() bool { return len(b.sessions) == 1 })
	reverse, err := b.Connect(t.Context(), Target{Peer: "client"})
	if err != nil {
		t.Fatal(err)
	}
	if reverse.Info().ID != s.Info().ID {
		t.Fatal("did not reuse accepted session")
	}
	cancelServe()
	if err = <-served; !errors.Is(err, context.Canceled) {
		t.Fatalf("Serve: %v", err)
	}
	response, err := reverse.Request(t.Context(), "echo", Message{Body: []byte("still connected")})
	if err != nil || string(response.Body) != "still connected" {
		t.Fatalf("session tied to listener context: %v", err)
	}
	if b.Stats().DialAttempts != 0 {
		t.Fatal("reverse reuse unexpectedly dialed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = a.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if stats := a.Stats(); stats.Sessions != 0 || stats.Handshakes != 0 || stats.ReservedBytes != 0 {
		t.Fatalf("endpoint leaked reservations: %+v", stats)
	}
}

func TestEndpointSingleflightCallerCancellation(t *testing.T) {
	checking, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	cfg := localConfig("client", nil)
	cfg.Authorize = func(AuthInfo, Peer) error { close(checking); <-release; return nil }
	a := endpoint(t, EndpointConfig{Session: cfg})
	b := endpoint(t, EndpointConfig{Session: localConfig("server", nil)})
	address, _, _ := serveEndpoint(t, b)
	target := Target{Peer: "server", Address: address}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	first := make(chan error, 1)
	go func() { _, err := a.Connect(firstCtx, target); first <- err }()
	waitSignal(t, checking)
	type connection struct {
		s   *Session
		err error
	}
	results := make(chan connection, 8)
	for range 8 {
		go func() { s, err := a.Connect(t.Context(), target); results <- connection{s, err} }()
	}
	waitEndpoint(t, a, func() bool {
		key := dialTargetKey{Target: target}
		return a.flights[key] != nil && a.flights[key].waiters == 9
	})
	cancelFirst()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	releaseOnce()
	var shared *Session
	for range 8 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if shared != nil && shared != got.s {
			t.Fatal("dial was not shared")
		}
		shared = got.s
	}
	if a.Stats().DialAttempts != 1 {
		t.Fatal(a.Stats())
	}
}

func TestEndpointSimultaneousDialSelectsSameConnection(t *testing.T) {
	reached := make(chan struct{}, 4)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	permission := func(AuthInfo, Peer) error { reached <- struct{}{}; <-release; return nil }
	aCfg, bCfg := localConfig("a", nil), localConfig("b", nil)
	aCfg.Authorize = permission
	bCfg.Authorize = permission
	a := endpoint(t, EndpointConfig{Session: aCfg})
	b := endpoint(t, EndpointConfig{Session: bCfg})
	aAddress, _, _ := serveEndpoint(t, a)
	bAddress, _, _ := serveEndpoint(t, b)
	results := make(chan error, 2)
	go func() { _, err := a.Connect(t.Context(), Target{Peer: "b", Address: bAddress}); results <- err }()
	go func() { _, err := b.Connect(t.Context(), Target{Peer: "a", Address: aAddress}); results <- err }()
	// Each acceptor first validates HELLO; the initiator validations follow WELCOME.
	for range 2 {
		waitSignal(t, reached)
	}
	releaseOnce()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	waitEndpoint(t, a, func() bool { return len(a.sessions) == 2 })
	waitEndpoint(t, b, func() bool { return len(b.sessions) == 2 })
	left, err := a.Connect(t.Context(), Target{Peer: "b"})
	if err != nil {
		t.Fatal(err)
	}
	right, err := b.Connect(t.Context(), Target{Peer: "a"})
	if err != nil {
		t.Fatal(err)
	}
	if left.Info().ID != right.Info().ID || left.role != Initiator || right.role != Acceptor {
		t.Fatal("default connection selection disagreed")
	}
}

func TestEndpointReconnectReportsChangedInstance(t *testing.T) {
	a := endpoint(t, EndpointConfig{Session: localConfig("client", nil), MaxDialAttempts: 1})
	b := endpoint(t, EndpointConfig{Session: localConfig("server", nil)})
	address, _, _ := serveEndpoint(t, b)
	old, err := a.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	waitSignal(t, old.Done())
	cfg := localConfig("server", nil)
	cfg.Local.Instance, err = newID()
	if err != nil {
		t.Fatal(err)
	}
	restarted := endpoint(t, EndpointConfig{Session: cfg})
	address, _, _ = serveEndpoint(t, restarted)
	fresh, err := a.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Info().ID == old.Info().ID || fresh.Info().Remote.Instance == old.Info().Remote.Instance {
		t.Fatal("reused old connection identity")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		select {
		case event := <-a.Events():
			if event.Kind == PeerInstanceChanged {
				if event.PreviousInstance != old.Info().Remote.Instance || event.Session.Remote.Instance != cfg.Local.Instance {
					t.Fatal(event)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("missing instance-change event")
		}
	}
}

func TestEndpointConnectionBudgetAndIdleRetirement(t *testing.T) {
	m := NewMux()
	if err := m.HandleChannel("hold", func(ctx context.Context, c *Channel) error {
		select {
		case <-ctx.Done():
		case <-c.Done():
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a := endpoint(t, EndpointConfig{Session: localConfig("client", nil), IdleTimeout: 100 * time.Millisecond, MaxSessions: 1})
	b := endpoint(t, EndpointConfig{Session: localConfig("server", m), MaxSessions: 1})
	address, _, _ := serveEndpoint(t, b)
	s, err := a.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.OpenChannel(t.Context(), "hold", nil)
	if err != nil {
		t.Fatal(err)
	}
	other := endpoint(t, EndpointConfig{Session: localConfig("other", nil), MaxDialAttempts: 1})
	if _, err = other.Connect(t.Context(), Target{Peer: "server", Address: address}); err == nil {
		t.Fatal("connection budget exceeded")
	}
	if stats := other.Stats(); stats.ReservedBytes != 0 || stats.Handshakes != 0 {
		t.Fatal(stats)
	}
	// Exercise the same admission lock used by the idle sweep without timing a
	// handler: an active Channel must remain registered even with an old timestamp.
	s.mu.Lock()
	s.lastOperation = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	a.retireIdle(time.Now())
	select {
	case <-s.Done():
		t.Fatal("retired a session with a live Channel")
	default:
	}
	if stats := s.Stats(); stats.Operations != 1 {
		t.Fatal(stats)
	}
	_ = c.Close()
	waitIdle(t, s)
	waitSignal(t, s.Done())
	waitEndpoint(t, a, func() bool { return len(a.sessions) == 0 })
	if stats := a.Stats(); stats.ReservedBytes != 0 {
		t.Fatal(stats)
	}
}

func TestEndpointRetryBudgetIsFinite(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	e := endpoint(t, EndpointConfig{Session: localConfig("client", nil), MaxDialAttempts: 3})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err = e.Connect(ctx, Target{Peer: "offline", Address: address}); err == nil {
		t.Fatal("dialed closed listener")
	}
	if stats := e.Stats(); stats.DialAttempts != 3 || stats.DialFailures != 3 || stats.ReservedBytes != 0 {
		t.Fatal(stats)
	}
}
