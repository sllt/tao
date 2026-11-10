package taonet

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestIsolatedOutboundOnlyClientAddsConnectionsAndSurvivesBulkFailure(t *testing.T) {
	m := NewMux()
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	blocked, canceled := make(chan struct{}), make(chan struct{})
	if err := m.HandleStream("bulk", func(ctx context.Context, _ *Stream) error { close(blocked); <-ctx.Done(); close(canceled); return nil }); err != nil {
		t.Fatal(err)
	}
	client := endpoint(t, EndpointConfig{Profile: Isolated, Session: localConfig("client", m)})
	server := endpoint(t, EndpointConfig{Profile: Isolated, Session: localConfig("server", nil)})
	address, _, _ := serveEndpoint(t, server)
	main, err := client.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, server, func() bool { return len(server.sessions) == 1 })
	control, err := server.Connect(t.Context(), Target{Peer: "client"})
	if err != nil {
		t.Fatal(err)
	}
	connections := map[SessionID]bool{control.info.ID: true}
	var bulk *Session
	for _, class := range []TrafficClass{ClassInteractive, ClassReply, ClassBulk} {
		s, err := server.ConnectClass(t.Context(), Target{Peer: "client"}, class)
		if err != nil {
			t.Fatal(err)
		}
		if s.info.Group != main.info.ID || s.info.Class != class || connections[s.info.ID] {
			t.Fatal("traffic classes did not get independent bound connections")
		}
		connections[s.info.ID] = true
		if class == ClassBulk {
			bulk = s
		}
	}
	if client.Stats().DialAttempts != 4 || server.Stats().DialAttempts != 0 {
		t.Fatalf("reverse connection counts: client=%+v server=%+v", client.Stats(), server.Stats())
	}
	stream, err := bulk.OpenStream(t.Context(), "bulk", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitSignal(t, blocked)
	writeDone := make(chan error, 1)
	go func() {
		_, err := io.CopyBuffer(stream, io.LimitReader(repeatedByte('b'), 8<<20), make([]byte, 32<<10))
		writeDone <- err
	}()
	// Keep an active bulk operation while exercising primary idle retirement.
	control.mu.Lock()
	control.lastOperation = time.Now().Add(-time.Hour)
	control.mu.Unlock()
	server.retireIdle(time.Now())
	if _, err = control.Request(t.Context(), "echo", Message{Body: []byte("control remains usable")}); err != nil {
		t.Fatal(err)
	}
	_ = bulk.Close()
	waitSignal(t, canceled)
	select {
	case err = <-writeDone:
		if err == nil {
			t.Fatal("closed bulk write succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("bulk writer stuck")
	}
	if _, err = control.Request(t.Context(), "echo", Message{}); err != nil {
		t.Fatal("bulk failure killed control:", err)
	}
	if main.Err() != nil {
		t.Fatal(main.Err())
	}
}

func TestIsolatedRejectsWrongInstanceAndProfile(t *testing.T) {
	client := endpoint(t, EndpointConfig{Profile: Isolated, Session: localConfig("client", nil)})
	server := endpoint(t, EndpointConfig{Profile: Isolated, Session: localConfig("server", nil)})
	address, _, _ := serveEndpoint(t, server)
	main, err := client.Connect(t.Context(), Target{Peer: "server", Address: address})
	if err != nil {
		t.Fatal(err)
	}
	waitEndpoint(t, server, func() bool { return len(server.sessions) == 1 })
	for _, wrongLocal := range []bool{false, true} {
		cfg := client.cfg.Session
		cfg.connection = connectionBinding{Isolated: true, Class: ClassBulk, Group: main.info.Group, RemoteInstance: main.info.Remote.Instance}
		other, err := newID()
		if err != nil {
			t.Fatal(err)
		}
		if wrongLocal {
			cfg.Local.Instance = other
		} else {
			cfg.connection.RemoteInstance = other
		}
		if s, err := Dial(t.Context(), address, cfg); err == nil {
			_ = s.Close()
			t.Fatal("mixed process instances in one group")
		}
	}
	if s, err := Dial(t.Context(), address, localConfig("compact", nil)); err == nil {
		_ = s.Close()
		t.Fatal("silently downgraded Isolated")
	}
	waitEndpoint(t, server, func() bool { return server.pending == 0 })
	if len(server.Sessions("client")) != 1 {
		t.Fatal("invalid auxiliary registered")
	}
}
