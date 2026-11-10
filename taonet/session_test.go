package taonet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func localConfig(id PeerID, mux *Mux) SessionConfig {
	return SessionConfig{Local: Peer{ID: id}, Mux: mux, Transport: TransportConfig{AllowInsecureLoopback: true}}
}

func connectPair(t *testing.T, a, b SessionConfig) (*Session, *Session, error, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	type accepted struct {
		s   *Session
		err error
	}
	result := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			result <- accepted{err: err}
			return
		}
		s, err := Accept(ctx, conn, b)
		result <- accepted{s, err}
	}()
	client, clientErr := Dial(ctx, listener.Addr().String(), a)
	if clientErr != nil {
		_ = listener.Close()
	}
	server := <-result
	if client != nil {
		t.Cleanup(func() { _ = client.Close() })
	}
	if server.s != nil {
		t.Cleanup(func() { _ = server.s.Close() })
	}
	return client, server.s, clientErr, server.err
}

func pair(t *testing.T, a, b SessionConfig) (*Session, *Session) {
	t.Helper()
	client, server, ce, se := connectPair(t, a, b)
	if ce != nil || se != nil {
		t.Fatalf("connect: client=%v server=%v", ce, se)
	}
	return client, server
}

func handle(t *testing.T, mux *Mux, route string, h RequestHandler) {
	t.Helper()
	if err := mux.HandleRequest(route, h); err != nil {
		t.Fatal(err)
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func waitIdle(t *testing.T, s *Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		s.mu.Lock()
		n := len(s.operations)
		changed := s.changedLocked()
		if n == 0 && (s.handlers != 0 || s.receivedBytes != 0 || s.sentBytes != 0) {
			t.Errorf("leaked budget: handlers=%d rx=%d tx=%d", s.handlers, s.receivedBytes, s.sentBytes)
		}
		s.mu.Unlock()
		if n == 0 {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("operations did not finish")
		}
	}
}

func requireOpError(t *testing.T, err error, code Code, outcome Outcome) *OpError {
	t.Helper()
	var op *OpError
	if !errors.As(err, &op) || op.Code != code || op.Outcome != outcome {
		t.Fatalf("error=%#v; want %s outcome %d", err, code, outcome)
	}
	return op
}

func TestReverseRequestAndConcurrentDirections(t *testing.T) {
	clientMux, serverMux := NewMux(), NewMux()
	echo := func(ctx context.Context, r *Request) (Message, error) { return r.Message, nil }
	handle(t, clientMux, "echo", echo)
	handle(t, serverMux, "echo", echo)
	handle(t, serverMux, "reverse", func(ctx context.Context, r *Request) (Message, error) {
		return r.Session().Request(ctx, "echo", r.Message)
	})
	client, server := pair(t, localConfig("client", clientMux), localConfig("server", serverMux))
	if client.Info().ID != server.Info().ID || client.Info().Remote != server.Info().Local || client.Info().ID == (ID{}) {
		t.Fatal("identity negotiation mismatch")
	}
	if err := clientMux.HandleRequest("late", echo); err == nil {
		t.Fatal("registered after freeze")
	}
	payload := bytes.Repeat([]byte("hello"), 30000)
	response, err := client.Request(t.Context(), "reverse", Message{Metadata: map[string]string{"trace": "one"}, Body: payload})
	if err != nil || !bytes.Equal(response.Body, payload) || response.Metadata["trace"] != "one" {
		t.Fatalf("reverse request: %v", err)
	}
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := client
			if i%2 == 1 {
				s = server
			}
			msg := Message{Body: []byte(fmt.Sprint(i))}
			got, err := s.Request(t.Context(), "echo", msg)
			if err != nil || !bytes.Equal(got.Body, msg.Body) {
				t.Errorf("concurrent request: %v", err)
			}
		}()
	}
	wg.Wait()
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestRequestRejectionAndHandlerErrors(t *testing.T) {
	m := NewMux()
	handle(t, m, "business", func(context.Context, *Request) (Message, error) {
		return Message{}, &StatusError{Code: CodePermissionDenied, Message: "private"}
	})
	handle(t, m, "panic", func(context.Context, *Request) (Message, error) { panic("private panic detail") })
	handle(t, m, "unicode-error", func(context.Context, *Request) (Message, error) {
		return Message{}, errors.New(strings.Repeat("中", 350))
	})
	handle(t, m, "app-code", func(context.Context, *Request) (Message, error) {
		return Message{}, &StatusError{Code: "app.counter.negative", Message: "negative delta"}
	})
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	if err := m.HandleRequest("echo", nil); err == nil {
		t.Fatal("accepted invalid registration")
	}
	cfg := localConfig("server", m)
	cfg.Limits.MaxMessageSize = 1024
	client, server := pair(t, localConfig("client", nil), cfg)
	_, err := client.Request(t.Context(), "missing", Message{})
	requireOpError(t, err, CodeNotFound, Rejected)
	_, err = client.Request(t.Context(), "business", Message{})
	requireOpError(t, err, CodePermissionDenied, Responded)
	_, err = client.Request(t.Context(), "panic", Message{})
	op := requireOpError(t, err, CodeInternal, Responded)
	if op.Message != "request handler panicked" {
		t.Fatal("panic details exposed")
	}
	_, err = client.Request(t.Context(), "unicode-error", Message{})
	requireOpError(t, err, CodeApplication, Responded)
	_, err = client.Request(t.Context(), "app-code", Message{})
	requireOpError(t, err, Code("app.counter.negative"), Responded)
	_, err = client.Request(t.Context(), "echo", Message{Body: make([]byte, 1025)})
	requireOpError(t, err, CodeMessageTooLarge, NotSent)
	_, err = client.Request(t.Context(), "echo", Message{Metadata: map[string]string{"large": string(make([]byte, 9<<10))}})
	requireOpError(t, err, CodeMessageTooLarge, NotSent)
	_, err = client.Request(t.Context(), "echo", Message{})
	if err != nil {
		t.Fatal(err)
	}
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestRequestCancelAndHandlerQuota(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	m := NewMux()
	handle(t, m, "wait", func(ctx context.Context, _ *Request) (Message, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return Message{}, ctx.Err()
	})
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	cfg := localConfig("server", m)
	cfg.Limits.MaxHandlers = 1
	client, server := pair(t, localConfig("client", nil), cfg)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := client.Request(ctx, "wait", Message{}); result <- err }()
	waitSignal(t, started)
	_, err := client.Request(t.Context(), "echo", Message{})
	requireOpError(t, err, CodeResourceExhausted, Rejected)
	cancel()
	err = <-result
	requireOpError(t, err, CodeCanceled, Unknown)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("lost cancellation cause")
	}
	waitSignal(t, canceled)
	waitIdle(t, server)
	_, err = client.Request(t.Context(), "echo", Message{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRequestDeadlineAndUnknownOnDisconnect(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprint(disconnect), func(t *testing.T) {
			started, canceled := make(chan struct{}), make(chan struct{})
			m := NewMux()
			handle(t, m, "wait", func(ctx context.Context, _ *Request) (Message, error) {
				close(started)
				<-ctx.Done()
				close(canceled)
				return Message{}, ctx.Err()
			})
			client, server := pair(t, localConfig("client", nil), localConfig("server", m))
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := client.Request(ctx, "wait", Message{}); result <- err }()
			waitSignal(t, started)
			if disconnect {
				_ = server.Close()
			}
			err := <-result
			var op *OpError
			if !errors.As(err, &op) || op.Outcome != Unknown {
				t.Fatalf("lost unknown outcome: %v", err)
			}
			if !disconnect && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline: %v", err)
			}
			waitSignal(t, canceled)
		})
	}
}

func TestShutdownDrainsAndTimesOutStuckHandler(t *testing.T) {
	for _, stuck := range []bool{false, true} {
		t.Run(fmt.Sprint(stuck), func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			m := NewMux()
			handle(t, m, "wait", func(ctx context.Context, _ *Request) (Message, error) {
				close(started)
				if stuck {
					<-release
				} else {
					select {
					case <-release:
					case <-ctx.Done():
						return Message{}, ctx.Err()
					}
				}
				return Message{Body: []byte("done")}, nil
			})
			client, server := pair(t, localConfig("client", nil), localConfig("server", m))
			result := make(chan error, 1)
			go func() { _, err := client.Request(t.Context(), "wait", Message{}); result <- err }()
			waitSignal(t, started)
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			shutdown := make(chan error, 1)
			go func() { shutdown <- server.Shutdown(ctx) }()
			// Wait on the actual GOAWAY transition before attempting a new request.
			for {
				client.mu.Lock()
				draining := client.peerDraining
				changed := client.changedLocked()
				client.mu.Unlock()
				if draining {
					break
				}
				select {
				case <-changed:
				case <-ctx.Done():
					t.Fatal("missing GOAWAY")
				}
			}
			_, err := client.Request(t.Context(), "wait", Message{})
			requireOpError(t, err, CodeUnavailable, NotSent)
			if stuck {
				if err = <-shutdown; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("shutdown: %v", err)
				}
				if err = <-result; err == nil {
					t.Fatal("closed request succeeded")
				}
			} else {
				// A second channel avoids closing release twice in cleanup.
				release <- struct{}{}
				if err = <-result; err != nil {
					t.Fatal(err)
				}
				if err = <-shutdown; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestOperationAuthorization(t *testing.T) {
	m := NewMux()
	handle(t, m, "private", func(context.Context, *Request) (Message, error) {
		t.Error("dispatched rejected operation")
		return Message{}, nil
	})
	cfg := localConfig("server", m)
	cfg.AuthorizeOperation = func(ctx context.Context, peer Peer, op OperationInfo) error {
		if peer.ID != "client" || op.Mode != "request" || op.Route != "private" || op.ID == 0 {
			t.Error("bad authorization input")
		}
		return errors.New("denied")
	}
	client, _ := pair(t, localConfig("client", nil), cfg)
	_, err := client.Request(t.Context(), "private", Message{})
	requireOpError(t, err, CodePermissionDenied, Rejected)
}
