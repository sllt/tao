package taonet

import (
	"context"
	"errors"
	"testing"
	"time"
)

func nextEvent(t *testing.T, s *Session) Event {
	t.Helper()
	select {
	case event, ok := <-s.Events():
		if !ok {
			t.Fatal("events closed")
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("missing event")
		return Event{}
	}
}

func TestNotifyCopiesBeforeReturning(t *testing.T) {
	checking, allow := make(chan struct{}), make(chan struct{})
	defer close(allow)
	received := make(chan Message, 1)
	m := NewMux()
	if err := m.HandleNotify("message", func(_ context.Context, msg Message) error { received <- msg; return nil }); err != nil {
		t.Fatal(err)
	}
	cfg := localConfig("server", m)
	cfg.AuthorizeOperation = func(ctx context.Context, _ Peer, _ OperationInfo) error {
		close(checking)
		select {
		case <-allow:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	client, server := pair(t, localConfig("client", nil), cfg)
	ctx, cancel := context.WithCancel(t.Context())
	msg := Message{Body: []byte("original"), Metadata: map[string]string{"value": "original"}}
	if err := client.Notify(ctx, "message", msg); err != nil {
		t.Fatal(err)
	}
	cancel()
	waitSignal(t, checking)
	msg.Body[0] = 'X'
	msg.Metadata["value"] = "changed"
	allow <- struct{}{}
	select {
	case got := <-received:
		if string(got.Body) != "original" || got.Metadata["value"] != "original" {
			t.Fatalf("input alias: %#v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification lost")
	}
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestNotifyConfirmationPrecedesHandlerCompletion(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	m := NewMux()
	err := m.HandleNotify("message", func(ctx context.Context, _ Message) error {
		close(started)
		select {
		case <-release:
			return errors.New("business failed")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client, server := pair(t, localConfig("client", nil), localConfig("server", m))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err = client.NotifyConfirmed(ctx, "message", Message{}); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started)
	select {
	case event := <-server.Events():
		t.Fatalf("handler finished before release: %v", event)
	default:
	}
	release <- struct{}{}
	event := nextEvent(t, server)
	if event.Kind != HandlerFailed || event.Route != "message" || errorCode(event.Err) != CodeApplication {
		t.Fatalf("handler event: %#v", event)
	}
	waitIdle(t, server)
}

func TestNotifyAsyncFailureAndAdmissionQuota(t *testing.T) {
	client, _ := pair(t, localConfig("client", nil), localConfig("server", nil))
	if err := client.Notify(t.Context(), "missing", Message{}); err != nil {
		t.Fatal(err)
	}
	event := nextEvent(t, client)
	if event.Kind != NotifyFailed {
		t.Fatal(event)
	}
	requireOpError(t, event.Err, CodeNotFound, Rejected)
	if err := client.NotifyConfirmed(t.Context(), "missing", Message{}); err == nil {
		t.Fatal("unknown notify route accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	requireOpError(t, client.Notify(ctx, "missing", Message{}), CodeCanceled, NotSent)
}

func TestNotifyConfirmationTimeoutDoesNotWithdraw(t *testing.T) {
	checking, allow, received := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(allow)
	m := NewMux()
	if err := m.HandleNotify("message", func(context.Context, Message) error { close(received); return nil }); err != nil {
		t.Fatal(err)
	}
	cfg := localConfig("server", m)
	cfg.AuthorizeOperation = func(ctx context.Context, _ Peer, _ OperationInfo) error {
		close(checking)
		select {
		case <-allow:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	client, server := pair(t, localConfig("client", nil), cfg)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- client.NotifyConfirmed(ctx, "message", Message{}) }()
	waitSignal(t, checking)
	cancel()
	requireOpError(t, <-result, CodeCanceled, Unknown)
	allow <- struct{}{}
	waitSignal(t, received)
	waitIdle(t, client)
	waitIdle(t, server)
}
