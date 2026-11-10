package taonet

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	native "github.com/sllt/tao/taonet/internal/mux"
)

func TestWireAdmissionAndNormalFIN(t *testing.T) {
	m := NewMux()
	handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
	cfg := localConfig("server", m)
	cfg.Limits.MaxMessageSize = 1024
	client, server := pair(t, localConfig("client", nil), cfg)
	for _, invalid := range []bool{true, false} {
		stream, err := client.raw.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		size := 0
		if invalid {
			size = 1025
		}
		if err = writeJSON(stream, recordOpen, wireOpen{ID: stream.ID(), Mode: "request", Route: "echo", TimeoutMS: 5000, Size: size}); err != nil {
			t.Fatal(err)
		}
		if invalid {
			var status StatusError
			if err = readJSON(stream, recordOpenError, &status); err != nil {
				t.Fatal(err)
			}
			if status.Code != CodeMessageTooLarge {
				t.Fatal(status)
			}
		} else {
			h, err := readRecordHeader(stream)
			if err != nil || h.kind != recordOpenOK {
				t.Fatalf("OPEN_OK: %v %v", h, err)
			}
			if err = writeBody(stream, nil); err != nil {
				t.Fatal(err)
			}
			var result wireResult
			if err = readJSON(stream, recordResult, &result); err != nil {
				t.Fatal(err)
			}
			if _, err = readBody(stream, 0); err != nil {
				t.Fatal(err)
			}
		}
		if err = stream.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		var extra [1]byte
		if _, err = stream.Read(extra[:]); err != io.EOF {
			t.Fatalf("normal completion used reset: %v", err)
		}
		waitSignal(t, stream.Done())
		if err = stream.Err(); err != nil {
			t.Fatal(err)
		}
		_ = stream.Close()
	}
	waitIdle(t, server)
}

func TestReceiveBudgetBeforeHandlerDispatch(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	m := NewMux()
	handle(t, m, "wait", func(ctx context.Context, _ *Request) (Message, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return Message{}, nil
	})
	cfg := localConfig("server", m)
	cfg.Limits.MaxMessageSize = 1024
	cfg.Limits.ReceiveBytes = 1024
	client, server := pair(t, localConfig("client", nil), cfg)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Request(ctx, "wait", Message{Body: make([]byte, 1024)}); result <- err }()
	waitSignal(t, started)
	_, err := client.Request(t.Context(), "wait", Message{Body: []byte{1}})
	requireOpError(t, err, CodeResourceExhausted, Rejected)
	cancel()
	<-result
	waitIdle(t, server)
}

func TestOperationLimitRejectsBeforeBody(t *testing.T) {
	started := make(chan struct{})
	m := NewMux()
	handle(t, m, "wait", func(ctx context.Context, _ *Request) (Message, error) {
		close(started)
		<-ctx.Done()
		return Message{}, ctx.Err()
	})
	cfg := localConfig("server", m)
	cfg.Limits.MaxOperations = 1
	client, server := pair(t, localConfig("client", nil), cfg)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Request(ctx, "wait", Message{}); result <- err }()
	waitSignal(t, started)
	_, err := client.Request(t.Context(), "wait", Message{})
	// Native refusal can race with writing OPEN, but no request body was sent.
	var op *OpError
	var reset *native.StreamError
	if !errors.As(err, &op) || !errors.As(err, &reset) || reset.Code != native.ResourceExhausted {
		t.Fatalf("quota error: %v", err)
	}
	if op.Outcome != Rejected && !(op.Outcome == NotSent && op.Operation == 0) {
		t.Fatalf("quota must be rejected or precede HN OPEN: %#v", op)
	}
	cancel()
	<-result
	waitIdle(t, server)
}
