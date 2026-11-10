package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type admissionCodec struct {
	CBORCodec
	entered chan struct{}
	release chan struct{}
}

func (c *admissionCodec) MarshalAppend(dst []byte, value any) ([]byte, error) {
	if value == "held" {
		close(c.entered)
		<-c.release
	}
	return c.CBORCodec.MarshalAppend(dst, value)
}

func TestCallSendAdmissionWaitsAndCancellationRemainsNotSent(t *testing.T) {
	codec := &admissionCodec{entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(codec.release) })
	defer unblock()
	options := NodeOptions{
		Register: []MessageType{{"text", 1, ""}},
		Network:  NetworkOptions{AllowInsecureLoopback: true},
	}
	server, err := StartNode("server@127.0.0.1:0", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	options.Codec = codec
	// Exactly one maximum encoder reservation fits, even though each actual
	// value in this test is tiny. Pausing that encoder makes pressure exact.
	options.Network.SendBytes = 2 * ((128 << 10) + 4096)
	caller, err := StartNode("caller@127.0.0.1:0", options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Stop(context.Background()) })
	connectNodes(t, caller, server)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if value, ok := message.(string); ok {
			return a.Reply(value)
		}
		return nil
	}))
	type result struct {
		value string
		err   error
	}
	startCall := func(ctx context.Context, value string) <-chan result {
		done := make(chan result, 1)
		go func() {
			got, err := caller.Call[string](ctx, pid, value)
			done <- result{got, err}
		}()
		return done
	}
	waitAdmission := func() {
		eventually(t, func() bool {
			caller.network.mu.Lock()
			defer caller.network.mu.Unlock()
			return caller.network.sendChanged != nil && len(caller.network.outgoing) == 2
		})
	}
	first := startCall(t.Context(), "held")
	signal(t, codec.entered)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	canceled := startCall(ctx, "canceled")
	waitAdmission()
	cancel()
	failed := <-canceled
	var remote *RemoteError
	if !errors.Is(failed.err, context.Canceled) || !errors.As(failed.err, &remote) || remote.Outcome != NotSent {
		t.Fatalf("canceled admission: %v", failed.err)
	}
	if server.NetworkStats().IncomingCalls != 0 {
		t.Fatal("waiting request reached the server before admission")
	}
	last := startCall(t.Context(), "after")
	// Both the canceled waiter and its replacement may share the same budget
	// notification. Releasing capacity must still let the replacement proceed.
	waitAdmission()
	unblock()
	for expected, done := range map[string]<-chan result{"held": first, "after": last} {
		got := <-done
		if got.err != nil || got.value != expected {
			t.Fatalf("admitted %s: %+v", expected, got)
		}
	}
	eventually(t, func() bool {
		stats := caller.NetworkStats()
		return stats.QueuedBytes == 0 && stats.QueuedMessages == 0 && stats.OutgoingCalls == 0
	})
}
