package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func defaultDeadlineNode(t *testing.T, name string, timeout time.Duration) *Node {
	t.Helper()
	n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{
		CallTimeout: timeout,
		Network:     NetworkOptions{AllowInsecureLoopback: true},
		Register:    []MessageType{{"count", 1, int64(0)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	return n
}

func TestDefaultRemoteDeadlineExpiresAdmittedAndWaitingCalls(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		server := networkNode(t, "server")
		caller := defaultDeadlineNode(t, "caller", 100*time.Millisecond)
		connectNodes(t, caller, server)
		accepted := make(chan context.Context, 1)
		pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
			if value, ok := message.(int64); ok {
				if value == 0 {
					return a.Reply(int64(0))
				}
				a.Defer()
				accepted <- a.Context()
			}
			return nil
		}))
		warm, cancelWarm := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := caller.Call[int64](warm, pid, int64(0))
		cancelWarm()
		if err != nil {
			t.Fatal(err)
		}
		if blocked {
			charge := caller.network.opts.SendBytes
			if err := caller.network.reserve(charge); err != nil {
				t.Fatal(err)
			}
			defer caller.network.release(charge)
		}
		result := make(chan error, 1)
		go func() {
			_, err := caller.Call[int64](context.Background(), pid, int64(1))
			result <- err
		}()
		select {
		case err := <-result:
			var remote *RemoteError
			if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &remote) {
				t.Fatalf("default deadline: %v", err)
			}
			if blocked && remote.Outcome != NotSent {
				t.Fatalf("waiting Call was submitted: %v", remote)
			}
			if !blocked && remote.Outcome != Unknown && remote.Outcome != Responded {
				t.Fatalf("admitted Call lost execution outcome: %v", remote)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("default timeout did not complete the Call")
		}
		if blocked {
			select {
			case <-accepted:
				t.Fatal("waiting Call reached the server")
			default:
			}
		} else {
			select {
			case ctx := <-accepted:
				signal(t, ctx.Done())
			case <-time.After(time.Second):
				t.Fatal("test did not exercise an admitted Call")
			}
		}
		eventually(t, func() bool { return caller.Stats().PendingCalls == 0 && caller.NetworkStats().OutgoingCalls == 0 })
	}
}

func TestDefaultRemoteScopeMaterializationPreservesContextAndCompletion(t *testing.T) {
	server := networkNode(t, "server")
	caller := defaultDeadlineNode(t, "caller", 5*time.Second)
	connectNodes(t, caller, server)
	entered, proceed := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(proceed) })
	defer unblock()
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			close(entered)
			<-proceed
			return a.Reply(int64(42))
		}
		return nil
	}))
	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "preserved")
	result := make(chan error, 1)
	go func() {
		value, err := caller.Call[int64](parent, pid, int64(1))
		if err == nil && value != 42 {
			err = errors.New("incorrect reply")
		}
		result <- err
	}()
	signal(t, entered)
	caller.network.mu.Lock()
	var scope *requestScope
	for _, out := range caller.network.outgoing {
		scope, _ = out.request.ctx.(*requestScope)
	}
	caller.network.mu.Unlock()
	if scope == nil {
		t.Fatal("default remote Call did not use its managed scope")
	}
	canceled := scope.waitCanceled()
	ctx := scope.context()
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.Equal(scope.deadline) || ctx.Value(key{}) != "preserved" {
		t.Fatal("materialized context lost deadline or parent value")
	}
	unblock()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reply did not complete the default Call")
	}
	signal(t, canceled)
	signal(t, ctx.Done())
	if context.Cause(ctx) != context.Canceled {
		t.Fatalf("completion context cause: %v", context.Cause(ctx))
	}
}
