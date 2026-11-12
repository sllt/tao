package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDefaultLocalCallBroadcastAndLateCompletionAfterReuse(t *testing.T) {
	n := testNode(t, NodeOptions{CallTimeout: time.Minute})
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "defer" {
			tickets <- a.Defer()
		}
		return nil
	}), WithName("worker"))
	call := func(to Addr) <-chan error {
		result := make(chan error, 1)
		go func() {
			value, err := n.Call[int](context.Background(), to, "defer")
			if err == nil && value != 42 {
				err = errors.New("incorrect result")
			}
			result <- err
		}()
		return result
	}
	for range 100 {
		result := call(pid)
		old := <-tickets
		scope, ok := old.request.ctx.(*requestScope)
		if !ok || scope.materialized.Load() != nil || old.request.callerDone == nil {
			t.Fatal("default local Call did not use an unmaterialized scope/private signal")
		}
		broadcast := old.request.waitDone()
		if broadcast == old.request.callerDone {
			t.Fatal("broadcast waiter can consume the caller's result")
		}
		var racers sync.WaitGroup
		racers.Go(func() { signal(t, broadcast) })
		racers.Go(func() { signal(t, old.request.waitDone()) })
		racers.Go(scope.timeout)
		racers.Go(func() {
			if err := old.ReplyChecked(42); err != nil && !errors.Is(err, ErrReplyExpired) {
				t.Error(err)
			}
		})
		racers.Wait()
		if err := <-result; err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		// The next call targets a node Ref, sharing the cache with direct PID
		// calls. The old request and its broadcast channel remain observable.
		nextResult := call(Ref{Node: n.Name(), Path: "worker"})
		next := <-tickets
		if old.request == next.request || old.request.callerDone != next.request.callerDone {
			t.Fatal("expected fresh request storage and a reused caller signal")
		}
		signal(t, old.request.waitDone())
		scope.timeout()
		if err := old.ReplyChecked(1); !errors.Is(err, ErrReplyExpired) && !errors.Is(err, ErrAlreadyReplied) {
			t.Fatalf("late reply: %v", err)
		}
		select {
		case err := <-nextResult:
			t.Fatalf("old completion woke a new call: %v", err)
		default:
		}
		if err := next.Reply(42); err != nil {
			t.Fatal(err)
		}
		if err := <-nextResult; err != nil {
			t.Fatal(err)
		}
	}
}

func TestDefaultLocalScopeContextAndDeadline(t *testing.T) {
	for _, materialize := range []bool{false, true} {
		t.Run(map[bool]string{false: "lazy", true: "public-context"}[materialize], func(t *testing.T) {
			n := testNode(t, NodeOptions{CallTimeout: 50 * time.Millisecond})
			type key struct{}
			parent := context.WithValue(context.Background(), key{}, "value")
			tickets := make(chan DeferredReply, 1)
			pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message == "defer" {
					if materialize {
						ctx := a.Context()
						if _, ok := ctx.Deadline(); !ok || ctx.Value(key{}) != "value" {
							t.Error("public Context lost deadline or parent value")
						}
					}
					tickets <- a.Defer()
				}
				return nil
			}))
			_, err := n.Call[int](parent, pid, "defer")
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("default deadline: %v", err)
			}
			reply := <-tickets
			scope := reply.request.ctx.(*requestScope)
			ctx := scope.context() // Also materialize after completion.
			signal(t, ctx.Done())
			if ctx.Value(key{}) != "value" || context.Cause(ctx) != context.DeadlineExceeded {
				t.Fatalf("completed context: value=%v cause=%v", ctx.Value(key{}), context.Cause(ctx))
			}
			if err := reply.ReplyChecked(1); !errors.Is(err, ErrReplyExpired) {
				t.Fatalf("late reply: %v", err)
			}
			if n.Stats().PendingCalls != 0 || n.network != nil {
				t.Fatal("local Call retained a pending request or required a network")
			}
		})
	}
}

func TestDefaultLocalCallStopCancelsPublicContext(t *testing.T) {
	for _, stopNode := range []bool{false, true} {
		t.Run(map[bool]string{false: "actor", true: "node"}[stopNode], func(t *testing.T) {
			n := testNode(t, NodeOptions{CallTimeout: time.Minute})
			entered, canceled := make(chan struct{}), make(chan struct{})
			pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message == "wait" {
					ctx := a.Context()
					close(entered)
					<-ctx.Done()
					if context.Cause(ctx) != context.Canceled {
						t.Error("owner stop changed the per-call cancellation cause")
					}
					close(canceled)
				}
				return nil
			}))
			result := make(chan error, 1)
			go func() { _, err := n.Call[any](context.Background(), pid, "wait"); result <- err }()
			signal(t, entered)
			var err error
			if stopNode {
				err = n.Stop(t.Context())
			} else {
				err = n.StopActor(t.Context(), pid)
			}
			if err != nil {
				t.Fatal(err)
			}
			signal(t, canceled)
			if err := <-result; !errors.Is(err, ErrStopped) {
				t.Fatalf("stopped call: %v", err)
			}
		})
	}
}

func TestDefaultLocalCallCapacityDoesNotConsumeAnotherResult(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 1, CallTimeout: time.Minute})
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "defer" {
			tickets <- a.Defer()
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() { _, err := n.Call[int](context.Background(), pid, "defer"); result <- err }()
	first := <-tickets
	if _, err := n.Call[int](context.Background(), pid, "defer"); !errors.Is(err, ErrCapacity) {
		t.Fatalf("pending limit: %v", err)
	}
	if n.Stats().PendingCalls != 1 {
		t.Fatal("rejected call changed pending ownership")
	}
	if err := first.Reply(42); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
