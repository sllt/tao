package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func scopeRequest(t *testing.T, n *Node, parent context.Context, duration time.Duration) (*request, *requestScope) {
	t.Helper()
	scope := newRequestScope(parent, time.Now().Add(duration))
	r, err := n.network.registerRequest(parent, scope.finish, PID{Node: n.Name(), ID: 99}, false, scope)
	if err != nil {
		t.Fatal(err)
	}
	scope.start()
	return r, scope
}

func TestIncomingScopeContextLifecycle(t *testing.T) {
	n := networkNode(t, "scopes")
	type valueKey struct{}
	for _, earlyContext := range []bool{false, true} {
		for _, mode := range []string{"reply", "cancel", "deadline"} {
			name := mode
			if earlyContext {
				name += "/context-first"
			}
			t.Run(name, func(t *testing.T) {
				parent, parentCancel := context.WithCancelCause(context.WithValue(t.Context(), valueKey{}, "retained"))
				defer parentCancel(nil)
				duration := time.Second
				if mode == "deadline" {
					duration = 10 * time.Millisecond
				}
				r, scope := scopeRequest(t, n, parent, duration)
				var ctx, child context.Context
				var notified chan struct{}
				if earlyContext {
					ctx = r.context()
					var cancel context.CancelFunc
					child, cancel = context.WithCancel(ctx)
					defer cancel()
					notified = make(chan struct{})
					defer context.AfterFunc(ctx, func() { close(notified) })()
				}
				switch mode {
				case "reply":
					if err := n.reply(r, r.owner, 42, nil, false); err != nil {
						t.Fatal(err)
					}
				case "cancel":
					scope.cancel()
				}
				signal(t, r.waitDone())
				if !earlyContext && scope.materialized.Load() != nil {
					t.Fatal("unused public context was materialized")
				}
				// A later parent cancellation cannot replace a settled reason.
				parentCancel(errors.New("late parent cause"))
				ctx = r.context()
				signal(t, ctx.Done())
				want := error(context.Canceled)
				if mode == "deadline" {
					want = context.DeadlineExceeded
				}
				if ctx.Err() != want || context.Cause(ctx) != want || ctx.Value(valueKey{}) != "retained" {
					t.Fatalf("context state: err=%v cause=%v value=%v", ctx.Err(), context.Cause(ctx), ctx.Value(valueKey{}))
				}
				deadline, ok := ctx.Deadline()
				if !ok || !deadline.Equal(scope.deadline) {
					t.Fatal("context lost the original deadline")
				}
				if earlyContext {
					signal(t, child.Done())
					signal(t, notified)
					if child.Err() != want || context.Cause(child) != want {
						t.Fatalf("child context: %v %v", child.Err(), context.Cause(child))
					}
				}
			})
		}
	}
}

func TestIncomingScopeMaterializationRacesCancellation(t *testing.T) {
	n := networkNode(t, "scope-race")
	for range 100 {
		r, scope := scopeRequest(t, n, t.Context(), time.Millisecond)
		var ctx context.Context
		var group sync.WaitGroup
		group.Go(func() { ctx = scope.context() })
		group.Go(scope.cancel)
		group.Wait()
		signal(t, r.waitDone())
		signal(t, ctx.Done())
		if ctx.Err() != r.err || context.Cause(ctx) != r.err {
			t.Fatalf("completion disagrees with materialized context: request=%v ctx=%v cause=%v", r.err, ctx.Err(), context.Cause(ctx))
		}
	}
	if n.Stats().PendingCalls != 0 {
		t.Fatal("canceled scopes retained requests")
	}
}
