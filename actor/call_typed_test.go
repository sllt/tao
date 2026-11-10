package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

type callAsOutcome struct {
	value int
	err   error
}

func TestActorCallAsReplyErrorsAddressesAndLegacyCall(t *testing.T) {
	n := testNode(t, NodeOptions{})
	business := errors.New("business refusal")
	callee := spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message {
		case "good":
			return a.Reply(42)
		case "wrong":
			return a.Reply("42")
		case "nil":
			return a.Reply(nil)
		case "business":
			return a.ReplyError(business)
		}
		return nil
	}), WithName("callee"))
	caller := spawn(t, n, factory(func(a *testBehavior, message any) error {
		command, ok := message.(string)
		if !ok {
			return nil
		}
		if command == "legacy" {
			value, err := a.Call(a.Context(), callee, "good")
			number, matches := value.(int)
			if err == nil && !matches {
				err = errors.New("legacy Call changed its reply")
			}
			return a.Reply(callAsOutcome{number, err})
		}
		var to Addr = callee
		request := command
		switch command {
		case "qualified-ref":
			to, request = Ref{Node: n.Name(), Path: "callee"}, "good"
		case "self-pid":
			to, request = a.PID(), "good"
		case "self-ref":
			to, request = Ref{Path: "caller"}, "good"
		}
		value, err := a.CallAs[int](a.Context(), to, request)
		return a.Reply(callAsOutcome{value, err})
	}), WithName("caller"))
	for _, test := range []struct {
		command string
		value   int
		err     error
	}{
		{"good", 42, nil},
		{"qualified-ref", 42, nil},
		{"legacy", 42, nil},
		{"business", 0, business},
		{"wrong", 0, ErrReplyType},
		{"nil", 0, ErrReplyType},
		{"self-pid", 0, ErrSelfCall},
		{"self-ref", 0, ErrSelfCall},
	} {
		t.Run(test.command, func(t *testing.T) {
			got, err := n.Call[callAsOutcome](t.Context(), caller, test.command)
			if err != nil || got.value != test.value || !errors.Is(got.err, test.err) {
				t.Fatalf("Actor call: value=%d inner=%v outer=%v; want %d, %v", got.value, got.err, err, test.value, test.err)
			}
		})
	}
}

func TestActorCallAsAcceptsNilPointerReplies(t *testing.T) {
	n := testNode(t, NodeOptions{})
	callee := spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message {
		case "nil":
			return a.Reply(nil)
		case "typed-nil":
			return a.Reply((*int)(nil))
		}
		return nil
	}))
	type outcome struct {
		value *int
		err   error
	}
	caller := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if command, ok := message.(string); ok {
			value, err := a.CallAs[*int](a.Context(), callee, command)
			return a.Reply(outcome{value, err})
		}
		return nil
	}))
	for _, command := range []string{"nil", "typed-nil"} {
		got, err := n.Call[outcome](t.Context(), caller, command)
		if err != nil || got.err != nil || got.value != nil {
			t.Fatalf("nil reply %q: %#v, %v", command, got, err)
		}
	}
}

func TestActorCallAsPreservesCallerAndActorCancellation(t *testing.T) {
	for _, mode := range []string{"caller", "actor-stop"} {
		t.Run(mode, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			entered, cancelled := make(chan struct{}), make(chan struct{})
			callee := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "wait":
					close(entered)
					<-a.Context().Done()
					close(cancelled)
				case "alive":
					return a.Reply(42)
				}
				return nil
			}))
			outcomes := make(chan callAsOutcome, 1)
			caller := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message == "call" {
					// Completion belongs to the controlled cancellation/Stop,
					// even if the handler returns before Stop finishes revocation.
					a.Defer()
					ctx := a.Context()
					if mode == "actor-stop" {
						// Actor.Call must still bind this explicit context to the
						// Actor lifetime. Calling Node.Call here would omit it.
						ctx = context.Background()
					}
					value, err := a.CallAs[int](ctx, callee, "wait")
					outcomes <- callAsOutcome{value, err}
				}
				return nil // The cancelled/stopped outer request no longer needs a reply.
			}))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			outer := make(chan error, 1)
			go func() {
				_, err := n.Call[any](ctx, caller, "call")
				outer <- err
			}()
			signal(t, entered)
			wantOuter := context.Canceled
			if mode == "actor-stop" {
				wantOuter = ErrStopped
				if err := n.StopActor(t.Context(), caller); err != nil {
					t.Fatal(err)
				}
			} else {
				cancel()
			}
			select {
			case got := <-outcomes:
				if got.value != 0 || !errors.Is(got.err, context.Canceled) {
					t.Fatalf("typed cancellation: %d, %v", got.value, got.err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("typed Actor call did not observe cancellation")
			}
			select {
			case err := <-outer:
				if !errors.Is(err, wantOuter) {
					t.Fatalf("outer cancellation: %v, want %v", err, wantOuter)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("outer request did not complete")
			}
			signal(t, cancelled)
			if value, err := n.Call[int](t.Context(), callee, "alive"); err != nil || value != 42 {
				t.Fatalf("request cancellation stopped the callee: %d, %v", value, err)
			}
		})
	}
}
