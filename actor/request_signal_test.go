package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestOutboundReplyCancellationRacesSignalReuse(t *testing.T) {
	caller, server := networkNode(t, "caller"), networkNode(t, "server")
	connectNodes(t, caller, server)
	deferred := make(chan DeferredReply, 1)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if value, ok := message.(int64); ok {
			if value == 1 {
				deferred <- a.Defer()
				return nil
			}
			return a.Reply(value)
		}
		return nil
	}))
	for range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			value, err := caller.Call[int64](ctx, pid, int64(1))
			if err == nil && value != 1 {
				err = errors.New("incorrect raced reply")
			}
			result <- err
		}()
		var reply DeferredReply
		select {
		case reply = <-deferred:
		case <-t.Context().Done():
			cancel()
			t.Fatal(t.Context().Err())
		}
		start := make(chan struct{})
		var racers sync.WaitGroup
		racers.Go(func() { <-start; cancel() })
		racers.Go(func() {
			<-start
			if err := reply.ReplyChecked(int64(1)); err != nil && !errors.Is(err, ErrReplyExpired) {
				t.Error(err)
			}
		})
		close(start)
		racers.Wait()
		if err := <-result; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		value, err := caller.Call[int64](context.Background(), pid, int64(2))
		if err != nil || value != 2 {
			t.Fatalf("next Call consumed a stale publication: %d %v", value, err)
		}
	}
}

func TestOutboundSignalReuseKeepsLateCompletionOnOriginalRequest(t *testing.T) {
	for _, expire := range []bool{false, true} {
		name := "reply"
		if expire {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			n := networkNode(t, "caller")
			w := n.network
			owner := PID{Node: "remote", ID: 1}
			ctx, cancel := context.WithCancel(t.Context())
			old, err := w.ownedRequest(ctx, cancel, owner, true)
			if err != nil {
				t.Fatal(err)
			}
			cleanup, resume := make(chan struct{}), make(chan struct{})
			defer close(resume)
			old.cancel = func() { close(cleanup); <-resume; cancel() }
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				if expire {
					n.expireRequest(old, context.Canceled)
				} else if err := n.reply(old, owner, "old", nil, false); err != nil {
					t.Error(err)
				}
			}()
			signal(t, old.callerDone)
			signal(t, cleanup)
			n.recycleCallSignal(old.callerDone)
			nextCtx, nextCancel := context.WithCancel(t.Context())
			next, err := w.ownedRequest(nextCtx, nextCancel, owner, true)
			if err != nil {
				t.Fatal(err)
			}
			if old == next || old.callerDone != next.callerDone {
				t.Fatal("expected a fresh request record sharing the cached wait signal")
			}
			want := ErrAlreadyReplied
			if expire {
				want = ErrReplyExpired
			}
			if err := n.reply(old, owner, "late", nil, false); !errors.Is(err, want) {
				t.Fatalf("late reply: %v", err)
			}
			if n.expireRequest(old, context.DeadlineExceeded) {
				t.Fatal("old request completed twice")
			}
			select {
			case <-next.callerDone:
				t.Fatal("late completion signaled the new call")
			default:
			}
			if err := n.reply(next, owner, "new", nil, false); err != nil {
				t.Fatal(err)
			}
			signal(t, next.callerDone)
			if next.value != "new" || next.err != nil {
				t.Fatalf("new result: %v %v", next.value, next.err)
			}
			n.recycleCallSignal(next.callerDone)
			// Cleanup may finish after the signal has already served another
			// call; it retains the original request/context, not a pooled record.
			t.Cleanup(func() { signal(t, finished) })
		})
	}
}

func TestOutboundRegistrationCapacityKeepsIDsAndCompletionIsolated(t *testing.T) {
	n, err := StartNode("", NodeOptions{Name: "caller", MaxPendingCalls: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	w := n.network
	owner := PID{Node: "peer", ID: 1}
	ctx, cancel := context.WithCancel(t.Context())
	first, err := w.ownedRequest(ctx, cancel, owner, true)
	if err != nil {
		t.Fatal(err)
	}
	defer n.expireRequest(first, context.Canceled)
	rejectedCtx, rejectedCancel := context.WithCancel(t.Context())
	rejected, err := w.ownedRequest(rejectedCtx, rejectedCancel, owner, true)
	if !errors.Is(err, ErrCapacity) || rejected != nil || rejectedCtx.Err() != context.Canceled {
		t.Fatalf("full registry admission: request=%v err=%v context=%v", rejected, err, rejectedCtx.Err())
	}
	select {
	case <-first.callerDone:
		t.Fatal("rejected registration completed an existing request")
	default:
	}
	if err := n.reply(first, owner, "first", nil, false); err != nil {
		t.Fatal(err)
	}
	signal(t, first.callerDone)
	n.recycleCallSignal(first.callerDone)
	nextCtx, nextCancel := context.WithCancel(t.Context())
	next, err := w.ownedRequest(nextCtx, nextCancel, owner, true)
	if err != nil {
		t.Fatal(err)
	}
	defer n.expireRequest(next, context.Canceled)
	if next.id != first.id+1 {
		t.Fatal("rejected registration consumed a request ID")
	}
	select {
	case <-next.callerDone:
		t.Fatal("reused signal carried a previous completion")
	default:
	}
	n.expireRequest(next, context.Canceled)
	signal(t, next.callerDone)
	n.recycleCallSignal(next.callerDone)
}
