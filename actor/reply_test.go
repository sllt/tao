package actor

import (
	"context"
	"errors"
	"testing"
)

func TestExpiredReplyDoesNotRestartActor(t *testing.T) {
	for _, businessError := range []bool{false, true} {
		t.Run(map[bool]string{false: "value", true: "business-error"}[businessError], func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			entered := make(chan struct{})
			replied := make(chan error, 1)
			pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
				if msg == "slow" {
					close(entered)
					<-a.Context().Done()
					var err error
					if businessError {
						err = a.ReplyError(errors.New("declined"))
					} else {
						err = a.Reply(1)
					}
					replied <- err
					return err
				}
				if msg == "ping" {
					return a.Reply(a.PID())
				}
				return nil
			}))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := n.Call[int](ctx, pid, "slow"); result <- err }()
			signal(t, entered)
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := <-replied; err != nil {
				t.Fatalf("ordinary late reply: %v", err)
			}
			if got, err := n.Call[PID](t.Context(), pid, "ping"); err != nil || got != pid {
				t.Fatalf("actor restarted: %v %v", got, err)
			}
		})
	}
}

func TestReplyKeepsBusinessAndProgrammingErrorsVisible(t *testing.T) {
	n := testNode(t, NodeOptions{})
	business := errors.Join(ErrReplyExpired, errors.New("business refusal"))
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg {
		case "business":
			return a.ReplyError(business)
		case "duplicate":
			if err := a.Reply(1); err != nil {
				return err
			}
			return a.Reply(2)
		case "fault":
			return business
		}
		return nil
	}), WithRestart(Temporary))
	if _, err := n.Call[int](t.Context(), pid, "business"); err != business {
		t.Fatalf("business error swallowed: %v", err)
	}
	p, _, _ := n.resolve(pid)
	if _, err := n.Call[int](t.Context(), pid, "duplicate"); err != nil {
		t.Fatal(err)
	}
	signal(t, p.done)
	if !errors.Is(p.stopReason, ErrAlreadyReplied) {
		t.Fatalf("duplicate reply: %v", p.stopReason)
	}
	pid = spawn(t, n, factory(func(_ *testBehavior, msg any) error {
		if msg == "fault" {
			return business
		}
		return nil
	}), WithRestart(Temporary))
	p, _, _ = n.resolve(pid)
	if err := n.Send(pid, "fault"); err != nil {
		t.Fatal(err)
	}
	signal(t, p.done)
	if !errors.Is(p.stopReason, ErrReplyExpired) {
		t.Fatal("Receive error was suppressed")
	}
	var zero DeferredReply
	if err := zero.Reply(1); err != ErrNotCall {
		t.Fatalf("invalid ticket: %v", err)
	}
}

func TestDeferredReplyExpiryIsOptionalToInspect(t *testing.T) {
	n := testNode(t, NodeOptions{})
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if msg == "defer" {
			tickets <- a.Defer()
		}
		return nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := n.Call[int](ctx, pid, "defer"); result <- err }()
	ticket := <-tickets
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := ticket.ReplyChecked(1); err != ErrReplyExpired {
		t.Fatal(err)
	}
	if err := ticket.Reply(1); err != nil {
		t.Fatal(err)
	}
	if err := ticket.ReplyErrorChecked(errors.New("declined")); err != ErrReplyExpired {
		t.Fatal(err)
	}
	if err := ticket.ReplyError(errors.New("declined")); err != nil {
		t.Fatal(err)
	}
}
