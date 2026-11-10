package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testBehavior struct {
	Actor
	receive func(*testBehavior, any) error
}

func (a *testBehavior) Receive(msg any) error { return a.receive(a, msg) }
func factory(handler func(*testBehavior, any) error) Factory {
	return func() Behavior { return &testBehavior{receive: handler} }
}
func testNode(t *testing.T, options NodeOptions) *Node {
	t.Helper()
	if options.Name == "" {
		options.Name = "test"
	}
	n, err := NewNode(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return n
}
func signal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("missing actor signal")
	}
}
func spawn(t *testing.T, n *Node, f Factory, options ...SpawnOption) PID {
	t.Helper()
	pid, err := n.Spawn(f, options...)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestSingleExecutorAndIdleWakeups(t *testing.T) {
	n := testNode(t, NodeOptions{})
	var active atomic.Int32
	count := 0
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if active.Add(1) != 1 {
			t.Error("concurrent Receive")
		}
		defer active.Add(-1)
		switch msg := msg.(type) {
		case int:
			count += msg
		case string:
			if msg == "get" {
				return a.Reply(count)
			}
		}
		return nil
	}), WithName("counter"))
	var senders sync.WaitGroup
	for range 32 {
		senders.Add(1)
		go func() {
			defer senders.Done()
			for range 16 {
				if err := n.Send(pid, 1); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	senders.Wait()
	for range 250 {
		got, err := n.Call[int](t.Context(), n.Ref("counter"), "get")
		if err != nil || got != 512 {
			t.Fatalf("counter=%d error=%v", got, err)
		}
	}
}

func TestDeferredReplySurvivesSubsequentReceiveContext(t *testing.T) {
	n := testNode(t, NodeOptions{})
	tickets := make(chan DeferredReply, 1)
	advanced := make(chan struct{})
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message {
		case "defer":
			tickets <- a.Defer()
		case "advance":
			if _, hasDeadline := a.Context().Deadline(); hasDeadline {
				return errors.New("previous Call context leaked into Send")
			}
			if a.Sender() != nil {
				return errors.New("unexpected sender on external Send")
			}
			close(advanced)
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() {
		value, err := n.Call[int](t.Context(), pid, "defer")
		if err == nil && value != 42 {
			err = fmt.Errorf("deferred value %d", value)
		}
		result <- err
	}()
	var ticket DeferredReply
	select {
	case ticket = <-tickets:
	case <-time.After(5 * time.Second):
		t.Fatal("missing reply ticket")
	}
	if err := n.Send(pid, "advance"); err != nil {
		t.Fatal(err)
	}
	signal(t, advanced)
	if err := ticket.Reply(42); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestMailboxIsBounded(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	processed := make(chan struct{}, 2)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case string:
			close(entered)
			<-release
		case int:
			processed <- struct{}{}
		}
		return nil
	}), WithMailboxSize(2))
	if err := n.Send(pid, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	for range 2 {
		if err := n.Send(pid, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.Send(pid, 1); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("mailbox overfilled: %v", err)
	}
	releaseOnce()
	for range 2 {
		signal(t, processed)
	}
}

func TestStartedCompletesBeforePublishingAndFailedStartCleansChildren(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release := make(chan struct{}), make(chan struct{})
	var startingPID PID
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	started := make(chan error, 1)
	go func() {
		_, err := n.Spawn(factory(func(a *testBehavior, msg any) error {
			if _, ok := msg.(Started); ok {
				startingPID = a.PID()
				close(entered)
				<-release
			}
			return nil
		}), WithName("starting"))
		started <- err
	}()
	signal(t, entered)
	if err := n.Send(n.Ref("starting"), 1); !errors.Is(err, ErrStarting) {
		t.Fatalf("published before Started: %v", err)
	}
	if err := n.Send(startingPID, 1); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("PID published before Started: %v", err)
	}
	if _, err := n.Spawn(factory(func(*testBehavior, any) error { return nil }), WithName("starting")); !errors.Is(err, ErrNameTaken) {
		t.Fatal(err)
	}
	releaseOnce()
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	if err := n.Send(startingPID, 1); err != nil {
		t.Fatalf("PID not published after Started: %v", err)
	}
	stopped := make(chan string, 2)
	_, err := n.Spawn(factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case Started:
			_, err := a.Spawn(factory(func(_ *testBehavior, msg any) error {
				if _, ok := msg.(Stopped); ok {
					stopped <- "child"
				}
				return nil
			}), WithName("child"))
			if err != nil {
				return err
			}
			return errors.New("initialization failed")
		case Stopped:
			stopped <- "parent"
		}
		return nil
	}), WithName("failing"))
	if err == nil {
		t.Fatal("failed startup succeeded")
	}
	if <-stopped != "child" || <-stopped != "parent" {
		t.Fatal("failed startup cleanup order")
	}
	if err = n.Send(n.Ref("failing/child"), 1); !errors.Is(err, ErrNoProcess) {
		t.Fatal(err)
	}
}

func TestChildrenStopInReverseOrder(t *testing.T) {
	n := testNode(t, NodeOptions{})
	order := make(chan string, 7)
	parent := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case Started:
			for _, name := range []string{"a", "b", "c"} {
				_, err := a.Spawn(factory(func(_ *testBehavior, msg any) error {
					switch msg.(type) {
					case Started:
						order <- "start-" + name
					case Stopped:
						order <- "stop-" + name
					}
					return nil
				}), WithName(name))
				if err != nil {
					return err
				}
			}
		case Stopped:
			order <- "stop-parent"
		}
		return nil
	}), WithName("parent"))
	if err := n.StopActor(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"start-a", "start-b", "start-c", "stop-c", "stop-b", "stop-a", "stop-parent"} {
		if got := <-order; got != want {
			t.Fatalf("got %s, want %s", got, want)
		}
	}
}

func TestCallsRepliesAndDeferredExpiration(t *testing.T) {
	n := testNode(t, NodeOptions{})
	tickets := make(chan DeferredReply, 1)
	business := errors.New("business error")
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg {
		case "reply":
			return a.Reply(42)
		case "business":
			_ = a.ReplyError(business)
		case "defer":
			tickets <- a.Defer()
		case "self":
			_, err := a.Call(a.Context(), a.PID(), "reply")
			_ = a.ReplyError(err)
		case "wait":
			<-a.Context().Done()
		}
		return nil
	}))
	if got, err := n.Call[int](t.Context(), pid, "reply"); got != 42 || err != nil {
		t.Fatalf("reply: %d %v", got, err)
	}
	if _, err := n.Call[int](t.Context(), pid, "business"); !errors.Is(err, business) {
		t.Fatal(err)
	}
	if _, err := n.Call[int](t.Context(), pid, "missing"); !errors.Is(err, ErrNoReply) {
		t.Fatal(err)
	}
	if _, err := n.Call[int](t.Context(), pid, "self"); !errors.Is(err, ErrSelfCall) {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		value, err := n.Call[int](t.Context(), pid, "defer")
		if err == nil && value != 99 {
			err = fmt.Errorf("wrong deferred response %d", value)
		}
		result <- err
	}()
	ticket := <-tickets
	if err := ticket.Reply(99); err != nil {
		t.Fatal(err)
	}
	if err := ticket.Reply(100); !errors.Is(err, ErrAlreadyReplied) {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	go func() { _, err := n.Call[int](t.Context(), pid, "defer"); result <- err }()
	ticket = <-tickets
	if err := n.StopActor(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if err := ticket.Reply(1); !errors.Is(err, ErrReplyExpired) {
		t.Fatal(err)
	}
}

func TestCancellationDoesNotBecomeNoReply(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered := make(chan struct{})
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if _, ok := msg.(string); ok {
			close(entered)
			<-a.Context().Done()
		}
		return nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := n.Call[any](ctx, pid, "wait"); result <- err }()
	signal(t, entered)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation changed to %v", err)
	}
}

func TestUnresponsiveStopRevokesRouteWithoutPretendingGoroutineExited(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	pid := spawn(t, n, factory(func(_ *testBehavior, msg any) error {
		switch msg.(type) {
		case string:
			close(entered)
			<-release
		case Stopped:
			close(stopped)
		}
		return nil
	}), WithName("blocked"))
	if err := n.Send(pid, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := n.StopActor(ctx, pid); !errors.Is(err, ErrUnresponsive) {
		t.Fatal(err)
	}
	if err := n.Send(n.Ref("blocked"), 1); !errors.Is(err, ErrUnresponsive) {
		t.Fatal(err)
	}
	if err := n.Send(pid, 1); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("unresponsive PID still published: %v", err)
	}
	if stats := n.Stats(); stats.Actors != 1 || stats.Unresponsive != 1 {
		t.Fatalf("unresponsive actor disappeared from accounting: %+v", stats)
	}
	select {
	case <-stopped:
		t.Fatal("claimed Stopped while Receive was blocked")
	default:
	}
	releaseOnce()
	signal(t, stopped)
}

func TestFactoryCannotReuseLiveBehavior(t *testing.T) {
	n := testNode(t, NodeOptions{})
	behavior := &testBehavior{receive: func(a *testBehavior, msg any) error {
		if msg == "get" {
			return a.Reply(1)
		}
		return nil
	}}
	first := spawn(t, n, func() Behavior { return behavior })
	if _, err := n.Spawn(func() Behavior { return behavior }); err == nil {
		t.Fatal("reused live actor")
	}
	if got, err := n.Call[int](t.Context(), first, "get"); got != 1 || err != nil {
		t.Fatalf("failed spawn touched original behavior: %d %v", got, err)
	}
}

func TestPendingCallLimitAndDefaultTimeout(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 1, CallTimeout: 100 * time.Millisecond})
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if _, ok := msg.(string); ok {
			tickets <- a.Defer()
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() { _, err := n.Call[any](context.Background(), pid, "defer"); result <- err }()
	ticket := <-tickets
	if _, err := n.Call[any](t.Context(), pid, "over-capacity"); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := ticket.Reply(nil); !errors.Is(err, ErrReplyExpired) {
		t.Fatal(err)
	}
	if stats := n.Stats(); stats.PendingCalls != 0 {
		t.Fatal(stats)
	}
}
