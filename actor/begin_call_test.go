package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBeginCallWireOrderAndOrigins(t *testing.T) {
	n := testNode(t, NodeOptions{CallTimeout: 20 * time.Millisecond})
	var value int
	var deferred DeferredReply
	deferReady := make(chan struct{})
	blockCh := make(chan struct{})

	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch m := msg.(type) {
		case int:
			value += m
			return nil
		case string:
			switch m {
			case "get":
				return a.Reply(value)
			case "reply_err_stopped":
				// Business ReplyError(ErrStopped) must still report Origin == Reply.
				return a.ReplyError(ErrStopped)
			case "noreply":
				return nil
			case "defer_wait":
				deferred = a.Defer()
				close(deferReady)
				return nil
			case "block":
				<-blockCh
				return a.Reply("unblocked")
			case "crash":
				return errors.New("boom")
			}
		}
		return nil
	}), WithRestart(Temporary))

	// 1. Ordered admission: Send(add 10) -> BeginCall(get) -> Send(add 5) -> BeginCall(get)
	if err := n.Send(pid, 10); err != nil {
		t.Fatal(err)
	}
	got1 := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "get", func(c CallCompletion) { got1 <- c }); err != nil {
		t.Fatal(err)
	}
	if err := n.Send(pid, 5); err != nil {
		t.Fatal(err)
	}
	got2 := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "get", func(c CallCompletion) { got2 <- c }); err != nil {
		t.Fatal(err)
	}
	c1 := <-got1
	c2 := <-got2
	if c1.Origin != Reply || c1.Err != nil || c1.Value != 10 {
		t.Fatalf("first call got %+v, want Reply 10", c1)
	}
	if c2.Origin != Reply || c2.Err != nil || c2.Value != 15 {
		t.Fatalf("second call got %+v, want Reply 15", c2)
	}

	// 2. Business ReplyError(ErrStopped) vs real process exit
	gotErrStopped := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "reply_err_stopped", func(c CallCompletion) { gotErrStopped <- c }); err != nil {
		t.Fatal(err)
	}
	ce := <-gotErrStopped
	if ce.Origin != Reply || !errors.Is(ce.Err, ErrStopped) {
		t.Fatalf("ReplyError(ErrStopped) got %+v, want Origin=Reply Err=ErrStopped", ce)
	}

	// 3. NoReply origin
	gotNoReply := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "noreply", func(c CallCompletion) { gotNoReply <- c }); err != nil {
		t.Fatal(err)
	}
	cn := <-gotNoReply
	if cn.Origin != NoReply || !errors.Is(cn.Err, ErrNoReply) {
		t.Fatalf("noreply got %+v, want Origin=NoReply Err=ErrNoReply", cn)
	}

	// 4. No implicit CallTimeout: context.Background() stays alive past Node CallTimeout (20ms).
	gotDefer := make(chan CallCompletion, 1)
	hDefer, err := n.BeginCall(context.Background(), pid, "defer_wait", func(c CallCompletion) { gotDefer <- c })
	if err != nil {
		t.Fatal(err)
	}
	<-deferReady
	time.Sleep(50 * time.Millisecond)
	select {
	case unexpected := <-gotDefer:
		t.Fatalf("BeginCall without deadline expired early: %+v", unexpected)
	default:
	}

	// Now crash the actor while deferred request is pending -> TargetExit!
	gotCrash := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "crash", func(c CallCompletion) { gotCrash <- c }); err != nil {
		t.Fatal(err)
	}
	cd := <-gotDefer
	cc := <-gotCrash
	if cd.Origin != TargetExit || !errors.Is(cd.Err, ErrProcessCrashed) {
		t.Fatalf("deferred on crash got %+v, want TargetExit/ErrProcessCrashed", cd)
	}
	if cc.Origin != TargetExit || !errors.Is(cc.Err, ErrProcessCrashed) {
		t.Fatalf("crashing call got %+v, want TargetExit/ErrProcessCrashed", cc)
	}
	if hDefer.Cancel() {
		t.Fatal("Cancel succeeded after TargetExit")
	}
	if err := deferred.ReplyChecked("too late"); !errors.Is(err, ErrReplyExpired) {
		t.Fatalf("late DeferredReply after TargetExit got %v, want ErrReplyExpired", err)
	}
	close(blockCh)
}

func TestBeginCallContextDeadlinesAndParentCancellation(t *testing.T) {
	n := testNode(t, NodeOptions{CallTimeout: 10 * time.Millisecond})

	type ctxKey struct{}
	customCause := errors.New("custom parent cancel cause")

	inspectCh := make(chan context.Context, 4)
	releaseHandler := make(chan struct{})
	enteredBlock := make(chan struct{})
	deferTicketCh := make(chan DeferredReply, 1)

	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg {
		case "hold":
			close(enteredBlock)
			<-releaseHandler
			return a.Reply("held")
		case "inspect_unexpired":
			ctx := a.Context()
			inspectCh <- ctx
			return a.Reply("ok")
		case "defer_no_ctx":
			deferTicketCh <- a.Defer()
			return nil
		case "wait_ctx_cancel":
			ctx := a.Context()
			<-ctx.Done()
			inspectCh <- ctx
			return nil
		}
		return nil
	}))

	// A. Materialized no-deadline context reports Deadline() == (time.Time{}, false) and preserves Value.
	parentVal := context.WithValue(context.Background(), ctxKey{}, "tao-val")
	resA := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(parentVal, pid, "inspect_unexpired", func(c CallCompletion) { resA <- c }); err != nil {
		t.Fatal(err)
	}
	if c := <-resA; c.Origin != Reply || c.Value != "ok" {
		t.Fatalf("inspect_unexpired got %+v", c)
	}
	matCtx := <-inspectCh
	if dl, ok := matCtx.Deadline(); ok || !dl.IsZero() {
		t.Fatalf("no-deadline materialized context reported Deadline=(%v, %v), want (zero, false)", dl, ok)
	}
	if got := matCtx.Value(ctxKey{}); got != "tao-val" {
		t.Fatalf("Value lost: %v", got)
	}

	// B. Explicit zero time.Time{} deadline is already expired (must NOT be treated as infinite wait).
	zeroCtx, zeroCancel := context.WithDeadline(context.Background(), time.Time{})
	defer zeroCancel()
	calledZero := false
	if h, err := n.BeginCall(zeroCtx, pid, "inspect_unexpired", func(CallCompletion) { calledZero = true }); !errors.Is(err, context.DeadlineExceeded) || h != nil || calledZero {
		t.Fatalf("zero deadline BeginCall got h=%v err=%v called=%v", h, err, calledZero)
	}

	// C. Queued (unpopped, never read Actor.Context) parent cancellation completes immediately.
	resHold := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "hold", func(c CallCompletion) { resHold <- c }); err != nil {
		t.Fatal(err)
	}
	<-enteredBlock

	queuedParent, cancelQueued := context.WithCancelCause(context.Background())
	resQueued := make(chan CallCompletion, 1)
	hQueued, err := n.BeginCall(queuedParent, pid, "inspect_unexpired", func(c CallCompletion) { resQueued <- c })
	if err != nil {
		t.Fatal(err)
	}
	cancelQueued(customCause)
	cq := <-resQueued
	if cq.Origin != Canceled || !errors.Is(cq.Err, context.Canceled) {
		t.Fatalf("queued parent cancel got %+v, want Canceled", cq)
	}
	if hQueued.Cancel() {
		t.Fatal("Cancel succeeded after parent cancellation")
	}
	// Release the blocker and verify the canceled queued message does not invoke Receive.
	close(releaseHandler)
	<-resHold
	select {
	case unexpected := <-inspectCh:
		t.Fatalf("canceled queued request still executed Receive: %v", unexpected)
	default:
	}

	// D. Deferred request (never called Actor.Context) completes when parent context is canceled.
	deferParent, cancelDeferParent := context.WithCancelCause(context.Background())
	resDefer := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(deferParent, pid, "defer_no_ctx", func(c CallCompletion) { resDefer <- c }); err != nil {
		t.Fatal(err)
	}
	ticket := <-deferTicketCh
	cancelDeferParent(customCause)
	cd := <-resDefer
	if cd.Origin != Canceled || !errors.Is(cd.Err, context.Canceled) {
		t.Fatalf("deferred parent cancel got %+v, want Canceled", cd)
	}
	if err := ticket.ReplyChecked("late"); !errors.Is(err, ErrReplyExpired) {
		t.Fatalf("ticket.Reply after parent cancel got %v, want ErrReplyExpired", err)
	}

	// E. Materialized context preserves custom Cause, and post-completion parent cause cannot overwrite committed cause.
	matParent, cancelMatParent := context.WithCancelCause(context.WithValue(context.Background(), ctxKey{}, "v2"))
	resMat := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(matParent, pid, "wait_ctx_cancel", func(c CallCompletion) { resMat <- c }); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	cancelMatParent(customCause)
	cm := <-resMat
	if cm.Origin != Canceled || !errors.Is(cm.Err, context.Canceled) {
		t.Fatalf("materialized cancel got %+v", cm)
	}
	observedCtx := <-inspectCh
	if gotCause := context.Cause(observedCtx); gotCause != customCause {
		t.Fatalf("materialized Cause=%v, want %v", gotCause, customCause)
	}

	// Also verify post-completion materialization detaches from a LATER parent cancellation cause!
	laterParent, cancelLater := context.WithCancelCause(context.WithValue(context.Background(), ctxKey{}, "keep"))
	scope := newContextRequestScope(laterParent)
	r := &request{node: n, ctx: &scope}
	scope.request = r
	manualCause := errors.New("committed first")
	scope.complete(manualCause, false)
	cancelLater(errors.New("later parent cause"))
	afterCtx := scope.context()
	if dl, ok := afterCtx.Deadline(); ok || !dl.IsZero() {
		t.Fatalf("post-completion no-deadline Deadline=(%v, %v)", dl, ok)
	}
	if gotCause := context.Cause(afterCtx); gotCause != manualCause {
		t.Fatalf("post-completion Cause=%v, want %v", gotCause, manualCause)
	}
	if afterCtx.Value(ctxKey{}) != "keep" {
		t.Fatal("post-completion Value lost")
	}
}

func TestBeginCallAdmissionFailuresNeverInvokeCallback(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 16})
	hold := make(chan struct{})
	entered := make(chan struct{})
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case Started, Stopped:
			return nil
		}
		if msg == "hold" {
			close(entered)
			<-hold
		}
		return a.Reply("ok")
	}), WithMailboxSize(1), WithRestart(Temporary))

	var callbacks atomic.Int32
	failHook := func(CallCompletion) { callbacks.Add(1) }

	// Nil ctx or nil callback
	if _, err := n.BeginCall(nil, pid, "x", failHook); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("nil ctx err=%v", err)
	}
	if _, err := n.BeginCall(context.Background(), pid, "x", nil); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("nil callback err=%v", err)
	}

	// Remote / nonexistent / stale PID
	if _, err := n.BeginCall(context.Background(), PID{Node: "other", ID: 1}, "x", failHook); !errors.Is(err, ErrRemoteUnavailable) {
		t.Fatalf("remote PID err=%v", err)
	}
	if _, err := n.BeginCall(context.Background(), PID{Node: n.Name(), BootID: n.BootID(), ID: 999999}, "x", failHook); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("missing PID err=%v", err)
	}
	if _, err := n.BeginCall(context.Background(), PID{Node: n.Name(), ID: pid.ID}, "x", failHook); !errors.Is(err, ErrStalePID) {
		t.Fatalf("stale PID err=%v", err)
	}

	// Occupy handler and fill the 1-slot mailbox so the next BeginCall hits ErrMailboxFull.
	holdDone := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "hold", func(c CallCompletion) { holdDone <- c }); err != nil {
		t.Fatal(err)
	}
	<-entered
	queuedDone := make(chan CallCompletion, 1)
	if _, err := n.BeginCall(context.Background(), pid, "queued", func(c CallCompletion) { queuedDone <- c }); err != nil {
		t.Fatal(err)
	}

	// Mailbox full, including while racing parent cancellation!
	for range 50 {
		parent, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		wg.Go(func() { cancel() })
		h, err := n.BeginCall(parent, pid, "overflow", failHook)
		wg.Wait()
		if err == nil || h != nil {
			t.Fatalf("expected admission failure on full mailbox, got h=%v err=%v", h, err)
		}
	}

	close(hold)
	<-holdDone
	<-queuedDone

	if callbacks.Load() != 0 {
		t.Fatalf("failed admissions invoked callback %d times", callbacks.Load())
	}
	if pending := n.Stats().PendingCalls; pending != 0 {
		t.Fatalf("leaked PendingCalls=%d", pending)
	}
}

func TestBeginCallConcurrentRacesExactlyOnce(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 512})
	for range 100 {
		tickets := make(chan DeferredReply, 8)
		pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
			switch msg {
			case "immediate":
				return a.Reply(1)
			case "defer":
				_ = a.Context()
				tickets <- a.Defer()
				return nil
			}
			return nil
		}), WithRestart(Temporary))

		parent, cancelParent := context.WithTimeout(context.Background(), 5*time.Millisecond)
		var count atomic.Int32
		done := make(chan CallCompletion, 2)
		var wg sync.WaitGroup
		wg.Go(func() {
			time.Sleep(100 * time.Microsecond)
			cancelParent()
		})
		h, err := n.BeginCall(parent, pid, "defer", func(c CallCompletion) {
			count.Add(1)
			done <- c
		})
		if err != nil {
			cancelParent()
			wg.Wait()
			_ = n.StopActor(context.Background(), pid)
			continue
		}
		wg.Go(func() { _ = h.Cancel() })
		wg.Go(func() {
			select {
			case d := <-tickets:
				_ = d.Reply(99)
			case <-time.After(10 * time.Millisecond):
			}
		})
		wg.Go(func() { _ = n.StopActor(context.Background(), pid) })

		c := <-done
		wg.Wait()
		cancelParent()
		if got := count.Load(); got != 1 {
			t.Fatalf("completion fired %d times (last=%+v)", got, c)
		}
		if c.Origin != Reply && c.Origin != Canceled && c.Origin != TargetExit {
			t.Fatalf("unexpected origin: %+v", c)
		}
	}
	if pending := n.Stats().PendingCalls; pending != 0 {
		t.Fatalf("leaked PendingCalls=%d", pending)
	}
}

func TestBeginCallCallbackBeforeReturnAndAfterFuncStopFalse(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 256})
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if msg == "fast" {
			return a.Reply("fast-ok")
		}
		return nil
	}))

	for range 200 {
		parent, cancel := context.WithCancel(context.Background())
		var count atomic.Int32
		done := make(chan CallCompletion, 2)
		var wg sync.WaitGroup
		wg.Go(func() { cancel() })
		h, err := n.BeginCall(parent, pid, "fast", func(c CallCompletion) {
			count.Add(1)
			done <- c
		})
		wg.Wait()
		if err != nil {
			if count.Load() != 0 {
				t.Fatalf("callback ran despite BeginCall error %v", err)
			}
			continue
		}
		if h == nil {
			t.Fatal("nil handle on nil error")
		}
		c := <-done
		if got := count.Load(); got != 1 {
			t.Fatalf("callback count=%d", got)
		}
		if c.Origin != Reply && c.Origin != Canceled {
			t.Fatalf("unexpected origin %+v", c)
		}
	}
	if pending := n.Stats().PendingCalls; pending != 0 {
		t.Fatalf("leaked PendingCalls=%d", pending)
	}
}
