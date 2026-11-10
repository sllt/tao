package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type asyncAdmission struct {
	ID  RequestID
	Err error
}

type asyncResultEvent struct {
	Caller PID
	Result CallResult
}

type asyncHeldCall struct {
	Name    string
	Reply   DeferredReply
	Context context.Context
}

func nextAsyncAdmission(t *testing.T, values <-chan asyncAdmission) asyncAdmission {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("missing asynchronous admission")
		return asyncAdmission{}
	}
}

func nextAsyncResult(t *testing.T, values <-chan asyncResultEvent) asyncResultEvent {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("missing asynchronous result")
		return asyncResultEvent{}
	}
}

func nextAsyncHeld(t *testing.T, values <-chan asyncHeldCall) asyncHeldCall {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("asynchronous worker did not enter its callee")
		return asyncHeldCall{}
	}
}

func nextAsyncBool(t *testing.T, values <-chan bool) bool {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("Actor did not report asynchronous cancellation")
		return false
	}
}

func waitAsyncQueued(t *testing.T, n *Node, pid PID, id RequestID) {
	t.Helper()
	p, err := n.resolvePID(pid)
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		r := p.asyncRequests[id]
		return r != nil && r.completed
	})
}

func TestAsyncRequestSerialResultBypassesFullMailboxAndRetainsQuota(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 1, MaxAsyncRequests: 1})
	held := make(chan asyncHeldCall, 1)
	callee := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		switch message {
		case "defer":
			held <- asyncHeldCall{"defer", a.Defer(), a.Context()}
		case "reply":
			return a.Reply(84)
		case "ping":
			return a.Reply(7)
		}
		return nil
	}))
	admissions := make(chan asyncAdmission, 2)
	results := make(chan asyncResultEvent, 2)
	probe, holdEntered := make(chan struct{}, 1), make(chan struct{})
	holdRelease := make(chan struct{})
	release := sync.OnceFunc(func() { close(holdRelease) })
	defer release()
	lateCancel := make(chan bool, 1)
	business := make(chan string, 2)
	var active atomic.Int32
	var concurrent, badSender, quotaHeldDuringResult atomic.Bool
	var first RequestID
	caller := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		if active.Add(1) != 1 {
			concurrent.Store(true)
		}
		defer active.Add(-1)
		switch message := message.(type) {
		case string:
			switch message {
			case "start":
				id, err := a.Request(context.Background(), callee, "defer")
				first = id
				admissions <- asyncAdmission{id, err}
			case "probe":
				probe <- struct{}{}
			case "hold":
				close(holdEntered)
				<-holdRelease
				lateCancel <- a.CancelRequest(first)
			case "one", "two":
				business <- message
			}
		case CallResult:
			if a.Sender() != nil {
				badSender.Store(true)
			}
			results <- asyncResultEvent{a.PID(), message}
			if message.ID == first {
				if n.Stats().AsyncRequests != 0 {
					quotaHeldDuringResult.Store(true)
				}
				id, err := a.Request(context.Background(), callee, "reply")
				admissions <- asyncAdmission{id, err}
			}
		}
		return nil
	}), WithMailboxSize(2))
	if err := n.Send(caller, "start"); err != nil {
		t.Fatal(err)
	}
	firstAdmission := nextAsyncAdmission(t, admissions)
	if firstAdmission.Err != nil || firstAdmission.ID.Caller != caller || firstAdmission.ID.ID == 0 {
		t.Fatalf("initial admission: %+v", firstAdmission)
	}
	ticket := nextAsyncHeld(t, held)
	if err := n.Send(caller, "probe"); err != nil {
		t.Fatal(err)
	}
	signal(t, probe) // Request returned and the caller keeps receiving before a reply.
	if err := n.Send(caller, "hold"); err != nil {
		t.Fatal(err)
	}
	signal(t, holdEntered)
	for _, message := range []string{"one", "two"} {
		if err := n.Send(caller, message); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.Send(caller, "overflow"); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("business mailbox was not full: %v", err)
	}
	if err := ticket.Reply.Reply(42); err != nil {
		t.Fatal(err)
	}
	waitAsyncQueued(t, n, caller, firstAdmission.ID)
	if stats := n.Stats(); stats.AsyncRequests != 1 || stats.PendingCalls != 0 {
		t.Fatalf("queued result did not retain only its completion quota: %+v", stats)
	}
	if detail, err := n.ActorSnapshot(caller); err != nil || detail.AsyncRequests != 1 || detail.Mailbox != 2 || detail.SystemQueue < 1 {
		t.Fatalf("queued result snapshot: %+v, %v", detail, err)
	}
	select {
	case result := <-results:
		t.Fatalf("result entered Receive concurrently with hold: %+v", result)
	default:
	}
	// A completed async operation holds its result slot, not an ordinary Call
	// slot. The caller's full business mailbox does not affect this other Call.
	if value, err := n.Call[int](t.Context(), callee, "ping"); err != nil || value != 7 {
		t.Fatalf("queued completion consumed ordinary Call quota: %d, %v", value, err)
	}
	denied := make(chan asyncAdmission, 1)
	other := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		if message == "try" {
			id, err := a.Request(context.Background(), callee, "reply")
			denied <- asyncAdmission{id, err}
		}
		return nil
	}))
	if err := n.Send(other, "try"); err != nil {
		t.Fatal(err)
	}
	if admission := nextAsyncAdmission(t, denied); !errors.Is(admission.Err, ErrCapacity) || admission.ID != (RequestID{}) {
		t.Fatalf("queued completion escaped global quota: %+v", admission)
	}
	release()
	if cancelled := nextAsyncBool(t, lateCancel); cancelled {
		t.Fatal("cancellation replaced a reply that had already completed")
	}
	firstResult := nextAsyncResult(t, results)
	if firstResult.Caller != caller || firstResult.Result.ID != firstAdmission.ID || firstResult.Result.Value != 42 || firstResult.Result.Err != nil {
		t.Fatalf("first result: %+v", firstResult)
	}
	second := nextAsyncAdmission(t, admissions)
	if second.Err != nil || second.ID.Caller != caller || second.ID == firstAdmission.ID {
		t.Fatalf("result consumption did not release admission: %+v", second)
	}
	if result := nextAsyncResult(t, results); result.Result.ID != second.ID || result.Result.Value != 84 || result.Result.Err != nil {
		t.Fatalf("second result: %+v", result)
	}
	for _, want := range []string{"one", "two"} {
		select {
		case got := <-business:
			if got != want {
				t.Fatalf("business FIFO: got %s want %s", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("business mailbox did not resume after result consumption")
		}
	}
	if concurrent.Load() || badSender.Load() || quotaHeldDuringResult.Load() || n.Stats().AsyncRequests != 0 {
		t.Fatal("result concurrency, sender or consumption quota contract changed")
	}
}

func TestAsyncRequestPerActorAndDefaultNodeAdmission(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 3}) // Default MaxAsyncRequests is also three.
	held := make(chan asyncHeldCall, 3)
	callee := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		if message == "defer" {
			held <- asyncHeldCall{"defer", a.Defer(), a.Context()}
		}
		return nil
	}))
	admissions := make(chan asyncAdmission, 6)
	results := make(chan asyncResultEvent, 3)
	newCaller := func(capacity int) PID {
		return spawn(t, n, FromFunc(func(a *Actor, message any) error {
			switch message := message.(type) {
			case int:
				for range message {
					id, err := a.Request(context.Background(), callee, "defer")
					admissions <- asyncAdmission{id, err}
				}
			case CallResult:
				results <- asyncResultEvent{a.PID(), message}
			}
			return nil
		}), WithMailboxSize(capacity))
	}
	first := newCaller(1)
	second := newCaller(2)
	third := newCaller(2)
	if err := n.Send(first, 2); err != nil {
		t.Fatal(err)
	}
	one, perActor := nextAsyncAdmission(t, admissions), nextAsyncAdmission(t, admissions)
	if one.Err != nil || one.ID.Caller != first || !errors.Is(perActor.Err, ErrCapacity) || perActor.ID != (RequestID{}) {
		t.Fatalf("per-Actor bound: %+v, %+v", one, perActor)
	}
	if err := n.Send(second, 2); err != nil {
		t.Fatal(err)
	}
	want := map[RequestID]bool{one.ID: true}
	for range 2 {
		admission := nextAsyncAdmission(t, admissions)
		if admission.Err != nil || admission.ID.Caller != second || want[admission.ID] {
			t.Fatalf("second Actor admission: %+v", admission)
		}
		want[admission.ID] = true
	}
	tickets := make([]asyncHeldCall, 3)
	for i := range tickets {
		tickets[i] = nextAsyncHeld(t, held)
	}
	if err := n.Send(third, 1); err != nil {
		t.Fatal(err)
	}
	if admission := nextAsyncAdmission(t, admissions); !errors.Is(admission.Err, ErrCapacity) || admission.ID != (RequestID{}) {
		t.Fatalf("default node bound: %+v", admission)
	}
	if stats := n.Stats(); stats.AsyncRequests != 3 || stats.PendingCalls != 3 {
		t.Fatalf("async workers registered more than one Call each: %+v", stats)
	}
	for _, check := range []struct {
		pid  PID
		want int
	}{{first, 1}, {second, 2}, {third, 0}} {
		if detail, err := n.ActorSnapshot(check.pid); err != nil || detail.AsyncRequests != check.want {
			t.Fatalf("per-Actor async count: %+v, %v", detail, err)
		}
	}
	for _, ticket := range tickets {
		if err := ticket.Reply.Reply(9); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		result := nextAsyncResult(t, results)
		if !want[result.Result.ID] || result.Caller != result.Result.ID.Caller || result.Result.Value != 9 || result.Result.Err != nil {
			t.Fatalf("admitted request result: %+v", result)
		}
		delete(want, result.Result.ID)
	}
	eventually(t, func() bool { return n.Stats().AsyncRequests == 0 && n.Stats().PendingCalls == 0 })
}

type asyncRequestPlan struct {
	Name    string
	Context context.Context
	To      Addr
	Message any
}

type asyncNamedAdmission struct {
	Name string
	asyncAdmission
}

type asyncCancelCommand struct {
	ID      RequestID
	Outcome chan bool
}

func TestAsyncRequestErrorsCancellationAndRemoteResults(t *testing.T) {
	n, peer := networkNode(t, "async-caller"), networkNode(t, "async-peer")
	connectNodes(t, n, peer)
	business := errors.New("async business refusal")
	held := make(chan asyncHeldCall, 2)
	local := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		switch message {
		case "business":
			return a.ReplyError(business)
		case "cancel", "timeout":
			held <- asyncHeldCall{message.(string), a.Defer(), a.Context()}
		}
		return nil
	}))
	remote := spawn(t, peer, FromFunc(func(a *Actor, message any) error {
		if command, ok := message.(remoteCommand); ok {
			return a.Reply(command.Delta)
		}
		return nil
	}))
	admissions := make(chan asyncNamedAdmission, 8)
	results := make(chan asyncResultEvent, 6)
	caller := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		switch message := message.(type) {
		case asyncRequestPlan:
			id, err := a.Request(message.Context, message.To, message.Message)
			admissions <- asyncNamedAdmission{message.Name, asyncAdmission{id, err}}
		case asyncCancelCommand:
			message.Outcome <- a.CancelRequest(message.ID)
		case string:
			if message == "self-request" {
				// Request can target this Actor, but its handler context must
				// not let a synchronous child inherit the async self-call bypass.
				if _, err := a.Call(a.Context(), a.PID(), "sync-self"); !errors.Is(err, ErrSelfCall) {
					return a.ReplyError(errors.New("synchronous self-call was not rejected inside asynchronous self Request"))
				}
				return a.Reply(31)
			}
		case CallResult:
			results <- asyncResultEvent{a.PID(), message}
		}
		return nil
	}))
	deadline, stopDeadline := context.WithTimeout(context.Background(), time.Second)
	defer stopDeadline()
	plans := []asyncRequestPlan{
		{"business", context.Background(), local, "business"},
		{"missing", context.Background(), Ref{Path: "not-present"}, "work"},
		{"self", context.Background(), caller, "self-request"},
		{"cancel", context.Background(), local, "cancel"},
		{"timeout", deadline, local, "timeout"},
		{"remote", context.Background(), remote, remoteCommand{Kind: "reply", Delta: 73}},
	}
	ids := make(map[string]RequestID)
	names := make(map[RequestID]string)
	for _, plan := range plans {
		if err := n.Send(caller, plan); err != nil {
			t.Fatal(err)
		}
		select {
		case admission := <-admissions:
			if admission.Err != nil || admission.ID.Caller != caller || admission.ID.ID == 0 || names[admission.ID] != "" {
				t.Fatalf("request admission: %+v", admission)
			}
			ids[admission.Name], names[admission.ID] = admission.ID, admission.Name
		case <-time.After(5 * time.Second):
			t.Fatal("missing named request admission")
		}
	}
	tickets := make(map[string]asyncHeldCall)
	for range 2 {
		ticket := nextAsyncHeld(t, held)
		tickets[ticket.Name] = ticket
	}
	cancelled := make(chan bool, 1)
	wrongOwner := ids["cancel"]
	wrongOwner.Caller = local
	if err := n.Send(caller, asyncCancelCommand{wrongOwner, cancelled}); err != nil {
		t.Fatal(err)
	}
	if nextAsyncBool(t, cancelled) {
		t.Fatal("foreign Caller in RequestID cancelled an operation")
	}
	if err := n.Send(caller, asyncCancelCommand{ids["cancel"], cancelled}); err != nil {
		t.Fatal(err)
	}
	if !nextAsyncBool(t, cancelled) {
		t.Fatal("unfinished Request could not be cancelled")
	}
	for range len(plans) {
		event := nextAsyncResult(t, results)
		name := names[event.Result.ID]
		if event.Caller != caller || name == "" {
			t.Fatalf("unexpected or duplicate result: %+v", event)
		}
		delete(names, event.Result.ID)
		wantErr := map[string]error{"business": business, "missing": ErrNoProcess, "cancel": context.Canceled, "timeout": context.DeadlineExceeded}[name]
		if !errors.Is(event.Result.Err, wantErr) {
			t.Fatalf("%s result: %v, want %v", name, event.Result.Err, wantErr)
		}
		if name == "self" {
			if event.Result.Value != 31 {
				t.Fatalf("asynchronous self Request did not complete: %#v", event.Result.Value)
			}
		} else if name == "remote" {
			if event.Result.Value != int64(73) {
				t.Fatalf("remote result lost its registered type: %#v", event.Result.Value)
			}
		} else if event.Result.Value != nil {
			t.Fatalf("failed %s Request returned a value: %#v", name, event.Result.Value)
		}
	}
	for _, name := range []string{"cancel", "timeout"} {
		select {
		case <-tickets[name].Context.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not reach the callee context", name)
		}
		if err := tickets[name].Reply.Reply(1); !errors.Is(err, ErrReplyExpired) {
			t.Fatalf("late %s reply: %v", name, err)
		}
	}
	eventually(t, func() bool { return n.Stats().AsyncRequests == 0 && n.Stats().PendingCalls == 0 })
}

type asyncUserContextKey struct{}

func TestAsyncRequestDiagnosticsSeparateSynchronousChildAndPreserveContext(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.EnableDiagnostics()
	type contextInfo struct {
		Value    any
		Deadline time.Time
		HasLimit bool
	}
	contexts := make(chan contextInfo, 2)
	causes := make(chan error, 2)
	backEntered := make(chan struct{})
	results := make(chan asyncResultEvent, 1)
	admissions := make(chan asyncAdmission, 1)
	innerErrors := make(chan error, 1)
	var first, second PID
	inspectContext := func(ctx context.Context) {
		deadline, ok := ctx.Deadline()
		contexts <- contextInfo{ctx.Value(asyncUserContextKey{}), deadline, ok}
	}
	first = spawn(t, n, FromFunc(func(a *Actor, message any) error {
		switch message := message.(type) {
		case asyncRequestPlan:
			id, err := a.Request(message.Context, second, "request")
			admissions <- asyncAdmission{id, err}
		case string:
			if message == "back" {
				ctx := a.Context()
				inspectContext(ctx)
				close(backEntered)
				<-ctx.Done()
				causes <- context.Cause(ctx)
			}
		case CallResult:
			results <- asyncResultEvent{a.PID(), message}
		}
		return nil
	}))
	second = spawn(t, n, FromFunc(func(a *Actor, message any) error {
		if message == "request" {
			ctx := a.Context()
			inspectContext(ctx)
			_, err := a.Call(ctx, first, "back")
			innerErrors <- err
			causes <- context.Cause(ctx)
		}
		return nil
	}))
	deadline := time.Now().Add(30 * time.Second)
	limited, stopLimit := context.WithDeadline(context.WithValue(context.Background(), asyncUserContextKey{}, "user-value"), deadline)
	defer stopLimit()
	ctx, cancel := context.WithCancelCause(limited)
	cause := errors.New("user cancelled asynchronous chain")
	defer cancel(cause)
	if err := n.Send(first, asyncRequestPlan{Context: ctx}); err != nil {
		t.Fatal(err)
	}
	admission := nextAsyncAdmission(t, admissions)
	if admission.Err != nil {
		t.Fatal(admission.Err)
	}
	signal(t, backEntered)
	for range 2 {
		info := <-contexts
		if info.Value != "user-value" || !info.HasLimit || !info.Deadline.Equal(deadline) {
			t.Fatalf("handler context lost user data or deadline: %+v", info)
		}
	}
	snapshot, err := n.Snapshot(SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	asyncEdge, syncEdge := false, false
	for _, call := range snapshot.Calls {
		if call.Caller == first.String() && call.Owner == second.String() {
			asyncEdge = call.Asynchronous
		}
		if call.Caller == second.String() && call.Owner == first.String() {
			syncEdge = !call.Asynchronous
		}
	}
	if !asyncEdge || !syncEdge || len(snapshot.Cycles) != 0 || snapshot.Stats.AsyncRequests != 1 || snapshot.CallsTotal != 2 {
		t.Fatalf("asynchronous edge became a waiting cycle: %+v", snapshot)
	}
	if detail, err := n.ActorSnapshot(first); err != nil || detail.AsyncRequests != 1 {
		t.Fatalf("Observer caller count: %+v, %v", detail, err)
	}
	cancel(cause)
	if result := nextAsyncResult(t, results); result.Result.ID != admission.ID || !errors.Is(result.Result.Err, context.Canceled) {
		t.Fatalf("cancelled graph result: %+v", result)
	}
	for range 2 {
		select {
		case got := <-causes:
			if !errors.Is(got, cause) {
				t.Fatalf("handler context lost cancellation cause: %v", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("handler context did not observe cancellation")
		}
	}
	if err := <-innerErrors; !errors.Is(err, context.Canceled) {
		t.Fatalf("synchronous child cancellation: %v", err)
	}
	eventually(t, func() bool { return n.Stats().AsyncRequests == 0 && n.Stats().PendingCalls == 0 })
}
