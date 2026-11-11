package actor

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"weak"
)

func TestDefaultLocalPIDCallCompletesWithoutNodeRequestLock(t *testing.T) {
	n := testNode(t, NodeOptions{})
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "call" {
			return a.Reply(42)
		}
		return nil
	}))
	result := make(chan error, 1)
	// Registry activity must not hold up a physical PID's admission or Reply.
	n.mu.Lock()
	go func() {
		value, err := n.Call[int](context.Background(), pid, "call")
		if err == nil && value != 42 {
			err = errors.New("incorrect result")
		}
		result <- err
	}()
	var err error
	select {
	case err = <-result:
	case <-time.After(5 * time.Second):
		err = errors.New("local PID Call waited for node.mu")
	}
	entries := len(n.pending)
	n.mu.Unlock()
	if err != nil || entries != 0 || n.network != nil {
		t.Fatalf("physical PID completion: err=%v node entries=%d network=%v", err, entries, n.network != nil)
	}
}

func TestDefaultLocalPIDDeferredRequestAppearsOnceInSnapshot(t *testing.T) {
	n := testNode(t, NodeOptions{CallTimeout: time.Minute})
	n.EnableDiagnostics()
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "defer" {
			tickets <- a.Defer()
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() {
		value, err := n.Call[int](context.Background(), pid, "defer")
		if err == nil && value != 42 {
			err = errors.New("incorrect deferred result")
		}
		result <- err
	}()
	ticket := <-tickets
	if ticket.request.kind != requestActorPID || ticket.request.actorRequests() == nil {
		t.Fatal("default local PID Call retained node ownership")
	}
	n.mu.Lock()
	entries := len(n.pending)
	n.mu.Unlock()
	view, err := n.Snapshot(SnapshotOptions{})
	if err != nil || entries != 0 || view.CallsTotal != 1 || len(view.Calls) != 1 {
		t.Fatalf("owner snapshot: entries=%d view=%+v err=%v", entries, view, err)
	}
	call := view.Calls[0]
	if !call.Deferred || call.Owner != pid.String() || call.Target != pid.String() || call.Started.IsZero() || call.Deadline.IsZero() {
		t.Fatalf("deferred Call metadata: %+v", call)
	}
	if err := ticket.Reply(42); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	signal(t, ticket.request.waitDone())
	if err := ticket.Reply(43); !errors.Is(err, ErrAlreadyReplied) {
		t.Fatalf("late reply: %v", err)
	}
	if n.Stats().PendingCalls != 0 {
		t.Fatal("completed owner retained shared quota")
	}
}

func TestDefaultLocalPIDFirstCallRacesStop(t *testing.T) {
	n := testNode(t, NodeOptions{})
	for range 100 {
		pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
			if message == "call" {
				return a.Reply(42)
			}
			return nil
		}), WithRestart(Temporary))
		p, _, err := n.resolve(pid)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Go(func() {
			<-start
			value, err := n.Call[int](context.Background(), pid, "call")
			if err != nil && !errors.Is(err, ErrStopped) && !errors.Is(err, ErrNoProcess) {
				t.Error(err)
			}
			if err == nil && value != 42 {
				t.Errorf("incomplete result: %d", value)
			}
		})
		group.Go(func() { <-start; p.requestStop(ErrStopped, true) })
		close(start)
		group.Wait()
		signal(t, p.done)
		if n.Stats().PendingCalls != 0 {
			t.Fatal("first local registration escaped Stop")
		}
		// The resolver can have returned p just before Stop. Such an in-flight
		// registrar must still join the same closed publication protocol.
		state := &localCallState{scope: requestScope{parent: context.Background(), deadline: time.Now().Add(time.Minute), hasDeadline: true}}
		if err := p.registerActorRequest(&state.request, &state.scope, nil); !errors.Is(err, ErrStopped) {
			t.Fatalf("resolved retired PID admitted a request: %v", err)
		}
	}
}

func TestDefaultLocalPIDAndNodeRequestsShareQuotaAndIDs(t *testing.T) {
	n := testNode(t, NodeOptions{MaxPendingCalls: 3, CallTimeout: time.Minute})
	tickets := make(chan DeferredReply, 1)
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "defer" {
			tickets <- a.Defer()
		}
		return nil
	}), WithName("worker"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 3)
	var held []DeferredReply
	for _, call := range []struct {
		ctx context.Context
		to  Addr
	}{{context.Background(), pid}, {context.Background(), Ref{Path: "worker"}}, {ctx, pid}} {
		go func() {
			value, err := n.Call[int](call.ctx, call.to, "defer")
			if err == nil && value != 42 {
				err = errors.New("incorrect mixed result")
			}
			results <- err
		}()
		held = append(held, <-tickets)
	}
	if held[0].request.kind != requestActorPID || held[1].request.kind != requestNode || held[2].request.kind != requestNode {
		t.Fatal("mixed Call completion domains changed")
	}
	if n.Stats().PendingCalls != 3 || held[0].request.id == held[1].request.id || held[1].request.id == held[2].request.id || held[0].request.id == held[2].request.id {
		t.Fatal("completion domains did not share quota and unique IDs")
	}
	for _, to := range []Addr{pid, Ref{Path: "worker"}} {
		if _, err := n.Call[int](context.Background(), to, "defer"); !errors.Is(err, ErrCapacity) {
			t.Fatalf("mixed capacity: %v", err)
		}
	}
	if n.nextRequest.Load() != 3 {
		t.Fatal("rejected registration consumed a request ID")
	}
	for _, ticket := range held {
		if err := ticket.Reply(42); err != nil {
			t.Fatal(err)
		}
	}
	for range held {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if n.Stats().PendingCalls != 0 {
		t.Fatal("mixed completions retained quota")
	}
}

func TestDefaultLocalPIDCallSharesQuotaWithRefIncomingAndOutgoing(t *testing.T) {
	server, err := StartNode("server@127.0.0.1:0", NodeOptions{MaxPendingCalls: 4, CallTimeout: time.Minute, Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"count", 1, int64(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	server.EnableDiagnostics()
	peer := networkNode(t, "peer")
	connectNodes(t, server, peer)
	tickets := make(chan DeferredReply, 1)
	behavior := factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			tickets <- a.Defer()
		}
		return nil
	})
	localPID := spawn(t, server, behavior, WithName("worker"))
	remotePID := spawn(t, peer, behavior)
	results := make(chan error, 4)
	var held []DeferredReply
	for _, call := range []struct {
		node *Node
		to   Addr
	}{{server, localPID}, {server, Ref{Path: "worker"}}, {peer, localPID}, {server, remotePID}} {
		go func() {
			value, err := call.node.Call[int64](context.Background(), call.to, int64(1))
			if err == nil && value != 42 {
				err = errors.New("incorrect network result")
			}
			results <- err
		}()
		select {
		case ticket := <-tickets:
			held = append(held, ticket)
		case err := <-results:
			t.Fatalf("request %d (%s -> %v) completed before Defer: %v", len(held), call.node.Name(), call.to, err)
		}
	}
	view, err := server.Snapshot(SnapshotOptions{})
	if err != nil || view.CallsTotal != 4 || len(view.Calls) != 4 {
		t.Fatalf("mixed domains snapshot: %+v %v", view, err)
	}
	for _, to := range []Addr{localPID, Ref{Path: "worker"}, remotePID} {
		if _, err := server.Call[int64](context.Background(), to, int64(2)); !errors.Is(err, ErrCapacity) {
			t.Fatalf("shared quota rejection for %v: %v", to, err)
		}
	}
	_, err = peer.Call[int64](t.Context(), localPID, int64(2))
	remoteError(t, err, ErrCapacity, Rejected)
	if server.Stats().PendingCalls != 4 || server.nextRequest.Load() != 4 {
		t.Fatal("rejected Call changed quota or request sequence")
	}
	for _, ticket := range held {
		if err := ticket.Reply(int64(42)); err != nil {
			t.Fatal(err)
		}
	}
	for range held {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool {
		return server.Stats().PendingCalls == 0 && peer.Stats().PendingCalls == 0 && server.NetworkStats().IncomingCalls == 0 && peer.NetworkStats().IncomingCalls == 0
	})
}

func TestCallDiagnosticTargetsPreserveAddressStrings(t *testing.T) {
	tickets := make(chan DeferredReply, 1)
	behavior := factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			tickets <- a.Defer()
		}
		return nil
	})
	plain := testNode(t, NodeOptions{})
	plain.EnableDiagnostics()
	plainPID := spawn(t, plain, behavior, WithName("plain"))
	directory := NewMemoryDirectory()
	caller := globalNetworkNode(t, "caller", directory)
	caller.EnableDiagnostics()
	server := globalNetworkNode(t, "server", directory)
	connectNodes(t, caller, server)
	localPID := spawn(t, caller, behavior, WithName("local"))
	remotePID := spawn(t, server, behavior, WithName("remote"))
	for _, test := range []struct {
		name string
		node *Node
		ctx  context.Context
		to   Addr
		want string
	}{
		{"local-pid", plain, context.Background(), plainPID, plainPID.String()},
		{"local-pid-context", plain, t.Context(), plainPID, plainPID.String()},
		{"local-ref", plain, context.Background(), Ref{Path: "plain"}, "plain"},
		{"local-node-ref", plain, context.Background(), Ref{Node: plain.Name(), Path: "plain"}, "node://" + plain.Name() + "/plain"},
		{"local-global-ref", caller, context.Background(), Ref{Path: "local"}, "local"},
		{"local-directory-pid", caller, context.Background(), localPID, localPID.String()},
		{"remote-pid", caller, context.Background(), remotePID, remotePID.String()},
		{"remote-node-ref", caller, context.Background(), Ref{Node: server.Name(), Path: "remote"}, "node://" + server.Name() + "/remote"},
		{"remote-global-ref", caller, context.Background(), Ref{Path: "remote"}, "remote"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := make(chan error, 1)
			go func() {
				value, err := test.node.Call[int64](test.ctx, test.to, int64(1))
				if err == nil && value != 42 {
					err = errors.New("incorrect diagnostic Call result")
				}
				result <- err
			}()
			ticket := <-tickets
			view, err := test.node.Snapshot(SnapshotOptions{})
			if err != nil || view.CallsTotal != 1 || len(view.Calls) != 1 || view.Calls[0].Target != test.want {
				t.Fatalf("Call target %q: %+v %v", test.want, view.Calls, err)
			}
			if err := ticket.Reply(int64(42)); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInlineLocalPIDCallLifecycleAndRaces(t *testing.T) {
	t.Run("snapshot-and-defer-promotion", func(t *testing.T) {
		n := testNode(t, NodeOptions{CallTimeout: time.Minute})
		entered, release := make(chan struct{}), make(chan struct{})
		tickets := make(chan DeferredReply, 1)
		pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
			switch message {
			case "hold":
				close(entered)
				<-release
				return a.Reply(7)
			case "defer":
				tickets <- a.Defer()
			case "noreply":
				return nil
			}
			return nil
		}))
		holdRes := make(chan error, 1)
		go func() {
			v, err := n.Call[int](context.Background(), pid, "hold")
			if err == nil && v != 7 {
				err = errors.New("unexpected hold reply")
			}
			holdRes <- err
		}()
		signal(t, entered)
		view, err := n.Snapshot(SnapshotOptions{})
		if err != nil || view.CallsTotal != 1 || len(view.Calls) != 1 || view.Calls[0].Owner != pid.String() {
			t.Fatalf("unpromoted inline snapshot: %+v %v", view, err)
		}
		close(release)
		if err := <-holdRes; err != nil {
			t.Fatal(err)
		}

		deferRes := make(chan error, 1)
		go func() {
			v, err := n.Call[int](context.Background(), pid, "defer")
			if err == nil && v != 42 {
				err = errors.New("unexpected deferred reply")
			}
			deferRes <- err
		}()
		ticket := <-tickets
		if ticket.request.actorRequests() == nil {
			t.Fatal("Defer did not promote inline PID Call to actorRequests")
		}
		view, err = n.Snapshot(SnapshotOptions{})
		if err != nil || view.CallsTotal != 1 || len(view.Calls) != 1 || !view.Calls[0].Deferred {
			t.Fatalf("promoted inline snapshot: %+v %v", view, err)
		}
		if err := ticket.Reply(42); err != nil {
			t.Fatal(err)
		}
		if err := <-deferRes; err != nil {
			t.Fatal(err)
		}
		if err := ticket.Reply(43); !errors.Is(err, ErrAlreadyReplied) {
			t.Fatalf("second deferred reply: %v", err)
		}
		if _, err := n.Call[int](context.Background(), pid, "noreply"); !errors.Is(err, ErrNoReply) {
			t.Fatalf("inline no-reply: %v", err)
		}
		if n.Stats().PendingCalls != 0 {
			t.Fatalf("pending calls leaked: %d", n.Stats().PendingCalls)
		}
	})

	t.Run("timeout-and-crash-stop-races", func(t *testing.T) {
		expiredReply := make(chan error, 1)
		expireNode := testNode(t, NodeOptions{CallTimeout: 2 * time.Millisecond})
		expirePID := spawn(t, expireNode, factory(func(a *testBehavior, message any) error {
			if message == "slow" {
				<-a.Context().Done()
				expiredReply <- a.Reply(1)
			}
			return nil
		}))
		if _, err := expireNode.Call[int](context.Background(), expirePID, "slow"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired inline Call got %v, want DeadlineExceeded", err)
		}
		if err := <-expiredReply; !errors.Is(err, ErrReplyExpired) {
			t.Fatalf("late inline Reply got %v, want ErrReplyExpired", err)
		}
		if expireNode.Stats().PendingCalls != 0 {
			t.Fatalf("expired inline Call leaked quota: %d", expireNode.Stats().PendingCalls)
		}

		raceNode := testNode(t, NodeOptions{CallTimeout: time.Nanosecond})
		racePID := spawn(t, raceNode, factory(func(a *testBehavior, message any) error {
			if message == "ping" {
				_ = a.Reply(1)
			}
			return nil
		}))
		for range 100 {
			v, err := raceNode.Call[int](context.Background(), racePID, "ping")
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("tight timeout Call got %v, want nil or DeadlineExceeded", err)
			}
			if err == nil && v != 1 {
				t.Fatalf("tight timeout Call got %d, want 1", v)
			}
		}
		eventually(t, func() bool { return raceNode.Stats().PendingCalls == 0 })

		boom := errors.New("boom")
		for range 50 {
			n := testNode(t, NodeOptions{CallTimeout: time.Minute})
			pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "crash":
					return boom
				case "defer":
					_ = a.Defer()
				case "ok":
					return a.Reply(1)
				}
				return nil
			}), WithRestart(Temporary))
			var wg sync.WaitGroup
			for _, msg := range []string{"ok", "defer", "crash", "ok"} {
				wg.Go(func() {
					_, err := n.Call[int](context.Background(), pid, msg)
					if err != nil && !errors.Is(err, boom) && !errors.Is(err, ErrStopped) && !errors.Is(err, ErrNoProcess) {
						t.Errorf("unexpected race error for %s: %v", msg, err)
					}
				})
			}
			wg.Go(func() { _, _ = n.Snapshot(SnapshotOptions{}) })
			wg.Wait()
			eventually(t, func() bool { return n.Stats().PendingCalls == 0 })
		}
	})

	t.Run("late-reply-preserves-deadline-cause", func(t *testing.T) {
		n := testNode(t, NodeOptions{})
		pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }))
		p, _, err := n.resolve(pid)
		if err != nil {
			t.Fatal(err)
		}
		for _, inline := range []bool{false, true} {
			state := &localCallState{
				request: request{inline: inline, node: n, owner: pid, kind: requestActorPID},
				scope:   requestScope{parent: context.Background(), deadline: time.Now().Add(-time.Second), hasDeadline: true},
			}
			r, scope := &state.request, &state.scope
			if inline {
				if err := n.reservePendingCall(); err != nil {
					t.Fatal(err)
				}
				r.ctx = scope
				scope.request = r
				scope.actorProc.Store(p)
			} else if err := p.registerActorRequest(r, scope, nil); err != nil {
				t.Fatal(err)
			}
			if err := n.reply(r, pid, "late", nil, false); !errors.Is(err, ErrReplyExpired) {
				t.Fatalf("inline=%t late Reply = %v", inline, err)
			}
			if !errors.Is(r.err, context.DeadlineExceeded) {
				t.Fatalf("inline=%t Call result = %v", inline, r.err)
			}
			ctx := scope.context()
			if ctx.Err() != context.DeadlineExceeded || context.Cause(ctx) != context.DeadlineExceeded {
				t.Fatalf("inline=%t Call timed out but handler Context reports Err=%v, Cause=%v", inline, ctx.Err(), context.Cause(ctx))
			}
		}
	})

	t.Run("default-call-timer-uses-remaining-budget", func(t *testing.T) {
		const budget = 250 * time.Millisecond
		n := testNode(t, NodeOptions{CallTimeout: budget})
		n.EnableDiagnostics()
		entered, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
			if msg == "block" {
				close(entered)
				<-release
			}
			if msg == "call" {
				_ = a.Reply(1)
			}
			return nil
		}))
		if err := n.Send(pid, "block"); err != nil {
			t.Fatal(err)
		}
		signal(t, entered)
		p, _, _ := n.resolve(pid)
		owner, err := p.requestOwner()
		if err != nil {
			t.Fatal(err)
		}
		owner.mu.Lock()
		unlock := sync.OnceFunc(owner.mu.Unlock)
		defer unlock()
		result := make(chan error, 1)
		go func() { _, err := n.Call[int](context.Background(), pid, "call"); result <- err }()
		waitUntil := time.Now().Add(2 * time.Second)
		for {
			stack := make([]byte, 1<<16)
			count := runtime.Stack(stack, true)
			if strings.Contains(string(stack[:count]), ").registerActorRequest(") {
				break
			}
			if time.Now().After(waitUntil) {
				t.Fatal("Call did not reach request registration")
			}
			runtime.Gosched()
		}
		<-time.After(2 * budget)
		releasedAt := time.Now()
		unlock()
		select {
		case err := <-result:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Call = %v", err)
			}
			if elapsed := time.Since(releasedAt); elapsed >= budget/2 {
				t.Fatalf("admission consumed the deadline, but Call waited another %s after admission resumed", elapsed)
			}
		case <-time.After(2 * budget):
			t.Fatal("Call never completed after admission resumed")
		}
	})

	t.Run("deferred-ticket-and-context-release-retired-process", func(t *testing.T) {
		for _, diag := range []bool{false, true} {
			n := testNode(t, NodeOptions{CallTimeout: time.Minute})
			if diag {
				n.EnableDiagnostics()
			}
			ticket, ctx, procRef, behRef := spawnStoppedDeferredWithRefs(t, n)
			for range 5 {
				runtime.GC()
				runtime.Gosched()
			}
			if procRef.Value() != nil || behRef.Value() != nil {
				t.Fatalf("diagnostics=%t expired DeferredReply/Context still retains retired process (%v) or behavior (%v)",
					diag, procRef.Value() != nil, behRef.Value() != nil)
			}
			runtime.KeepAlive(ticket)
			runtime.KeepAlive(ctx)
		}
	})
}

func spawnStoppedDeferredWithRefs(t *testing.T, n *Node) (DeferredReply, context.Context, weak.Pointer[process], weak.Pointer[testBehavior]) {
	t.Helper()
	type held struct {
		ticket DeferredReply
		ctx    context.Context
	}
	tickets := make(chan held, 1)
	var behRef weak.Pointer[testBehavior]
	pid := spawn(t, n, func() Behavior {
		b := &testBehavior{receive: func(a *testBehavior, msg any) error {
			if msg == "defer" {
				tickets <- held{ticket: a.Defer(), ctx: a.Context()}
			}
			return nil
		}}
		behRef = weak.Make(b)
		return b
	}, WithRestart(Temporary))
	p, _, _ := n.resolve(pid)
	procRef := weak.Make(p)
	p = nil
	result := make(chan error, 1)
	go func() { _, err := n.Call[int](context.Background(), pid, "defer"); result <- err }()
	got := <-tickets
	if err := n.StopActor(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped call = %v", err)
	}
	return got.ticket, got.ctx, procRef, behRef
}
