package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestActorRequestReplyCancelStopHaveOneCompletion(t *testing.T) {
	n := networkNode(t, "owner-race")
	n.EnableDiagnostics()
	for range 100 {
		pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
		p, _, _ := n.resolve(pid)
		state := &incomingRequestState{scope: requestScope{parent: n.network.ctx, deadline: time.Now().Add(time.Hour)}}
		r, err := n.network.registerActorRequest(&state.request, p, &state.scope, requestKey{Caller: "peer", ID: 1}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var finished atomic.Int32
		r.bindCompletion(&requestCompletion{run: func(*request) { finished.Add(1) }})
		start := make(chan struct{})
		var racers sync.WaitGroup
		racers.Go(func() {
			<-start
			err := n.reply(r, pid, 42, nil, false)
			if err != nil && !errors.Is(err, ErrReplyExpired) {
				t.Error(err)
			}
		})
		racers.Go(func() { <-start; state.scope.cancel() })
		racers.Go(func() { <-start; p.requestStop(ErrStopped, true) })
		racers.Go(func() { <-start; signal(t, r.waitDone()) })
		racers.Go(func() { <-start; _, _ = n.Snapshot(SnapshotOptions{}) })
		close(start)
		racers.Wait()
		signal(t, p.done)
		if finished.Load() != 1 || n.Stats().PendingCalls != 0 {
			t.Fatalf("completion count=%d pending=%d", finished.Load(), n.Stats().PendingCalls)
		}
		if r.err == nil && r.value != 42 {
			t.Fatal("published incomplete result")
		}
		if r.err != nil && !errors.Is(r.err, context.Canceled) && !errors.Is(r.err, ErrStopped) {
			t.Fatalf("unexpected result: %v", r.err)
		}
		if err := n.reply(r, pid, 43, nil, false); err == nil {
			t.Fatal("late reply won twice")
		}
	}
}

func TestActorRequestFirstAdmissionRacesStop(t *testing.T) {
	n := networkNode(t, "server")
	for range 100 {
		pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }))
		p, _, _ := n.resolve(pid)
		state := &incomingRequestState{scope: requestScope{parent: n.network.ctx, deadline: time.Now().Add(time.Hour)}}
		start := make(chan struct{})
		var r *request
		var err error
		var group sync.WaitGroup
		group.Go(func() {
			<-start
			n.network.mu.Lock()
			r, err = n.network.registerActorRequest(&state.request, p, &state.scope, requestKey{Caller: "peer", ID: 1}, nil)
			n.network.mu.Unlock()
		})
		group.Go(func() { <-start; p.requestStop(ErrStopped, true) })
		close(start)
		group.Wait()
		signal(t, p.done)
		if err != nil && !errors.Is(err, ErrStopped) {
			t.Fatal(err)
		}
		if r != nil {
			signal(t, r.waitDone())
			if err := n.reply(r, pid, 1, nil, false); !errors.Is(err, ErrReplyExpired) {
				t.Fatalf("late reply: %v", err)
			}
		}
		if n.Stats().PendingCalls != 0 {
			t.Fatal("first registration escaped Stop")
		}
		if _, err := n.network.registerActorRequest(new(request), p, newRequestScope(t.Context(), time.Now().Add(time.Hour)), requestKey{}, nil); !errors.Is(err, ErrStopped) {
			t.Fatalf("closed owner admitted another request: %v", err)
		}
	}
}

func TestActorRequestDeferredWaitSurvivesReplaceAndHandoff(t *testing.T) {
	testActorRequestDeferredWaitSurvivesReplaceAndHandoff(t, false)
}

func TestDefaultLocalPIDDeferredWaitSurvivesReplaceAndHandoff(t *testing.T) {
	testActorRequestDeferredWaitSurvivesReplaceAndHandoff(t, true)
}

func testActorRequestDeferredWaitSurvivesReplaceAndHandoff(t *testing.T, local bool) {
	for _, move := range []bool{false, true} {
		name := "replace"
		if move {
			name = "handoff"
		}
		t.Run(name, func(t *testing.T) {
			d := NewMemoryDirectory()
			server := globalNetworkNode(t, "server", d)
			caller := networkNode(t, "caller")
			connectNodes(t, caller, server)
			server.EnableDiagnostics()
			if move {
				target := globalNetworkNode(t, "target", d, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				connectNodes(t, server, target)
			}
			tickets := make(chan DeferredReply, 1)
			snapshot := make(chan struct{})
			pid := spawn(t, server, migratingCounter(7, func(a *testBehavior, message any) error {
				if cmd, ok := message.(remoteCommand); ok && cmd.Kind == "defer" {
					tickets <- a.Defer()
				}
				if _, ok := message.(Snapshotting); ok {
					close(snapshot)
				}
				return nil
			}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
			called := make(chan error, 1)
			callNode, callContext := caller, t.Context()
			if local {
				callNode, callContext = server, context.Background()
			}
			go func() {
				value, err := callNode.Call[int64](callContext, pid, remoteCommand{Kind: "defer"})
				if err == nil && value != 42 {
					err = errors.New("wrong deferred result")
				}
				called <- err
			}()
			ticket := <-tickets
			if ticket.request.kind != requestActorPID {
				t.Fatal("incoming PID retained node ownership")
			}
			server.mu.Lock()
			entries := len(server.pending)
			server.mu.Unlock()
			if entries != 0 {
				t.Fatal("incoming request was double-indexed at node level")
			}
			view, err := server.Snapshot(SnapshotOptions{})
			if err != nil || view.CallsTotal != 1 || len(view.Calls) != 1 || !view.Calls[0].Deferred {
				t.Fatalf("owner snapshot: %+v %v", view, err)
			}
			p, _, _ := server.resolve(pid)
			changed := make(chan error, 1)
			go func() {
				if move {
					_, err := server.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target", Version: "2"})
					changed <- err
				} else {
					changed <- server.Replace(t.Context(), pid, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				}
			}()
			eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.change != nil || p.handoff != nil })
			select {
			case <-snapshot:
				t.Fatal("snapshot overtook DeferredReply")
			default:
			}
			if err := ticket.Reply(int64(42)); err != nil {
				t.Fatal(err)
			}
			if err := <-called; err != nil {
				t.Fatal(err)
			}
			if err := <-changed; err != nil {
				t.Fatal(err)
			}
			if err := ticket.Reply(int64(43)); !errors.Is(err, ErrAlreadyReplied) {
				t.Fatalf("retired owner changed completion: %v", err)
			}
			value, err := server.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"})
			if err != nil || value != 7 {
				t.Fatalf("replacement state: %d %v", value, err)
			}
			eventually(t, func() bool { return server.Stats().PendingCalls == 0 && server.NetworkStats().IncomingCalls == 0 })
		})
	}
}

func TestActorRequestSharesCapacityWithLocalCall(t *testing.T) {
	server, err := StartNode("server@127.0.0.1:0", NodeOptions{MaxPendingCalls: 2, Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"count", 1, int64(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop(context.Background()) })
	caller := networkNode(t, "caller")
	connectNodes(t, caller, server)
	tickets := make(chan DeferredReply, 2)
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			tickets <- a.Defer()
		}
		return nil
	}))
	results := make(chan error, 2)
	go func() { _, err := server.Call[int64](t.Context(), pid, int64(1)); results <- err }()
	first := <-tickets
	go func() { _, err := caller.Call[int64](t.Context(), pid, int64(2)); results <- err }()
	second := <-tickets
	if first.request.kind == requestActorPID || second.request.kind != requestActorPID || server.Stats().PendingCalls != 2 {
		t.Fatal("mixed request ownership/quota")
	}
	_, err = caller.Call[int64](t.Context(), pid, int64(3))
	remoteError(t, err, ErrCapacity, Rejected)
	if err := first.Reply(int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := second.Reply(int64(2)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return server.Stats().PendingCalls == 0 && server.NetworkStats().IncomingCalls == 0 })
}
