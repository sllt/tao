package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
		state := &localCallState{scope: requestScope{parent: context.Background(), deadline: time.Now().Add(time.Minute)}}
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
		held = append(held, <-tickets)
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
