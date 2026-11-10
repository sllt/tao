package actor

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/sllt/tao/taonet"
)

func pidRequest(t *testing.T, n *Node) *outgoingCall {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	out, err := n.network.registerPIDRequest(new(outgoingRequestState), ctx, cancel, PID{Node: "peer", BootID: [16]byte{1}, ID: 1}, PID{})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPIDRequestSharesQuotaIDsAndSnapshotWithLocalRequests(t *testing.T) {
	n, err := StartNode("", NodeOptions{Name: "caller", MaxPendingCalls: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	n.EnableDiagnostics()
	out := pidRequest(t, n)
	defer n.expireRequest(out.request, context.Canceled)
	local, err := n.network.coreRequest(t.Context(), PID{Node: n.Name(), BootID: n.BootID(), ID: 99})
	if err != nil {
		t.Fatal(err)
	}
	defer n.expireRequest(local, context.Canceled)
	n.mu.Lock()
	localEntries := len(n.pending)
	n.mu.Unlock()
	if localEntries != 1 || n.Stats().PendingCalls != 2 || local.id == out.request.id {
		t.Fatal("direct PID was double-indexed or escaped shared quota/IDs")
	}
	ctx, cancel := context.WithCancel(t.Context())
	if rejected, err := n.network.registerPIDRequest(new(outgoingRequestState), ctx, cancel, out.request.owner, PID{}); !errors.Is(err, ErrCapacity) || rejected != nil || ctx.Err() != context.Canceled {
		t.Fatalf("remote capacity rejection: %v %v %v", rejected, err, ctx.Err())
	}
	if rejected, err := n.network.coreRequest(t.Context(), local.owner); !errors.Is(err, ErrCapacity) || rejected != nil {
		t.Fatalf("local capacity rejection: %v %v", rejected, err)
	}
	if n.nextRequest.Load() != 2 {
		t.Fatal("capacity rejection consumed an ID")
	}
	snapshot, err := n.Snapshot(SnapshotOptions{})
	if err != nil || snapshot.CallsTotal != 2 || len(snapshot.Calls) != 2 {
		t.Fatalf("snapshot lost a registry: %+v %v", snapshot, err)
	}
	if err := n.reply(out.request, out.request.owner, 42, nil, false); err != nil {
		t.Fatal(err)
	}
	signal(t, out.request.callerDone)
	n.recycleCallSignal(out.request.callerDone)
	if n.Stats().PendingCalls != 1 || n.NetworkStats().OutgoingCalls != 0 {
		t.Fatal("remote completion did not retire its index/quota")
	}
	next := pidRequest(t, n)
	if next.request.id != 3 {
		t.Fatal("shared ID sequence changed after rejected admission")
	}
	n.expireRequest(next.request, context.Canceled)
	n.expireRequest(local, context.Canceled)
	if n.Stats().PendingCalls != 0 {
		t.Fatal("completion leaked quota")
	}
}

func TestPIDRequestConcurrentMixedCapacityAndIDExhaustion(t *testing.T) {
	for _, exhausted := range []bool{false, true} {
		n, err := StartNode("", NodeOptions{Name: "caller", MaxPendingCalls: 7})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		if exhausted {
			n.nextRequest.Store(math.MaxUint64 - 1)
		}
		admitted := make(chan *request, 64)
		var group sync.WaitGroup
		for i := range 64 {
			group.Go(func() {
				var r *request
				var err error
				if i%2 == 0 {
					ctx, cancel := context.WithCancel(t.Context())
					out, failure := n.network.registerPIDRequest(new(outgoingRequestState), ctx, cancel, PID{Node: "peer", ID: 1}, PID{})
					err = failure
					if out != nil {
						r = out.request
					}
				} else {
					r, err = n.network.coreRequest(t.Context(), PID{Node: n.Name(), ID: 1})
				}
				if err == nil {
					admitted <- r
				} else if !errors.Is(err, ErrCapacity) {
					t.Error(err)
				}
			})
		}
		group.Wait()
		close(admitted)
		want := 7
		if exhausted {
			want = 1
		}
		if n.Stats().PendingCalls != want || len(admitted) != want {
			t.Fatalf("shared admission limit: %d / %d", n.Stats().PendingCalls, len(admitted))
		}
		ids := make(map[uint64]bool)
		for r := range admitted {
			if r.id == 0 || ids[r.id] {
				t.Fatal("ID wrapped or collided")
			}
			ids[r.id] = true
			n.expireRequest(r, context.Canceled)
		}
		if n.Stats().PendingCalls != 0 {
			t.Fatal("rejected/exhausted admission leaked quota")
		}
		if exhausted && n.nextRequest.Load() != math.MaxUint64 {
			t.Fatal("ID exhaustion wrapped")
		}
	}
}

func TestPIDRequestCompletionRacesAndPreservesCommittedOutcome(t *testing.T) {
	n := networkNode(t, "caller")
	for range 100 {
		ctx, cancel := context.WithCancel(t.Context())
		out, err := n.network.registerPIDRequest(new(outgoingRequestState), ctx, cancel, PID{Node: "peer", ID: 1}, PID{})
		if err != nil {
			t.Fatal(err)
		}
		r := out.request
		broadcast := r.waitDone()
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Go(func() { <-start; _ = n.reply(r, r.owner, 42, nil, false) })
		group.Go(func() { <-start; cancel(); _, _ = n.network.cancelCall(ctx, out, requestKey{}) })
		group.Go(func() { <-start; n.expireRequest(r, ErrNodeUnreachable) })
		close(start)
		group.Wait()
		signal(t, broadcast)
		signal(t, r.callerDone)
		value, failure := n.network.cancelCall(ctx, out, requestKey{})
		if value != r.value || failure != r.err {
			t.Fatal("late cancellation rewrote the winner")
		}
		if n.Stats().PendingCalls != 0 || n.NetworkStats().OutgoingCalls != 0 {
			t.Fatal("raced completion leaked/double-freed quota")
		}
		n.recycleCallSignal(r.callerDone)
	}
	for _, issued := range []bool{false, true} {
		out := pidRequest(t, n)
		n.network.mu.Lock()
		out.issued = issued
		n.network.mu.Unlock()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := n.network.cancelCall(ctx, out, requestKey{})
		var remote *RemoteError
		want := NotSent
		if issued {
			want = Unknown
		}
		if !errors.As(err, &remote) || remote.Outcome != want || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel outcome: %v", err)
		}
	}
}

func TestPIDRequestLateCleanupCannotWakeReusedCallerSignal(t *testing.T) {
	n := networkNode(t, "caller")
	old := pidRequest(t, n)
	cleanup, resume, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	cancel := old.request.cancel
	old.request.cancel = func() { close(cleanup); <-resume; cancel() }
	go func() { _ = n.reply(old.request, old.request.owner, 42, nil, false); close(finished) }()
	signal(t, old.request.callerDone)
	signal(t, cleanup)
	n.recycleCallSignal(old.request.callerDone)
	next := pidRequest(t, n)
	if old.request == next.request || old.request.callerDone != next.request.callerDone {
		t.Fatal("expected fresh state and a reused signal")
	}
	if err := n.reply(old.request, old.request.owner, "late", nil, false); !errors.Is(err, ErrAlreadyReplied) {
		t.Fatal(err)
	}
	if n.expireRequest(old.request, context.DeadlineExceeded) {
		t.Fatal("old completion won twice")
	}
	select {
	case <-next.request.callerDone:
		t.Fatal("old completion woke a new caller")
	default:
	}
	unblock()
	signal(t, finished)
	if err := n.reply(next.request, next.request.owner, 43, nil, false); err != nil {
		t.Fatal(err)
	}
	signal(t, next.request.callerDone)
	if next.request.value != 43 || n.Stats().PendingCalls != 0 {
		t.Fatal("new result/quota corrupted")
	}
}

func TestPIDRequestSnapshotRaceAndFailedAdmission(t *testing.T) {
	n := networkNode(t, "caller")
	n.EnableDiagnostics()
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := n.Snapshot(SnapshotOptions{}); err != nil {
				t.Error(err)
			}
		}
	})
	for range 100 {
		out := pidRequest(t, n)
		_ = n.reply(out.request, out.request.owner, 42, nil, false)
		signal(t, out.request.callerDone)
		n.recycleCallSignal(out.request.callerDone)
	}
	close(stop)
	sampler.Wait()
	_, err := n.Call[any](t.Context(), PID{Node: "peer", ID: 1}, struct{ Unsupported chan int }{})
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Outcome != NotSent {
		t.Fatalf("encoding failure: %v", err)
	}
	if n.Stats().PendingCalls != 0 || n.NetworkStats().OutgoingCalls != 0 {
		t.Fatal("failed encoding leaked registration")
	}
	if err := n.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	_, err = n.Call[any](context.Background(), PID{Node: "peer", ID: 1}, int64(1))
	if !errors.Is(err, ErrClosed) || n.Stats().PendingCalls != 0 {
		t.Fatalf("closed admission: %v", err)
	}
}

func TestPIDRequestNodeCloseCompletesCallerAndReleasesInput(t *testing.T) {
	caller, server := networkNode(t, "caller"), networkNode(t, "server")
	connectNodes(t, caller, server)
	entered := make(chan struct{})
	pid := spawn(t, server, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(int64); ok {
			a.Defer()
			close(entered)
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() { _, err := caller.Call[int64](context.Background(), pid, int64(1)); result <- err }()
	signal(t, entered)
	caller.network.mu.Lock()
	for _, out := range caller.network.outgoing {
		if out.request.kind != requestRemotePID {
			t.Error("public PID Call used the legacy registry")
		}
	}
	caller.network.mu.Unlock()
	if err := caller.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrNodeUnreachable) {
		t.Fatalf("closed Call: %v", err)
	}
	if caller.Stats().PendingCalls != 0 || caller.NetworkStats().OutgoingCalls != 0 {
		t.Fatal("close retained remote requests")
	}
	if len(caller.network.endpoint.Sessions(taonet.PeerID("server"))) != 0 {
		t.Fatal("close retained transport sessions")
	}
}
