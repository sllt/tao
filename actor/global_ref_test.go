package actor

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

func globalNetworkNode(t *testing.T, name string, directory Directory, kinds ...KindSpec) *Node {
	t.Helper()
	n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{Kinds: kinds, Directory: directory, Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"command", 1, remoteCommand{}}, {"count", 1, int64(0)}, {"handoff_blob", 1, handoffBlob{}}}})
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

func globalCounter(t *testing.T, n *Node, value int64, calls *atomic.Int32) PID {
	t.Helper()
	return spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if cmd, ok := msg.(remoteCommand); ok {
			if calls != nil {
				calls.Add(1)
			}
			switch cmd.Kind {
			case "add":
				value += cmd.Delta
			case "get":
				return a.Reply(value)
			case "crash":
				return errors.New("restart")
			case "redirect_reply":
				return a.ReplyError(ErrRedirect)
			}
		}
		return nil
	}), WithName("counter"))
}

func TestGlobalRegistrationConflictAndVisibility(t *testing.T) {
	d := NewMemoryDirectory()
	a := testNode(t, NodeOptions{Name: "a", Directory: d})
	b := testNode(t, NodeOptions{Name: "b", Directory: d})
	entered, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := a.Spawn(factory(func(_ *testBehavior, msg any) error {
			if _, ok := msg.(Started); ok {
				close(entered)
				<-release
			}
			return nil
		}), WithName("counter"))
		result <- err
	}()
	signal(t, entered)
	if _, err := d.Lookup(t.Context(), "counter"); !errors.Is(err, ErrDirectoryNotFound) {
		t.Fatalf("published before Started: %v", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if _, err := b.Spawn(factory(func(*testBehavior, any) error { return nil }), WithName("counter")); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("global duplicate: %v", err)
	}
	if err := b.SendConfirmed(t.Context(), Ref{Node: "b", Path: "counter"}, "msg"); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("failed registration remained visible: %v", err)
	}
}

func TestGlobalRefRoutesAndNodeBoundRefStaysBound(t *testing.T) {
	d := NewMemoryDirectory()
	a, b := globalNetworkNode(t, "a", d), globalNetworkNode(t, "b", d)
	connectNodes(t, a, b)
	globalCounter(t, b, 40, nil)
	if err := a.SendConfirmed(t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "add", Delta: 2}); err != nil {
		t.Fatal(err)
	}
	if value, err := a.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"}); err != nil || value != 42 {
		t.Fatalf("global call: %d %v", value, err)
	}
	if _, err := a.Call[int64](t.Context(), Ref{Node: "a", Path: "counter"}, remoteCommand{Kind: "get"}); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("node-bound Ref escaped its node: %v", err)
	}
	if _, err := a.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "redirect_reply"}); err == nil {
		t.Fatal("missing business error")
	} else {
		remoteError(t, err, ErrRedirect, Responded)
	}
}

func TestGlobalRestartChangesPIDAndEpoch(t *testing.T) {
	d := NewMemoryDirectory()
	a, b := globalNetworkNode(t, "a", d), globalNetworkNode(t, "b", d)
	connectNodes(t, a, b)
	oldPID := globalCounter(t, b, 7, nil)
	old, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Send(oldPID, remoteCommand{Kind: "crash"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		route, err := d.Lookup(t.Context(), "counter")
		return err == nil && route.PID() != oldPID
	})
	current, err := d.Lookup(t.Context(), "counter")
	if err != nil || current.Epoch != old.Epoch+1 {
		t.Fatalf("restart route: %+v %v", current, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	value, err := a.network.call(ctx, routedRef{record: old}, remoteCommand{Kind: "get"}, PID{})
	if err != nil || value != int64(7) {
		t.Fatalf("refresh old epoch: %v %v", value, err)
	}
}

func TestGlobalRedirectKeepsCallIDAndCanReturnToCaller(t *testing.T) {
	for _, backToCaller := range []bool{false, true} {
		t.Run(map[bool]string{false: "third_node", true: "caller_node"}[backToCaller], func(t *testing.T) {
			d := NewMemoryDirectory()
			a, b := globalNetworkNode(t, "a", d), globalNetworkNode(t, "b", d)
			connectNodes(t, a, b)
			target := a
			if !backToCaller {
				target = globalNetworkNode(t, "c", d)
				connectNodes(t, a, target)
			}
			oldPID := globalCounter(t, b, 1, nil)
			old, err := d.Lookup(t.Context(), "counter")
			if err != nil {
				t.Fatal(err)
			}
			if err := b.StopActor(t.Context(), oldPID); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			globalCounter(t, target, 42, &calls)
			a.mu.Lock()
			before := a.nextRequest.Load()
			a.mu.Unlock()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			value, err := a.network.call(ctx, routedRef{record: old}, remoteCommand{Kind: "get"}, PID{})
			if err != nil || value != int64(42) || calls.Load() != 1 {
				t.Fatalf("rerouted call: %v %v executions=%d", value, err, calls.Load())
			}
			a.mu.Lock()
			requests := a.nextRequest.Load() - before
			a.mu.Unlock()
			want := uint64(1)
			if backToCaller {
				want++
			} // One local target request bridges the original caller request.
			if requests != want {
				t.Fatalf("recreated outgoing CallID: allocated=%d want=%d", requests, want)
			}
		})
	}
}

type lookupFailureDirectory struct {
	Directory
	unavailable atomic.Bool
}

func (d *lookupFailureDirectory) Lookup(ctx context.Context, path string) (RouteRecord, error) {
	if d.unavailable.Load() {
		return RouteRecord{}, io.ErrUnexpectedEOF
	}
	return d.Directory.Lookup(ctx, path)
}

func TestGlobalDirectoryPartitionDoesNotStealOwner(t *testing.T) {
	d := NewMemoryDirectory()
	partition := &lookupFailureDirectory{Directory: d}
	a, b := globalNetworkNode(t, "a", d), globalNetworkNode(t, "b", partition)
	connectNodes(t, a, b)
	var calls atomic.Int32
	pid := globalCounter(t, b, 1, &calls)
	partition.unavailable.Store(true)
	err := a.SendConfirmed(t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "add"})
	remoteError(t, err, ErrDirectoryUnavailable, Rejected)
	if calls.Load() != 0 {
		t.Fatal("partitioned receiver admitted a global message")
	}
	if _, err := a.Spawn(factory(func(*testBehavior, any) error { return nil }), WithName("counter")); !errors.Is(err, ErrNameTaken) {
		t.Fatalf("stole owner: %v", err)
	}
	if route, err := d.Lookup(t.Context(), "counter"); err != nil || route.PID() != pid {
		t.Fatalf("owner changed: %+v %v", route, err)
	}
	partition.unavailable.Store(false)
}

func TestGlobalUnknownCallIsNotRetried(t *testing.T) {
	d := NewMemoryDirectory()
	a, b := globalNetworkNode(t, "a", d), globalNetworkNode(t, "b", d)
	connectNodes(t, a, b)
	var executions atomic.Int32
	entered := make(chan struct{})
	spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			executions.Add(1)
			close(entered)
			<-actor.Context().Done()
		}
		return nil
	}), WithName("counter"))
	result := make(chan error, 1)
	go func() { _, err := a.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{}); result <- err }()
	signal(t, entered)
	for _, session := range a.network.endpoint.Sessions(taonet.PeerID("b")) {
		if session.Info().Class == taonet.ClassControl {
			_ = session.Close()
		}
	}
	select {
	case err := <-result:
		remoteError(t, err, ErrNodeUnreachable, Unknown)
	case <-time.After(3 * time.Second):
		t.Fatal("call did not resolve")
	}
	if executions.Load() != 1 {
		t.Fatalf("replayed Unknown: %d", executions.Load())
	}
}

type lostClaimDirectory struct{ Directory }

func (d lostClaimDirectory) Claim(ctx context.Context, route RouteRecord) (RouteRecord, error) {
	_, err := d.Directory.Claim(ctx, route)
	if err != nil {
		return RouteRecord{}, err
	}
	return RouteRecord{}, &DirectoryError{Operation: "claim", Unknown: true, Cause: io.ErrUnexpectedEOF}
}
func TestGlobalClaimLostResponseIsResolvedBeforePublish(t *testing.T) {
	d := lostClaimDirectory{NewMemoryDirectory()}
	n := testNode(t, NodeOptions{Directory: d})
	pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }), WithName("counter"))
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil || route.PID() != pid {
		t.Fatalf("resolved claim: %+v %v", route, err)
	}
	if err := n.Send(Ref{Path: "counter"}, "hello"); err != nil {
		t.Fatal(err)
	}
}

type staleRouteDirectory struct {
	Directory
	stale RouteRecord
	reads atomic.Int32
}

func (d *staleRouteDirectory) Lookup(context.Context, string) (RouteRecord, error) {
	d.reads.Add(1)
	return d.stale, nil
}
func TestGlobalRedirectHasFiniteRetryBudget(t *testing.T) {
	d := NewMemoryDirectory()
	b := globalNetworkNode(t, "b", d)
	pid := globalCounter(t, b, 1, nil)
	old, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	if err := b.StopActor(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	stale := &staleRouteDirectory{Directory: d, stale: old}
	a := globalNetworkNode(t, "a", stale)
	connectNodes(t, a, b)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, err = a.network.call(ctx, routedRef{record: old}, remoteCommand{Kind: "get"}, PID{})
	remoteError(t, err, ErrRedirect, Rejected)
	if reads := stale.reads.Load(); reads != 3 {
		t.Fatalf("ownership lookups=%d want one before dialing plus two redirect refreshes", reads)
	}
	if a.nextRequest.Load() != 1 {
		t.Fatalf("CallID changed on redirects: %d", a.nextRequest.Load())
	}
}
