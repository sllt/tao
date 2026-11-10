package actor

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type replacementState struct{ Values map[string]int64 }
type replacementCommand struct {
	kind   string
	amount int64
}
type replacementBehavior struct {
	Actor
	state   replacementState
	version int64
	hook    func(*replacementBehavior, any) error
}

func (a *replacementBehavior) Receive(message any) error {
	if a.hook != nil {
		if err := a.hook(a, message); err != nil {
			return err
		}
	}
	switch message := message.(type) {
	case Started:
		a.state = replacementState{Values: map[string]int64{"count": a.version}}
	case Snapshotting:
		return a.Reply(a.state)
	case Upgraded:
		return message.Restore(&a.state)
	case replacementCommand:
		switch message.kind {
		case "add":
			a.state.Values["count"] += message.amount
		case "get":
			return a.Reply(a.state.Values["count"])
		case "crash":
			return errors.New("restart replacement")
		}
	}
	return nil
}
func replacementFactory(version int64, hook func(*replacementBehavior, any) error) Factory {
	return func() Behavior { return &replacementBehavior{version: version, hook: hook} }
}
func replacementKind(version string, factory Factory) KindSpec {
	return KindSpec{Name: "counter", Version: version, Factory: factory}
}

func TestReplaceTransfersOnlyRefMailboxAndUpdatesFactory(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	old := spawn(t, n, replacementFactory(1, func(_ *replacementBehavior, msg any) error {
		if command, ok := msg.(replacementCommand); ok && command.kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithName("counter"), WithKind("counter", "1"))
	if err := n.Send(old, replacementCommand{kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	result := make(chan error, 1)
	go func() {
		result <- n.Replace(t.Context(), Ref{Path: "counter"}, replacementKind("2", replacementFactory(200, nil)))
	}()
	n.mu.Lock()
	p := n.processes[old.ID]
	n.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.change != nil })
	if err := n.Send(Ref{Path: "counter"}, replacementCommand{kind: "add", amount: 2}); err != nil {
		t.Fatal(err)
	}
	if err := n.Send(old, replacementCommand{kind: "add", amount: 1000}); err != nil {
		t.Fatal(err)
	}
	pidCall := make(chan error, 1)
	go func() { _, err := n.Call[int64](t.Context(), old, replacementCommand{kind: "get"}); pidCall <- err }()
	eventually(t, func() bool { return n.Stats().PendingCalls == 1 })
	refCall := make(chan int64, 1)
	go func() {
		value, err := n.Call[int64](t.Context(), Ref{Path: "counter"}, replacementCommand{kind: "get"})
		if err != nil {
			t.Error(err)
		}
		refCall <- value
	}()
	eventually(t, func() bool { return n.Stats().PendingCalls == 2 })
	resume()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := <-pidCall; !errors.Is(err, ErrReplaced) {
		t.Fatalf("PID call transferred: %v", err)
	}
	if got := <-refCall; got != 3 {
		t.Fatalf("mailbox ordering/count=%d", got)
	}
	current, _, err := n.resolve(Ref{Path: "counter"})
	if err != nil || current.pid == old || current.opts.version != "2" {
		t.Fatalf("replacement identity: %v %v", current, err)
	}
	if err := n.Send(current.pid, replacementCommand{kind: "crash"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { p, _, err := n.resolve(Ref{Path: "counter"}); return err == nil && p.pid != current.pid })
	if got, err := n.Call[int64](t.Context(), Ref{Path: "counter"}, replacementCommand{kind: "get"}); err != nil || got != 200 {
		t.Fatalf("restart retained old Factory: %d %v", got, err)
	}
}

func TestReplacePreparationFailureLeavesIndependentSourceState(t *testing.T) {
	n := testNode(t, NodeOptions{})
	old := spawn(t, n, replacementFactory(7, nil), WithName("counter"))
	failure := errors.New("incompatible snapshot")
	kind := replacementKind("2", replacementFactory(200, func(a *replacementBehavior, msg any) error {
		if upgraded, ok := msg.(Upgraded); ok {
			if err := upgraded.Restore(&a.state); err != nil {
				return err
			}
			a.state.Values["count"] = 999
			return failure
		}
		return nil
	}))
	err := n.Replace(t.Context(), old, kind)
	var replacement *ReplaceError
	if !errors.Is(err, failure) || !errors.As(err, &replacement) || replacement.Committed {
		t.Fatalf("rollback result: %v", err)
	}
	if got, err := n.Call[int64](t.Context(), old, replacementCommand{kind: "get"}); err != nil || got != 7 {
		t.Fatalf("source mutated by failed preparation: %d %v", got, err)
	}
}

func TestReplaceWaitsForDeferredReplyAndActivatesBeforeMailbox(t *testing.T) {
	n := testNode(t, NodeOptions{})
	deferred := make(chan DeferredReply, 1)
	snapshot := make(chan struct{})
	old := spawn(t, n, replacementFactory(4, func(a *replacementBehavior, msg any) error {
		if command, ok := msg.(replacementCommand); ok && command.kind == "defer" {
			deferred <- a.Defer()
		}
		if _, ok := msg.(Snapshotting); ok {
			close(snapshot)
		}
		return nil
	}), WithName("counter"))
	call := make(chan error, 1)
	// Exercise Replace's broadcast waiter alongside the default local caller's
	// independently cached result signal.
	go func() {
		_, err := n.Call[int64](context.Background(), old, replacementCommand{kind: "defer"})
		call <- err
	}()
	reply := <-deferred
	result := make(chan error, 1)
	go func() {
		result <- n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, func(a *replacementBehavior, msg any) error {
			if _, ok := msg.(Activated); ok {
				a.state.Values["count"] += 100
			}
			return nil
		})))
	}()
	n.mu.Lock()
	p := n.processes[old.ID]
	n.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.change != nil })
	select {
	case <-snapshot:
		t.Fatal("snapshot ran before deferred completion")
	default:
	}
	if err := reply.Reply(int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := <-call; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got, err := n.Call[int64](t.Context(), Ref{Path: "counter"}, replacementCommand{kind: "get"}); err != nil || got != 104 {
		t.Fatalf("activation order: %d %v", got, err)
	}
}

func TestReplaceActivatedFailureNeverRestoresOldOwner(t *testing.T) {
	d := NewMemoryDirectory()
	n := testNode(t, NodeOptions{Directory: d})
	old := spawn(t, n, replacementFactory(1, nil), WithName("counter"), WithKind("counter", "1"))
	failure := errors.New("activation failed")
	err := n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, func(_ *replacementBehavior, msg any) error {
		if _, ok := msg.(Activated); ok {
			return failure
		}
		return nil
	})))
	var replacement *ReplaceError
	if !errors.Is(err, failure) || !errors.As(err, &replacement) || !replacement.Committed {
		t.Fatalf("post-commit result: %v", err)
	}
	eventually(t, func() bool {
		p, _, err := n.resolve(Ref{Node: n.Name(), Path: "counter"})
		return err == nil && p.pid != old && p.pid != replacement.PID
	})
	if got, err := n.Call[int64](t.Context(), Ref{Path: "counter"}, replacementCommand{kind: "get"}); err != nil || got != 200 {
		t.Fatalf("new owner recovery: %d %v", got, err)
	}
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil || route.Version != "2" || route.PID() == old {
		t.Fatalf("old owner restored: %+v %v", route, err)
	}
}

type uncertainSwapDirectory struct {
	Directory
	lose             atomic.Bool
	readsFail        atomic.Bool
	commit           bool
	mu               sync.Mutex
	expected, wanted RouteRecord
}

func (d *uncertainSwapDirectory) Lookup(ctx context.Context, path string) (RouteRecord, error) {
	if d.readsFail.Load() {
		return RouteRecord{}, io.ErrUnexpectedEOF
	}
	return d.Directory.Lookup(ctx, path)
}
func (d *uncertainSwapDirectory) SwapInstance(ctx context.Context, expected, wanted RouteRecord) (RouteRecord, error) {
	if d.lose.CompareAndSwap(true, false) {
		d.mu.Lock()
		d.expected, d.wanted = expected, wanted
		d.mu.Unlock()
		if d.commit {
			if _, err := d.Directory.SwapInstance(ctx, expected, wanted); err != nil {
				return RouteRecord{}, err
			}
		}
		d.readsFail.Store(true)
		return RouteRecord{}, &DirectoryError{Operation: "swap", Unknown: true, Cause: io.ErrUnexpectedEOF}
	}
	return d.Directory.SwapInstance(ctx, expected, wanted)
}

func TestReplaceUnknownCommitRequiresResolutionAndFencesDelayedCAS(t *testing.T) {
	for _, committed := range []bool{true, false} {
		t.Run(map[bool]string{true: "committed", false: "delayed"}[committed], func(t *testing.T) {
			d := &uncertainSwapDirectory{Directory: NewMemoryDirectory(), commit: committed}
			d.lose.Store(true)
			n := testNode(t, NodeOptions{Directory: d})
			old := spawn(t, n, replacementFactory(9, nil), WithName("counter"), WithKind("counter", "1"))
			err := n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, nil)))
			var replacement *ReplaceError
			if !errors.Is(err, ErrOutcomeUnknown) || !errors.As(err, &replacement) || !replacement.Unknown {
				t.Fatalf("unknown: %v", err)
			}
			if err := n.Send(old, replacementCommand{kind: "add", amount: 1}); !errors.Is(err, ErrMigrating) {
				t.Fatalf("source unfrozen while unknown: %v", err)
			}
			d.readsFail.Store(false)
			err = n.ResolveReplace(t.Context(), old)
			if committed {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				if !errors.Is(err, ErrMigrating) {
					t.Fatalf("fenced rollback: %v", err)
				}
			}
			route, err := d.Lookup(t.Context(), "counter")
			if err != nil {
				t.Fatal(err)
			}
			if (route.PID() != old) != committed || route.Epoch != 2 {
				t.Fatalf("resolved route: %+v committed=%t", route, committed)
			}
			if !committed {
				d.mu.Lock()
				expected, wanted := d.expected, d.wanted
				d.mu.Unlock()
				if _, err := d.Directory.SwapInstance(t.Context(), expected, wanted); !errors.Is(err, ErrDirectoryConflict) {
					t.Fatalf("late CAS bypassed fence: %v", err)
				}
			}
			if got, err := n.Call[int64](t.Context(), Ref{Path: "counter"}, replacementCommand{kind: "get"}); err != nil || got != 9 {
				t.Fatalf("resolved state: %d %v", got, err)
			}
		})
	}
}

func TestReplacePreparationBlocksExternalActorActions(t *testing.T) {
	n := testNode(t, NodeOptions{})
	old := spawn(t, n, replacementFactory(1, nil), WithName("counter"))
	var checks atomic.Int32
	err := n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, func(a *replacementBehavior, msg any) error {
		if started, ok := msg.(Started); ok && started.Restoring {
			if err := a.Send(old, replacementCommand{}); !errors.Is(err, ErrRestoring) {
				return errors.New("restoring Send was allowed")
			}
			if _, err := a.Spawn(replacementFactory(0, nil)); !errors.Is(err, ErrRestoring) {
				return errors.New("restoring Spawn was allowed")
			}
			checks.Add(1)
		}
		return nil
	})))
	if err != nil || checks.Load() != 1 {
		t.Fatalf("restoring restrictions: %v checks=%d", err, checks.Load())
	}
}

func TestReplaceSnapshotTimeoutDoesNotResumeBlockedSource(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	old := spawn(t, n, replacementFactory(1, func(_ *replacementBehavior, msg any) error {
		if _, ok := msg.(Snapshotting); ok {
			close(entered)
			<-release
		}
		return nil
	}), WithName("counter"))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	err := n.Replace(ctx, old, replacementKind("2", replacementFactory(200, nil)))
	signal(t, entered)
	if !errors.Is(err, ErrUnresponsive) {
		t.Fatalf("blocked snapshot: %v", err)
	}
	if err := n.Send(Ref{Path: "counter"}, replacementCommand{}); !errors.Is(err, ErrUnresponsive) {
		t.Fatalf("blocked actor still exposed: %v", err)
	}
	resume()
	eventually(t, func() bool { return n.Stats().Actors == 0 })
}

func TestTransferredLocalCallStillCancelsBeforeActivation(t *testing.T) {
	n := testNode(t, NodeOptions{})
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	old := spawn(t, n, replacementFactory(1, func(_ *replacementBehavior, msg any) error {
		if command, ok := msg.(replacementCommand); ok && command.kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithName("counter"))
	if err := n.Send(old, replacementCommand{kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	active, finishActivation := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(finishActivation) })
	defer unblock()
	replaced := make(chan error, 1)
	go func() {
		replaced <- n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, func(_ *replacementBehavior, msg any) error {
			if _, ok := msg.(Activated); ok {
				close(active)
				<-finishActivation
			}
			return nil
		})))
	}()
	n.mu.Lock()
	p := n.processes[old.ID]
	n.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.change != nil })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	called := make(chan error, 1)
	go func() {
		_, err := n.Call[int64](ctx, Ref{Path: "counter"}, replacementCommand{kind: "get"})
		called <- err
	}()
	eventually(t, func() bool { return n.Stats().PendingCalls == 1 })
	resume()
	signal(t, active)
	cancel()
	select {
	case err := <-called:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("transferred Call cancellation waited on old PID ownership")
	}
	unblock()
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	if n.Stats().PendingCalls != 0 {
		t.Fatal("canceled transferred request leaked")
	}
}

func TestReplaceRejectsNonLeafAndOversizedSnapshot(t *testing.T) {
	n := testNode(t, NodeOptions{MaxSnapshotBytes: 8})
	parent := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(Started); ok {
			_, err := a.Spawn(replacementFactory(1, nil))
			return err
		}
		return nil
	}), WithName("parent"))
	if err := n.Replace(t.Context(), parent, replacementKind("2", replacementFactory(2, nil))); !errors.Is(err, ErrHandoffUnsupported) {
		t.Fatalf("non-leaf replace: %v", err)
	}
	leaf := spawn(t, n, replacementFactory(1, nil), WithName("leaf"))
	if err := n.Replace(t.Context(), leaf, replacementKind("2", replacementFactory(2, nil))); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("snapshot budget: %v", err)
	}
	if value, err := n.Call[int64](t.Context(), leaf, replacementCommand{kind: "get"}); err != nil || value != 1 {
		t.Fatalf("failed snapshot changed source: %d %v", value, err)
	}
}

func TestReplaceBlockedPreparationRollsBackWithoutSharingSource(t *testing.T) {
	n := testNode(t, NodeOptions{})
	old := spawn(t, n, replacementFactory(5, nil), WithName("counter"))
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- n.Replace(ctx, old, replacementKind("2", replacementFactory(200, func(_ *replacementBehavior, msg any) error {
			if started, ok := msg.(Started); ok && started.Restoring {
				close(entered)
				<-release
			}
			return nil
		})))
	}()
	signal(t, entered)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare timeout: %v", err)
	}
	eventually(t, func() bool { return n.Stats().Frozen == 0 })
	if value, err := n.Call[int64](t.Context(), old, replacementCommand{kind: "get"}); err != nil || value != 5 {
		t.Fatalf("source did not resume: %d %v", value, err)
	}
	eventually(t, func() bool { return n.Stats().Unresponsive == 1 && n.Stats().Preparing == 1 })
	resume()
	eventually(t, func() bool { return n.Stats().Actors == 1 && n.Stats().Preparing == 0 })
}

func TestReplaceOldPIDMonitorTerminates(t *testing.T) {
	n := testNode(t, NodeOptions{})
	old := spawn(t, n, replacementFactory(1, nil), WithName("counter"))
	down := make(chan Down, 1)
	spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message := message.(type) {
		case Started:
			_, err := a.Monitor(old)
			return err
		case Down:
			down <- message
		}
		return nil
	}))
	if err := n.Replace(t.Context(), old, replacementKind("2", replacementFactory(200, nil))); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-down:
		if event.PID != old || !errors.Is(event.Reason, ErrReplaced) {
			t.Fatalf("old PID Down: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("old PID monitor did not terminate")
	}
}
