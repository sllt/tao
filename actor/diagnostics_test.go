package actor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type inspectedCounter struct {
	Actor
	count            int
	entered, release chan struct{}
	inspections      *atomic.Int32
}

func (a *inspectedCounter) Receive(message any) error {
	switch message := message.(type) {
	case int:
		a.count += message
	case string:
		if message == "block" {
			close(a.entered)
			<-a.release
		} else if message == "get" {
			return a.Reply(a.count)
		}
	}
	return nil
}
func (a *inspectedCounter) Inspect(context.Context) (any, error) {
	if a.inspections != nil {
		a.inspections.Add(1)
	}
	return map[string]any{"count": a.count, "identity": a.Name()}, nil
}

func TestDiagnosticsAreOptionalAndSnapshotDoesNotReadBehavior(t *testing.T) {
	n := testNode(t, NodeOptions{})
	pid := spawn(t, n, func() Behavior { return &inspectedCounter{} }, WithName("counter"))
	before, err := n.ActorSnapshot(pid)
	if err != nil || before.Metrics != nil {
		t.Fatalf("diagnostics were enabled by default: %+v %v", before, err)
	}
	n.EnableDiagnostics()
	for range 100 {
		if err := n.Send(pid, 1); err != nil {
			t.Fatal(err)
		}
	}
	if value, err := n.Call[int](t.Context(), pid, "get"); err != nil || value != 100 {
		t.Fatalf("counter: %d %v", value, err)
	}
	data, err := n.Inspect(t.Context(), pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Send(pid, 1); err != nil {
		t.Fatal(err)
	}
	var state struct{ Count int }
	if err := json.Unmarshal(data, &state); err != nil || state.Count != 100 {
		t.Fatalf("inspection shared mutable state: %s %v", data, err)
	}
	after, err := n.ActorSnapshot(pid)
	if err != nil || after.Metrics == nil || after.Metrics.Handled < 100 || after.Metrics.MailboxHighWater == 0 {
		t.Fatalf("metrics: %+v %v", after, err)
	}
	if parsed, err := ParsePID(after.PID); err != nil || parsed != pid {
		t.Fatalf("PID round trip: %+v %v", parsed, err)
	}
}

func TestInspectWaitsForReceiveAndCanceledQueueSlotIsReleased(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.EnableDiagnostics()
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	var inspections atomic.Int32
	pid := spawn(t, n, func() Behavior {
		return &inspectedCounter{entered: entered, release: release, inspections: &inspections}
	}, WithName("blocked"))
	if err := n.Send(pid, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if _, err := n.Inspect(ctx, pid); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inspection did not time out: %v", err)
	}
	if inspections.Load() != 0 {
		t.Fatal("Inspector ran concurrently with Receive")
	}
	info, err := n.ActorSnapshot(pid)
	if err != nil || info.State != "running" || info.Metrics.CurrentMessage != "string" || info.SystemQueue != 0 {
		t.Fatalf("blocked actor metadata: %+v %v", info, err)
	}
	resume()
	if _, err := n.Inspect(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotsStayRaceFreeDuringLifecycleChanges(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.EnableDiagnostics()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			snapshot, err := n.Snapshot(SnapshotOptions{Limit: 7})
			if err != nil || len(snapshot.Actors) > 7 {
				t.Errorf("bounded snapshot: %v", err)
			}
		}
	})
	for i := range 30 {
		pid := spawn(t, n, func() Behavior { return &inspectedCounter{} }, WithName("temporary"))
		if err := n.Send(pid, i); err != nil {
			t.Fatal(err)
		}
		if err := n.StopActor(t.Context(), pid); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestSnapshotUsesPhysicalParentAndShowsCallWait(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.EnableDiagnostics()
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	target := spawn(t, n, func() Behavior { return &inspectedCounter{entered: entered, release: release} }, WithName("target"))
	if err := n.Send(target, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	var child PID
	parent := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(Started); ok {
			var err error
			child, err = a.Spawn(factory(func(a *testBehavior, msg any) error {
				if _, ok := msg.(int); ok {
					_, _ = a.Call(a.Context(), target, "get")
				}
				return nil
			}), WithName("caller"))
			return err
		}
		return nil
	}), WithName("parent"))
	if err := n.Send(child, 1); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return n.Stats().PendingCalls == 1 })
	snapshot, err := n.Snapshot(SnapshotOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Calls) != 1 || snapshot.Calls[0].Caller != child.String() || snapshot.Calls[0].Owner != target.String() {
		t.Fatalf("missing call edge: %+v", snapshot.Calls)
	}
	info, err := n.ActorSnapshot(child)
	if err != nil || info.Parent != parent.String() {
		t.Fatalf("parent relation: %+v %v", info, err)
	}
	resume()
}

func TestInspectorUnsupportedAndCycleCandidates(t *testing.T) {
	n := testNode(t, NodeOptions{})
	pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }))
	if _, err := n.Inspect(t.Context(), pid); !errors.Is(err, ErrInspectUnsupported) {
		t.Fatal(err)
	}
	cycles := callCycles([]CallSnapshot{{Caller: "a", Owner: "b"}, {Caller: "b", Owner: "a", Deferred: true}, {Caller: "c", Owner: "d"}})
	if len(cycles) != 1 || len(cycles[0]) != 2 {
		t.Fatalf("cycle detection: %v", cycles)
	}
}

func TestDiagnosticsReportHandoffIdentityAndParent(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	target := globalNetworkNode(t, "target", d, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, nil)})
	source.EnableDiagnostics()
	target.EnableDiagnostics()
	connectNodes(t, source, target)
	var child PID
	parent := spawn(t, source, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(Started); ok {
			var err error
			child, err = a.Spawn(migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
			return err
		}
		return nil
	}), WithName("group"))
	before, err := source.ActorSnapshot(child)
	if err != nil || before.Parent != parent.String() {
		t.Fatalf("source parent: %+v %v", before, err)
	}
	moved, err := source.Handoff(t.Context(), child, HandoffOptions{TargetNode: "target"})
	if err != nil {
		t.Fatal(err)
	}
	after, err := target.ActorSnapshot(moved.Target.PID())
	if err != nil || after.PID == before.PID || after.Parent != "" || after.Path != "group/counter" || after.Metrics == nil {
		t.Fatalf("handoff identity/guardian: %+v %v", after, err)
	}
	if _, err := source.ActorSnapshot(child); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("old identity remained visible: %v", err)
	}
}

func TestSnapshotFindsLiveSynchronousCallCycle(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.EnableDiagnostics()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var a, b PID
	a = spawn(t, n, factory(func(actor *testBehavior, message any) error {
		if message == "start" {
			_, _ = actor.Call(ctx, b, "wait")
		}
		return nil
	}), WithName("a"))
	b = spawn(t, n, factory(func(actor *testBehavior, message any) error {
		if message == "wait" {
			_, _ = actor.Call(ctx, a, "finish")
		}
		return nil
	}), WithName("b"))
	if err := n.Send(a, "start"); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return n.Stats().PendingCalls == 2 })
	snapshot, err := n.Snapshot(SnapshotOptions{})
	if err != nil || len(snapshot.Cycles) != 1 || len(snapshot.Cycles[0]) != 2 {
		t.Fatalf("live cycle: %+v %v", snapshot.Cycles, err)
	}
	cancel()
	eventually(t, func() bool { return n.Stats().PendingCalls == 0 })
}
