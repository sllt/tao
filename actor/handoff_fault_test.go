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

type faultyMoveDirectory struct {
	Directory
	commit    bool
	lose      atomic.Bool
	readsFail atomic.Bool
	failReads bool
	after     func()
}

func (d *faultyMoveDirectory) CommitMove(ctx context.Context, id MoveID) (MoveRecord, error) {
	if !d.lose.CompareAndSwap(true, false) {
		return d.Directory.CommitMove(ctx, id)
	}
	if d.commit {
		if _, err := d.Directory.CommitMove(ctx, id); err != nil {
			return MoveRecord{}, err
		}
	}
	if d.failReads {
		d.readsFail.Store(true)
	}
	if d.after != nil {
		d.after()
	}
	return MoveRecord{}, &DirectoryError{Operation: "commit_move", Unknown: true, Cause: io.ErrUnexpectedEOF}
}
func (d *faultyMoveDirectory) ReadMove(ctx context.Context, id MoveID) (MoveRecord, error) {
	if d.readsFail.Load() {
		return MoveRecord{}, io.ErrUnexpectedEOF
	}
	return d.Directory.ReadMove(ctx, id)
}

func TestHandoffLostCommitResponseAndExplicitResolution(t *testing.T) {
	for _, scenario := range []struct {
		name              string
		commit, failReads bool
	}{{"committed", true, false}, {"unknown_committed", true, true}, {"delayed_commit", false, false}} {
		t.Run(scenario.name, func(t *testing.T) {
			base := NewMemoryDirectory()
			d := &faultyMoveDirectory{Directory: base, commit: scenario.commit, failReads: scenario.failReads}
			d.lose.Store(true)
			source := globalNetworkNode(t, "source", d)
			target := globalNetworkNode(t, "target", base, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, nil)})
			connectNodes(t, source, target)
			pid := spawn(t, source, migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
			result, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"})
			if scenario.failReads {
				if !result.Unknown || !errors.Is(err, ErrOutcomeUnknown) {
					t.Fatalf("unknown result: %+v %v", result, err)
				}
				if err := source.Send(pid, remoteCommand{Kind: "add", Delta: 1000}); !errors.Is(err, ErrMigrating) {
					t.Fatalf("unknown source resumed: %v", err)
				}
				d.readsFail.Store(false)
				result, err = source.ResolveHandoff(t.Context(), result.MoveID)
			}
			if scenario.commit {
				if err != nil || result.State != MoveCommitted || result.Unknown {
					t.Fatalf("commit readback: %+v %v", result, err)
				}
				if value, err := target.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"}); err != nil || value != 42 {
					t.Fatalf("target state: %d %v", value, err)
				}
			} else {
				if err == nil || result.State != MoveAborted || result.Unknown {
					t.Fatalf("rollback: %+v %v", result, err)
				}
				if _, err := base.CommitMove(t.Context(), result.MoveID); !errors.Is(err, ErrDirectoryConflict) {
					t.Fatalf("late commit escaped Abort fence: %v", err)
				}
				if value, err := source.Call[int64](t.Context(), pid, remoteCommand{Kind: "get"}); err != nil || value != 42 {
					t.Fatalf("source state: %d %v", value, err)
				}
			}
		})
	}
}

func TestHandoffSourceDisconnectAfterCommitActivatesTarget(t *testing.T) {
	base := NewMemoryDirectory()
	d := &faultyMoveDirectory{Directory: base, commit: true, failReads: true}
	d.lose.Store(true)
	source := globalNetworkNode(t, "source", d)
	target := globalNetworkNode(t, "target", base, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, nil)})
	connectNodes(t, source, target)
	d.after = func() { _ = source.network.endpoint.Close() }
	pid := spawn(t, source, migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	result, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"})
	if !result.Unknown || !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("source outcome: %+v %v", result, err)
	}
	eventually(t, func() bool { _, _, err := target.resolve(Ref{Node: "target", Path: "counter"}); return err == nil })
	if value, err := target.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"}); err != nil || value != 42 {
		t.Fatalf("target did not recover committed move: %d %v", value, err)
	}
	d.readsFail.Store(false)
	if err := source.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffPreparationFailureRestoresSource(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	kind := KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, func(_ *testBehavior, msg any) error {
		if _, ok := msg.(Upgraded); ok {
			return errors.New("incompatible snapshot")
		}
		return nil
	})}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	pid := spawn(t, source, migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	result, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target", Version: "2"})
	if err == nil || result.State != MoveAborted {
		t.Fatalf("failed preparation: %+v %v", result, err)
	}
	if value, err := source.Call[int64](t.Context(), pid, remoteCommand{Kind: "get"}); err != nil || value != 42 {
		t.Fatalf("source rollback: %d %v", value, err)
	}
	if target.Stats().Actors != 0 {
		t.Fatal("failed preparation leaked target")
	}
}

func TestTransferredCallCancellationReachesNewOwner(t *testing.T) {
	d := NewMemoryDirectory()
	source, caller := globalNetworkNode(t, "source", d), globalNetworkNode(t, "caller", d)
	active, canceled := make(chan struct{}), make(chan struct{})
	kind := KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, func(a *testBehavior, msg any) error {
		if command, ok := msg.(remoteCommand); ok && command.Kind == "wait" {
			close(active)
			<-a.Context().Done()
			close(canceled)
		}
		return nil
	})}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	connectNodes(t, caller, source)
	entered, release := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(release) })
	defer resume()
	pid := spawn(t, source, migratingCounter(1, func(_ *testBehavior, msg any) error {
		if command, ok := msg.(remoteCommand); ok && command.Kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	if err := source.Send(pid, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	moveDone := make(chan error, 1)
	go func() {
		_, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"})
		moveDone <- err
	}()
	source.mu.Lock()
	p := source.processes[pid.ID]
	source.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.handoff != nil })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	callDone := make(chan error, 1)
	go func() {
		_, err := caller.Call[int64](ctx, Ref{Path: "counter"}, remoteCommand{Kind: "wait"})
		callDone <- err
	}()
	eventually(t, func() bool { return source.Stats().PendingCalls == 1 })
	resume()
	if err := <-moveDone; err != nil {
		t.Fatal(err)
	}
	signal(t, active)
	cancel()
	if err := <-callDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancel: %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("new owner waited for original deadline instead of cancellation")
	}
}

func TestDrainHandoffReportsPartialCommitAndCanResume(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	var reject atomic.Bool
	reject.Store(true)
	kind := KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, func(a *testBehavior, msg any) error {
		if _, ok := msg.(Upgraded); ok && a.Name() == "b" && reject.Load() {
			return errors.New("reject b")
		}
		return nil
	})}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	for _, name := range []string{"a", "b"} {
		spawn(t, source, migratingCounter(42, nil), WithName(name), WithKind("counter", "1"), WithHandoff(true))
	}
	report, err := source.DrainAndHandoff(t.Context(), HandoffOptions{TargetNode: "target"})
	if err == nil || report.Stopped || len(report.Committed) != 1 || report.Committed[0].Path != "a" || len(report.RolledBack) != 1 || report.RolledBack[0].Path != "b" {
		t.Fatalf("partial report: %+v %v", report, err)
	}
	a, err := d.Lookup(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	reject.Store(false)
	report, err = source.DrainAndHandoff(t.Context(), HandoffOptions{TargetNode: "target"})
	if err != nil || !report.Stopped || len(report.Committed) != 1 || report.Committed[0].Path != "b" {
		t.Fatalf("resumed drain: %+v %v", report, err)
	}
	if after, err := d.Lookup(t.Context(), "a"); err != nil || after != a {
		t.Fatalf("resuming recreated committed actor: %+v %v", after, err)
	}
	for _, path := range []string{"a", "b"} {
		if value, err := target.Call[int64](t.Context(), Ref{Path: path}, remoteCommand{Kind: "get"}); err != nil || value != 42 {
			t.Fatalf("%s state: %d %v", path, value, err)
		}
	}
}

func TestHandoffTargetStopsAfterCommitDoesNotRestoreSource(t *testing.T) {
	base := NewMemoryDirectory()
	d := &faultyMoveDirectory{Directory: base, commit: true}
	d.lose.Store(true)
	source := globalNetworkNode(t, "source", d)
	target := globalNetworkNode(t, "target", base, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, nil)})
	connectNodes(t, source, target)
	d.after = func() {
		if err := target.Stop(t.Context()); err != nil {
			t.Error(err)
		}
	}
	pid := spawn(t, source, migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	result, err := source.Handoff(ctx, pid, HandoffOptions{TargetNode: "target"})
	if err == nil || result.State != MoveCommitted || result.Unknown {
		t.Fatalf("lost target: %+v %v", result, err)
	}
	if record, err := base.Lookup(t.Context(), "counter"); err != nil || record.PID() != result.Target.PID() {
		t.Fatalf("rolled back committed owner: %+v %v", record, err)
	}
	if _, _, err := source.resolve(pid); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("old PID restored: %v", err)
	}
}

type handoffBlob struct{ Data []byte }

func TestHandoffCopiesTailArrivingDuringBulkTransfer(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	restoring, continueRestore := make(chan struct{}), make(chan struct{})
	resumeTarget := sync.OnceFunc(func() { close(continueRestore) })
	defer resumeTarget()
	var blobs atomic.Int32
	kind := KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, func(_ *testBehavior, msg any) error {
		switch msg.(type) {
		case Upgraded:
			close(restoring)
			<-continueRestore
		case handoffBlob:
			blobs.Add(1)
		}
		return nil
	})}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	entered, release := make(chan struct{}), make(chan struct{})
	resumeSource := sync.OnceFunc(func() { close(release) })
	defer resumeSource()
	pid := spawn(t, source, migratingCounter(40, func(_ *testBehavior, msg any) error {
		if command, ok := msg.(remoteCommand); ok && command.Kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	if err := source.Send(pid, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	moveDone := make(chan error, 1)
	go func() {
		_, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"})
		moveDone <- err
	}()
	source.mu.Lock()
	p := source.processes[pid.ID]
	source.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.handoff != nil })
	blob := handoffBlob{Data: make([]byte, 64<<10)}
	for range 20 {
		if err := source.Send(Ref{Path: "counter"}, blob); err != nil {
			t.Fatal(err)
		}
	}
	resumeSource()
	signal(t, restoring)
	if err := source.Send(Ref{Path: "counter"}, remoteCommand{Kind: "add", Delta: 2}); err != nil {
		t.Fatalf("tail was sealed before transfer backpressure: %v", err)
	}
	callDone := make(chan callResultForHandoff, 1)
	go func() {
		value, err := source.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"})
		callDone <- callResultForHandoff{value, err}
	}()
	eventually(t, func() bool { return source.Stats().PendingCalls == 1 })
	resumeTarget()
	if err := <-moveDone; err != nil {
		t.Fatal(err)
	}
	if result := <-callDone; result.err != nil || result.value != 42 {
		t.Fatalf("tail Call: %+v", result)
	}
	if blobs.Load() != 20 {
		t.Fatalf("bulk mailbox lost/replayed: %d", blobs.Load())
	}
}

func TestPendingGlobalCallCanFollowTwoHandoffs(t *testing.T) {
	d := NewMemoryDirectory()
	a, caller := globalNetworkNode(t, "a", d), globalNetworkNode(t, "caller", d)
	bEntered, bRelease := make(chan struct{}), make(chan struct{})
	resumeB := sync.OnceFunc(func() { close(bRelease) })
	defer resumeB()
	b := globalNetworkNode(t, "b", d, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, func(_ *testBehavior, msg any) error {
		if _, ok := msg.(Activated); ok {
			close(bEntered)
			<-bRelease
		}
		return nil
	})})
	c := globalNetworkNode(t, "c", d, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, nil)})
	connectNodes(t, a, b)
	connectNodes(t, b, c)
	connectNodes(t, caller, a)
	aEntered, aRelease := make(chan struct{}), make(chan struct{})
	resumeA := sync.OnceFunc(func() { close(aRelease) })
	defer resumeA()
	pid := spawn(t, a, migratingCounter(42, func(_ *testBehavior, msg any) error {
		if command, ok := msg.(remoteCommand); ok && command.Kind == "block" {
			close(aEntered)
			<-aRelease
		}
		return nil
	}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	if err := a.Send(pid, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, aEntered)
	first := make(chan error, 1)
	go func() { _, err := a.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "b"}); first <- err }()
	a.mu.Lock()
	p := a.processes[pid.ID]
	a.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.handoff != nil })
	called := make(chan callResultForHandoff, 1)
	go func() {
		value, err := caller.Call[int64](t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "get"})
		called <- callResultForHandoff{value, err}
	}()
	eventually(t, func() bool { return a.Stats().PendingCalls == 1 })
	resumeA()
	signal(t, bEntered)
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	go func() { _, err := b.Handoff(t.Context(), route.PID(), HandoffOptions{TargetNode: "c"}); second <- err }()
	b.mu.Lock()
	q := b.processes[route.ProcessID]
	b.mu.Unlock()
	eventually(t, func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.handoff != nil })
	resumeB()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if result := <-called; result.err != nil || result.value != 42 {
		t.Fatalf("two-hop reply: %+v", result)
	}
}

func TestHandoffCancelBeforeTargetStagesCall(t *testing.T) {
	d := NewMemoryDirectory()
	source, caller := globalNetworkNode(t, "source", d), globalNetworkNode(t, "caller", d)
	preparing, ready := make(chan struct{}), make(chan struct{})
	resumeTarget := sync.OnceFunc(func() { close(ready) })
	defer resumeTarget()
	var executions atomic.Int32
	target := globalNetworkNode(t, "target", d, KindSpec{Name: "counter", Version: "1", Factory: migratingCounter(0, func(_ *testBehavior, msg any) error {
		if _, ok := msg.(Upgraded); ok {
			close(preparing)
			<-ready
		}
		if _, ok := msg.(remoteCommand); ok {
			executions.Add(1)
		}
		return nil
	})})
	connectNodes(t, source, target)
	connectNodes(t, caller, source)
	blocked, release := make(chan struct{}), make(chan struct{})
	resumeSource := sync.OnceFunc(func() { close(release) })
	defer resumeSource()
	pid := spawn(t, source, migratingCounter(42, func(_ *testBehavior, msg any) error {
		if command, ok := msg.(remoteCommand); ok && command.Kind == "block" {
			close(blocked)
			<-release
		}
		return nil
	}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	if err := source.Send(pid, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, blocked)
	moved := make(chan error, 1)
	go func() { _, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"}); moved <- err }()
	source.mu.Lock()
	p := source.processes[pid.ID]
	source.mu.Unlock()
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.handoff != nil })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	called := make(chan error, 1)
	go func() {
		_, err := caller.Call[int64](ctx, Ref{Path: "counter"}, remoteCommand{Kind: "get"})
		called <- err
	}()
	eventually(t, func() bool { return source.Stats().PendingCalls == 1 })
	resumeSource()
	signal(t, preparing)
	eventually(t, func() bool {
		caller.network.mu.Lock()
		defer caller.network.mu.Unlock()
		for _, out := range caller.network.outgoing {
			if out.routing != nil && out.routing.move != (MoveID{}) {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-called; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		target.network.mu.Lock()
		defer target.network.mu.Unlock()
		for _, r := range target.network.handoffs {
			r.mu.Lock()
			count := len(r.canceled)
			r.mu.Unlock()
			if count > 0 {
				return true
			}
		}
		return false
	})
	resumeTarget()
	if err := <-moved; err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return target.Stats().QueuedMessages == 0 && target.Stats().PendingCalls == 0 })
	if executions.Load() != 0 {
		t.Fatal("canceled transferred request executed")
	}
}
