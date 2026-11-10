package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func migratingCounter(initial int64, hook func(*testBehavior, any) error) Factory {
	return func() Behavior {
		count := initial
		return factory(func(a *testBehavior, msg any) error {
			if hook != nil {
				if err := hook(a, msg); err != nil {
					return err
				}
			}
			switch msg := msg.(type) {
			case Snapshotting:
				return a.Reply(count)
			case Upgraded:
				return msg.Restore(&count)
			case remoteCommand:
				switch msg.Kind {
				case "add":
					count += msg.Delta
				case "get":
					return a.Reply(count)
				}
			}
			return nil
		})()
	}
}

func TestHandoffMovesStateAndGlobalCalls(t *testing.T) {
	for _, callerLocation := range []string{"source", "target", "outbound"} {
		t.Run(callerLocation, func(t *testing.T) {
			d := NewMemoryDirectory()
			source := globalNetworkNode(t, "source", d)
			replyEntered, replyRelease := make(chan struct{}), make(chan struct{})
			releaseReply := sync.OnceFunc(func() { close(replyRelease) })
			defer releaseReply()
			var targetGets atomic.Int32
			kind := KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, func(_ *testBehavior, msg any) error {
				if command, ok := msg.(remoteCommand); ok && command.Kind == "get" && targetGets.Add(1) == 1 {
					close(replyEntered)
					<-replyRelease
				}
				return nil
			})}
			target := globalNetworkNode(t, "target", d, kind)
			connectNodes(t, source, target)
			caller := source
			if callerLocation == "target" {
				caller = target
			}
			if callerLocation == "outbound" {
				var err error
				caller, err = StartNode("", NodeOptions{Name: "caller", Directory: d, Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"command", 1, remoteCommand{}}, {"count", 1, int64(0)}}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := caller.Stop(context.Background()); err != nil {
						t.Error(err)
					}
				})
				connectNodes(t, caller, source)
			}
			entered, resume := make(chan struct{}), make(chan struct{})
			releaseSource := sync.OnceFunc(func() { close(resume) })
			defer releaseSource()
			old := spawn(t, source, migratingCounter(40, func(_ *testBehavior, msg any) error {
				if command, ok := msg.(remoteCommand); ok && command.Kind == "block" {
					close(entered)
					<-resume
				}
				return nil
			}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
			if err := source.Send(old, remoteCommand{Kind: "block"}); err != nil {
				t.Fatal(err)
			}
			signal(t, entered)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			moving := make(chan handoffCompletion, 1)
			go func() {
				result, err := source.Handoff(ctx, old, HandoffOptions{TargetNode: "target", Version: "2"})
				moving <- handoffCompletion{result, err}
			}()
			source.mu.Lock()
			p := source.processes[old.ID]
			source.mu.Unlock()
			eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.handoff != nil })
			if err := caller.SendConfirmed(ctx, Ref{Path: "counter"}, remoteCommand{Kind: "add", Delta: 2}); err != nil {
				t.Fatal(err)
			}
			called := make(chan callResultForHandoff, 1)
			go func() {
				value, err := caller.Call[int64](ctx, Ref{Path: "counter"}, remoteCommand{Kind: "get"})
				called <- callResultForHandoff{value, err}
			}()
			eventually(t, func() bool { return source.Stats().PendingCalls == 1 })
			if err := source.Send(old, remoteCommand{Kind: "add", Delta: 1000}); err != nil {
				t.Fatal(err)
			}
			releaseSource()
			completion := <-moving
			if completion.err != nil || completion.result.State != MoveCommitted {
				t.Fatalf("handoff: %+v %v", completion.result, completion.err)
			}
			signal(t, replyEntered)
			if callerLocation == "outbound" {
				if err := source.Stop(ctx); err != nil {
					t.Fatal(err)
				}
			}
			releaseReply()
			select {
			case result := <-called:
				if result.err != nil || result.value != 42 {
					t.Fatalf("transferred Call: %+v", result)
				}
			case <-ctx.Done():
				t.Fatal("transferred Call stalled")
			}
			if targetGets.Load() != 1 {
				t.Fatalf("duplicate execution: %d", targetGets.Load())
			}
			if callerLocation == "outbound" {
				if value, err := caller.network.call(ctx, routedRef{record: completion.result.Source}, remoteCommand{Kind: "get"}, PID{}); err != nil || value != int64(42) {
					t.Fatalf("stale route dialed stopped source: %v %v", value, err)
				}
			}
			if callerLocation != "outbound" {
				if err := source.Send(Ref{Node: "source", Path: "counter"}, remoteCommand{}); !errors.Is(err, ErrNoProcess) {
					t.Fatalf("node-bound Ref followed migration: %v", err)
				}
			}
			if value, err := caller.Call[int64](ctx, Ref{Path: "counter"}, remoteCommand{Kind: "get"}); err != nil || value != 42 {
				t.Fatalf("new global route: %d %v", value, err)
			}
		})
	}
}

type callResultForHandoff struct {
	value int64
	err   error
}

func TestHandoffUnsupportedKindRollsBackWithoutChangingEpoch(t *testing.T) {
	d := NewMemoryDirectory()
	source, target := globalNetworkNode(t, "source", d), globalNetworkNode(t, "target", d)
	connectNodes(t, source, target)
	pid := spawn(t, source, migratingCounter(42, nil), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
	before, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	result, err := source.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target"})
	if !errors.Is(err, ErrHandoffUnsupported) || result.State != MoveAborted {
		t.Fatalf("preflight rollback: %+v %v", result, err)
	}
	if after, err := d.Lookup(t.Context(), "counter"); err != nil || after != before {
		t.Fatalf("preflight changed owner: %+v %v", after, err)
	}
	if got, err := source.Call[int64](t.Context(), pid, remoteCommand{Kind: "get"}); err != nil || got != 42 {
		t.Fatalf("source state: %d %v", got, err)
	}
}
