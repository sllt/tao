package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type lifecycleEvent struct {
	name    string
	pid     PID
	started bool
	reason  error
}

func lifecycle(t *testing.T, ch <-chan lifecycleEvent) lifecycleEvent {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("missing lifecycle event")
		return lifecycleEvent{}
	}
}
func readyPID(t *testing.T, n *Node, pid PID) {
	t.Helper()
	n.mu.Lock()
	p := n.processes[pid.ID]
	n.mu.Unlock()
	if p == nil {
		t.Fatal("started process disappeared")
	}
	signal(t, p.started)
	if p.startErr != nil {
		t.Fatal(p.startErr)
	}
}

func TestSupervisionStrategies(t *testing.T) {
	for _, strategy := range []Strategy{OneForOne, OneForAll, RestForOne} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			events := make(chan lifecycleEvent, 32)
			parent := spawn(t, n, factory(func(a *testBehavior, msg any) error {
				if _, ok := msg.(Started); ok {
					if err := a.SetStrategy(strategy, 5, time.Second); err != nil {
						return err
					}
					for _, name := range []string{"a", "b", "c"} {
						_, err := a.Spawn(factory(func(child *testBehavior, msg any) error {
							switch msg := msg.(type) {
							case Started:
								events <- lifecycleEvent{name, child.PID(), true, nil}
							case Stopped:
								events <- lifecycleEvent{name, child.PID(), false, msg.Reason}
							case string:
								if msg == "fail" {
									return errors.New("failed")
								}
								return child.Reply(child.PID())
							}
							return nil
						}), WithName(name))
						if err != nil {
							return err
						}
					}
				}
				return nil
			}), WithName("parent"), WithRestart(Temporary))
			old := map[string]PID{}
			for range 3 {
				event := lifecycle(t, events)
				old[event.name] = event.pid
			}
			if err := n.Send(old["b"], "fail"); err != nil {
				t.Fatal(err)
			}
			want := map[string]bool{"b": true}
			if strategy == OneForAll {
				want["a"] = true
			}
			if strategy != OneForOne {
				want["c"] = true
			}
			fresh := map[string]PID{}
			for len(fresh) < len(want) {
				event := lifecycle(t, events)
				if event.started {
					if !want[event.name] {
						t.Fatal("unexpected restarted sibling", event.name)
					}
					fresh[event.name] = event.pid
					readyPID(t, n, event.pid)
				}
			}
			for _, name := range []string{"a", "b", "c"} {
				pid, err := n.Call[PID](t.Context(), n.Ref("parent/"+name), "pid")
				if err != nil {
					t.Fatal(err)
				}
				if (pid != old[name]) != want[name] {
					t.Fatalf("wrong restart scope for %s", name)
				}
			}
			if err := n.Send(old["b"], 1); !errors.Is(err, ErrNoProcess) {
				t.Fatal("old PID survived restart", err)
			}
			if err := n.StopActor(t.Context(), parent); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRestartPoliciesAndExplicitStop(t *testing.T) {
	for _, policy := range []RestartPolicy{Permanent, Transient, Temporary} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			events := make(chan lifecycleEvent, 4)
			var starts atomic.Int32
			pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
				switch msg.(type) {
				case Started:
					starts.Add(1)
					events <- lifecycleEvent{pid: a.PID(), started: true}
				case string:
					a.Stop(nil)
				case Stopped:
					events <- lifecycleEvent{pid: a.PID()}
				}
				return nil
			}), WithName("worker"), WithRestart(policy))
			lifecycle(t, events)
			if err := n.Send(pid, "normal"); err != nil {
				t.Fatal(err)
			}
			lifecycle(t, events)
			if policy == Permanent {
				event := lifecycle(t, events)
				if !event.started || event.pid == pid {
					t.Fatal("permanent child did not restart")
				}
				readyPID(t, n, event.pid)
				if err := n.StopActor(t.Context(), event.pid); err != nil {
					t.Fatal(err)
				}
				if starts.Load() != 2 {
					t.Fatal("explicit stop restarted child")
				}
			} else {
				eventually(t, func() bool { _, _, err := n.resolve(n.Ref("worker")); return errors.Is(err, ErrNoProcess) })
				if starts.Load() != 1 {
					t.Fatal("normal exit restarted non-permanent child")
				}
			}
		})
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("actor state did not advance")
		}
	}
}

func TestRestartIntensityCountsFailedStartupRounds(t *testing.T) {
	n := testNode(t, NodeOptions{})
	var attempts atomic.Int32
	parentStopped := make(chan error, 1)
	var child PID
	parent := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg := msg.(type) {
		case Started:
			if err := a.SetStrategy(OneForOne, 2, time.Second); err != nil {
				return err
			}
			var err error
			child, err = a.Spawn(factory(func(_ *testBehavior, msg any) error {
				switch msg.(type) {
				case Started:
					if attempts.Add(1) > 1 {
						return errors.New("cannot restart")
					}
				case string:
					return errors.New("crash")
				}
				return nil
			}), WithName("child"))
			return err
		case Stopped:
			parentStopped <- msg.Reason
		}
		return nil
	}), WithName("parent"), WithRestart(Temporary))
	if err := n.Send(child, "fail"); err != nil {
		t.Fatal(err)
	}
	select {
	case reason := <-parentStopped:
		if !errors.Is(reason, ErrMaxRestartsExceeded) {
			t.Fatal(reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor never escalated")
	}
	if attempts.Load() != 3 {
		t.Fatalf("startup attempts=%d", attempts.Load())
	}
	eventually(t, func() bool { _, _, err := n.resolve(parent); return errors.Is(err, ErrNoProcess) })
}

func TestPreserveRefDoesNotReplayPIDOrExecutingMessage(t *testing.T) {
	for _, policy := range []RestartMailbox{DropMailbox, PreserveRef} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			started := make(chan lifecycleEvent, 4)
			entered, release := make(chan struct{}), make(chan struct{})
			once := sync.OnceFunc(func() { close(release) })
			defer once()
			var generation atomic.Int32
			f := func() Behavior {
				gen := generation.Add(1)
				var messages []string
				return &testBehavior{receive: func(a *testBehavior, msg any) error {
					switch msg {
					case "crash":
						close(entered)
						<-release
						return errors.New("crash")
					case "ref", "pid":
						messages = append(messages, msg.(string))
					case "queued-call":
						return a.Reply(gen)
					case "read":
						return a.Reply(append([]string(nil), messages...))
					}
					if _, ok := msg.(Started); ok {
						started <- lifecycleEvent{pid: a.PID(), started: true}
					}
					return nil
				}}
			}
			old := spawn(t, n, f, WithName("worker"), WithRestartMailbox(policy))
			lifecycle(t, started)
			if err := n.Send(old, "crash"); err != nil {
				t.Fatal(err)
			}
			signal(t, entered)
			if err := n.Send(n.Ref("worker"), "ref"); err != nil {
				t.Fatal(err)
			}
			if err := n.Send(old, "pid"); err != nil {
				t.Fatal(err)
			}
			type reply struct {
				generation int32
				err        error
			}
			result := make(chan reply, 1)
			go func() {
				gen, err := n.Call[int32](t.Context(), n.Ref("worker"), "queued-call")
				result <- reply{gen, err}
			}()
			eventually(t, func() bool {
				n.mu.Lock()
				p := n.processes[old.ID]
				n.mu.Unlock()
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.mailbox.size == 3
			})
			once()
			fresh := lifecycle(t, started)
			readyPID(t, n, fresh.pid)
			got := <-result
			if policy == PreserveRef {
				if got.err != nil || got.generation != 2 {
					t.Fatalf("preserved call: %+v", got)
				}
			} else if !errors.Is(got.err, ErrProcessCrashed) {
				t.Fatal(got.err)
			}
			messages, err := n.Call[[]string](t.Context(), n.Ref("worker"), "read")
			if err != nil {
				t.Fatal(err)
			}
			if policy == PreserveRef {
				if len(messages) != 1 || messages[0] != "ref" {
					t.Fatal(messages)
				}
			} else if len(messages) != 0 {
				t.Fatal(messages)
			}
			if generation.Load() != 2 {
				t.Fatal("replayed executing message")
			}
		})
	}
}
