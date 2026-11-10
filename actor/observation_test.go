package actor

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

type monitorCommand struct {
	target PID
	cancel bool
}
type linkCommand struct {
	target PID
	unlink bool
	trap   bool
}

func TestMonitorOldPIDAndDemonitorPendingDown(t *testing.T) {
	n := testNode(t, NodeOptions{})
	ready := make(chan lifecycleEvent, 4)
	target := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case Started:
			ready <- lifecycleEvent{pid: a.PID(), started: true}
		case string:
			return errors.New("crash")
		}
		return nil
	}), WithName("target"))
	lifecycle(t, ready)
	downs := make(chan Down, 4)
	observer := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg := msg.(type) {
		case monitorCommand:
			ref, err := a.Monitor(msg.target)
			if err != nil {
				return a.ReplyError(err)
			}
			if msg.cancel {
				a.Demonitor(ref)
			}
			return a.Reply(ref)
		case Down:
			downs <- msg
		}
		return nil
	}))
	ref, err := n.Call[MonitorRef](t.Context(), observer, monitorCommand{target: target})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Send(target, "crash"); err != nil {
		t.Fatal(err)
	}
	fresh := lifecycle(t, ready)
	readyPID(t, n, fresh.pid)
	select {
	case down := <-downs:
		if down.Ref != ref || down.PID != target || !errors.Is(down.Reason, ErrProcessCrashed) {
			t.Fatal(down)
		}
	case <-t.Context().Done():
		t.Fatal("missing Down")
	}
	if err = n.StopActor(t.Context(), fresh.pid); err != nil {
		t.Fatal(err)
	}
	// A monitor remains tied to the old physical PID; a second registration can
	// be removed while its immediate no-process Down is still in the system queue.
	if _, err = n.Call[MonitorRef](t.Context(), observer, monitorCommand{target: target, cancel: true}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.observations == 0 })
	select {
	case down := <-downs:
		t.Fatalf("old monitor rebound or Demonitor lost race: %v", down)
	default:
	}
}

func TestMonitorQuotaReservesPendingNotification(t *testing.T) {
	n := testNode(t, NodeOptions{MaxObservations: 1, MaxObservationsPerActor: 1})
	missing := PID{Node: n.Name(), BootID: n.BootID(), ID: 999}
	downs := make(chan Down, 2)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case string:
			first, err := a.Monitor(missing)
			if err != nil {
				return a.ReplyError(err)
			}
			if _, err = a.Monitor(missing); !errors.Is(err, ErrCapacity) {
				return fmt.Errorf("expected quota rejection, got %v", err)
			}
			a.Demonitor(first)
			last, err := a.Monitor(missing)
			if err != nil {
				return a.ReplyError(err)
			}
			return a.Reply(last)
		case Down:
			downs <- msg.(Down)
		}
		return nil
	}))
	ref, err := n.Call[MonitorRef](t.Context(), pid, "monitor")
	if err != nil {
		t.Fatal(err)
	}
	down := <-downs
	if down.Ref != ref || !errors.Is(down.Reason, ErrNoProcess) {
		t.Fatal(down)
	}
	eventually(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.observations == 0 })
	select {
	case <-downs:
		t.Fatal("canceled notification delivered")
	default:
	}
}

func TestLinksAreBidirectionalAndTrapExitPreservesActor(t *testing.T) {
	for _, scenario := range []string{"trap", "fatal-forward", "fatal-reverse", "unlink", "normal"} {
		t.Run(scenario, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			exits := make(chan Exit, 2)
			stopped := make(chan PID, 2)
			behavior := factory(func(a *testBehavior, msg any) error {
				switch msg := msg.(type) {
				case linkCommand:
					a.SetTrapExit(msg.trap)
					if msg.unlink {
						a.Unlink(msg.target)
						return a.Reply(true)
					}
					err := a.Link(a.Context(), msg.target)
					if err != nil {
						return a.ReplyError(err)
					}
					return a.Reply(true)
				case Exit:
					exits <- msg
				case Stopped:
					stopped <- a.PID()
				case string:
					if msg == "fail" {
						return errors.New("linked crash")
					}
					return a.Reply(true)
				}
				return nil
			})
			a := spawn(t, n, behavior, WithName("a"), WithRestart(Temporary))
			b := spawn(t, n, behavior, WithName("b"), WithRestart(Temporary))
			if _, err := n.Call[bool](t.Context(), a, linkCommand{target: b, trap: scenario == "trap"}); err != nil {
				t.Fatal(err)
			}
			if scenario == "unlink" {
				if _, err := n.Call[bool](t.Context(), a, linkCommand{target: b, unlink: true}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "normal" {
				if err := n.StopActor(t.Context(), b); err != nil {
					t.Fatal(err)
				}
			} else {
				victim := b
				if scenario == "fatal-reverse" {
					victim = a
				}
				if err := n.Send(victim, "fail"); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "fatal-forward", "fatal-reverse":
				for range 2 {
					<-stopped
				}
				eventually(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return len(n.processes) == 0 })
			case "trap":
				exit := <-exits
				if exit.PID != b || !errors.Is(exit.Reason, ErrProcessCrashed) {
					t.Fatal(exit)
				}
				if _, err := n.Call[bool](t.Context(), a, "alive"); err != nil {
					t.Fatal(err)
				}
			default:
				<-stopped
				if _, err := n.Call[bool](t.Context(), a, "alive"); err != nil {
					t.Fatal(err)
				}
				select {
				case <-exits:
					t.Fatal("unexpected Exit")
				default:
				}
			}
			eventually(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.observations == 0 })
		})
	}
}

func TestLinkAdmissionCannotCreateHalfLink(t *testing.T) {
	n := testNode(t, NodeOptions{MaxObservations: 1})
	target := spawn(t, n, factory(func(*testBehavior, any) error { return nil }))
	observer := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		if _, ok := msg.(string); ok {
			_ = a.ReplyError(a.Link(context.Background(), target))
		}
		return nil
	}))
	if _, err := n.Call[any](t.Context(), observer, "link"); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.observations != 0 || len(n.links) != 0 {
		t.Fatal("partial link consumed quota")
	}
}
