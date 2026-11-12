package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Stop the wall-clock timer so tests can order expiry against mailbox/control
// operations without sleeps. fire still uses the production admission path.
func manualActorTimer(t *testing.T, ref TimerRef) {
	t.Helper()
	p := ref.timer.owner.Load()
	p.mu.Lock()
	stopped := ref.timer.timer.Stop()
	p.mu.Unlock()
	if !stopped {
		t.Fatal("test timer fired before manual expiry")
	}
}

func TestSendAfterAdmissionAndMailboxBackpressure(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(map[bool]string{false: "admitted", true: "full"}[full], func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			refs, seen := make(chan TimerRef, 1), make(chan Addr, 1)
			release := make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			defer resume()
			pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
				switch msg {
				case "schedule":
					ref, err := a.SendAfter(time.Hour, "tick")
					if err != nil {
						return err
					}
					refs <- ref
					<-release
				case "tick":
					seen <- a.Sender()
				case "barrier":
					return a.Reply(1)
				}
				return nil
			}), WithMailboxSize(1))
			if err := n.Send(pid, "schedule"); err != nil {
				t.Fatal(err)
			}
			ref := <-refs
			manualActorTimer(t, ref)
			if full {
				if err := n.Send(pid, "filler"); err != nil {
					t.Fatal(err)
				}
			}
			ref.timer.fire()
			signal(t, ref.Done())
			if ref.Cancel() {
				t.Fatal("Cancel retracted an already-claimed send")
			}
			if full && !errors.Is(ref.Err(), ErrMailboxFull) {
				t.Fatalf("timer result: %v", ref.Err())
			}
			if !full && ref.Err() != nil {
				t.Fatal(ref.Err())
			}
			select {
			case <-seen:
				t.Fatal("timer executed concurrently with Receive")
			default:
			}
			resume()
			if !full {
				select {
				case sender := <-seen:
					if sender != pid {
						t.Fatalf("sender=%v", sender)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("tick missing")
				}
			}
		})
	}
}

func TestSendAfterCancelRacesExpiry(t *testing.T) {
	n := testNode(t, NodeOptions{})
	count := 0
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg {
		case "schedule":
			ref, err := a.SendAfter(time.Hour, "tick")
			if err != nil {
				return err
			}
			return a.Reply(ref)
		case "tick":
			count++
		case "count":
			return a.Reply(count)
		}
		return nil
	}))
	want := 0
	for range 100 {
		ref, err := n.Call[TimerRef](t.Context(), pid, "schedule")
		if err != nil {
			t.Fatal(err)
		}
		manualActorTimer(t, ref)
		var wg sync.WaitGroup
		var canceled bool
		wg.Go(func() { canceled = ref.Cancel() })
		wg.Go(ref.timer.fire)
		wg.Wait()
		signal(t, ref.Done())
		if canceled {
			if ref.Err() != context.Canceled {
				t.Fatal(ref.Err())
			}
		} else {
			if ref.Err() != nil {
				t.Fatal(ref.Err())
			}
			want++
		}
		got, err := n.Call[int](t.Context(), pid, "count")
		if err != nil || got != want {
			t.Fatalf("count=%d want=%d err=%v", got, want, err)
		}
		if ref.timer.owner.Load() != nil {
			t.Fatal("completed timer retained its owner")
		}
	}
}

func TestSendAfterImmediateAndRestartOwnership(t *testing.T) {
	n := testNode(t, NodeOptions{})
	type startedTimer struct {
		pid PID
		ref TimerRef
	}
	started := make(chan startedTimer, 2)
	fired := make(chan PID, 2)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case Started:
			ref, err := a.SendAfter(time.Hour, "old")
			if err != nil {
				return err
			}
			started <- startedTimer{a.PID(), ref}
			_, err = a.SendAfter(0, "now")
			return err
		}
		switch msg {
		case "now":
			fired <- a.PID()
		case "old":
			t.Error("stopped instance's timer was delivered")
		case "crash":
			return errors.New("restart")
		}
		return nil
	}))
	first := <-started
	if got := <-fired; got != pid {
		t.Fatalf("immediate timer target=%v", got)
	}
	if err := n.Send(pid, "crash"); err != nil {
		t.Fatal(err)
	}
	second := <-started
	signal(t, first.ref.Done())
	if first.ref.Err() != ErrStopped || second.pid == pid {
		t.Fatalf("restart: %v %v", first.ref.Err(), second.pid)
	}
	first.ref.timer.fire()
	if got := <-fired; got != second.pid {
		t.Fatal("new instance did not get its own timer")
	}
	if err := n.StopActor(t.Context(), second.pid); err != nil {
		t.Fatal(err)
	}
	signal(t, second.ref.Done())
	if second.ref.Err() != ErrStopped || second.ref.timer.owner.Load() != nil {
		t.Fatal("Stop retained timer")
	}
}

func TestSendAfterDuringNodeStopPreservesReverseShutdown(t *testing.T) {
	n := testNode(t, NodeOptions{})
	earlyEntered, allowSchedule := make(chan struct{}), make(chan struct{})
	lateStopping, finishLate := make(chan struct{}), make(chan struct{})
	resumeEarly := sync.OnceFunc(func() { close(allowSchedule) })
	resumeLate := sync.OnceFunc(func() { close(finishLate) })
	defer resumeEarly()
	defer resumeLate()
	type scheduledTimer struct {
		ref TimerRef
		err error
	}
	type stoppedActor struct {
		name     string
		reason   error
		timerErr error
	}
	scheduled := make(chan scheduledTimer, 1)
	stopped := make(chan stoppedActor, 2)
	early := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg := msg.(type) {
		case string:
			if msg == "schedule" {
				close(earlyEntered)
				<-allowSchedule
				ref, err := a.SendAfter(time.Hour, "tick")
				scheduled <- scheduledTimer{ref, err}
				return err // The ordinary documented handler pattern.
			}
		case Stopped:
			_, err := a.SendAfter(time.Hour, "too-late")
			stopped <- stoppedActor{"early", msg.Reason, err}
		}
		return nil
	}))
	spawn(t, n, factory(func(_ *testBehavior, msg any) error {
		if event, ok := msg.(Stopped); ok {
			close(lateStopping)
			<-finishLate
			stopped <- stoppedActor{name: "late", reason: event.Reason}
		}
		return nil
	}))
	if err := n.Send(early, "schedule"); err != nil {
		t.Fatal(err)
	}
	signal(t, earlyEntered)
	stopResult := make(chan error, 1)
	go func() { stopResult <- n.Stop(t.Context()) }()
	signal(t, lateStopping)
	// The later sibling holds shutdown here. The earlier one is still active,
	// even though the node has stopped accepting new top-level work.
	resumeEarly()
	var timer TimerRef
	select {
	case result := <-scheduled:
		if result.err != nil {
			t.Fatalf("active sibling's SendAfter during Node.Stop: %v", result.err)
		}
		timer = result.ref
	case <-time.After(5 * time.Second):
		t.Fatal("active sibling did not schedule its timer")
	}
	select {
	case event := <-stopped:
		t.Fatalf("sibling stopped before later child completed: %+v", event)
	default:
	}
	select {
	case <-timer.Done():
		t.Fatalf("timer completed before its owner's shutdown: %v", timer.Err())
	default:
	}
	resumeLate()
	select {
	case err := <-stopResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Node.Stop did not finish")
	}
	first, second := <-stopped, <-stopped
	if first.name != "late" || second.name != "early" || first.reason != nil || second.reason != nil {
		t.Fatalf("shutdown order/reasons: %+v, %+v", first, second)
	}
	if second.timerErr != ErrStopped {
		t.Fatalf("Stopped callback admitted a new timer: %v", second.timerErr)
	}
	signal(t, timer.Done())
	if timer.Err() != ErrStopped || timer.timer.owner.Load() != nil {
		t.Fatal("owner shutdown did not cancel and release its timer")
	}
}
