package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

type watchRemote struct {
	target PID
	remove MonitorRef
}

func remoteObserver(t *testing.T, n *Node) (PID, <-chan Down) {
	t.Helper()
	downs := make(chan Down, 8)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg := msg.(type) {
		case watchRemote:
			if msg.remove.ID != 0 {
				a.Demonitor(msg.remove)
				return a.Reply(MonitorRef{})
			}
			ref, err := a.Monitor(msg.target)
			if err != nil {
				return a.ReplyError(err)
			}
			return a.Reply(ref)
		case Down:
			downs <- msg
		}
		return nil
	}), WithRestart(Temporary))
	return pid, downs
}
func nextDown(t *testing.T, downs <-chan Down) Down {
	t.Helper()
	select {
	case down := <-downs:
		return down
	case <-time.After(5 * time.Second):
		t.Fatal("remote Down missing")
		return Down{}
	}
}

func TestRemoteMonitorTerminationDemonitorAndUnreachable(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	target := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			return actor.Reply(int64(1))
		}
		return nil
	}), WithName("target"), WithRestart(Temporary))
	observer, downs := remoteObserver(t, a)
	ref, err := a.Call[MonitorRef](t.Context(), observer, watchRemote{target: target})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return len(b.remoteMonitors) == 1 })
	if _, err = a.Call[MonitorRef](t.Context(), observer, watchRemote{remove: ref}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return len(b.remoteMonitors) == 0 })
	ref, err = a.Call[MonitorRef](t.Context(), observer, watchRemote{target: target})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return len(b.remoteMonitors) == 1 })
	if err = b.StopActor(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if down := nextDown(t, downs); down.Ref != ref || down.PID != target || down.Reason != nil {
		t.Fatal(down)
	}
	missing, err := a.Call[MonitorRef](t.Context(), observer, watchRemote{target: target})
	if err != nil {
		t.Fatal(err)
	}
	if down := nextDown(t, downs); down.Ref != missing || !errors.Is(down.Reason, ErrNoProcess) {
		t.Fatal(down)
	}
	stillAlive := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			return actor.Reply(int64(9))
		}
		return nil
	}), WithRestart(Temporary))
	ref, err = a.Call[MonitorRef](t.Context(), observer, watchRemote{target: stillAlive})
	if err != nil {
		t.Fatal(err)
	}
	// The preceding missing-target Down can reach the observer before its ACK
	// removes that remote record. Wait for this identity, not the total count.
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.remoteMonitors[ref] != nil })
	for _, session := range a.network.endpoint.Sessions("b") {
		if session.Info().Class == taonet.ClassControl {
			_ = session.Close()
		}
	}
	if down := nextDown(t, downs); down.Ref != ref || !errors.Is(down.Reason, ErrNodeUnreachable) {
		t.Fatal(down)
	}
	if value, err := b.Call[int64](t.Context(), stillAlive, remoteCommand{}); value != 9 || err != nil {
		t.Fatalf("unreachable was treated as remote death: %d %v", value, err)
	}
	connectNodes(t, a, b)
	if err = b.StopActor(t.Context(), stillAlive); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return a.Stats().Observations == 0 && b.Stats().Observations == 0 && b.NetworkStats().ControlReservations == 0
	})
	select {
	case down := <-downs:
		t.Fatalf("observation automatically restored or duplicated: %v", down)
	default:
	}
}

func TestRemoteLinkExitInBothDirectionsAndUnlink(t *testing.T) {
	for _, scenario := range []string{"trap", "forward", "reverse", "unlink", "normal"} {
		t.Run(scenario, func(t *testing.T) {
			a, b := networkNode(t, "a"), networkNode(t, "b")
			connectNodes(t, a, b)
			exits := make(chan Exit, 4)
			stopped := make(chan PID, 4)
			f := factory(func(actor *testBehavior, msg any) error {
				switch msg := msg.(type) {
				case linkCommand:
					actor.SetTrapExit(msg.trap)
					if msg.unlink {
						actor.Unlink(msg.target)
						return actor.Reply(true)
					}
					if err := actor.Link(actor.Context(), msg.target); err != nil {
						return actor.ReplyError(err)
					}
					return actor.Reply(true)
				case Exit:
					exits <- msg
				case Stopped:
					stopped <- actor.PID()
				case string:
					if msg == "fail" {
						return errors.New("failure")
					}
					return actor.Reply(true)
				}
				return nil
			})
			left, right := spawn(t, a, f, WithRestart(Temporary)), spawn(t, b, f, WithRestart(Temporary))
			if _, err := a.Call[bool](t.Context(), left, linkCommand{target: right, trap: scenario == "trap"}); err != nil {
				t.Fatal(err)
			}
			if scenario == "unlink" {
				if _, err := a.Call[bool](t.Context(), left, linkCommand{target: right, unlink: true}); err != nil {
					t.Fatal(err)
				}
				eventually(t, func() bool { return a.Stats().Observations == 0 && b.Stats().Observations == 0 })
			}
			if scenario == "normal" {
				if err := b.StopActor(t.Context(), right); err != nil {
					t.Fatal(err)
				}
			} else if scenario == "reverse" {
				if err := a.Send(left, "fail"); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := b.Send(right, "fail"); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "forward", "reverse":
				for range 2 {
					select {
					case <-stopped:
					case <-time.After(5 * time.Second):
						t.Fatal("remote link did not propagate")
					}
				}
			case "trap":
				select {
				case exit := <-exits:
					if exit.PID != right || !errors.Is(exit.Reason, ErrProcessCrashed) {
						t.Fatal(exit)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("missing remote Exit")
				}
				if _, err := a.Call[bool](t.Context(), left, "alive"); err != nil {
					t.Fatal(err)
				}
			default:
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Fatal("target did not stop")
				}
				if _, err := a.Call[bool](t.Context(), left, "alive"); err != nil {
					t.Fatal(err)
				}
			}
			eventually(t, func() bool {
				return a.Stats().Observations == 0 && b.Stats().Observations == 0 && a.NetworkStats().ControlReservations == 0 && b.NetworkStats().ControlReservations == 0
			})
		})
	}
}

func TestRemoteLinkCanceledBeforeAcknowledgmentCleansBothEdges(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	left := spawn(t, a, factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
	right := spawn(t, b, factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
	a.mu.Lock()
	observer := a.processes[left.ID]
	a.mu.Unlock()
	b.mu.Lock()
	unlock := sync.OnceFunc(b.mu.Unlock)
	defer unlock()
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- a.network.link(ctx, observer, right) }()
	eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		record := a.links[left][right]
		return record != nil && record.scope != (wireScope{})
	})
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unlock()
	eventually(t, func() bool {
		return a.Stats().Observations == 0 && b.Stats().Observations == 0 && a.NetworkStats().ControlReservations == 0 && b.NetworkStats().ControlReservations == 0
	})
}

func TestRemoteControlProgressesWithFullInboxAndSendQueue(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	// Lower only the adapter's local admission count; the wire limits remain
	// negotiated normally. Three jobs suffice to demonstrate independent control.
	a.network.mu.Lock()
	a.network.opts.SendMessages = 3
	a.network.mu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	once := sync.OnceFunc(func() { close(release) })
	defer once()
	target := spawn(t, b, factory(func(_ *testBehavior, msg any) error {
		if cmd, ok := msg.(remoteCommand); ok && cmd.Kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithMailboxSize(512), WithRestart(Temporary))
	observer, downs := remoteObserver(t, a)
	if err := a.SendConfirmed(t.Context(), target, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	for range 255 {
		if err := a.SendConfirmed(t.Context(), target, remoteCommand{}); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if err := a.Send(target, remoteCommand{}); err != nil {
			t.Fatal(err)
		}
	}
	remoteError(t, a.Send(target, remoteCommand{}), ErrCapacity, NotSent)
	ref, err := a.Call[MonitorRef](t.Context(), observer, watchRemote{target: target})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.remoteMonitors[ref] != nil })
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err = b.StopActor(ctx, target); !errors.Is(err, ErrUnresponsive) {
		t.Fatal(err)
	}
	if down := nextDown(t, downs); down.Ref != ref || !errors.Is(down.Reason, ErrUnresponsive) {
		t.Fatal(down)
	}
	once()
}
