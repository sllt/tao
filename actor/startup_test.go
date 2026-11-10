package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStartupWaitCancellationAndTimerReuse(t *testing.T) {
	for _, parentCancel := range []bool{false, true} {
		name := "timeout"
		if parentCancel {
			name = "parent-cancel"
		}
		t.Run(name, func(t *testing.T) {
			n := testNode(t, NodeOptions{StartupTimeout: 100 * time.Millisecond})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, canceled := make(chan struct{}), make(chan struct{})
			result := make(chan error, 1)
			go func() {
				_, err := n.spawn(n.root, ctx, factory(func(a *testBehavior, msg any) error {
					if _, ok := msg.(Started); ok {
						close(entered)
						<-a.Context().Done()
						close(canceled)
						return a.Context().Err()
					}
					return nil
				}))
				result <- err
			}()
			signal(t, entered)
			want := error(context.DeadlineExceeded)
			if parentCancel {
				cancel()
				want = context.Canceled
			}
			if err := <-result; !errors.Is(err, want) || !errors.Is(err, ErrUnresponsive) {
				t.Fatalf("startup error: %v", err)
			}
			signal(t, canceled)
			for range 20 {
				pid := spawn(t, n, factory(func(*testBehavior, any) error { return nil }), WithRestart(Temporary))
				if err := n.StopActor(t.Context(), pid); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestLeafStoppedCallbackRetainsShutdownWatchdog(t *testing.T) {
	n := testNode(t, NodeOptions{ShutdownTimeout: 50 * time.Millisecond})
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	pid := spawn(t, n, factory(func(a *testBehavior, msg any) error {
		switch msg.(type) {
		case string:
			a.Stop(nil)
		case Stopped:
			close(entered)
			<-release
		}
		return nil
	}), WithRestart(Temporary))
	if err := n.Send(pid, "stop"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	// There is no external StopActor waiter to mark the process unresponsive.
	// The leaf's own cleanup watchdog must revoke its address.
	eventually(t, func() bool { return n.Stats().Unresponsive == 1 })
	if err := n.Send(pid, "again"); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("blocked Stopped callback remained addressable: %v", err)
	}
}
