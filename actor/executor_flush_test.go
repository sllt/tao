package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func nextWork(t *testing.T, seen <-chan int, want int) {
	t.Helper()
	select {
	case value := <-seen:
		if value != want {
			t.Fatalf("runnable Actor received %d, want %d", value, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runnable Actor did not make progress")
	}
}

func TestRunNextFlushStopsPendingWatchAndRearms(t *testing.T) {
	n := testNode(t, NodeOptions{})
	seen := make(chan int, 2)
	count := 0
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message := message.(type) {
		case int:
			count++
			seen <- message
		case string:
			return a.Reply(count)
		}
		return nil
	}))
	p, _, _ := n.resolve(pid)
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
	// A future real watch makes the successful Stop branch deterministic.
	turn := &executorTurn{cache: &n.executors.shards[0], watched: true}
	turn.timer = time.AfterFunc(time.Hour, turn.watch)
	defer turn.close()
	source := &process{}
	source.executor.Store(turn)
	if err := p.enqueueFrom(envelope{message: 1}, nil, source); err != nil {
		t.Fatal(err)
	}
	turn.flush()
	turn.mu.Lock()
	watched, stillActive := turn.watched, turn.timer.Stop()
	turn.mu.Unlock()
	if watched || stillActive {
		t.Fatal("explicit flush retained its pending notification")
	}
	if turn.take() != nil {
		t.Fatal("flush left a second execution claim")
	}
	nextWork(t, seen, 1)
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
	if err := p.enqueueFrom(envelope{message: 2}, nil, source); err != nil {
		t.Fatal(err)
	}
	// No explicit take or flush follows: offer must rearm the zero-delay
	// independent callback after the prior notification was canceled.
	nextWork(t, seen, 2)
	if value, err := n.Call[int](t.Context(), pid, "barrier"); err != nil || value != 2 {
		t.Fatalf("execution claim: %d want 2: %v", value, err)
	}
}

func TestRunNextFlushStartedWatchKeepsReofferAndCloseSafe(t *testing.T) {
	for _, closeTurn := range []bool{false, true} {
		name := "reoffer"
		if closeTurn {
			name = "close"
		}
		t.Run(name, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			seen := make(chan int, 3)
			count := 0
			pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message := message.(type) {
				case int:
					count++
					seen <- message
				case string:
					return a.Reply(count)
				}
				return nil
			}))
			p, _, _ := n.resolve(pid)
			eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
			turn := &executorTurn{cache: &n.executors.shards[0], watched: true}
			entered, release := make(chan struct{}), make(chan struct{})
			resume := sync.OnceFunc(func() { close(release) })
			defer resume()
			defer turn.close()
			finished := make(chan struct{}, 2)
			var callbacks atomic.Int32
			turn.timer = time.AfterFunc(0, func() {
				if callbacks.Add(1) == 1 {
					close(entered)
					<-release
				}
				turn.watch()
				finished <- struct{}{}
			})
			signal(t, entered) // The callback has started, so Stop must fail.
			source := &process{}
			source.executor.Store(turn)
			if err := p.enqueueFrom(envelope{message: 1}, nil, source); err != nil {
				t.Fatal(err)
			}
			turn.flush()
			turn.mu.Lock()
			watched := turn.watched
			turn.mu.Unlock()
			if !watched {
				t.Fatal("flush revoked an already-started callback")
			}
			nextWork(t, seen, 1)
			eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
			if err := p.enqueueFrom(envelope{message: 2}, nil, source); err != nil {
				t.Fatal(err)
			}
			turn.mu.Lock()
			pending := turn.next == p && turn.pending.Load() && turn.watched
			turn.mu.Unlock()
			if !pending || callbacks.Load() != 1 {
				t.Fatal("reoffer replaced the notification still owned by the old callback")
			}
			select {
			case <-seen:
				t.Fatal("reoffer ran before its held callback or Close claimed it")
			default:
			}
			if closeTurn {
				turn.close()
				if turn.offer(p) {
					t.Fatal("closed turn accepted a new publication")
				}
				nextWork(t, seen, 2)
			}
			resume()
			signal(t, finished)
			if !closeTurn {
				nextWork(t, seen, 2)
				eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
				if err := p.enqueueFrom(envelope{message: 3}, nil, source); err != nil {
					t.Fatal(err)
				}
				nextWork(t, seen, 3)
				signal(t, finished)
			}
			want := 3
			if closeTurn {
				want = 2
			}
			if value, err := n.Call[int](t.Context(), pid, "barrier"); err != nil || value != want {
				t.Fatalf("execution claim: %d want %d: %v", value, want, err)
			}
		})
	}
}
