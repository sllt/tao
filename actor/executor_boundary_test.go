package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutorReadyQueueEmptyBudgetKeepsClaim(t *testing.T) {
	for _, test := range []struct {
		name   string
		shards int
	}{{"one-shard", 1}, {"cross-shard", 2}} {
		t.Run(test.name, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(2, test.shards)
			var targets [3]PID
			var handled atomic.Int32
			const steps = 130
			turns := make(chan *executorTurn, steps)
			result := make(chan error, 1)
			for index := range targets {
				targets[index] = spawn(t, n, factory(func(a *testBehavior, message any) error {
					if message != "step" {
						return nil
					}
					turn, err := untimedTurn(a.proc)
					if err != nil {
						result <- err
						return nil
					}
					turns <- turn
					if handled.Add(1) == steps {
						result <- nil
						return nil
					}
					if err := a.Send(targets[(index+1)%len(targets)], "step"); err != nil {
						result <- err
					}
					return nil
				}))
			}
			for i := range n.executors.shards {
				cache := &n.executors.shards[i]
				idleExecutorReady(t, cache)
				// Remove only quiescent bootstrap timer state. Any timer now
				// created by this isolated chain is an extra ready dispatch.
				cache.mu.Lock()
				if cache.readyTimer != nil {
					cache.readyTimer.Stop()
					cache.readyTimer = nil
				}
				cache.mu.Unlock()
			}
			if err := n.Send(targets[0], "step"); err != nil {
				t.Fatal(err)
			}
			nextResult(t, result)
			first := <-turns
			for range steps - 1 {
				if used := <-turns; used != first {
					t.Fatal("empty budget boundary dispatched an already-owned claim")
				}
			}
			for i := range n.executors.shards {
				cache := &n.executors.shards[i]
				cache.mu.Lock()
				notified := cache.readyTimer != nil || cache.readyWatched || cache.ready.size != 0
				cache.mu.Unlock()
				if notified {
					t.Fatal("empty budget boundary entered the shared ready path")
				}
			}
		})
	}
}

func TestExecutorReadyQueueBudgetTransfersClaimsBeforeBlockingOrClose(t *testing.T) {
	for _, mode := range []string{"blocking-ready", "close"} {
		t.Run(mode, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(2, 2)
			var targets [3]PID
			var counts [3]atomic.Int32
			var handled atomic.Int32
			boundary := make(chan *executorTurn, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			result := make(chan error, 2)
			for index := range targets {
				targets[index] = spawn(t, n, factory(func(a *testBehavior, message any) error {
					switch message {
					case "barrier":
						return a.Reply(counts[index].Load())
					case "step":
						counts[index].Add(1)
						count := handled.Add(1)
						// Close may dispatch the terminal claim via p.run, which
						// needs no executor turn because it sends no further work.
						if count >= 65 {
							if count == 65 {
								result <- nil
							} else {
								result <- errors.New("run-next claim executed twice")
							}
							return nil
						}
						turn, err := untimedTurn(a.proc)
						if err != nil {
							result <- err
							return nil
						}
						if err := a.Send(targets[(index+1)%len(targets)], "step"); err != nil {
							result <- err
							return nil
						}
						if count == 64 {
							if !turn.pending.Load() {
								result <- errors.New("budget boundary did not hold a next claim")
								return nil
							}
							boundary <- turn
							<-release
						}
					}
					return nil
				}))
			}
			shard := targets[0].ID % uint64(len(n.executors.shards))
			if targets[1].ID%uint64(len(n.executors.shards)) == shard {
				t.Fatal("boundary target did not cross PID shards")
			}
			cache := &n.executors.shards[shard]
			var queued []PID
			var queuedCounts [2]atomic.Int32
			firstTurn := make(chan *executorTurn, 1)
			secondRan, firstRelease := make(chan struct{}), make(chan struct{})
			unblockFirst := sync.OnceFunc(func() { close(firstRelease) })
			defer unblockFirst()
			for len(queued) < len(queuedCounts) {
				index := len(queued)
				pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
					switch message {
					case "work":
						queuedCounts[index].Add(1)
						if index == 0 {
							firstTurn <- a.proc.executor.Load()
							<-firstRelease
						} else {
							close(secondRan)
						}
					case "barrier":
						return a.Reply(queuedCounts[index].Load())
					}
					return nil
				}))
				if pid.ID%uint64(len(n.executors.shards)) == shard {
					queued = append(queued, pid)
				}
			}
			for i := range n.executors.shards {
				idleExecutorReady(t, &n.executors.shards[i])
			}
			if err := n.Send(targets[0], "step"); err != nil {
				t.Fatal(err)
			}
			var turn *executorTurn
			select {
			case turn = <-boundary:
			case <-time.After(5 * time.Second):
				t.Fatal("chain did not reach its budget boundary")
			}
			resume := pauseExecutorReady(t, cache)
			defer resume()
			for _, pid := range queued {
				if err := n.Send(pid, "work"); err != nil {
					t.Fatal(err)
				}
			}
			closed := make(chan struct{})
			func() {
				cache.mu.Lock()
				defer cache.mu.Unlock()
				if mode == "blocking-ready" {
					// Transfer the notification to the budget rotation itself.
					// It must arm the remaining queue before running its head.
					cache.readyTimer.Stop()
					cache.readyWatched = false
				}
				unblock()
				// The worker must first own next locally, then wait for cache.mu.
				// Close races that rotation, rather than taking an untaken slot.
				eventually(t, func() bool { return !turn.pending.Load() })
				if mode == "close" {
					go func() { cache.close(); close(closed) }()
				}
			}()
			if mode == "close" {
				signal(t, closed)
			}
			select {
			case used := <-firstTurn:
				if mode == "blocking-ready" && used != turn {
					t.Fatal("budget rotation did not claim the FIFO head locally")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("FIFO head did not enter its handler")
			}
			// Both the other FIFO claim and the rotated cross-shard next must
			// run while the selected FIFO head is still blocked in user code.
			signal(t, secondRan)
			nextResult(t, result)
			unblockFirst()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for i, pid := range targets {
				value, err := n.Call[int32](ctx, pid, "barrier")
				if want := int32((67 - i) / len(targets)); err != nil || value != want {
					t.Fatalf("chain execution claims: got %d, want %d: %v", value, want, err)
				}
			}
			for _, pid := range queued {
				value, err := n.Call[int32](ctx, pid, "barrier")
				if err != nil || value != 1 {
					t.Fatalf("FIFO execution claim ran %d times, want 1: %v", value, err)
				}
			}
		})
	}
}
