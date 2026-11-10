package actor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutorStartBypassesReadyCallback(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		name := "new-worker"
		if reuse {
			name = "idle-worker"
		}
		t.Run(name, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(2, 1)
			cache := &n.executors.shards[0]
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			owner := spawn(t, n, factory(func(_ *testBehavior, message any) error {
				if message == "block" {
					close(entered)
					<-release
				}
				return nil
			}))
			idleExecutorReady(t, cache)
			if err := n.Send(owner, "block"); err != nil {
				t.Fatal(err)
			}
			signal(t, entered)
			var warmed *executorTurn
			if reuse {
				turns := make(chan *executorTurn, 1)
				spawn(t, n, factory(func(a *testBehavior, message any) error {
					if _, ok := message.(Started); ok {
						turns <- a.proc.executor.Load()
					}
					return nil
				}))
				warmed = <-turns
			}
			eventually(t, func() bool {
				cache.mu.Lock()
				defer cache.mu.Unlock()
				idle := 0
				if reuse {
					idle = 1
				}
				return cache.busy == 1 && len(cache.idle) == idle && !cache.readyWatched
			})
			resume := pauseExecutorReady(t, cache)
			defer resume()
			var starts atomic.Int32
			started := make(chan *executorTurn, 1)
			ordinary := make(chan struct{}, 1)
			type spawnResult struct {
				pid PID
				err error
			}
			spawned := make(chan spawnResult, 1)
			go func() {
				pid, err := n.Spawn(factory(func(a *testBehavior, message any) error {
					switch message {
					case "ordinary":
						ordinary <- struct{}{}
					default:
						if _, ok := message.(Started); ok {
							starts.Add(1)
							started <- a.proc.executor.Load()
						}
					}
					return nil
				}))
				spawned <- spawnResult{pid, err}
			}()
			var child PID
			select {
			case result := <-spawned:
				if result.err != nil {
					t.Fatal(result.err)
				}
				child = result.pid
			case <-time.After(5 * time.Second):
				t.Fatal("Started waited for the disabled ready callback")
			}
			if used := <-started; reuse && used != warmed {
				t.Fatal("Started did not reuse the available idle worker")
			}
			eventually(t, func() bool {
				cache.mu.Lock()
				defer cache.mu.Unlock()
				return cache.busy == 1 && len(cache.idle) == 1
			})
			if err := n.Send(child, "ordinary"); err != nil {
				t.Fatal(err)
			}
			cache.mu.Lock()
			queued := cache.ready.size
			cache.mu.Unlock()
			if queued != 1 {
				t.Fatal("ordinary message bypassed the ready queue with a busy worker")
			}
			resume()
			signal(t, ordinary)
			if starts.Load() != 1 {
				t.Fatal("initial execution claim ran Started more than once")
			}
		})
	}
}

func TestExecutorStartNestedSpawnWithoutReadyCallback(t *testing.T) {
	for _, fromStarted := range []bool{true, false} {
		name := "started"
		if !fromStarted {
			name = "receive"
		}
		t.Run(name, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(1, 1)
			cache := &n.executors.shards[0]
			result := make(chan error, 1)
			var leaves atomic.Int32
			child := factory(func(a *testBehavior, message any) error {
				if _, ok := message.(Started); !ok {
					return nil
				}
				_, err := a.Spawn(factory(func(_ *testBehavior, message any) error {
					if _, ok := message.(Started); ok {
						leaves.Add(1)
					}
					return nil
				}))
				return err
			})
			parent := factory(func(a *testBehavior, message any) error {
				_, starting := message.(Started)
				if fromStarted && starting || !fromStarted && message == "spawn" {
					_, err := a.Spawn(child)
					result <- err
				}
				return nil
			})
			idleExecutorReady(t, cache)
			resume := pauseExecutorReady(t, cache)
			defer resume()
			spawned := make(chan error, 1)
			go func() {
				pid, err := n.Spawn(parent)
				if err == nil && !fromStarted {
					err = n.Send(pid, "spawn")
				}
				spawned <- err
			}()
			nextResult(t, result)
			nextResult(t, spawned)
			if leaves.Load() != 1 {
				t.Fatal("nested synchronous Spawn did not start exactly one leaf")
			}
		})
	}
}

func TestExecutorStartSupervisionReplacementBypassesReadyCallback(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(2, 1)
	cache := &n.executors.shards[0]
	var starts atomic.Int32
	children := make(chan PID, 2)
	stopping, stopRelease := make(chan struct{}), make(chan struct{})
	unblockStop := sync.OnceFunc(func() { close(stopRelease) })
	defer unblockStop()
	child := func() Behavior {
		var generation int32
		return &testBehavior{receive: func(a *testBehavior, message any) error {
			switch message.(type) {
			case Started:
				generation = starts.Add(1)
				children <- a.PID()
			case Stopped:
				if generation == 1 {
					close(stopping)
					<-stopRelease
				}
			default:
				if message == "fail" {
					return errors.New("restart child")
				}
			}
			return nil
		}}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	parent := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if _, ok := message.(Started); ok {
			_, err := a.Spawn(child, WithName("child"), WithRestart(Permanent))
			return err
		}
		if message == "hold" {
			close(entered)
			<-release
		}
		return nil
	}))
	oldPID := <-children
	old, _, _ := n.resolve(oldPID)
	parentProcess, _, _ := n.resolve(parent)
	idleExecutorReady(t, cache)
	if err := n.Send(parent, "hold"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	if err := n.Send(oldPID, "fail"); err != nil {
		t.Fatal(err)
	}
	signal(t, stopping)
	eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.ready.size == 0 && !cache.readyWatched
	})
	resume := pauseExecutorReady(t, cache)
	defer resume()
	unblockStop()
	signal(t, old.done)
	eventually(t, func() bool {
		parentProcess.mu.Lock()
		defer parentProcess.mu.Unlock()
		return len(parentProcess.system) != 0
	})
	unblock()
	select {
	case replacement := <-children:
		if replacement == oldPID || starts.Load() != 2 {
			t.Fatal("supervision did not start exactly one new process")
		}
		eventually(t, func() bool {
			_, err := n.resolvePID(replacement)
			return err == nil
		})
	case <-time.After(5 * time.Second):
		t.Fatal("supervision replacement waited for the disabled ready callback")
	}
}

func TestExecutorStartCloseKeepsInitialClaim(t *testing.T) {
	t.Run("closed-cache", func(t *testing.T) {
		n := testNode(t, NodeOptions{})
		n.executors.init(1, 1)
		n.executors.close()
		var starts atomic.Int32
		pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
			if _, ok := message.(Started); ok {
				starts.Add(1)
			}
			if message == "count" {
				return a.Reply(starts.Load())
			}
			return nil
		}))
		value, err := n.Call[int32](t.Context(), pid, "count")
		if err != nil || value != 1 {
			t.Fatalf("closed cache initial claim: %d %v", value, err)
		}
	})
	t.Run("node-stop", func(t *testing.T) {
		n := testNode(t, NodeOptions{})
		n.executors.init(1, 1)
		cache := &n.executors.shards[0]
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		var starts, stops atomic.Int32
		spawned := make(chan error, 1)
		go func() {
			_, err := n.Spawn(factory(func(_ *testBehavior, message any) error {
				switch message.(type) {
				case Started:
					starts.Add(1)
					close(entered)
					<-release
				case Stopped:
					stops.Add(1)
				}
				return nil
			}))
			spawned <- err
		}()
		signal(t, entered)
		stopped := make(chan error, 1)
		go func() { stopped <- n.Stop(t.Context()) }()
		eventually(t, func() bool {
			cache.mu.Lock()
			defer cache.mu.Unlock()
			return cache.closed
		})
		unblock()
		nextResult(t, stopped)
		select {
		case err := <-spawned:
			if !errors.Is(err, ErrStopped) {
				t.Fatalf("concurrent Stop startup result: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Stop lost the initial execution claim")
		}
		if starts.Load() != 1 || stops.Load() != 1 {
			t.Fatalf("startup callbacks after Stop: %d Started, %d Stopped", starts.Load(), stops.Load())
		}
	})
}
