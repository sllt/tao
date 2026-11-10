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

// Hold the independent callback only after it has no notification to own.
// Workers can still drain the FIFO. Tests release this hold before cleanup.
func pauseExecutorReady(t *testing.T, cache *executorCache) func() {
	t.Helper()
	cache.mu.Lock()
	if cache.ready.size != 0 || cache.readyWatched {
		cache.mu.Unlock()
		t.Fatal("ready callback was not quiescent")
	}
	if cache.readyTimer != nil {
		cache.readyTimer.Stop()
	}
	cache.readyTimer = time.AfterFunc(time.Hour, cache.watchReady)
	cache.readyWatched = true
	cache.mu.Unlock()
	return sync.OnceFunc(func() {
		cache.mu.Lock()
		cache.readyTimer.Stop()
		cache.readyWatched = false
		if !cache.closed && cache.ready.size != 0 {
			cache.watchReadyLocked()
		}
		cache.mu.Unlock()
	})
}

func idleExecutorReady(t *testing.T, cache *executorCache) {
	t.Helper()
	eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.busy == 0 && cache.ready.size == 0 && !cache.readyWatched
	})
}

func TestExecutorReadyQueueAwakeWorkerDrainsAndReusesTurn(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
	cache := &n.executors.shards[0]
	type block struct {
		entered chan *executorTurn
		release chan struct{}
	}
	owner := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if msg, ok := message.(block); ok {
			msg.entered <- a.proc.executor.Load()
			<-msg.release
		}
		return nil
	}))
	const actors, messages = 48, 3
	turns := make(chan *executorTurn, actors*messages)
	var targets [actors]PID
	for i := range targets {
		targets[i] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			if message == "work" {
				turns <- a.proc.executor.Load()
			}
			return nil
		}))
	}
	// The second burst wraps the retained FIFO storage. Repeated messages
	// must retain only one queued execution claim for each Actor.
	for range 2 {
		idleExecutorReady(t, cache)
		entered, release := make(chan *executorTurn, 1), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		if err := n.Send(owner, block{entered, release}); err != nil {
			t.Fatal(err)
		}
		var turn *executorTurn
		select {
		case turn = <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("owner did not enter its handler")
		}
		resume := pauseExecutorReady(t, cache)
		defer resume()
		for _, pid := range targets {
			for range messages {
				if err := n.Send(pid, "work"); err != nil {
					t.Fatal(err)
				}
			}
		}
		cache.mu.Lock()
		queued := cache.ready.size
		cache.mu.Unlock()
		if queued != actors {
			t.Fatalf("queued %d execution claims for %d Actors", queued, actors)
		}
		unblock()
		for range actors * messages {
			select {
			case used := <-turns:
				if used != turn {
					t.Fatal("ready queue required a different worker handoff")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("awake worker did not drain the ready queue")
			}
		}
		resume()
	}
}

func TestExecutorReadyQueueProgressWithAllWorkersBlocked(t *testing.T) {
	for _, blocking := range []string{"channel", "mutex", "io", "actor-call", "node-call"} {
		t.Run(blocking, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(1, 1)
			var barrier sync.Mutex
			if blocking == "mutex" {
				barrier.Lock()
			}
			done := make(chan struct{})
			finish := sync.OnceFunc(func() {
				if blocking == "mutex" {
					barrier.Unlock()
				}
				close(done)
			})
			defer finish()
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			target := spawn(t, n, factory(func(_ *testBehavior, message any) error {
				if message == "release" {
					if blocking == "io" {
						if _, err := writer.Write([]byte{1}); err != nil {
							return err
						}
					}
					finish()
				}
				return nil
			}))
			callee := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message == "ask" {
					<-done
					return a.Reply(42)
				}
				return nil
			}))
			result := make(chan error, 1)
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message != "go" {
					return nil
				}
				// Node.Send deliberately bypasses turn.next: progress must be
				// protected for the shared FIFO, not just the run-next slot.
				if err := n.Send(target, "release"); err != nil {
					result <- err
					return nil
				}
				var err error
				switch blocking {
				case "channel":
					<-done
				case "mutex":
					barrier.Lock()
					barrier.Unlock()
				case "io":
					var value [1]byte
					_, err = io.ReadFull(reader, value[:])
				case "actor-call":
					var value any
					value, err = a.Call(t.Context(), callee, "ask")
					if err == nil && value != 42 {
						err = errors.New("wrong Actor.Call result")
					}
				case "node-call":
					var value int
					value, err = n.Call[int](t.Context(), callee, "ask")
					if err == nil && value != 42 {
						err = errors.New("wrong Node.Call result")
					}
				}
				result <- err
				return nil
			}))
			entered, release := make(chan struct{}, 8), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			for range cap(entered) {
				pid := spawn(t, n, factory(func(_ *testBehavior, message any) error {
					if message == "block" {
						entered <- struct{}{}
						<-release
					}
					return nil
				}))
				if err := n.Send(pid, "block"); err != nil {
					t.Fatal(err)
				}
				signal(t, entered)
			}
			if err := n.Send(source, "go"); err != nil {
				t.Fatal(err)
			}
			nextResult(t, result)
		})
	}
}

func TestExecutorReadyQueueCallbackReleasesRemainingBacklog(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
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
	firstEntered, firstRelease := make(chan struct{}), make(chan struct{})
	unblockFirst := sync.OnceFunc(func() { close(firstRelease) })
	defer unblockFirst()
	first := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			close(firstEntered)
			<-firstRelease
		}
		return nil
	}))
	secondRan := make(chan struct{})
	second := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			close(secondRan)
		}
		return nil
	}))
	idleExecutorReady(t, cache)
	if err := n.Send(owner, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	resume := pauseExecutorReady(t, cache)
	defer resume()
	for _, pid := range []PID{first, second} {
		if err := n.Send(pid, "work"); err != nil {
			t.Fatal(err)
		}
	}
	go cache.watchReady()
	signal(t, firstEntered)
	signal(t, secondRan) // neither the owner nor the callback's first task returned
}

func TestExecutorReadyQueueRunNextBudgetServesFIFO(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
	cache := &n.executors.shards[0]
	var targets [2]PID
	var handled atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	result := make(chan error, 1)
	for index := range targets {
		targets[index] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			if message != "step" {
				return nil
			}
			if _, err := untimedTurn(a.proc); err != nil {
				result <- err
				return nil
			}
			count := handled.Add(1)
			if count == 1 {
				close(entered)
				<-release
			}
			if count == 130 {
				result <- nil
				return nil
			}
			if err := a.Send(targets[1-index], "step"); err != nil {
				result <- err
			}
			return nil
		}))
	}
	observed := make(chan int32, 1)
	observer := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "observe" {
			observed <- handled.Load()
		}
		return nil
	}))
	idleExecutorReady(t, cache)
	if err := n.Send(targets[0], "step"); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	resume := pauseExecutorReady(t, cache)
	defer resume()
	if err := n.Send(observer, "observe"); err != nil {
		t.Fatal(err)
	}
	unblock()
	nextResult(t, result)
	select {
	case count := <-observed:
		if count != 64 {
			t.Fatalf("FIFO ran after %d run-next turns, want 64", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run-next chain did not serve the FIFO")
	}
}

func TestExecutorReadyQueueNodeCloseDrainsClaims(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
	cache := &n.executors.shards[0]
	const actors = 32
	ran := make(chan struct{}, actors)
	var targets [actors]PID
	for i := range targets {
		targets[i] = spawn(t, n, factory(func(_ *testBehavior, message any) error {
			if message == "work" {
				ran <- struct{}{}
			}
			return nil
		}))
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	// The owner is the last child: Node.Stop waits for it before stopping
	// the queued targets, making their work observable during cache close.
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
	resume := pauseExecutorReady(t, cache)
	defer resume()
	for _, pid := range targets {
		if err := n.Send(pid, "work"); err != nil {
			t.Fatal(err)
		}
	}
	stopped := make(chan error, 1)
	go func() { stopped <- n.Stop(t.Context()) }()
	for range actors {
		signal(t, ran)
	}
	cache.mu.Lock()
	drained := cache.closed && cache.ready.size == 0 && len(cache.idle) == 0
	cache.mu.Unlock()
	if !drained {
		t.Fatal("node close retained queued or idle claims")
	}
	unblock()
	nextResult(t, stopped)
}

func TestExecutorReadyQueueClosePreservesClaimedIdleHandoff(t *testing.T) {
	n := testNode(t, NodeOptions{})
	ran := make(chan struct{})
	pid := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			close(ran)
		}
		return nil
	}))
	p, _, _ := n.resolve(pid)
	eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
	jobs := make(chan *process, 1)
	cache := &executorCache{maxIdle: 1, idle: []chan *process{jobs}}
	turn := &executorTurn{cache: cache, timer: time.AfterFunc(time.Hour, func() {}), watched: true}
	source := &process{}
	source.executor.Store(turn)
	if err := p.enqueueFrom(envelope{message: "work"}, nil, source); err != nil {
		t.Fatal(err)
	}
	claimed := turn.take()
	if claimed != p {
		t.Fatal("failed to obtain the Actor's execution claim")
	}
	cache.submit(claimed)
	cache.close() // the claimed channel still contains work, not an idle worker
	received, ok := <-jobs
	if !ok || received != p {
		t.Fatal("close discarded a handoff already owned by a waking worker")
	}
	go cache.run(received)
	signal(t, ran)
	eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.busy == 0
	})
}

func TestExecutorReadyQueueWorkerWatchAndCloseHaveOneClaim(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
	cache := &n.executors.shards[0]
	const actors = 64
	var counts [actors]atomic.Int32
	var targets [actors]PID
	for i := range targets {
		targets[i] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			switch message {
			case "work":
				counts[i].Add(1)
			case "barrier":
				return a.Reply(counts[i].Load())
			}
			return nil
		}))
	}
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
	resume := pauseExecutorReady(t, cache)
	defer resume()
	for _, pid := range targets {
		if err := n.Send(pid, "work"); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var racers sync.WaitGroup
	racers.Go(func() { <-start; unblock() })
	racers.Go(func() { <-start; cache.watchReady() })
	racers.Go(func() { <-start; cache.close() })
	close(start)
	racers.Wait()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, pid := range targets {
		value, err := n.Call[int32](ctx, pid, "barrier")
		if err != nil || value != 1 {
			t.Fatalf("queued execution claim ran %d times, want 1: %v", value, err)
		}
	}
}
