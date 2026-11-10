package actor

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Explicit handoff tests park a started callback until Node.Close. Stop then
// fails, so flush cannot rearm a replacement behind this fixture. Close still
// drains the pending slot before the original watch finishes its cleanup.
func untimedTurn(p *process) (*executorTurn, error) {
	turn := p.executor.Load()
	if turn == nil {
		return nil, errors.New("missing executor turn")
	}
	var started <-chan struct{}
	turn.mu.Lock()
	if turn.timer == nil {
		entered, closed := make(chan struct{}), p.node.Done()
		turn.timer = time.AfterFunc(0, func() {
			close(entered)
			<-closed
			turn.watch()
		})
		turn.watched = true
		started = entered
	}
	turn.mu.Unlock()
	if started != nil {
		<-started
	}
	return turn, nil
}

func nextResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runnable Actor did not make progress")
	}
}

func TestRunNextLocalHandoff(t *testing.T) {
	n := testNode(t, NodeOptions{})
	type step struct{ count int }
	var targets [2]PID
	var mu sync.Mutex
	var turns []*executorTurn
	result := make(chan error, 1)
	for index := range targets {
		targets[index] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			msg, ok := message.(step)
			if !ok {
				return nil
			}
			turn, err := untimedTurn(a.proc)
			if err != nil {
				result <- err
				return nil
			}
			mu.Lock()
			turns = append(turns, turn)
			mu.Unlock()
			if msg.count == 200 {
				result <- nil
				return nil
			}
			if err := a.Send(targets[1-index], step{msg.count + 1}); err != nil {
				result <- err
			}
			return nil
		}))
	}
	for _, pid := range targets {
		p, _, _ := n.resolve(pid)
		eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
	}
	if err := n.Send(targets[0], step{1}); err != nil {
		t.Fatal(err)
	}
	nextResult(t, result)
	mu.Lock()
	defer mu.Unlock()
	if len(turns) != 200 || turns[0] != turns[1] {
		t.Fatal("short handoff did not reuse the executor")
	}
}

func TestRunNextSecondSendReleasesTargetAndClearsCache(t *testing.T) {
	n := testNode(t, NodeOptions{})
	seen := make(chan struct{}, 4)
	targetPID := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			seen <- struct{}{}
		}
		return nil
	}))
	target, _, _ := n.resolve(targetPID)
	eventually(t, func() bool { target.mu.Lock(); defer target.mu.Unlock(); return !target.running })
	returned := make(chan error, 1)
	var used *executorTurn
	sourcePID := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message != "go" {
			return nil
		}
		var err error
		used, err = untimedTurn(a.proc)
		if err != nil {
			returned <- err
			return nil
		}
		if err = a.Send(targetPID, "work"); err != nil {
			returned <- err
			return nil
		}
		if !used.pending.Load() || used.target != nil || !used.offeredInReceive {
			returned <- errors.New("first send did not use next or allocated a cache entry")
			return nil
		}
		if err = a.Send(targetPID, "work"); err != nil {
			returned <- err
			return nil
		}
		if used.pending.Load() || used.target != target || !used.multiSend || !used.offeredInReceive {
			returned <- errors.New("second send retained next or lost its cache")
			return nil
		}
		<-seen
		<-seen // timer disabled: only the second Send can release this target
		if err = n.StopActor(a.Context(), targetPID); err != nil {
			returned <- err
			return nil
		}
		if err = a.Send(targetPID, "work"); !errors.Is(err, ErrNoProcess) {
			returned <- errors.New("cached target bypassed PID revocation")
			return nil
		}
		returned <- nil
		return nil
	}))
	if err := n.Send(sourcePID, "go"); err != nil {
		t.Fatal(err)
	}
	nextResult(t, returned)
	source, _, _ := n.resolve(sourcePID)
	eventually(t, func() bool { source.mu.Lock(); defer source.mu.Unlock(); return !source.running })
	// The idle publication follows invoke's cache clear. No more work is sent
	// to this Node until the worker-owned fields have been inspected.
	if used.target != nil || used.multiSend || used.offeredInReceive {
		t.Fatal("Receive retained its destination cache")
	}
}

func TestRunNextReceiveOriginClearsOnReturnAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		name := "return"
		if panics {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			turn := &executorTurn{cache: &executorCache{}, timer: time.AfterFunc(time.Hour, func() {}), watched: true}
			defer func() {
				turn.take() // The isolated claim has no Node to schedule during close.
				turn.close()
			}()
			target := new(process)
			p := new(process)
			p.executor.Store(turn)
			p.behavior = &testBehavior{receive: func(_ *testBehavior, _ any) error {
				if !turn.offer(target) || !turn.offeredInReceive {
					return errors.New("successful offer did not mark its Receive origin")
				}
				if panics {
					panic("test Receive panic")
				}
				return nil
			}}
			err := p.invoke(envelope{message: "offer"})
			if (err != nil) != panics {
				t.Fatalf("Receive result: %v", err)
			}
			if turn.offeredInReceive || turn.multiSend || turn.target != nil || !turn.pending.Load() {
				t.Fatal("Receive did not clear only its worker hints while retaining next")
			}
			if got := turn.take(); got != target {
				t.Fatal("clearing Receive origin lost the pending execution claim")
			}
		})
	}
}

func TestRunNextLongChainLetsIndependentActorProgress(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	n := testNode(t, NodeOptions{})
	var targets [2]PID
	var handled atomic.Int32
	started := make(chan struct{})
	result := make(chan error, 1)
	const limit = 100000
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
				close(started)
			}
			if count == limit {
				result <- nil
				return nil
			}
			if err := a.Send(targets[1-index], "step"); err != nil {
				result <- err
			}
			return nil
		}))
	}
	progress := make(chan int32, 1)
	independent := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "observe" {
			progress <- handled.Load()
		}
		return nil
	}))
	for _, pid := range targets {
		p, _, _ := n.resolve(pid)
		eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
	}
	go func() { <-started; _ = n.Send(independent, "observe") }()
	if err := n.Send(targets[0], "step"); err != nil {
		t.Fatal(err)
	}
	nextResult(t, result)
	select {
	case count := <-progress:
		if count >= limit {
			t.Fatal("independent Actor waited for the whole chain")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("independent Actor did not run")
	}
}

func TestRunNextExplicitActorOperationsReleaseSlot(t *testing.T) {
	for _, operation := range []string{"call", "call-canceled", "spawn", "spawn-invalid", "spawn-kind", "reply", "reply-error", "monitor", "demonitor", "link", "unlink", "stop"} {
		t.Run(operation, func(t *testing.T) {
			done := make(chan struct{})
			child := factory(func(_ *testBehavior, message any) error {
				if _, ok := message.(Started); ok {
					<-done
				}
				return nil
			})
			n := testNode(t, NodeOptions{Kinds: []KindSpec{{Name: "child", Version: "1", Factory: child}}})
			target := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "ping":
					close(done)
				case "ask":
					return a.Reply(42)
				}
				return nil
			}))
			targetProcess, _, _ := n.resolve(target)
			eventually(t, func() bool { targetProcess.mu.Lock(); defer targetProcess.mu.Unlock(); return !targetProcess.running })
			returned := make(chan error, 1)
			failure := errors.New("business result")
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message != "go" {
					return nil
				}
				turn, err := untimedTurn(a.proc)
				if err == nil {
					err = a.Send(target, "ping")
				}
				if err != nil {
					returned <- err
					return nil
				}
				if !turn.pending.Load() {
					returned <- errors.New("test did not hold a pending next slot")
					return nil
				}
				switch operation {
				case "call":
					var value any
					value, err = a.Call(a.Context(), target, "ask")
					if err == nil && value != 42 {
						err = errors.New("wrong Call result")
					}
				case "call-canceled":
					ctx, cancel := context.WithCancel(a.Context())
					cancel()
					_, err = a.Call(ctx, target, "ask")
					if errors.Is(err, context.Canceled) {
						err = nil
					} else {
						err = errors.New("Call did not preserve canceled context")
					}
				case "spawn":
					_, err = a.Spawn(child)
				case "spawn-invalid":
					_, err = a.Spawn(nil)
					if err != nil {
						err = nil
					} else {
						err = errors.New("nil factory was accepted")
					}
				case "spawn-kind":
					_, err = a.SpawnKind("child", "1")
				case "reply":
					err = a.Reply(42)
				case "reply-error":
					err = a.ReplyError(failure)
				case "monitor":
					_, err = a.Monitor(target)
				case "demonitor":
					a.Demonitor(MonitorRef{})
				case "link":
					err = a.Link(a.Context(), target)
				case "unlink":
					a.Unlink(target)
				case "stop":
					a.Stop(nil)
				}
				if err != nil {
					returned <- err
					return nil
				}
				<-done // Without explicit flush the disabled watchdog cannot help.
				returned <- nil
				return nil
			}), WithRestart(Temporary))
			if operation == "reply" || operation == "reply-error" {
				value, err := n.Call[int](t.Context(), source, "go")
				if operation == "reply" && (err != nil || value != 42) {
					t.Fatalf("reply: %d %v", value, err)
				}
				if operation == "reply-error" && !errors.Is(err, failure) {
					t.Fatal(err)
				}
			} else if err := n.Send(source, "go"); err != nil {
				t.Fatal(err)
			}
			nextResult(t, returned)
		})
	}
}

func TestRunNextWatchdogProgressWithAllWorkersBlocked(t *testing.T) {
	for _, blocking := range []string{"channel", "mutex", "node-call"} {
		t.Run(blocking, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(1, 1)
			var barrier sync.Mutex
			if blocking == "mutex" {
				barrier.Lock()
			}
			done := make(chan struct{})
			target := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "ping":
					if blocking == "mutex" {
						barrier.Unlock()
					}
					close(done)
				case "ask":
					return a.Reply(42)
				}
				return nil
			}))
			entered, release := make(chan struct{}, 8), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			for range 8 {
				pid := spawn(t, n, factory(func(_ *testBehavior, message any) error {
					if message == "block" {
						entered <- struct{}{}
						<-release
					}
					return nil
				}))
				_ = n.Send(pid, "block")
				signal(t, entered)
			}
			result := make(chan error, 1)
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message != "go" {
					return nil
				}
				if err := a.Send(target, "ping"); err != nil {
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
				case "node-call":
					// Node APIs have no current-Actor identity. This is deliberately
					// an opaque wait, covered by the independent timed takeover.
					var value int
					value, err = n.Call[int](context.Background(), target, "ask")
					if err == nil && value != 42 {
						err = errors.New("wrong nested result")
					}
				}
				result <- err
				return nil
			}))
			if err := n.Send(source, "go"); err != nil {
				t.Fatal(err)
			}
			nextResult(t, result)
		})
	}
}

func TestRunNextTakeWatchAndCloseHaveOneExecutionClaim(t *testing.T) {
	n := testNode(t, NodeOptions{})
	var count atomic.Int32
	targetPID := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "work" {
			count.Add(1)
		}
		if message == "barrier" {
			return a.Reply(count.Load())
		}
		return nil
	}))
	target, _, _ := n.resolve(targetPID)
	for iteration := int32(1); iteration <= 100; iteration++ {
		eventually(t, func() bool { target.mu.Lock(); defer target.mu.Unlock(); return !target.running })
		turn := &executorTurn{cache: &n.executors.shards[0], timer: time.AfterFunc(time.Hour, func() {}), watched: true}
		source := &process{}
		source.executor.Store(turn)
		if err := target.enqueueFrom(envelope{message: "work"}, nil, source); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var racers sync.WaitGroup
		racers.Go(func() {
			<-start
			if p := turn.take(); p != nil {
				p.schedule()
			}
		})
		racers.Go(func() { <-start; turn.watch() })
		racers.Go(func() { <-start; turn.flush() })
		racers.Go(func() { <-start; turn.close() })
		close(start)
		racers.Wait()
		value, err := n.Call[int32](t.Context(), targetPID, "barrier")
		if err != nil || value != iteration {
			t.Fatalf("execution claim: %d want %d: %v", value, iteration, err)
		}
		if turn.offer(target) {
			t.Fatal("closed turn accepted a new publication")
		}
		turn.cache.mu.Lock()
		delete(turn.cache.turns, turn)
		turn.cache.mu.Unlock()
	}
}

func TestRunNextNodeCloseDrainsPendingSlot(t *testing.T) {
	n := testNode(t, NodeOptions{})
	targetRan, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	target := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			close(targetRan)
		}
		return nil
	}))
	targetProcess, _, _ := n.resolve(target)
	eventually(t, func() bool { targetProcess.mu.Lock(); defer targetProcess.mu.Unlock(); return !targetProcess.running })
	source := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message != "go" {
			return nil
		}
		turn, err := untimedTurn(a.proc)
		if err != nil {
			return err
		}
		if err := a.Send(target, "work"); err != nil {
			return err
		}
		if !turn.pending.Load() {
			return errors.New("test did not hold a next slot")
		}
		close(entered)
		<-release
		return nil
	}))
	_ = n.Send(source, "go")
	signal(t, entered)
	stopped := make(chan error, 1)
	go func() { stopped <- n.Stop(t.Context()) }()
	signal(t, targetRan) // Cache Close, not the disabled timer, releases the slot.
	unblock()
	nextResult(t, stopped)
}

func TestRunNextDelayedWatchClaimsCurrentPublicationOnce(t *testing.T) {
	n := testNode(t, NodeOptions{})
	seen := make(chan struct{}, 2)
	pid := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "work" {
			seen <- struct{}{}
		}
		return nil
	}))
	p, _, _ := n.resolve(pid)
	turn := &executorTurn{cache: &n.executors.shards[0], timer: time.AfterFunc(time.Hour, func() {}), watched: true}
	defer turn.close()
	source := &process{}
	source.executor.Store(turn)
	for iteration := range 2 {
		eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return !p.running })
		if err := p.enqueueFrom(envelope{message: "work"}, nil, source); err != nil {
			t.Fatal(err)
		}
		if iteration == 1 {
			turn.watch() // A callback armed for the prior publication can claim this one.
			turn.watch()
			if turn.take() != nil {
				t.Fatal("watch left a second execution claim")
			}
		} else {
			claimed := turn.take()
			if claimed != p {
				t.Fatal("publication lost its execution claim")
			}
			turn.mu.Lock()
			watched, active := turn.watched, turn.timer.Reset(time.Hour)
			turn.mu.Unlock()
			if !watched || !active {
				t.Fatal("local taking canceled the reusable short-chain notification")
			}
			claimed.schedule()
		}
		signal(t, seen)
	}
}
