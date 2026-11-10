package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutorReadyBatchDispatchesIdleBeforeBlockedBacklog(t *testing.T) {
	n := testNode(t, NodeOptions{})
	const idleWorkers, tasks = 5, 6
	n.executors.init(idleWorkers, 1)
	cache := &n.executors.shards[0]
	ownerEntered, ownerRelease := make(chan struct{}), make(chan struct{})
	unblockOwner := sync.OnceFunc(func() { close(ownerRelease) })
	defer unblockOwner()
	owner := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		if message == "block" {
			close(ownerEntered)
			<-ownerRelease
		}
		return nil
	}))
	warmed, warmRelease := make(chan struct{}, idleWorkers), make(chan struct{})
	unblockWarm := sync.OnceFunc(func() { close(warmRelease) })
	defer unblockWarm()
	entered, workRelease := make(chan int, tasks), make(chan struct{})
	unblockWork := sync.OnceFunc(func() { close(workRelease) })
	defer unblockWork()
	var counts [tasks]atomic.Int32
	var targets [tasks]PID
	for i := range targets {
		targets[i] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			switch message {
			case "warm":
				warmed <- struct{}{}
				<-warmRelease
			case "work":
				counts[i].Add(1)
				entered <- i
				if i < tasks-1 {
					<-workRelease
				}
			case "barrier":
				return a.Reply(counts[i].Load())
			}
			return nil
		}))
	}
	idleExecutorReady(t, cache)
	if err := n.Send(owner, "block"); err != nil {
		t.Fatal(err)
	}
	signal(t, ownerEntered)
	for _, pid := range targets[:idleWorkers] {
		if err := n.Send(pid, "warm"); err != nil {
			t.Fatal(err)
		}
	}
	for range idleWorkers {
		signal(t, warmed)
	}
	unblockWarm()
	eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.busy == 1 && len(cache.idle) == idleWorkers && cache.ready.size == 0 && !cache.readyWatched
	})
	// Hold the next independent notification to inspect exactly one callback.
	// Its real watch resumes after four already-idle workers have their claims.
	notified, notifyRelease := make(chan struct{}), make(chan struct{})
	notify := sync.OnceFunc(func() { close(notified) })
	unblockNotify := sync.OnceFunc(func() { close(notifyRelease) })
	defer unblockNotify()
	cache.mu.Lock()
	if cache.readyTimer != nil {
		cache.readyTimer.Stop()
	}
	cache.readyTimer = time.AfterFunc(time.Hour, func() {
		notify()
		<-notifyRelease
		cache.watchReady()
	})
	cache.readyWatched = true
	cache.mu.Unlock()
	for _, pid := range targets {
		if err := n.Send(pid, "work"); err != nil {
			t.Fatal(err)
		}
	}
	cache.watchReady()
	mask := 0
	for range 4 {
		select {
		case index := <-entered:
			mask |= 1 << index
		case <-time.After(5 * time.Second):
			t.Fatal("one ready callback did not release its batch of idle workers")
		}
	}
	signal(t, notified)
	cache.mu.Lock()
	queued, idle, busy := cache.ready.size, len(cache.idle), cache.busy
	cache.mu.Unlock()
	if mask != 15 || queued != 2 || idle != 1 || busy != 5 {
		t.Fatalf("batch claims: mask=%b queued=%d idle=%d busy=%d", mask, queued, idle, busy)
	}
	unblockNotify()
	for range 2 {
		select {
		case index := <-entered:
			mask |= 1 << index
		case <-time.After(5 * time.Second):
			t.Fatal("blocked batch handlers prevented independent backlog progress")
		}
	}
	if mask != 63 {
		t.Fatalf("backlog lost or duplicated an execution claim: mask=%b", mask)
	}
	unblockWork()
	unblockOwner()
	for _, pid := range targets {
		value, err := n.Call[int32](t.Context(), pid, "barrier")
		if err != nil || value != 1 {
			t.Fatalf("batched Actor handled %d messages, want 1: %v", value, err)
		}
	}
}

func TestExecutorReadyBatchClosePreservesClaimedHandoffs(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(1, 1)
	const idleWorkers, tasks = 5, 6
	var counts [tasks]atomic.Int32
	var targets [tasks]PID
	seen := make(chan struct{}, tasks)
	for i := range targets {
		targets[i] = spawn(t, n, factory(func(a *testBehavior, message any) error {
			switch message {
			case "work":
				counts[i].Add(1)
				seen <- struct{}{}
			case "barrier":
				return a.Reply(counts[i].Load())
			}
			return nil
		}))
	}
	idleExecutorReady(t, &n.executors.shards[0])
	// These parked-worker channels deliberately have no receiver until after
	// Close. The callback transfers claims into their one-entry handoff slots.
	cache := &executorCache{maxIdle: idleWorkers, readyWatched: true, readyTimer: time.AfterFunc(time.Hour, func() {})}
	defer cache.close()
	var channels [idleWorkers]chan *process
	for i := range channels {
		channels[i] = make(chan *process, 1)
		cache.idle = append(cache.idle, channels[i])
	}
	turn := &executorTurn{cache: cache, timer: time.AfterFunc(time.Hour, func() {}), watched: true}
	source := &process{}
	source.executor.Store(turn)
	source.behavior = &testBehavior{receive: func(_ *testBehavior, message any) error {
		return message.(*process).enqueueFrom(envelope{message: "work"}, nil, source)
	}}
	for _, pid := range targets {
		p, _, err := n.resolve(pid)
		if err != nil {
			t.Fatal(err)
		}
		// Each simulated Receive offers exactly one claim. Use invoke's real
		// origin cleanup before moving that retained claim to the shared FIFO.
		if err := source.invoke(envelope{message: p}); err != nil {
			t.Fatal(err)
		}
		if turn.offeredInReceive || !turn.pending.Load() {
			t.Fatal("Receive origin cleanup lost or retained the wrong claim state")
		}
		claim := turn.take()
		if claim != p {
			t.Fatal("failed to obtain one initial execution claim")
		}
		cache.mu.Lock()
		cache.ready.push(claim)
		cache.mu.Unlock()
	}
	cache.watchReady()
	cache.close()
	claimed, closed := 0, 0
	for _, jobs := range channels {
		select {
		case p, ok := <-jobs:
			if !ok {
				closed++
				continue
			}
			if p == nil {
				t.Error("claimed handoff contained no execution claim")
				continue
			}
			claimed++
			go cache.run(p)
		default:
			t.Error("Close left an empty open idle channel")
		}
	}
	if claimed != 4 || closed != 1 {
		t.Errorf("Close retained %d claimed handoffs and closed %d idle channels, want 4 and 1", claimed, closed)
	}
	for range tasks {
		signal(t, seen)
	}
	for _, pid := range targets {
		value, err := n.Call[int32](t.Context(), pid, "barrier")
		if err != nil || value != 1 {
			t.Fatalf("Close execution claim ran %d times, want 1: %v", value, err)
		}
	}
	eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return cache.busy == 0
	})
}
