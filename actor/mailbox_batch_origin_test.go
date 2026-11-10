package actor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This regression deliberately uses only the preceding batch-switch fields
// and API, so it can also demonstrate the old cross-Receive premature flush.
func TestPureBatchEveryReceiveSendKeepsNextUntilSwitch(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(2, 1)
	cache := &n.executors.shards[0]
	targetEntered := make(chan *executorTurn, 1)
	targetRelease := make(chan struct{})
	resumeTarget := sync.OnceFunc(func() { close(targetRelease) })
	defer resumeTarget()
	targetSeen := make(chan int, 17)
	var targetCalls atomic.Int32
	target := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "barrier" {
			return a.Reply(targetCalls.Load())
		}
		if value, ok := message.(int); ok {
			targetCalls.Add(1)
			targetSeen <- value
			if value == 0 {
				targetEntered <- a.proc.executor.Load()
				<-targetRelease
			}
		}
		return nil
	}), WithMailboxSize(17))
	gate, gateRelease := make(chan struct{}), make(chan struct{})
	openGate := sync.OnceFunc(func() { close(gateRelease) })
	defer openGate()
	result := make(chan error, 1)
	var sourceTurn *executorTurn
	var sourceCalls atomic.Int32
	source := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "gate" {
			close(gate)
			<-gateRelease
			return nil
		}
		if message == "barrier" {
			return a.Reply(sourceCalls.Load())
		}
		value, ok := message.(int)
		if !ok {
			return nil
		}
		if value == 0 {
			var err error
			sourceTurn, err = untimedTurn(a.proc)
			if err != nil {
				result <- err
				return err
			}
		}
		// Every Receive, including all sixteen detached messages, sends to
		// the same still-running target. There is no one-shot special case.
		if err := a.Send(target, value); err != nil {
			result <- err
			return err
		}
		sourceCalls.Add(1)
		if value == 16 {
			result <- nil
		}
		return nil
	}), WithMailboxSize(17))
	p, _, _ := n.resolve(source)
	idleExecutorReady(t, cache)
	if err := n.Send(source, "gate"); err != nil {
		t.Fatal(err)
	}
	signal(t, gate)
	resumeReady := pauseExecutorReady(t, cache)
	defer resumeReady()
	for value := range 17 {
		if err := n.Send(source, value); err != nil {
			t.Fatal(err)
		}
	}
	openGate()
	select {
	case turn := <-targetEntered:
		if turn != sourceTurn {
			t.Fatal("per-Receive sends did not retain next for the publishing worker")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("sixteen per-Receive sends flushed next before batch switch; source processed %d messages", sourceCalls.Load())
	}
	p.mu.Lock()
	retained := p.running && p.batchState.Load() == 0 && p.mailbox.size == 1
	p.mu.Unlock()
	cache.mu.Lock()
	queued := cache.ready.size == 1 && cache.ready.items[cache.ready.head] == p && cache.readyWatched
	cache.mu.Unlock()
	if !retained || !queued || p.executor.Load() != nil || sourceCalls.Load() != 16 {
		t.Fatal("per-Receive sends did not leave one cleared source claim after sixteen messages")
	}
	resumeReady()
	nextResult(t, result) // Source sends its remaining message while target blocks.
	resumeTarget()
	for value := range 17 {
		nextWork(t, targetSeen, value)
	}
	for _, pid := range []PID{source, target} {
		if count, err := n.Call[int32](t.Context(), pid, "barrier"); err != nil || count != 17 {
			t.Fatalf("batch relay processed %d messages, want 17: %v", count, err)
		}
	}
}
