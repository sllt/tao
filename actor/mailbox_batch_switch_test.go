package actor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPureBatchSwitchArmsSourceBeforeBlockedNext(t *testing.T) {
	n := testNode(t, NodeOptions{})
	n.executors.init(2, 1)
	targetEntered, targetRelease := make(chan struct{}), make(chan struct{})
	resumeTarget := sync.OnceFunc(func() { close(targetRelease) })
	defer resumeTarget()
	used := make(chan *executorTurn, 1)
	target := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "work" {
			used <- a.proc.executor.Load()
			close(targetEntered)
			<-targetRelease
		}
		return nil
	}))
	gate, gateRelease := make(chan struct{}), make(chan struct{})
	openGate := sync.OnceFunc(func() { close(gateRelease) })
	defer openGate()
	result := make(chan error, 1)
	var sourceTurn *executorTurn
	source := spawn(t, n, factory(func(a *testBehavior, message any) error {
		if message == "gate" {
			close(gate)
			<-gateRelease
			return nil
		}
		value, ok := message.(int)
		if !ok {
			return nil
		}
		if value == 0 {
			var err error
			sourceTurn, err = untimedTurn(a.proc)
			if err == nil {
				err = a.Send(target, "work")
			}
			if err != nil {
				result <- err
				return err
			}
		}
		if value == 16 {
			<-targetEntered // opaque wait: the taken target is already running
			var err error
			if a.proc.executor.Load() == sourceTurn {
				err = errors.New("source reused the worker blocked in the taken target")
			}
			result <- err
		}
		return nil
	}), WithMailboxSize(17))
	if err := n.Send(source, "gate"); err != nil {
		t.Fatal(err)
	}
	signal(t, gate)
	for value := range 17 {
		if err := n.Send(source, value); err != nil {
			t.Fatal(err)
		}
	}
	openGate()
	signal(t, targetEntered)
	if turn := <-used; turn != sourceTurn {
		t.Fatal("batch target did not use the publishing worker")
	}
	nextResult(t, result) // no test-controlled ready notification is installed
}

func TestPureBatchSwitchRetainsClaimsFIFOAndProgress(t *testing.T) {
	for _, mode := range []string{"ready", "close", "system-close", "stop-close"} {
		t.Run(mode, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(2, 1)
			cache := &n.executors.shards[0]
			targetEntered := make(chan *executorTurn, 1)
			targetRelease := make(chan struct{})
			resumeTarget := sync.OnceFunc(func() { close(targetRelease) })
			defer resumeTarget()
			var targetCalls atomic.Int32
			target := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "work":
					targetCalls.Add(1)
					targetEntered <- a.proc.executor.Load()
					<-targetRelease
				case "barrier":
					return a.Reply(targetCalls.Load())
				}
				return nil
			}))
			gate, gateRelease := make(chan struct{}), make(chan struct{})
			openGate := sync.OnceFunc(func() { close(gateRelease) })
			defer openGate()
			batchEntered, batchRelease := make(chan struct{}), make(chan struct{})
			resumeBatch := sync.OnceFunc(func() { close(batchRelease) })
			defer resumeBatch()
			sourceDone, sourceStopped := make(chan struct{}), make(chan struct{}, 1)
			seen := make(chan any, 64)
			setup := make(chan error, 1)
			var counts [33]atomic.Int32
			var sourceCalls, stopped atomic.Int32
			var sourceTurn *executorTurn
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "gate":
					close(gate)
					<-gateRelease
				case "system":
					seen <- message
				case "barrier":
					return a.Reply(sourceCalls.Load())
				default:
					if _, ok := message.(Stopped); ok {
						stopped.Add(1)
						sourceStopped <- struct{}{}
						return nil
					}
					value, ok := message.(int)
					if !ok {
						return nil
					}
					counts[value].Add(1)
					sourceCalls.Add(1)
					seen <- value
					if value == 0 {
						var err error
						sourceTurn, err = untimedTurn(a.proc)
						if err == nil {
							err = a.Send(target, "work")
						}
						setup <- err
						close(batchEntered)
						<-batchRelease
					}
					if value == len(counts)-1 {
						close(sourceDone)
					}
				}
				return nil
			}), WithMailboxSize(17), WithRestart(Temporary))
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
			signal(t, batchEntered)
			nextResult(t, setup)
			if held := p.batchState.Load() &^ batchInterrupted; held != 15 {
				t.Fatalf("did not detach the expected private suffix: %d", held)
			}
			if err := n.Send(source, 17); err != nil {
				t.Fatal(err)
			}
			if err := n.Send(source, 18); !errors.Is(err, ErrMailboxFull) {
				t.Fatalf("private suffix escaped capacity before switching: %v", err)
			}
			resumeBatch()
			select {
			case used := <-targetEntered:
				if used != sourceTurn {
					t.Fatal("batch boundary did not execute next on the original worker")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("batch boundary sent next to ready instead of taking it locally")
			}
			p.mu.Lock()
			retained := p.running && p.batchState.Load() == 0 && p.queuedLocked() == 2 && p.mailbox.size == 2
			p.mu.Unlock()
			cache.mu.Lock()
			queued := cache.ready.size == 1 && cache.ready.items[cache.ready.head] == p && cache.readyWatched
			cache.mu.Unlock()
			if !retained || !queued || p.executor.Load() != nil {
				t.Fatal("switch did not transfer one cleared source claim without its old context")
			}
			for value := 18; value < len(counts); value++ {
				if err := n.Send(source, value); err != nil {
					t.Fatalf("returned batch still consumed capacity at %d: %v", value, err)
				}
			}
			if err := n.Send(source, len(counts)); !errors.Is(err, ErrMailboxFull) {
				t.Fatalf("requeued source exceeded logical capacity: %v", err)
			}

			if mode == "stop-close" {
				result := make(chan error, 1)
				go func() { result <- n.StopActor(t.Context(), source) }()
				eventually(t, func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.stopRequested })
				var racing sync.WaitGroup
				racing.Go(cache.close)
				racing.Go(resumeReady)
				racing.Wait()
				nextResult(t, result)
				signal(t, sourceStopped)
				if stopped.Load() != 1 || p.batchState.Load() != 0 {
					t.Fatal("Stop/Close lost or repeated the transferred source claim")
				}
			} else {
				if mode == "system-close" && !p.enqueueSystem("system") {
					t.Fatal("control admission failed while source was queued")
				}
				if mode != "ready" {
					cache.close()
				}
				resumeReady()
				signal(t, sourceDone) // the locally taken target is still blocked
			}
			wantCount := len(counts)
			if mode == "stop-close" {
				wantCount = len(mailboxBatch{})
			}
			for value := range wantCount {
				if mode == "system-close" && value == len(mailboxBatch{}) {
					if got := <-seen; got != "system" {
						t.Fatalf("queued control lost priority at the restored boundary: %v", got)
					}
				}
				if got := <-seen; got != value {
					t.Fatalf("batch switch FIFO: got %v want %d", got, value)
				}
			}
			for value := range counts {
				want := int32(0)
				if value < wantCount {
					want = 1
				}
				if count := counts[value].Load(); count != want {
					t.Fatalf("source message %d processed %d times, want %d", value, count, want)
				}
			}
			resumeTarget()
			if count, err := n.Call[int32](t.Context(), target, "barrier"); err != nil || count != 1 {
				t.Fatalf("taken target claim processed %d times, want 1: %v", count, err)
			}
			if mode != "stop-close" {
				if count, err := n.Call[int32](t.Context(), source, "barrier"); err != nil || count != int32(len(counts)) {
					t.Fatalf("transferred source claim processed %d messages: %v", count, err)
				}
			}
		})
	}
}
