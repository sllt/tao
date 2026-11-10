package actor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func holdPureBatchWatch(p *process, entered, release, finished chan struct{}) (*executorTurn, error) {
	turn := p.executor.Load()
	if turn == nil {
		return nil, errors.New("pure batch has no executor turn")
	}
	turn.mu.Lock()
	if turn.timer != nil {
		turn.mu.Unlock()
		return nil, errors.New("pure batch watch was already initialized")
	}
	var first, completed sync.Once
	turn.timer = time.AfterFunc(0, func() {
		first.Do(func() { close(entered); <-release })
		turn.watch()
		completed.Do(func() { close(finished) })
	})
	turn.watched = true
	turn.mu.Unlock()
	<-entered // A successful Stop cannot remove the held notification.
	return turn, nil
}

func TestPureBatchHeldNextProgressAcrossReceive(t *testing.T) {
	for _, mode := range []string{"go-channel", "next-receive-send"} {
		t.Run(mode, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(1, 1)
			cache := &n.executors.shards[0]
			targetDone := make(chan struct{})
			targetValues := make(chan int, 2)
			target := spawn(t, n, factory(func(_ *testBehavior, message any) error {
				if value, ok := message.(int); ok {
					targetValues <- value
					if value == 1 {
						close(targetDone)
					}
				}
				return nil
			}))
			targetProcess, _, _ := n.resolve(target)
			gate, open := make(chan struct{}), make(chan struct{})
			openGate := sync.OnceFunc(func() { close(open) })
			defer openGate()
			watchEntered, watchRelease, watchFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			resumeWatch := sync.OnceFunc(func() { close(watchRelease) })
			defer resumeWatch()
			returned := make(chan error, 1)
			seen := make(chan int, 3)
			var turn *executorTurn
			var failure error
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				if message == "gate" {
					close(gate)
					<-open
					return nil
				}
				value, ok := message.(int)
				if !ok {
					return nil
				}
				switch value {
				case 0:
					turn, failure = holdPureBatchWatch(a.proc, watchEntered, watchRelease, watchFinished)
					if failure == nil {
						failure = a.Send(target, 1)
					}
				case 1:
					if failure == nil && !turn.pending.Load() {
						failure = errors.New("pure batch released next before the following Receive")
					}
					if mode == "next-receive-send" {
						if failure == nil {
							failure = a.Send(target, 2)
						}
						if failure == nil {
							turn.mu.Lock()
							inherited := turn.next == targetProcess && turn.pending.Load()
							turn.mu.Unlock()
							if !inherited || turn.multiSend || turn.target != nil || turn.offeredInReceive {
								failure = errors.New("first Send in a new Receive did not preserve the inherited next claim")
							}
						}
					} else {
						returned <- failure
						if failure == nil {
							<-targetDone // Deliberately opaque to the Actor APIs.
						}
					}
				case 2:
					if mode == "next-receive-send" {
						if failure == nil && (turn.multiSend || turn.target != nil || turn.offeredInReceive) {
							failure = errors.New("Receive retained the prior Send cache hint")
						}
						returned <- failure
						if failure == nil {
							<-targetDone
						}
					}
				}
				seen <- value
				return nil
			}), WithMailboxSize(4))
			if err := n.Send(source, "gate"); err != nil {
				t.Fatal(err)
			}
			signal(t, gate)
			resumeReady := pauseExecutorReady(t, cache)
			defer resumeReady()
			for value := range 3 {
				if err := n.Send(source, value); err != nil {
					t.Fatal(err)
				}
			}
			openGate()
			nextResult(t, returned)
			cache.mu.Lock()
			queued := cache.ready.size
			cache.mu.Unlock()
			wantQueued := 0
			if queued != wantQueued {
				t.Fatalf("cross-Receive ownership: ready=%d want %d", queued, wantQueued)
			}
			resumeWatch()
			resumeReady()
			signal(t, watchFinished)
			for value := range 3 {
				nextWork(t, seen, value)
			}
			nextWork(t, targetValues, 1)
			if mode == "next-receive-send" {
				nextWork(t, targetValues, 2)
			}
			p, _, _ := n.resolve(source)
			eventually(t, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()
				return !p.running && p.queuedLocked() == 0 && p.batchState.Load() == 0
			})
		})
	}
}

func TestPureBatchHeldNextInterruptRestoresSuffix(t *testing.T) {
	for _, action := range []string{"system", "stop"} {
		t.Run(action, func(t *testing.T) {
			n := testNode(t, NodeOptions{})
			n.executors.init(1, 1)
			cache := &n.executors.shards[0]
			targetDone := make(chan struct{})
			target := spawn(t, n, factory(func(_ *testBehavior, message any) error {
				if message == "work" {
					close(targetDone)
				}
				return nil
			}))
			gate, open := make(chan struct{}), make(chan struct{})
			entered, batchRelease := make(chan struct{}), make(chan struct{})
			openGate := sync.OnceFunc(func() { close(open) })
			resumeBatch := sync.OnceFunc(func() { close(batchRelease) })
			defer openGate()
			defer resumeBatch()
			watchEntered, watchRelease, watchFinished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			resumeWatch := sync.OnceFunc(func() { close(watchRelease) })
			defer resumeWatch()
			controlEntered := make(chan struct{})
			seen := make(chan any, 6)
			var processed atomic.Int32
			var failure error
			source := spawn(t, n, factory(func(a *testBehavior, message any) error {
				switch message {
				case "gate":
					close(gate)
					<-open
				case "system":
					seen <- message
					close(controlEntered)
					<-targetDone
				default:
					if value, ok := message.(int); ok {
						processed.Add(1)
						seen <- value
						if value == 0 {
							_, failure = holdPureBatchWatch(a.proc, watchEntered, watchRelease, watchFinished)
							if failure == nil {
								failure = a.Send(target, "work")
							}
							close(entered)
							<-batchRelease
						}
					}
				}
				return nil
			}), WithMailboxSize(4))
			if err := n.Send(source, "gate"); err != nil {
				t.Fatal(err)
			}
			signal(t, gate)
			resumeReady := pauseExecutorReady(t, cache)
			defer resumeReady()
			for value := range 4 {
				if err := n.Send(source, value); err != nil {
					t.Fatal(err)
				}
			}
			openGate()
			signal(t, entered)
			if failure != nil {
				t.Fatal(failure)
			}
			p, _, _ := n.resolve(source)
			if remaining := p.batchState.Load() &^ batchInterrupted; remaining != 3 {
				t.Fatalf("held suffix: %d want 3", remaining)
			}
			if err := n.Send(source, 4); err != nil {
				t.Fatal(err)
			}
			if err := n.Send(source, 5); !errors.Is(err, ErrMailboxFull) {
				t.Fatalf("held suffix escaped mailbox capacity: %v", err)
			}
			stopped := make(chan error, 1)
			if action == "stop" {
				go func() { stopped <- n.StopActor(t.Context(), source) }()
			} else if !p.enqueueSystem("system") {
				t.Fatal("system admission failed")
			}
			eventually(t, func() bool { return p.batchState.Load()&batchInterrupted != 0 })
			if action == "system" {
				resumeBatch()
				signal(t, controlEntered)
				p.mu.Lock()
				restored := p.batchState.Load() == 0 && p.mailbox.size == 4
				for index := 0; restored && index < 4; index++ {
					restored = p.mailbox.at(index).message == index+1
				}
				p.mu.Unlock()
				if !restored {
					t.Fatal("control overtook suffix restoration or changed FIFO")
				}
			}
			resumeReady()
			signal(t, targetDone) // Only explicit Stop or the outer control flush can release it.
			if action == "stop" {
				resumeBatch()
				nextResult(t, stopped)
				if processed.Load() != 1 || p.batchState.Load() != 0 {
					t.Fatal("Stop executed or retained the held suffix")
				}
			} else {
				for _, want := range []any{0, "system", 1, 2, 3, 4} {
					select {
					case got := <-seen:
						if got != want {
							t.Fatalf("restored FIFO/control priority: got %v want %v", got, want)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("restored suffix did not make progress")
					}
				}
				eventually(t, func() bool {
					p.mu.Lock()
					defer p.mu.Unlock()
					return !p.running && p.queuedLocked() == 0 && p.batchState.Load() == 0
				})
			}
			resumeWatch()
			signal(t, watchFinished)
		})
	}
}
