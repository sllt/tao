package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPureBatchRestoreAcrossWrapGrowthAndMetadata(t *testing.T) {
	m := mailbox{limit: 64}
	for i := range 16 {
		_ = m.push(envelope{message: i})
	}
	for range 10 {
		m.pop()
	}
	for i := 16; i < 20; i++ {
		_ = m.push(envelope{message: i})
	}
	var batch [8]mailboxMessage
	if n := m.takePure(batch[:]); n != len(batch) {
		t.Fatalf("batch size %d", n)
	}
	owner := &request{}
	for i := 20; i < 48; i++ {
		env := envelope{message: i}
		if i == 20 {
			env.request, env.toRef = owner, true
		}
		if err := m.push(env); err != nil {
			t.Fatal(err)
		}
	}
	// Two entries were claimed. The remaining six still precede all arrivals;
	// restoring them requires growth even though ordinary push did not.
	m.prependPure(batch[2:])
	for i := 12; i < 48; i++ {
		env := m.pop()
		if env.message != i || (env.request == owner) != (i == 20) || env.toRef != (i == 20) {
			t.Fatalf("restored FIFO/metadata at %d: %+v", i, env)
		}
	}
	for _, value := range m.buffer {
		if value.message != nil || value.sender != nil {
			t.Fatal("drained mailbox retained a batch reference")
		}
	}
	for _, meta := range m.metadata {
		if meta.request != nil || meta.toRef {
			t.Fatal("drained mailbox retained control metadata")
		}
	}
}

func TestPureBatchStopsBeforeRequestLeaseRefOrGlobal(t *testing.T) {
	for _, boundary := range []envelope{{request: &request{}}, {release: func() {}}, {toRef: true}, {global: &RouteRecord{}}} {
		m := mailbox{limit: 8}
		for i := range 3 {
			_ = m.push(envelope{message: i})
		}
		boundary.message = "boundary"
		_ = m.push(boundary)
		_ = m.push(envelope{message: "after"})
		var batch [8]mailboxMessage
		if n := m.takePure(batch[:]); n != 3 {
			t.Fatalf("crossed control metadata: %d", n)
		}
		if n := m.takePure(batch[:]); n != 0 {
			t.Fatal("batched a request/lease/Ref/global message")
		}
		if m.pop().message != "boundary" || m.pop().message != "after" {
			t.Fatal("batch changed suffix ordering")
		}
	}
}

func TestPureBatchFinalItemPreservesControlAndArrival(t *testing.T) {
	n := testNode(t, NodeOptions{})
	gate, open := make(chan struct{}), make(chan struct{})
	last, finish := make(chan struct{}), make(chan struct{})
	openGate, finishLast := sync.OnceFunc(func() { close(open) }), sync.OnceFunc(func() { close(finish) })
	defer openGate()
	defer finishLast()
	seen := make(chan any, 18)
	pid := spawn(t, n, factory(func(_ *testBehavior, message any) error {
		switch message {
		case "gate":
			close(gate)
			<-open
		case 15:
			seen <- message
			close(last)
			<-finish
		case "system":
			seen <- message
		default:
			if _, ok := message.(int); ok {
				seen <- message
			}
		}
		return nil
	}), WithMailboxSize(16))
	_ = n.Send(pid, "gate")
	signal(t, gate)
	for i := range 16 {
		if err := n.Send(pid, i); err != nil {
			t.Fatal(err)
		}
	}
	openGate()
	signal(t, last)
	p, _, _ := n.resolve(pid)
	if count := p.batchState.Load() &^ batchInterrupted; count != 0 {
		t.Fatalf("final item still consumes capacity: %d", count)
	}
	if err := n.Send(pid, 16); err != nil {
		t.Fatal(err)
	}
	if !p.enqueueSystem("system") {
		t.Fatal("system admission failed")
	}
	finishLast()
	for i := range 16 {
		if value := <-seen; value != i {
			t.Fatalf("FIFO: %v want %d", value, i)
		}
	}
	if value := <-seen; value != "system" {
		t.Fatalf("cleanup lost control priority: %v", value)
	}
	if value := <-seen; value != 16 {
		t.Fatalf("cleanup lost new arrival: %v", value)
	}
	eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.running && p.queuedLocked() == 0 && p.batchState.Load() == 0
	})
}

func TestPureBatchCapacitySelfSendStatsAndSystemInterrupt(t *testing.T) {
	n := testNode(t, NodeOptions{})
	gate, open := make(chan struct{}), make(chan struct{})
	entered, resume := make(chan struct{}), make(chan struct{})
	openGate := sync.OnceFunc(func() { close(open) })
	resumeBatch := sync.OnceFunc(func() { close(resume) })
	defer openGate()
	defer resumeBatch()
	seen := make(chan any, 6)
	pid := spawn(t, n, factory(func(a *testBehavior, message any) error {
		switch message {
		case "gate":
			close(gate)
			<-open
		case 0:
			seen <- message
			if err := a.Send(a.PID(), 4); err != nil {
				t.Errorf("executing message did not free its slot: %v", err)
			}
			if err := a.Send(a.PID(), 5); !errors.Is(err, ErrMailboxFull) {
				t.Errorf("unclaimed batch escaped capacity: %v", err)
			}
			close(entered)
			<-resume
		case 1, 2, 3, 4, "system":
			seen <- message
		}
		return nil
	}), WithMailboxSize(4))
	_ = n.Send(pid, "gate")
	signal(t, gate)
	for i := range 4 {
		if err := n.Send(pid, i); err != nil {
			t.Fatal(err)
		}
	}
	openGate()
	signal(t, entered)
	p, _, _ := n.resolve(pid)
	if got := p.batchState.Load() &^ batchInterrupted; got != 3 {
		t.Fatalf("did not exercise held batch: %d", got)
	}
	if got := n.Stats().QueuedMessages; got != 4 {
		t.Fatalf("Stats omitted held messages: %d", got)
	}
	detail, err := n.ActorSnapshot(pid)
	if err != nil || detail.Mailbox != 4 {
		t.Fatalf("Observer omitted held messages: %+v %v", detail, err)
	}
	if err := n.Send(pid, 6); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("external producer exceeded capacity: %v", err)
	}
	if !p.enqueueSystem("system") {
		t.Fatal("system admission failed")
	}
	resumeBatch()
	for _, expected := range []any{0, "system", 1, 2, 3, 4} {
		if got := <-seen; got != expected {
			t.Fatalf("FIFO/system priority: got %v want %v", got, expected)
		}
	}
	eventually(t, func() bool { return n.Stats().QueuedMessages == 0 })
}

func TestPureBatchStopReplaceAndHandoffInterrupt(t *testing.T) {
	for _, action := range []string{"stop", "replace", "handoff"} {
		t.Run(action, func(t *testing.T) {
			directory := NewMemoryDirectory()
			n := testNode(t, NodeOptions{Directory: directory})
			if action == "handoff" {
				n = globalNetworkNode(t, "source", directory)
				target := globalNetworkNode(t, "target", directory, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				connectNodes(t, n, target)
			}
			gate, open := make(chan struct{}), make(chan struct{})
			entered, resume := make(chan struct{}), make(chan struct{})
			openGate := sync.OnceFunc(func() { close(open) })
			resumeBatch := sync.OnceFunc(func() { close(resume) })
			defer openGate()
			defer resumeBatch()
			var processed atomic.Int32
			pid := spawn(t, n, migratingCounter(0, func(_ *testBehavior, message any) error {
				if cmd, ok := message.(remoteCommand); ok {
					if cmd.Kind == "gate" {
						close(gate)
						<-open
					}
					if cmd.Kind == "add" && processed.Add(1) == 1 {
						close(entered)
						<-resume
					}
				}
				return nil
			}), WithName("counter"), WithKind("counter", "1"), WithHandoff(true))
			_ = n.Send(pid, remoteCommand{Kind: "gate"})
			signal(t, gate)
			for range 4 {
				if err := n.Send(pid, remoteCommand{Kind: "add", Delta: 1}); err != nil {
					t.Fatal(err)
				}
			}
			openGate()
			signal(t, entered)
			p, _, _ := n.resolve(pid)
			if got := p.batchState.Load() &^ batchInterrupted; got != 3 {
				t.Fatalf("no active batch: %d", got)
			}
			result := make(chan error, 1)
			go func() {
				switch action {
				case "stop":
					result <- n.StopActor(t.Context(), pid)
				case "replace":
					result <- n.Replace(t.Context(), pid, KindSpec{Name: "counter", Version: "2", Factory: migratingCounter(0, nil)})
				case "handoff":
					_, err := n.Handoff(t.Context(), pid, HandoffOptions{TargetNode: "target", Version: "2"})
					result <- err
				}
			}()
			eventually(t, func() bool { return p.batchState.Load()&batchInterrupted != 0 })
			if action != "stop" {
				if err := n.SendConfirmed(t.Context(), Ref{Path: "counter"}, remoteCommand{Kind: "add", Delta: 100}); err != nil {
					t.Fatal(err)
				}
			}
			resumeBatch()
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if processed.Load() != 1 || p.batchState.Load()&^batchInterrupted != 0 {
				t.Fatal("control operation executed or retained the held PID suffix")
			}
			if action != "stop" {
				value, err := n.Call[int64](context.Background(), Ref{Path: "counter"}, remoteCommand{Kind: "get"})
				if err != nil || value != 101 {
					t.Fatalf("snapshot/Ref transfer: %d %v", value, err)
				}
			}
		})
	}
}
