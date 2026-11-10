package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

type remoteCommand struct {
	Kind  string `json:"kind"`
	Delta int64  `json:"delta"`
}

type decodeBarrier struct{ entered, release chan struct{} }

var pausedDecoder atomic.Pointer[decodeBarrier]

type pausedMessage struct {
	Value int64 `json:"value"`
}

func (msg *pausedMessage) UnmarshalJSON(data []byte) error {
	if barrier := pausedDecoder.Load(); barrier != nil {
		close(barrier.entered)
		<-barrier.release
	}
	type plain pausedMessage
	return json.Unmarshal(data, (*plain)(msg))
}

func (msg *pausedMessage) UnmarshalCBOR(data []byte) error {
	if barrier := pausedDecoder.Load(); barrier != nil {
		close(barrier.entered)
		<-barrier.release
	}
	type plain pausedMessage
	return (CBORCodec{}).Unmarshal(data, (*plain)(msg))
}

func networkNode(t *testing.T, name string) *Node {
	t.Helper()
	n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"command", 1, remoteCommand{}}, {"count", 1, int64(0)}, {"pid", 1, PID{}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	return n
}
func connectNodes(t *testing.T, a, b *Node) {
	t.Helper()
	if err := a.Connect(t.Context(), b.Name(), b.ListenAddr()); err != nil {
		t.Fatal(err)
	}
}
func remoteError(t *testing.T, err error, want error, outcome DeliveryOutcome) {
	t.Helper()
	var delivery *RemoteError
	if !errors.Is(err, want) || !errors.As(err, &delivery) || delivery.Outcome != outcome {
		t.Fatalf("got %#v; want %v outcome %d", err, want, outcome)
	}
}

func TestPeerPipeSubmitsNextMessageBeforePriorAcceptance(t *testing.T) {
	firstArrived := make(chan struct{})
	m := taonet.NewMux()
	if err := m.HandleChannel(inboxRoute, func(ctx context.Context, c *taonet.Channel) error {
		first, err := c.Receive(ctx)
		if err != nil {
			return err
		}
		close(firstArrived)
		second, err := c.Receive(ctx) // A stop-and-wait sender deadlocks here.
		if err != nil {
			return err
		}
		for i, d := range []*taonet.Delivery{first, second} {
			packet, err := unmarshalPacket(d.Message().Body)
			if err != nil {
				return err
			}
			if packet.Sequence != uint64(i+1) {
				return fmt.Errorf("packet sequence %d", packet.Sequence)
			}
		}
		if err := first.Reject(rejectCode(ErrMailboxFull)); err != nil {
			return err
		}
		first.Release()
		if err := second.Accept(); err != nil {
			return err
		}
		second.Release()
		<-ctx.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	instance := taonet.ID{1}
	endpoint, err := taonet.NewEndpoint(taonet.EndpointConfig{Profile: taonet.Isolated, Session: taonet.SessionConfig{Local: taonet.Peer{ID: "sink", Instance: instance, Namespace: "hive.actor"}, Mux: m, Transport: taonet.TransportConfig{AllowInsecureLoopback: true}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = endpoint.Serve(t.Context(), listener) }()
	a := networkNode(t, "source")
	if err := a.Connect(t.Context(), "sink", listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	pid := PID{Node: "sink", BootID: [16]byte(instance), ID: 1}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() { firstResult <- a.SendConfirmed(ctx, pid, remoteCommand{Delta: 1}) }()
	signal(t, firstArrived)
	if err := a.SendConfirmed(ctx, pid, remoteCommand{Delta: 2}); err != nil {
		t.Fatal(err)
	}
	remoteError(t, <-firstResult, ErrMailboxFull, Rejected)
	eventually(t, func() bool { return a.NetworkStats().QueuedMessages == 0 })
}

func TestRemoteCallsUseIndependentRepliesAndBidirectionalConnections(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	for _, n := range []*Node{a, b} {
		var count int64
		spawn(t, n, factory(func(actor *testBehavior, msg any) error {
			if cmd, ok := msg.(remoteCommand); ok {
				switch cmd.Kind {
				case "add":
					count += cmd.Delta
				case "get":
					return actor.Reply(count)
				case "sender":
					return actor.Reply(actor.Sender().(PID))
				case "fail":
					_ = actor.ReplyError(errors.New("business refusal"))
				}
			}
			return nil
		}), WithName("counter"))
	}
	toB := a.Ref("node://b/counter")
	if err := a.SendConfirmed(t.Context(), toB, remoteCommand{Kind: "add", Delta: 41}); err != nil {
		t.Fatal(err)
	}
	if err := a.Send(toB, remoteCommand{Kind: "add", Delta: 1}); err != nil {
		t.Fatal(err)
	}
	if got, err := a.Call[int64](t.Context(), toB, remoteCommand{Kind: "get"}); got != 42 || err != nil {
		t.Fatalf("remote counter: %d %v", got, err)
	}
	if _, err := a.Call[int64](t.Context(), toB, remoteCommand{Kind: "fail"}); err == nil {
		t.Fatal("business failure lost")
	} else {
		remoteError(t, err, ErrRemoteApplication, Responded)
	}
	var concurrent sync.WaitGroup
	for i := range 16 {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			node, target := a, Ref{Node: "b", Path: "counter"}
			if i%2 == 1 {
				node, target = b, Ref{Node: "a", Path: "counter"}
			}
			if _, err := node.Call[int64](t.Context(), target, remoteCommand{Kind: "get"}); err != nil {
				t.Error(err)
			}
		}()
	}
	concurrent.Wait()
	caller := spawn(t, a, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			reply, err := actor.Call(actor.Context(), Ref{Node: "b", Path: "counter"}, remoteCommand{Kind: "sender"})
			if err != nil {
				return actor.ReplyError(err)
			}
			return actor.Reply(reply)
		}
		return nil
	}))
	if got, err := a.Call[PID](t.Context(), caller, remoteCommand{}); got != caller || err != nil {
		t.Fatalf("remote Sender: %v %v", got, err)
	}
	if a.NetworkStats().Endpoint.Sessions < 3 || b.NetworkStats().Endpoint.Sessions < 3 {
		t.Fatal("inbox/reply/control were not isolated")
	}
}

func TestRemoteMailboxFullAndOldBootAreDefiniteRejections(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	entered, release := make(chan struct{}), make(chan struct{})
	once := sync.OnceFunc(func() { close(release) })
	defer once()
	pid := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if cmd, ok := msg.(remoteCommand); ok && cmd.Kind == "block" {
			close(entered)
			<-release
		}
		return nil
	}), WithName("blocked"), WithMailboxSize(1))
	if err := a.SendConfirmed(t.Context(), pid, remoteCommand{Kind: "block"}); err != nil {
		t.Fatal(err)
	}
	signal(t, entered)
	if err := a.SendConfirmed(t.Context(), pid, remoteCommand{}); err != nil {
		t.Fatal(err)
	}
	remoteError(t, a.SendConfirmed(t.Context(), pid, remoteCommand{}), ErrMailboxFull, Rejected)
	_, callErr := a.Call[any](t.Context(), pid, remoteCommand{})
	remoteError(t, callErr, ErrMailboxFull, Rejected)
	missing := pid
	missing.ID += 1000
	_, callErr = a.Call[any](t.Context(), missing, remoteCommand{})
	remoteError(t, callErr, ErrNoProcess, Rejected)
	pid.BootID[0] ^= 1
	remoteError(t, a.SendConfirmed(t.Context(), pid, remoteCommand{}), ErrStalePID, NotSent)
	once()
}

func TestRemoteCancellationAndMissingRegistration(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	entered, canceled := make(chan struct{}), make(chan struct{})
	pid := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if cmd, ok := msg.(remoteCommand); ok {
			if cmd.Kind == "wait" {
				close(entered)
				<-actor.Context().Done()
				close(canceled)
			} else {
				return actor.Reply(int64(7))
			}
		}
		return nil
	}), WithName("service"))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := a.Call[int64](ctx, pid, remoteCommand{Kind: "wait"}); result <- err }()
	signal(t, entered)
	cancel()
	remoteError(t, <-result, context.Canceled, Unknown)
	signal(t, canceled)
	if got, err := a.Call[int64](t.Context(), pid, remoteCommand{}); got != 7 || err != nil {
		t.Fatalf("cancel damaged subsequent call: %d %v", got, err)
	}
	remoteError(t, a.Send(pid, "unregistered"), ErrUnregisteredType, NotSent)
	eventually(t, func() bool {
		return a.Stats().PendingCalls == 0 && b.Stats().PendingCalls == 0 && b.NetworkStats().IncomingCalls == 0
	})
}

func TestRemoteCancelBeforeInboxAdmission(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	var executions atomic.Int32
	pid := spawn(t, b, factory(func(_ *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			executions.Add(1)
		}
		return nil
	}))
	interactive, err := a.network.connection(t.Context(), "b", taonet.ClassInteractive)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := interactive.OpenChannel(t.Context(), inboxRoute, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer inbox.Close()
	main, err := a.network.connection(t.Context(), "b", taonet.ClassControl)
	if err != nil {
		t.Fatal(err)
	}
	control, err := main.OpenChannel(t.Context(), controlRoute, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	key := requestKey{a.Name(), a.BootID(), 9001}
	scope := wireScope{interactive.Info().ID, inbox.ID()}
	payload, _ := marshalPacket(actorPacket{Kind: "cancel", Key: key, Scope: scope, Sequence: 1})
	if err = control.SendConfirmed(t.Context(), taonet.Message{Body: payload}); err != nil {
		t.Fatal(err)
	}
	message, err := a.network.registry.Encode(remoteCommand{})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := encodeAddress(pid)
	payload, _ = marshalPacket(actorPacket{Kind: "call", Key: key, Target: target, BudgetMS: 1000, Message: message, Sequence: 1})
	err = inbox.SendConfirmed(t.Context(), taonet.Message{Body: payload})
	var op *taonet.OpError
	if !errors.As(err, &op) || op.Code != rejectCode(context.Canceled) || op.Outcome != taonet.Rejected {
		t.Fatalf("early cancel not honored: %v", err)
	}
	if executions.Load() != 0 {
		t.Fatal("canceled request reached Receive")
	}
	key.ID++
	payload, _ = marshalPacket(actorPacket{Kind: "call", Key: key, Target: target, BudgetMS: 1000, Message: message, Sequence: 2})
	if err = inbox.SendConfirmed(t.Context(), taonet.Message{Body: payload}); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		b.network.mu.Lock()
		defer b.network.mu.Unlock()
		_, active := b.network.incoming[key]
		return !active && executions.Load() == 1
	})
	payload, _ = marshalPacket(actorPacket{Kind: "cancel", Key: key, Scope: scope, Sequence: 2})
	if err = control.SendConfirmed(t.Context(), taonet.Message{Body: payload}); err != nil {
		t.Fatal(err)
	}
	if b.NetworkStats().CancellationRecords != 0 {
		t.Fatal("late cancellation retained an unnecessary tombstone")
	}
}

func TestRemoteDisconnectNeverReplaysUnknownCall(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	entered := make(chan struct{})
	var executions atomic.Int32
	pid := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			executions.Add(1)
			close(entered)
			<-actor.Context().Done()
		}
		return nil
	}))
	result := make(chan error, 1)
	go func() { _, err := a.Call[any](t.Context(), pid, remoteCommand{}); result <- err }()
	signal(t, entered)
	for _, session := range a.network.endpoint.Sessions("b") {
		if session.Info().Class == taonet.ClassControl {
			_ = session.Close()
		}
	}
	err := <-result
	var failure *RemoteError
	if !errors.As(err, &failure) || failure.Outcome != Unknown {
		t.Fatalf("disconnect outcome: %v", err)
	}
	if executions.Load() != 1 {
		t.Fatal("request replayed")
	}
	if err = a.Connect(t.Context(), "b", b.ListenAddr()); err != nil {
		t.Fatal(fmt.Errorf("new connection failed: %w", err))
	}
}

func TestRemoteCancellationDuringDecodePreventsAdmission(t *testing.T) {
	barrier := &decodeBarrier{entered: make(chan struct{}), release: make(chan struct{})}
	pausedDecoder.Store(barrier)
	defer pausedDecoder.Store(nil)
	release := sync.OnceFunc(func() { close(barrier.release) })
	defer release()
	makeNode := func(name string) *Node {
		n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"paused", 1, (*pausedMessage)(nil)}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Stop(context.Background()) })
		return n
	}
	a, b := makeNode("a"), makeNode("b")
	connectNodes(t, a, b)
	var calls atomic.Int32
	target := spawn(t, b, factory(func(a *testBehavior, msg any) error {
		if _, ok := msg.(*pausedMessage); ok {
			calls.Add(1)
			_ = a.Reply(nil)
		}
		return nil
	}))
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := a.Call[any](ctx, target, &pausedMessage{Value: 1}); result <- err }()
	signal(t, barrier.entered)
	a.network.mu.Lock()
	var scope wireScope
	for _, call := range a.network.outgoing {
		scope = call.scope
	}
	a.network.mu.Unlock()
	cancel()
	remoteError(t, <-result, context.Canceled, Unknown)
	eventually(t, func() bool { return b.NetworkStats().CancellationRecords == 1 })
	release()
	eventually(t, func() bool {
		b.network.mu.Lock()
		defer b.network.mu.Unlock()
		return b.network.inboxProgress[scope] == 1 && len(b.network.canceled) == 0
	})
	if calls.Load() != 0 {
		t.Fatal("cancellation during decode was lost")
	}
}

func TestRemoteRefUsesNewInstanceAfterReconnect(t *testing.T) {
	a, b := networkNode(t, "a"), networkNode(t, "b")
	connectNodes(t, a, b)
	makeCounter := func(value int64) Factory {
		return factory(func(a *testBehavior, msg any) error {
			if _, ok := msg.(remoteCommand); ok {
				return a.Reply(value)
			}
			return nil
		})
	}
	old := spawn(t, b, makeCounter(1), WithName("counter"))
	ref := a.Ref("node://b/counter")
	if value, err := a.Call[int64](t.Context(), ref, remoteCommand{}); value != 1 || err != nil {
		t.Fatal(value, err)
	}
	oldSessions := a.network.endpoint.Sessions("b")
	if err := b.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Remote Stop does not synchronously deliver EOF to this node. Connect
	// intentionally reuses a locally live connection, so prove disconnection
	// before testing the subsequent reconnect (as the Endpoint test does).
	for _, session := range oldSessions {
		signal(t, session.Done())
	}
	fresh := networkNode(t, "b")
	spawn(t, fresh, makeCounter(2), WithName("counter"))
	connectNodes(t, a, fresh)
	a.network.mu.Lock()
	connectedBoot := a.network.currentBoot["b"]
	a.network.mu.Unlock()
	if connectedBoot != fresh.BootID() {
		t.Fatalf("Connect returned before switching instance: got %x want %x", connectedBoot, fresh.BootID())
	}
	if value, err := a.Call[int64](t.Context(), ref, remoteCommand{}); value != 2 || err != nil {
		t.Fatalf("Ref stayed on old instance: %d %v", value, err)
	}
	remoteError(t, a.SendConfirmed(t.Context(), old, remoteCommand{}), ErrStalePID, NotSent)
}
