package actor

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestHandoffCodecMismatchLeavesOwnershipAndTargetUnchanged(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	target, err := StartNode("target@127.0.0.1:0", NodeOptions{
		Directory: d, Codec: JSONCodec{},
		Kinds:   []KindSpec{replacementKind("1", replacementFactory(1, nil))},
		Network: NetworkOptions{AllowInsecureLoopback: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Stop(context.Background()) })
	connectNodes(t, source, target)
	spawn(t, source, replacementFactory(1, nil), WithName("counter"), WithKind("counter", "1"))
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.network.handoffRequest(t.Context(), "target", handoffControl{Kind: "reserve", Move: MoveID{1}, Source: route, Mailbox: 16})
	remoteError(t, err, ErrCodecMismatch, Responded)
	if target.Stats().Actors != 0 {
		t.Fatal("codec mismatch allocated a target actor")
	}
	if current, err := d.Lookup(t.Context(), route.Path); err != nil || current != route {
		t.Fatalf("codec mismatch changed ownership: %+v %v", current, err)
	}
}

func TestHandoffReservationIsIdempotentUnpublishedAndAbortable(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	var constructed atomic.Int32
	kind := KindSpec{Name: "counter", Version: "2", Factory: func() Behavior { constructed.Add(1); return &replacementBehavior{version: 2} }}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	pid := spawn(t, source, replacementFactory(42, nil), WithName("counter"), WithKind("counter", "1"))
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	packet := handoffControl{Kind: "reserve", Move: MoveID{1}, Source: route, Version: "2", Mailbox: 16}
	reserved, err := source.network.handoffRequest(t.Context(), "target", packet)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.Target.Owner != "target" || reserved.Target.Version != "2" || reserved.Target.PID().BootID != target.BootID() {
		t.Fatalf("reservation: %+v", reserved)
	}
	again, err := source.network.handoffRequest(t.Context(), "target", packet)
	if err != nil || again != reserved {
		t.Fatalf("repeat reservation: %+v %v", again, err)
	}
	if constructed.Load() != 0 {
		t.Fatal("reservation invoked user Factory")
	}
	target.mu.Lock()
	prepared := target.processes[reserved.Target.ProcessID]
	published := prepared != nil && prepared.published.Load()
	target.mu.Unlock()
	if prepared == nil || published {
		t.Fatalf("reservation published=%t", published)
	}
	if current, err := d.Lookup(t.Context(), "counter"); err != nil || current.PID() != pid || current != route {
		t.Fatalf("preflight changed owner: %+v %v", current, err)
	}
	move, err := d.PrepareMove(t.Context(), route, reserved.Target, packet.Move)
	if err != nil {
		t.Fatal(err)
	}
	packet.Kind = "abort"
	aborted, err := source.network.handoffRequest(t.Context(), "target", packet)
	if err != nil || aborted.State != MoveAborted {
		t.Fatalf("abort reservation: %+v %v", aborted, err)
	}
	if target.Stats().Actors != 0 {
		t.Fatal("aborted reservation leaked target PID")
	}
	if move, err = d.ReadMove(t.Context(), move.ID); err != nil || move.State != MoveAborted {
		t.Fatalf("move rollback: %+v %v", move, err)
	}
	if _, err := source.network.handoffRequest(t.Context(), "target", packet); err != nil {
		t.Fatalf("repeat abort: %v", err)
	}
}

func TestHandoffPreflightRejectsUnsupportedKindAndWrongSourceBoot(t *testing.T) {
	d := NewMemoryDirectory()
	source, target := globalNetworkNode(t, "source", d), globalNetworkNode(t, "target", d)
	connectNodes(t, source, target)
	spawn(t, source, replacementFactory(1, nil), WithName("counter"), WithKind("counter", "1"))
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	packet := handoffControl{Kind: "reserve", Move: MoveID{1}, Source: route, Mailbox: 16}
	_, err = source.network.handoffRequest(t.Context(), "target", packet)
	remoteError(t, err, ErrHandoffUnsupported, Responded)
	packet.Source.BootID = [16]byte{99}
	_, err = source.network.handoffRequest(t.Context(), "target", packet)
	remoteError(t, err, ErrInvalidMessage, Responded)
	if target.Stats().Actors != 0 {
		t.Fatal("failed preflight allocated actor")
	}
}

func TestHandoffAbortCannotUndoCommittedDirectory(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	target := globalNetworkNode(t, "target", d, replacementKind("1", replacementFactory(1, nil)))
	connectNodes(t, source, target)
	spawn(t, source, replacementFactory(1, nil), WithName("counter"), WithKind("counter", "1"))
	route, err := d.Lookup(t.Context(), "counter")
	if err != nil {
		t.Fatal(err)
	}
	packet := handoffControl{Kind: "reserve", Move: MoveID{1}, Source: route, Mailbox: 16}
	reserved, err := source.network.handoffRequest(t.Context(), "target", packet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.PrepareMove(t.Context(), route, reserved.Target, packet.Move); err != nil {
		t.Fatal(err)
	}
	committed, err := d.CommitMove(t.Context(), packet.Move)
	if err != nil {
		t.Fatal(err)
	}
	packet.Kind = "abort"
	_, err = source.network.handoffRequest(t.Context(), "target", packet)
	remoteError(t, err, ErrMoveCommitted, Responded)
	if current, err := d.Lookup(t.Context(), "counter"); err != nil || current != committed.Target {
		t.Fatalf("abort changed committed route: %+v %v", current, err)
	}
	if target.Stats().Actors != 1 {
		t.Fatal("abort discarded committed target")
	}
}
