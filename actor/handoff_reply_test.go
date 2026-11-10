package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

func TestThirdNodeReplyRequiresCommittedOwnershipProof(t *testing.T) {
	d := NewMemoryDirectory()
	caller, source, target := globalNetworkNode(t, "caller", d), globalNetworkNode(t, "source", d), globalNetworkNode(t, "target", d)
	connectNodes(t, caller, target)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	origin, err := d.Claim(ctx, RouteRecord{Path: "counter", Owner: source.Name(), BootID: source.BootID(), ProcessID: 1})
	if err != nil {
		t.Fatal(err)
	}
	move, err := d.PrepareMove(ctx, origin, RouteRecord{Path: origin.Path, Owner: target.Name(), BootID: target.BootID(), ProcessID: 1}, MoveID{1})
	if err != nil {
		t.Fatal(err)
	}
	r, err := caller.network.coreRequest(ctx, origin.PID())
	if err != nil {
		t.Fatal(err)
	}
	caller.network.mu.Lock()
	caller.network.outgoing[r.id] = &outgoingCall{request: r, peer: origin.Owner, boot: origin.BootID, issued: true, routing: &outgoingRoute{route: origin}}
	caller.network.mu.Unlock()
	defer func() {
		caller.network.mu.Lock()
		delete(caller.network.outgoing, r.id)
		caller.network.mu.Unlock()
		caller.expireRequest(r, context.Canceled)
	}()
	send := func(id MoveID) error {
		packet := actorPacket{Kind: "reply", Key: requestKey{Caller: caller.Name(), Boot: caller.BootID(), ID: r.id}, Move: id}
		result := make(chan error, 1)
		err := target.network.queueMessage(caller.Name(), taonet.ClassReply, packet, int64(42), PID{}, nil, result)
		if err != nil {
			return err
		}
		select {
		case err := <-result:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	rejected := func(id MoveID) {
		t.Helper()
		if err := send(id); err != nil {
			t.Fatal(err)
		}
		// Replies have no acceptance ACK. Observe the receiver's rejection
		// before checking that the original request remains pending.
		for {
			select {
			case event := <-caller.NetworkEvents():
				if event.Kind == "reply_rejected" && event.RequestID == r.id {
					return
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	rejected(MoveID{})
	rejected(move.ID)
	select {
	case <-r.done:
		t.Fatal("invalid reply completed the original call")
	default:
	}
	if _, err := d.CommitMove(ctx, move.ID); err != nil {
		t.Fatal(err)
	}
	if err := send(move.ID); err != nil {
		t.Fatal(err)
	}
	signal(t, r.done)
	if r.err != nil || r.value != int64(42) {
		t.Fatalf("third-node reply: %v %v", r.value, r.err)
	}
	if err := caller.network.verifyMovedReply(ctx, move.ID, RouteRecord{}, target.Name(), target.BootID()); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("PID call gained relocation rights: %v", err)
	}
}

func TestMovedReplyProvenanceSurvivesLocalReplaceAndAbortedMove(t *testing.T) {
	d := NewMemoryDirectory()
	ctx := t.Context()
	origin, err := d.Claim(ctx, RouteRecord{Path: "counter", Owner: "a", BootID: [16]byte{1}, ProcessID: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := d.PrepareMove(ctx, origin, RouteRecord{Path: origin.Path, Owner: "b", BootID: [16]byte{2}, ProcessID: 1}, MoveID{1})
	if err != nil {
		t.Fatal(err)
	}
	first, err = d.CommitMove(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	wanted := first.Target
	wanted.ProcessID = 2
	replaced, err := d.SwapInstance(ctx, first.Target, wanted)
	if err != nil || replaced.MoveID != first.ID {
		t.Fatalf("local swap lost provenance: %+v %v", replaced, err)
	}
	target := RouteRecord{Path: origin.Path, Owner: "c", BootID: [16]byte{3}, ProcessID: 1}
	interrupted, err := d.PrepareMove(ctx, replaced, target, MoveID{2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.AbortMove(ctx, interrupted.ID); err != nil {
		t.Fatal(err)
	}
	resumed, err := d.Lookup(ctx, origin.Path)
	if err != nil || resumed.MoveID != first.ID {
		t.Fatalf("abort lost provenance: %+v %v", resumed, err)
	}
	last, err := d.PrepareMove(ctx, resumed, target, MoveID{3})
	if err != nil {
		t.Fatal(err)
	}
	last, err = d.CommitMove(ctx, last.ID)
	if err != nil {
		t.Fatal(err)
	}
	n := testNode(t, NodeOptions{Directory: d})
	w := &network{node: n}
	if err := w.verifyMovedReply(ctx, last.ID, origin, "c", [16]byte{3}); err != nil {
		t.Fatalf("valid move chain rejected: %v", err)
	}
	if err := w.verifyMovedReply(ctx, last.ID, origin, "c", [16]byte{99}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("wrong boot accepted: %v", err)
	}
}
