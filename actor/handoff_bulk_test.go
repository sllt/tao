package actor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/tao/taonet"
)

type handoffCounter struct {
	Actor
	count      int64
	executions *atomic.Int32
}

func (a *handoffCounter) Receive(message any) error {
	switch message := message.(type) {
	case Snapshotting:
		return a.Reply(a.count)
	case Upgraded:
		return message.Restore(&a.count)
	case remoteCommand:
		switch message.Kind {
		case "add":
			a.count += message.Delta
			if a.executions != nil {
				a.executions.Add(1)
			}
		case "get":
			return a.Reply(a.count)
		}
	}
	return nil
}

func TestHandoffBulkPreparesOrderedMailboxBeforeActivation(t *testing.T) {
	d := NewMemoryDirectory()
	source := globalNetworkNode(t, "source", d)
	var executions atomic.Int32
	kind := KindSpec{Name: "counter", Version: "1", Factory: func() Behavior { return &handoffCounter{executions: &executions} }}
	target := globalNetworkNode(t, "target", d, kind)
	connectNodes(t, source, target)
	spawn(t, source, func() Behavior { return &handoffCounter{count: 7} }, WithName("counter"), WithKind("counter", "1"))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	origin, err := d.Lookup(ctx, "counter")
	if err != nil {
		t.Fatal(err)
	}
	packet := handoffControl{Kind: "reserve", Move: MoveID{1}, Source: origin, Mailbox: 16}
	reserved, err := source.network.handoffRequest(ctx, "target", packet)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.PrepareMove(ctx, origin, reserved.Target, packet.Move); err != nil {
		t.Fatal(err)
	}
	session, err := source.network.connection(ctx, "target", taonet.ClassBulk)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := session.OpenStream(ctx, handoffBulkRoute, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	snapshot, _ := json.Marshal(int64(7))
	digest := sha256.New()
	payload, err := writeHandoffFrame(stream, handoffFrame{Kind: "begin", Move: packet.Move, Snapshot: snapshot})
	if err != nil {
		t.Fatal(err)
	}
	digest.Write(payload)
	for i, delta := range []int64{2, 3} {
		message, err := source.network.registry.Encode(remoteCommand{Kind: "add", Delta: delta})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := writeHandoffFrame(stream, handoffFrame{Kind: "message", Sequence: uint64(i + 1), Envelope: &handoffEnvelope{Message: message}})
		if err != nil {
			t.Fatal(err)
		}
		digest.Write(payload)
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	if _, err := writeHandoffFrame(stream, handoffFrame{Kind: "barrier", Sequence: 2, Digest: sum}); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	ack, _, err := readHandoffFrame(stream, 8192)
	if err != nil || ack.Kind != "prepared" || ack.Move != packet.Move || ack.Sequence != 2 || ack.Digest != sum {
		t.Fatalf("PreparedAck: %+v %v", ack, err)
	}
	if _, err := io.Copy(io.Discard, stream); err != nil {
		t.Fatal(err)
	}
	if executions.Load() != 0 || target.Stats().QueuedMessages != 2 {
		t.Fatalf("prepared target ran early: executions=%d stats=%+v", executions.Load(), target.Stats())
	}
	if _, err := d.CommitMove(ctx, packet.Move); err != nil {
		t.Fatal(err)
	}
	packet.Kind = "activate"
	active, err := source.network.handoffRequest(ctx, "target", packet)
	if err != nil || active.State != MoveCommitted {
		t.Fatalf("activation: %+v %v", active, err)
	}
	if value, err := target.Call[int64](ctx, Ref{Path: "counter"}, remoteCommand{Kind: "get"}); err != nil || value != 12 {
		t.Fatalf("restored mailbox: %d %v", value, err)
	}
	if executions.Load() != 2 {
		t.Fatalf("replayed/omitted mailbox: %d", executions.Load())
	}
	p, _, err := target.resolve(Ref{Node: target.Name(), Path: "counter"})
	if err != nil || p.parent != target.root {
		t.Fatalf("target guardian: %v %v", p, err)
	}
	if _, err := source.network.handoffRequest(ctx, "target", packet); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
}

func TestHandoffBulkRejectsIncompleteTailWithoutExecuting(t *testing.T) {
	for _, fault := range []string{"sequence", "digest"} {
		t.Run(fault, func(t *testing.T) {
			d := NewMemoryDirectory()
			source := globalNetworkNode(t, "source", d)
			var executions atomic.Int32
			kind := KindSpec{Name: "counter", Version: "1", Factory: func() Behavior { return &handoffCounter{executions: &executions} }}
			target := globalNetworkNode(t, "target", d, kind)
			connectNodes(t, source, target)
			spawn(t, source, func() Behavior { return &handoffCounter{} }, WithName("counter"), WithKind("counter", "1"))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			origin, err := d.Lookup(ctx, "counter")
			if err != nil {
				t.Fatal(err)
			}
			packet := handoffControl{Kind: "reserve", Move: MoveID{1}, Source: origin, Mailbox: 16}
			reserved, err := source.network.handoffRequest(ctx, "target", packet)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.PrepareMove(ctx, origin, reserved.Target, packet.Move); err != nil {
				t.Fatal(err)
			}
			session, err := source.network.connection(ctx, "target", taonet.ClassBulk)
			if err != nil {
				t.Fatal(err)
			}
			stream, err := session.OpenStream(ctx, handoffBulkRoute, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			payload, err := writeHandoffFrame(stream, handoffFrame{Kind: "begin", Move: packet.Move, Snapshot: []byte("0")})
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(payload)
			sequence := uint64(0)
			if fault == "sequence" {
				sequence = 1
			} else {
				sum[0] ^= 1
			}
			// A rejected barrier can reset the stream before its final local
			// Write resumes, or before CloseWrite. Neither must succeed after
			// deliberately sending invalid input.
			_, _ = writeHandoffFrame(stream, handoffFrame{Kind: "barrier", Sequence: sequence, Digest: sum})
			_ = stream.CloseWrite()
			if ack, _, err := readHandoffFrame(stream, 8192); err == nil {
				t.Fatalf("bad final barrier accepted: %+v", ack)
			}
			if ctx.Err() != nil || session.Err() != nil || stream.Err() == nil {
				t.Fatalf("expected prompt stream rejection, not timeout/session failure: %v %v %v", ctx.Err(), session.Err(), stream.Err())
			}
			packet.Kind = "abort"
			if _, err := source.network.handoffRequest(ctx, "target", packet); err != nil {
				t.Fatal(err)
			}
			if executions.Load() != 0 || target.Stats().Actors != 0 {
				t.Fatalf("failed transfer executed/leaked: %d %+v", executions.Load(), target.Stats())
			}
		})
	}
}
