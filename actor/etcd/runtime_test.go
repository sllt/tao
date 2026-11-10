package etcddirectory

import (
	"context"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
)

type counterCommand struct {
	Add int64
	Get bool
}
type counterActor struct {
	actor.Actor
	value int64
}

func (a *counterActor) Receive(message any) error {
	switch msg := message.(type) {
	case actor.Snapshotting:
		return a.Reply(a.value)
	case actor.Upgraded:
		return msg.Restore(&a.value)
	}
	if command, ok := message.(counterCommand); ok {
		a.value += command.Add
		if command.Get {
			return a.Reply(a.value)
		}
	}
	return nil
}

func TestGlobalRefWithEtcdAndTwoNetworkNodes(t *testing.T) {
	client, _ := startEtcd(t)
	d, err := New(client, "")
	if err != nil {
		t.Fatal(err)
	}
	create := func(name string) *actor.Node {
		node, err := actor.StartNode(name+"@127.0.0.1:0", actor.NodeOptions{Kinds: []actor.KindSpec{{Name: "counter", Version: "2", Factory: func() actor.Behavior { return &counterActor{} }}}, Directory: d, Network: actor.NetworkOptions{AllowInsecureLoopback: true}, Register: []actor.MessageType{{Name: "command", Version: 1, Sample: counterCommand{}}, {Name: "count", Version: 1, Sample: int64(0)}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := node.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
		return node
	}
	a, b := create("a"), create("b")
	if err := a.Connect(t.Context(), "b", b.ListenAddr()); err != nil {
		t.Fatal(err)
	}
	pid, err := b.Spawn(func() actor.Behavior { return &counterActor{} }, actor.WithName("counter"), actor.WithKind("counter", "1"), actor.WithHandoff(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SendConfirmed(t.Context(), actor.Ref{Path: "counter"}, counterCommand{Add: 42}); err != nil {
		t.Fatal(err)
	}
	value, err := a.Call[int64](t.Context(), actor.Ref{Path: "counter"}, counterCommand{Get: true})
	if err != nil || value != 42 {
		t.Fatalf("global etcd call: %d %v", value, err)
	}
	record, err := d.Lookup(t.Context(), "counter")
	if err != nil || record.PID() != pid {
		t.Fatalf("owner: %+v %v", record, err)
	}
	if err := b.Replace(t.Context(), pid, actor.KindSpec{Name: "counter", Version: "2", Factory: func() actor.Behavior { return &counterActor{} }}); err != nil {
		t.Fatal(err)
	}
	value, err = a.Call[int64](t.Context(), actor.Ref{Path: "counter"}, counterCommand{Get: true})
	if err != nil || value != 42 {
		t.Fatalf("replaced global etcd call: %d %v", value, err)
	}
	current, err := d.Lookup(t.Context(), "counter")
	if err != nil || current.PID() == pid || current.Epoch != record.Epoch+1 || current.Version != "2" {
		t.Fatalf("replacement CAS: %+v %v", current, err)
	}
	c := create("c")
	if err := b.Connect(t.Context(), "c", c.ListenAddr()); err != nil {
		t.Fatal(err)
	}
	if err := a.Connect(t.Context(), "c", c.ListenAddr()); err != nil {
		t.Fatal(err)
	}
	moved, err := b.Handoff(t.Context(), current.PID(), actor.HandoffOptions{TargetNode: "c"})
	if err != nil || moved.State != actor.MoveCommitted {
		t.Fatalf("etcd handoff: %+v %v", moved, err)
	}
	if err := b.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = a.Call[int64](t.Context(), actor.Ref{Path: "counter"}, counterCommand{Get: true})
	if err != nil || value != 42 {
		t.Fatalf("after source shutdown: %d %v", value, err)
	}
	owner, err := d.Lookup(t.Context(), "counter")
	if err != nil || owner.Owner != "c" || owner.Epoch != current.Epoch+1 {
		t.Fatalf("handoff owner: %+v %v", owner, err)
	}
}
