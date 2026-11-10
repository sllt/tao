package actor

import (
	"errors"
	"testing"
)

func TestFromFuncGetsFreshBindingOnSpawnAndRestart(t *testing.T) {
	n := testNode(t, NodeOptions{})
	started := make(chan PID, 3)
	f := FromFunc(func(a *Actor, message any) error {
		switch message := message.(type) {
		case Started:
			started <- a.PID()
		case string:
			if message == "crash" {
				return errors.New("restart function actor")
			}
			return a.Reply(a.PID())
		}
		return nil
	})
	a := spawn(t, n, f, WithName("a"))
	b := spawn(t, n, f, WithName("b"))
	if <-started != a || <-started != b {
		t.Fatal("function factory shared its Actor binding")
	}
	if err := n.Send(a, "crash"); err != nil {
		t.Fatal(err)
	}
	var fresh PID
	select {
	case fresh = <-started:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	if fresh == a {
		t.Fatal("restart reused old identity")
	}
	// The notification is sent inside Started; readiness follows its return.
	readyPID(t, n, fresh)
	for _, target := range []struct {
		ref  Ref
		want PID
	}{{n.Ref("a"), fresh}, {n.Ref("b"), b}} {
		got, err := n.Call[PID](t.Context(), target.ref, "pid")
		if err != nil || got != target.want {
			t.Fatalf("binding: %v %v", got, err)
		}
	}
}

func TestActorSpawnKindUsesRegisteredFactoryAndPhysicalParent(t *testing.T) {
	n := testNode(t, NodeOptions{Kinds: []KindSpec{{Name: "child", Version: "1", Factory: FromFunc(func(a *Actor, message any) error {
		if message == "parent" {
			return a.Reply(a.Parent())
		}
		return nil
	})}}})
	var child PID
	parent := spawn(t, n, FromFunc(func(a *Actor, message any) error {
		if _, ok := message.(Started); ok {
			var err error
			child, err = a.SpawnKind("child", "1", WithName("child"))
			return err
		}
		return nil
	}), WithName("parent"))
	got, err := n.Call[PID](t.Context(), child, "parent")
	if err != nil || got != parent {
		t.Fatalf("child parent: %v %v", got, err)
	}
	detail, err := n.ActorSnapshot(child)
	if err != nil || detail.Path != "parent/child" || detail.Kind != "child" || detail.Version != "1" {
		t.Fatalf("registered child: %+v %v", detail, err)
	}
	if err := n.StopActor(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	if err := n.Send(child, "parent"); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("child outlived parent: %v", err)
	}
}
