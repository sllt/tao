package actor

import (
	"errors"
	"testing"
)

func TestSupervisorFactoryLifecycleAndStrategy(t *testing.T) {
	n := testNode(t, NodeOptions{})
	starts := make(chan PID, 4)
	stops := make(chan string, 4)
	child := func(name string) Factory {
		return FromFunc(func(a *Actor, msg any) error {
			switch msg.(type) {
			case Started:
				starts <- a.PID()
			case Stopped:
				stops <- name
			}
			if msg == "crash" {
				return errors.New("planned failure")
			}
			if msg == "pid" {
				return a.Reply(a.PID())
			}
			return nil
		})
	}
	spec := SupervisorSpec{Strategy: RestForOne, Children: []ChildSpec{{Name: "one", Factory: child("one")}, {Name: "two", Factory: child("two")}}}
	factory := Supervisor(spec)
	spec.Children[0].Name = "changed-after-construction"
	parent, err := n.Spawn(factory, WithName("tree"))
	if err != nil {
		t.Fatal(err)
	}
	first, second := <-starts, <-starts
	if got, err := n.Call[PID](t.Context(), Ref{Path: "tree/one"}, "pid"); err != nil || got != first {
		t.Fatalf("child path: %v %v", got, err)
	}
	if err := n.Send(first, "crash"); err != nil {
		t.Fatal(err)
	}
	nextFirst, nextSecond := <-starts, <-starts
	if nextFirst == first || nextSecond == second {
		t.Fatal("RestForOne failed to restart later child")
	}
	<-stops
	<-stops
	if err := n.StopActor(t.Context(), parent); err != nil {
		t.Fatal(err)
	}
	if a, b := <-stops, <-stops; a != "two" || b != "one" {
		t.Fatalf("stop order: %s %s", a, b)
	}
}

func TestSupervisorFactoryFailedStartupCleansChildren(t *testing.T) {
	n := testNode(t, NodeOptions{})
	stopped := make(chan struct{})
	_, err := n.Spawn(Supervisor(SupervisorSpec{Children: []ChildSpec{
		{Name: "first", Factory: FromFunc(func(_ *Actor, msg any) error {
			if _, ok := msg.(Stopped); ok {
				close(stopped)
			}
			return nil
		})},
		{Name: "invalid", Factory: nil},
	}}), WithName("tree"))
	if err == nil {
		t.Fatal("invalid child factory accepted")
	}
	signal(t, stopped)
	if err := n.Send(Ref{Path: "tree/first"}, "check"); !errors.Is(err, ErrNoProcess) {
		t.Fatalf("partial subtree survived: %v", err)
	}
}
