package main

import (
	"context"
	"fmt"
	"log"

	"github.com/sllt/tao/actor"
)

type add int
type get struct{}
type counter struct {
	actor.Actor
	value int
}

func (c *counter) Receive(message any) error {
	switch message := message.(type) {
	case add:
		c.value += int(message)
	case get:
		return c.Reply(c.value)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	node, err := actor.NewNode(actor.NodeOptions{Name: "local"})
	if err != nil {
		return err
	}
	defer node.Stop(context.Background())
	if _, err = node.Spawn(actor.Supervisor(actor.SupervisorSpec{
		Children: []actor.ChildSpec{{Name: "counter", Factory: func() actor.Behavior { return &counter{} }}},
	}), actor.WithName("app")); err != nil {
		return err
	}
	ref := node.Ref("app/counter")
	if err = node.Send(ref, add(40)); err != nil {
		return err
	}
	if err = node.Send(ref, add(2)); err != nil {
		return err
	}
	value, err := node.Call[int](context.Background(), ref, get{})
	if err != nil {
		return err
	}
	fmt.Printf("counter=%d\n", value)
	return nil
}
