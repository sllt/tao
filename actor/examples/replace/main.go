package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/sllt/tao/actor"
)

type add struct{ Amount int64 }
type get struct{}
type counter struct {
	actor.Actor
	value, multiplier int64
}

func (c *counter) Receive(message any) error {
	switch message := message.(type) {
	case add:
		c.value += message.Amount * c.multiplier
	case get:
		if err := c.Reply(c.value); !errors.Is(err, actor.ErrReplyExpired) {
			return err
		}
	case actor.Snapshotting:
		return c.Reply(c.value)
	case actor.Upgraded:
		return message.Restore(&c.value)
	}
	return nil
}

func main() {
	v1 := actor.KindSpec{Name: "counter", Version: "1", Factory: func() actor.Behavior { return &counter{multiplier: 1} }}
	v2 := actor.KindSpec{Name: "counter", Version: "2", Factory: func() actor.Behavior { return &counter{multiplier: 2} }}
	node, err := actor.NewNode(actor.NodeOptions{Name: "local", Kinds: []actor.KindSpec{v1, v2}})
	if err != nil {
		log.Fatal(err)
	}
	defer node.Stop(context.Background())
	old, err := node.SpawnKind("counter", "1", actor.WithName("counter"))
	if err != nil {
		log.Fatal(err)
	}
	ref := node.Ref("counter")
	if err := node.Send(ref, add{42}); err != nil {
		log.Fatal(err)
	}
	before, err := node.Call[int64](context.Background(), ref, get{})
	if err != nil {
		log.Fatal(err)
	}
	if err := node.Replace(context.Background(), old, v2); err != nil {
		log.Fatal(err)
	}
	restored, err := node.Call[int64](context.Background(), ref, get{})
	if err != nil {
		log.Fatal(err)
	}
	if err := node.Send(ref, add{1}); err != nil {
		log.Fatal(err)
	}
	after, err := node.Call[int64](context.Background(), ref, get{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("before=%d; restored=%d; v2 add(1)=%d\n", before, restored, after)
}
