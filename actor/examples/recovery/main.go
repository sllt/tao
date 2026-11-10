package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/sllt/tao/actor"
)

type ready struct{}
type worker struct {
	actor.Actor
	ready chan<- actor.PID
}

func (w *worker) Receive(message any) error {
	switch message := message.(type) {
	case actor.Started:
		return w.Send(w.PID(), ready{})
	case ready:
		w.ready <- w.PID()
	case string:
		if message == "crash" {
			return errors.New("demonstration failure")
		}
		if err := w.Reply("recovered"); !errors.Is(err, actor.ErrReplyExpired) {
			return err
		}
	}
	return nil
}

type observer struct {
	actor.Actor
	target actor.PID
	down   chan<- actor.Down
}

func (o *observer) Receive(message any) error {
	switch message := message.(type) {
	case actor.Started:
		_, err := o.Monitor(o.target)
		return err
	case actor.Down:
		o.down <- message
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	node, err := actor.NewNode(actor.NodeOptions{Name: "local"})
	if err != nil {
		return err
	}
	defer node.Stop(context.Background())
	readyQueue := make(chan actor.PID, 2)
	down := make(chan actor.Down, 1)
	old, err := node.Spawn(func() actor.Behavior { return &worker{ready: readyQueue} }, actor.WithName("worker"))
	if err != nil {
		return err
	}
	<-readyQueue
	if _, err = node.Spawn(func() actor.Behavior { return &observer{target: old, down: down} }, actor.WithRestart(actor.Temporary)); err != nil {
		return err
	}
	if err = node.Send(old, "crash"); err != nil {
		return err
	}
	select {
	case event := <-down:
		fmt.Printf("observed old PID exit: %v\n", errors.Is(event.Reason, actor.ErrProcessCrashed))
	case <-ctx.Done():
		return ctx.Err()
	}
	var fresh actor.PID
	select {
	case fresh = <-readyQueue:
	case <-ctx.Done():
		return ctx.Err()
	}
	value, err := node.Call[string](ctx, node.Ref("worker"), "get")
	if err != nil {
		return err
	}
	fmt.Printf("same Ref, new PID: %v; reply=%s\n", fresh != old, value)
	return nil
}
