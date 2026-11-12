// This demo runs three TCP nodes in one process. MemoryDirectory is shared only
// for this local demonstration; separate processes should use the etcd adapter.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/sllt/tao/actor"
)

type command struct {
	Add int64
	Get bool
}
type counter struct {
	actor.Actor
	value int64
}

func (c *counter) Receive(message any) error {
	switch message := message.(type) {
	case command:
		c.value += message.Add
		if message.Get {
			return c.Reply(c.value)
		}
	case actor.Snapshotting:
		return c.Reply(c.value)
	case actor.Upgraded:
		return message.Restore(&c.value)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	directory := actor.NewMemoryDirectory()
	create := func(name, address string) (*actor.Node, error) {
		return actor.StartNode(address, actor.NodeOptions{Name: name, Directory: directory, Network: actor.NetworkOptions{AllowInsecureLoopback: true}, Register: []actor.MessageType{{Name: "command", Version: 1, Sample: command{}}, {Name: "count", Version: 1, Sample: int64(0)}}, Kinds: []actor.KindSpec{{Name: "counter", Version: "1", Factory: func() actor.Behavior { return &counter{} }}}})
	}
	source, err := create("source", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer source.Stop(context.Background())
	target, err := create("target", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer target.Stop(context.Background())
	caller, err := create("caller", "")
	if err != nil {
		return err
	}
	defer caller.Stop(context.Background())
	if err := source.Connect(ctx, "target", target.ListenAddr()); err != nil {
		return err
	}
	if err := caller.Connect(ctx, "source", source.ListenAddr()); err != nil {
		return err
	}
	if err := caller.Connect(ctx, "target", target.ListenAddr()); err != nil {
		return err
	}
	if _, err := source.SpawnKind("counter", "1", actor.WithName("counter"), actor.WithHandoff(true)); err != nil {
		return err
	}
	ref := actor.Ref{Path: "counter"}
	if err := caller.SendConfirmed(ctx, ref, command{Add: 42}); err != nil {
		return err
	}
	report, err := source.DrainAndHandoff(ctx, actor.HandoffOptions{TargetNode: "target"})
	if err != nil {
		return err
	}
	value, err := caller.Call[int64](ctx, ref, command{Get: true})
	if err != nil {
		return err
	}
	fmt.Printf("committed=%d; counter=%d; source stopped=%t; caller listens=%t\n", len(report.Committed), value, report.Stopped, caller.ListenAddr() != "")
	return nil
}
