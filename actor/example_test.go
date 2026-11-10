package actor_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/sllt/tao/actor"
)

func ExampleFromFunc() {
	node, err := actor.NewNode(actor.NodeOptions{Name: "local"})
	if err != nil {
		panic(err)
	}
	defer node.Stop(context.Background())
	pid, err := node.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		if name, ok := message.(string); ok {
			// A caller that stopped waiting is not a failure of this actor.
			if err := self.Reply("hello " + name); !errors.Is(err, actor.ErrReplyExpired) {
				return err
			}
		}
		return nil
	}), actor.WithName("greeter"))
	if err != nil {
		panic(err)
	}
	value, err := node.Call[string](context.Background(), pid, "Hive")
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	// Output: hello Hive
}

func ExampleParseRef() {
	ref, err := actor.ParseRef("node://east/app/counter")
	fmt.Println(ref.Node, ref.Path, err)
	_, err = actor.ParseRef("node://east/app//counter")
	fmt.Println(errors.Is(err, actor.ErrInvalidRef))
	// Output:
	// east app/counter <nil>
	// true
}

// Request frees the caller to handle the worker's callback before the original
// result arrives. A synchronous A -> B -> A Call would keep A waiting instead.
func ExampleActor_Request() {
	node, err := actor.NewNode(actor.NodeOptions{Name: "async"})
	if err != nil {
		panic(err)
	}
	defer node.Stop(context.Background())
	worker, err := node.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		if message == "compute" {
			value, err := self.CallAs[int](self.Context(), self.Sender(), "base")
			if err != nil {
				if err := self.ReplyError(err); !errors.Is(err, actor.ErrReplyExpired) {
					return err
				}
				return nil
			}
			if err := self.Reply(value + 2); !errors.Is(err, actor.ErrReplyExpired) {
				return err
			}
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}
	completed := make(chan actor.CallResult, 1)
	owner, err := node.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		switch message := message.(type) {
		case string:
			switch message {
			case "start":
				id, err := self.Request(context.Background(), worker, "compute")
				if err != nil {
					completed <- actor.CallResult{ID: id, Err: err}
				}
			case "base":
				if err := self.Reply(40); !errors.Is(err, actor.ErrReplyExpired) {
					return err
				}
			}
		case actor.CallResult:
			completed <- message
		}
		return nil
	}))
	if err != nil {
		panic(err)
	}
	if err := node.Send(owner, "start"); err != nil {
		panic(err)
	}
	result := <-completed
	fmt.Println(result.Value, result.Err)
	// Output: 42 <nil>
}
