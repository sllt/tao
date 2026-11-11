# Tao Actor

English | [简体中文](README_zh.md)

`github.com/sllt/tao/actor` provides an OTP-style Actor runtime for Go with sequential message processing, bounded mailboxes, typed request/reply, hierarchical supervision, and multi-node communication.
The Go package name is `actor`, requiring Go 1.27+.

## Quick Start

Run from the repository root:

```sh
go run ./actor/examples/counter
```

The following example spawns an actor and performs a typed synchronous `Call`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/sllt/tao/actor"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	node, err := actor.NewNode(actor.NodeOptions{Name: "local"})
	if err != nil {
		return err
	}
	defer node.Stop(ctx)

	pid, err := node.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		name, ok := message.(string)
		if !ok {
			return nil
		}
		err := self.Reply("hello " + name)
		if errors.Is(err, actor.ErrReplyExpired) {
			return nil
		}
		return err
	}))
	if err != nil {
		return err
	}

	reply, err := node.Call[string](ctx, pid, "Tao")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
}
```

Output: `hello Tao`.
For stateful actors, embed `actor.Actor` in a struct and implement `Receive(any) error`; the factory function constructs a fresh instance on initial spawn and on every supervisor restart. `FromFunc` is convenient for stateless services and tests (any mutable state captured by a closure is not automatically isolated across restarts).

## Core API Overview

| Operation | Entry Point | Outcome |
| :--- | :--- | :--- |
| Spawn instance | `Node.Spawn` / `Actor.Spawn` | Waits for `Started` to complete and returns `PID` |
| One-way message | `Node.Send` / `Actor.Send` | Returns admission or delivery error; does not wait for handler execution |
| Synchronous call | `Node.Call[T]` / `Actor.CallAs[T]` | Returns typed result and error |
| Ordered call admission | `Node.BeginCall` | Admits call to a local `PID` synchronously and delivers `CallCompletion` via non-blocking callback |
| Asynchronous request | `Actor.Request` | Returns `RequestID`; delivers `CallResult` back to `Receive` |
| Reply to call | `Reply` / `ReplyError` | Sends business result or error to the active caller |
| Deferred reply | `Defer` | Returns a single-use `DeferredReply` ticket usable from other goroutines or future turns |
| Lifecycle observation | `Monitor` / `Link` | Delivers `Down` / `Exit` messages to `Receive` |
| State handoff | `Replace` / `Handoff` | Replaces local behavior in-place or migrates a leaf actor across nodes |

`NewNode` creates a local-only node without opening network listeners; use `StartNode` and `Connect` for remote communication.
`PID` identifies a specific actor instance and becomes stale after a restart or replacement; `Ref` identifies a logical path. Use `ParseRef` when validating external or configuration input.

## Usage Guidelines

- Returning a non-nil error or panicking from `Receive` triggers supervision. Return business rejections via `ReplyError(err)` and return `nil` from `Receive`; callers that time out or cancel may cause `Reply` to return `ErrReplyExpired`, which handlers can explicitly ignore.
- Actor state and capabilities (`Sender`, `Context`, `Send`, `Call`, `Reply`) must only be accessed synchronously inside `Receive`. Completion of asynchronous `Request` calls is also delivered sequentially through `Receive`.
- Local messages pass Go values directly without copying; never mutate slices, maps, or pointers after sending them.
- Mailboxes and pending call tables are bounded; handle `ErrMailboxFull`, `ErrCapacity`, and context deadlines explicitly.
- Remote operations that fail with `Unknown` outcome are never replayed automatically. `Replace` and `Handoff` do not provide persistent crash recovery after process loss.

For full semantics, async request quotas, and migration contracts, see the [Actor Guide](../docs/actor.md).

## Examples & Extensions

| Example / Module | Description |
| :--- | :--- |
| [counter](examples/counter) | Local `Send` and typed `Call` / `Reply` |
| [recovery](examples/recovery) | Supervisor restart, `PID` invalidation, and stable `Ref` |
| [two-node](examples/two-node) | Cross-process remote `SendConfirmed` and `Call` |
| [replace](examples/replace), [handoff](examples/handoff) | In-place behavior upgrade and cross-node state migration |
| [HTTP API](examples/http-api/README.md) | HTTP request mapping, business error classification, and graceful shutdown |
| [Web Observer](observer/README.md) | Embedded UI for supervision trees, mailboxes, and state inspection |
| [etcd Directory](etcd/README.md) | Cluster-wide global `Ref` ownership and atomic route handoff |
| [Protobuf Codec](codec/protobuf/README.md) | Optional Protobuf wire codec for business messages |
| [Erlang Adapter](erlang/README.md) | Native Erlang/OTP distribution v6 & `gen_server` interoperability |
| [Soak](examples/soak/README.md) | Continuous mixed load and fault-injection verification |

## Testing & License

Run inside `actor/`:

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```

See the [Contributing Guide](../CONTRIBUTING.md) for development conventions. Licensed under the [MIT License](../LICENSE).
