# Actor Guide

English | [简体中文](actor_zh.md)

This guide covers the runtime semantics of `github.com/sllt/tao/actor`. For a quick-start walkthrough of spawning and calling actors, see the [Actor README](../actor/README.md).

## Instances & Logical Addresses

`Node.Spawn` accepts a `Factory` function, constructs the actor instance, waits for its `Started` message to succeed, and publishes its identity. Behavior types embed `actor.Actor` and implement `Receive(any) error`. Because `Factory` is called on initial `Spawn` and on every supervisor restart, never share mutable closure state across instances.

| Address Type | Semantics |
| :--- | :--- |
| `PID` | Contains node name, `BootID`, and numeric instance ID; binds to a specific instance and becomes stale after restart or replacement |
| `Ref{Node: "east", Path: "app/counter"}` | Binds to a logical path on a specific node; resolves to the new instance after a restart |
| `Ref{Path: "app/counter"}` | Resolves locally when no `Directory` is configured; acts as a cluster-wide global logical path when a `Directory` is configured |

`actor.ParseRef` parses a bare path or `node://node/path` URL into a `Ref` (returning `ErrInvalidRef` on malformed input). It does not trim whitespace, decode percent-escapes, or clean dot segments, and rejects empty segments, `.`/`..`, credentials, ports, query strings, and fragments. `Node.Ref` is a convenience constructor rather than an external input validator.

## Message Processing & Ownership

`Receive` executes sequentially for a given actor instance. When an actor's mailbox is empty, it releases its execution claim until a new message wakes it; a blocked actor never monopolizes a fixed-size worker pool. Each mailbox enforces a hard capacity limit (default `1024` messages); `ErrMailboxFull` indicates that the message was not admitted into the mailbox.

System notifications and user messages have separate scheduling priorities—do not assume they form a single unified FIFO queue. `Stop`, `Replace`, `Handoff`, and `Inspect` operations run at clean execution boundaries between turns.

Actor capabilities (`Sender`, `Context`, `Send`, `Call`, `Reply`) must only be used on the synchronous call stack of `Receive`. Concurrent callers outside the actor use `Node` methods; asynchronous replies from external goroutines or later turns use `DeferredReply`. Local messages pass Go values without copying—never mutate shared maps, slices, or pointers after sending.

## Synchronous Calls & Error Handling

`Node.Call[T]` and package-level `actor.Call[T]` return typed replies. Inside an actor, use `CallAs[T]` or `Call` (which returns `any`). A reply type mismatch returns `ErrReplyType`; `nil` is valid for nilable types (interfaces, pointers, slices, maps, funcs, channels) and invalid for scalar types such as `int`.

When the caller's context has no deadline, `Call` applies `NodeOptions.CallTimeout` (default 5 seconds). Caller cancellation, target stop, or target crash invalidates the reply ticket. Calling oneself synchronously returns `ErrSelfCall`. Synchronous cycles (`A -> B -> A`) can deadlock and should be refactored to use `Actor.Request`.

| Scenario | Handler Action |
| :--- | :--- |
| Business success | `Reply(value)` |
| Business rejection | `ReplyError(businessError)`, then return `nil` from `Receive` |
| Reply asynchronously / later | Call `Defer()` and retain the single-use `DeferredReply` ticket |
| `Receive` returns without `Reply` or `Defer` | Caller receives `ErrNoReply` |
| Caller timed out or canceled before reply | `Reply` returns `ErrReplyExpired` |
| Actor internal failure | Return a non-nil error or panic from `Receive` to trigger supervision |

`Reply` returns the delivery status of the reply itself, which is distinct from an actor crash. Services that wish to ignore late replies when callers stop waiting can filter `ErrReplyExpired`:

```go
err := a.Reply(value)
if errors.Is(err, actor.ErrReplyExpired) {
	return nil
}
return err
```

Apply the same check to `DeferredReply.Reply` / `ReplyError`. Duplicate replies or invalid ticket usage should still be surfaced rather than blindly ignored.

## Asynchronous Requests

`Actor.Request(ctx, target, message)` initiates a non-blocking outbound call and returns a `RequestID` (or an admission error). Once admitted, routing, call quotas, timeouts, and the peer's reply are delivered back to the requester's system mailbox as a `CallResult{ID, Value, Err}` message and processed sequentially by `Receive`.

- Correlate in-flight state by `RequestID`. Results arrive in completion order rather than submission order, with `Sender() == nil`.
- Deadlines start at `Request` submission (falling back to `CallTimeout` if `ctx` has no deadline). Passing `a.Context()` inherits cancellation from the currently active inbound `Call`; pass an independent context for background work.
- `Defer` defers an inbound call's reply, whereas `Request` initiates an outbound call. To forward a call asynchronously, save `Defer()` and complete the `DeferredReply` when `CallResult` arrives.
- An actor may `Request` itself (the request is processed after the current `Receive` returns, followed by `CallResult`), whereas synchronous `Call` to self is rejected with `ErrSelfCall`.
- `CancelRequest(id)` requests cancellation of an uncompleted operation; a `true` return does not guarantee the reply did not win the race, nor does it roll back remote side effects.

`NodeOptions.MaxAsyncRequests` bounds the sum of in-flight `Request`s and unconsumed `CallResult`s on the node (defaulting to `MaxPendingCalls`), while each actor is bounded by its `MailboxSize`. A quota slot is released only when `Receive` begins processing `CallResult`.

`Stop` cancels in-flight requests and discards unconsumed results. `Replace` and `Handoff` return `ErrAsyncPending` if any async request is still running or awaiting consumption. See `ExampleActor_Request` in [example_test.go](../actor/example_test.go) and the async bridge in the [Soak Example](../actor/examples/soak/main.go).

## Supervision & Lifecycle Observation

The default supervision policy is `OneForOne` with `Permanent` children, alongside `OneForAll`, `RestForOne`, `Transient`, and `Temporary`. Explicit `StopActor`, parent shutdown, and supervisor-initiated shutdown do not trigger `Permanent` restarts.

By default, an actor's old mailbox is discarded on restart; `PreserveRef` retains unexecuted messages addressed to logical `Ref`s while discarding messages addressed to the old `PID`. If an actor fails to stop before its shutdown deadline, the runtime marks its path `ErrUnresponsive` so a replacement instance does not race with a hung goroutine.

`Monitor` watches a specific `PID` and delivers a `Down` message on termination or node unreachability (`ErrNodeUnreachable` does not prove the remote process exited). `Demonitor` cancels an unconsumed monitor. `Link` establishes a bidirectional failure relationship; abnormal exits propagate unless `SetTrapExit(true)` converts them into `Exit` messages.

## Networking & Codecs

`NewNode` does not listen on the network. Use `StartNode("node1@127.0.0.1:0", opts)` (or pass an empty listen address for an outbound-only node) and `Connect` (or `Network.Peers`) to establish peer links.

By default, nodes use `Isolated` connection groups separating `inbox`, `reply`, and `control` traffic. Remote sends preserve FIFO order per peer/class, and business replies bypass the requester's user mailbox. Business payloads default to CBOR (`CBORCodec`), cleanly separated from the `HA/2` wire envelope; `JSONCodec` and the optional [Protobuf Codec](../actor/codec/protobuf/README.md) are also supported.

All network message types (including primitive reply types) must be explicitly registered with a stable `Name` and `Version`.

| Operation | Meaning of `nil` Error |
| :--- | :--- |
| Remote `Send` | Accepted into the local outbound send queue |
| `SendConfirmed` | Accepted into the remote actor's mailbox |
| `Call` | Business reply received from the remote handler |

`RemoteError` exposes `Code` and `Outcome` (`NotSent`, `Rejected`, `Responded`, `Unknown`) and supports `errors.Is`. Operations ending in `Unknown` are never automatically replayed.

## Directory & State Handoff

`MemoryDirectory` supports single-process clusters and tests; multi-process clusters use the [etcd Directory](../actor/etcd/README.md). Network partitions never automatically seize route ownership; stale routes are re-resolved only when a peer explicitly rejects admission (`Rejected` / `NotSent` with `ErrRedirect`).

### `Replace` (In-Place Upgrade)

`Node.Replace(ctx, target, kind)` upgrades a leaf actor in-place on the same node. It freezes the actor at a `Receive` boundary, waits for pending `DeferredReply` tickets, requests a read-only state snapshot via `Snapshotting`, initializes the new instance with `Started{Restoring: true}` and `Upgraded.Restore`, commits the route swap, delivers `Activated`, and transfers eligible `Ref`-addressed mailbox messages.

### `Handoff` (Cross-Node Migration)

`Node.Handoff(ctx, target, options)` migrates a handoff-enabled leaf actor to another node where the target `Kind`/`Version` is registered. The runtime freezes consumption, streams the state snapshot and global `Ref` mailbox backlog (plus any tail delta), and atomically commits ownership via `Directory.CommitMove` before activating the target instance. `DrainAndHandoff` migrates all eligible actors on a node during rolling upgrades or node drain.

## Diagnostics

`Node.Stats` reports live counts of actors, queued messages, pending calls, async requests, and lifecycle transitions. When diagnostics are enabled (`Node.EnableDiagnostics` or via [Web Observer](../actor/observer/README.md)), `Snapshot`, `ActorSnapshot`, and `Node.Inspect` provide per-actor timing, call graphs, and serialized state inspection.
