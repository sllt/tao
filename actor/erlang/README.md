# Tao Actor Erlang Adapter

English | [简体中文](README_zh.md)

`github.com/sllt/tao/actor/erlang` (package `erlangactor`) bridges local Tao Actors (`github.com/sllt/tao/actor`) with native Erlang/OTP distribution v6 (`github.com/sllt/tao/erlang`).
It requires Go 1.27+. Neither `actor` nor `taonet` imports `erlang` or `erlangactor`.

## Quick Start

Run from the repository root:

```sh
go run ./actor/erlang/examples/bridge
```

Output:

```text
bridged_counter=42
```

## Exporting Local Actors & Calling BEAM

```go
bridge, err := erlangactor.Attach(taoNode, erlangNode)
if err != nil {
	return err
}
defer bridge.Close(ctx)

// Export a concrete local Tao PID under an Erlang registered name with an explicit Schema.
binding, err := bridge.Export("counter", counterPID, counterSchema)

// Inside a Tao Actor's Receive, call BEAM asynchronously without blocking the Actor mailbox:
reqID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
	Target:  erlang.Registered("orders", "beam@127.0.0.1"),
	Request: etf.Tuple{etf.Atom("lookup"), int64(42)},
})
// Handle actor.CallResult{ID: reqID} in a subsequent Receive turn.
```

- **Explicit `Schema`**: `DecodeCall`, `DecodeCast`, `DecodeInfo`, and `EncodeReply` perform deterministic value translation outside Actor runtime locks (`erlangactor.Funcs` and `erlangactor.PassthroughSchema()` are provided). `Actor.Sender()` is never forged; pass `WithSourceEnvelope(true)` to wrap inbound messages in `erlangactor.Inbound{Peer, From, HasFrom, Message}`.
- **Ordered Inbound Admission**: Per-peer wire order is preserved—`gen_server:cast` uses `Node.Send` and `gen_server:call` uses `Node.BeginCall` synchronously on the peer's single ordered dispatcher without blocking on business execution.
- **Inbound Call Budget**: Inbound `$gen_call` messages do not carry caller timeout values and are not truncated to `actor.NodeOptions.CallTimeout`. Configure `WithInboundCallTimeout(d)` when an explicit server-side execution deadline is desired.
- **Reply-Before-DOWN**: If a handler commits `Reply` / `DeferredReply.Reply` and stops in the same turn, the `Binding` drains pre-stop replies onto the peer writer before emitting `DOWN`, while uncompleted `Defer` tickets fail with `TargetExit` (no duplicate business reply). Business `ReplyError(actor.ErrStopped)` is encoded as a normal `Reply`, never mistaken for Actor termination.
- **Non-Blocking Outbound Connector**: `bridge.Outbound()` defers inside `Receive` and submits per-peer FIFO operations via `Client.CallAsync`, enabling safe `Tao Actor A -> BEAM -> Tao Actor A` callbacks when `A` uses `Actor.Request`.

## Lifecycle, Overload & Migration Boundaries

- **Borrowed Nodes**: `Attach` borrows `taoNode` and `erlangNode`. Normal teardown order is `bridge.Close(ctx)` -> `erlangNode.Shutdown(ctx)` -> `taoNode.Stop(ctx)`; out-of-order shutdown or EPMD registration loss automatically closes all bindings and cleans up adaptive guardian `Monitor` groups.
- **`TrapExit` & `exit(Pid, kill)`**: `WithTrapExit(true)` delivers trappable Erlang exits as `erlangactor.Exit` DTOs (separate from `actor.Exit`). Explicit `exit(BindingPid, kill)` is untrappable on the Erlang proxy (`DOWN` reason `killed`), cancels active bridge calls, and leaves the local Tao Actor running unless `WithStopActor(true)` is configured.
- **Bounded Overload**: Full Tao mailboxes return `{error, {tao, overloaded}}` (via `Schema.EncodeReply`) for `$gen_call`. Because `$gen_cast` / `!` have no reply channel, mailbox exhaustion terminates that `Binding` with `{tao, overloaded, DetailBinary}` rather than silently dropping messages while pretending healthy.
- **`Replace` & `Handoff`**: Guardian `Monitor`s observe target Actors without blocking `Node.Handoff`. Because `Binding` targets a concrete local `actor.PID`, the recommended migration sequence is: stop new traffic -> drain application state -> `binding.Close(ctx)` -> `Handoff` -> re-export the new `PID` on the target node. Direct `Handoff` without `binding.Close` succeeds and terminates the source `Binding` with `{tao, handed_off, DetailBinary}`.

## Testing & License

Run from `actor/erlang/`:

```sh
GOWORK=off go test -race ./...
```

Licensed under the [MIT License](../../LICENSE).
