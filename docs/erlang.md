# Erlang/OTP Interoperability Guide

English | [简体中文](erlang_zh.md)

Tao provides native **Erlang distribution v6 and `gen_server` messaging interoperability** across two independent modules:

- [`github.com/sllt/tao/erlang`](../erlang/README.md) (Go 1.26+, zero external dependencies): Standalone Erlang node, EPMD v6 discovery, TCP and `inet_tls` v6 handshakes, owned ETF (`erlang/etf`), 2048-slot atom caches, distribution fragments, virtual `Client`s, exported `Endpoint`s, and process monitors/links.
- [`github.com/sllt/tao/actor/erlang`](../actor/erlang/README.md) (package `erlangactor`, Go 1.27+): Thin adapter bridging local Tao Actors with `erlang.Node` using explicit `Schema` translation, ordered local admission (`Node.BeginCall`), and non-blocking outbound `Actor.Request` execution.

Neither `actor` nor `taonet` imports `erlang` or `erlangactor`.

## Architecture & Scope

A Go process participates directly in an Erlang/Elixir cluster (as a hidden node by default) without running a BEAM sidecar. Tao Actors retain `taonet` (`HA/2` + CBOR) for Tao-to-Tao traffic while exposing explicitly bound local `PID`s over native Erlang distribution on a separate port.

| Capability | Behavior | Explicit Boundary |
| :--- | :--- | :--- |
| Discovery & Handshake | External EPMD v6 (`ALIVE2_X_RESP`, `PORT_PLEASE2_REQ`) or `StaticRoutes`; v6 `'N'` handshake, MD5 cookie digest, simultaneous-connect tie-break, post-auth `alive` replacement | Static ASCII `name@host` names; does not spawn EPMD or implement legacy `'n'` handshakes |
| Transport | Native TCP (packet-2 -> packet-4) and `-proto_dist inet_tls` (packet-4 -> packet-4) | TLS never falls back to plaintext; independent of `taonet` ALPN/certificates |
| `net_adm:ping/1` | Built-in `net_kernel` answers `{is_auth, Node}` with `yes` on authenticated peers | Hidden node by default (`Published: false`); reserved names cannot be user-exported |
| `gen_server` Messaging | `!`, `gen_server:cast`, `gen_server:call`, `gen_server:send_request`, plain `Ref`, `[alias \| Ref]`, nested alias tags, cross-process `gen_server:reply` | `Cast`/`Send` success indicates bounded local queue admission, not remote mailbox receipt |
| Monitors & Links | `MONITOR_P` / `DEMONITOR_P` / `MONITOR_P_EXIT` (21/28) preserving raw watched `Atom`/`PID`; `LINK` / `UNLINK_ID` / `UNLINK_ID_ACK` (35/36) including dead-local-PID ACKs | Bound to a specific proxy instance; does not transfer across same-name re-exports |
| Non-Goals | Inbound `{'system', From, Req}` immediately replies `{error, {tao, unsupported}}` | No `sys:*`, `gen_server:stop`, `start_link`, remote `spawn`, `rpc`/`erpc`, `global`, `pg`, or Mnesia; OTP 30 `ALTACT_SIG_SEND` (opcode 37) is future work |

Verified against official BEAM releases across **OTP 27, OTP 28, and OTP 29** over both TCP and `inet_tls`.

## Identity & ETF Ownership (`erlang/etf`)

Erlang `PID`, `Port`, and `Ref` values keep their full node name, 32-bit `Creation`, and words without truncating into `actor.PID`. Within a node incarnation, allocated IDs increase monotonically and are never recycled.

`etf.Term` uses explicit Go types (`etf.Atom`, `int64` / `*big.Int`, `float64`, `etf.Binary`, `etf.BitBinary`, `etf.Tuple`, `etf.List`, `etf.Nil`, `etf.Map`, `etf.PID`, `etf.Port`, `etf.Ref`, `etf.Export`, `etf.Fun`):

- Arbitrary Go strings are never guessed as atoms; use `etf.Atom`, `etf.UTF8Binary`, or `etf.Charlist` explicitly.
- Decoded terms own independent memory so network buffers return immediately to the pool.
- `etf.Map` preserves key/value pairs for arbitrary Erlang keys (including tuples/lists/binaries) and rejects duplicate keys under exact Erlang `=:=` semantics (`etf.TermEqual`).
- `etf.Fun` values are decoded into structured terms and re-encoded per target connection's atom cache rather than replaying raw wire slices.

## Bridging Tao Actors (`erlangactor`)

### Inbound Exports & Ordered Admission

`bridge.Export(name, localPID, schema, opts...)` creates a virtual Erlang proxy bound to `localPID` and monitored by an adaptive guardian group:

1. Each peer connection runs a single ordered dispatcher after updating its 2048-slot receive atom cache and reassembling any `131,69`/`131,70` fragments.
2. `gen_server:cast` and `!` admit via `Node.Send`, while `gen_server:call` admits synchronously via `Node.BeginCall` without waiting for `Receive` to finish—guaranteeing `cast(add)` followed by `call(get)` from the same caller enters the target mailbox in wire order.
3. Inbound `$gen_call` messages carry no caller timeout on the wire and are not truncated to `actor.NodeOptions.CallTimeout`; pass `erlangactor.WithInboundCallTimeout(d)` to enforce an explicit server-side execution budget.
4. When a target Actor commits `Reply` / `DeferredReply.Reply` and stops in the same turn, the binding drains all pre-stop replies onto the peer writer before emitting `DOWN` (`Reply-before-DOWN`), while uncompleted `Defer` tickets settle as `CompletionTargetExit` without generating a duplicate reply.

### Outbound Calls from Actors

`bridge.Outbound()` is a local Tao `actor.PID`. Inside `Receive`, call BEAM targets asynchronously with `Actor.Request` so the caller remains responsive to re-entrant callbacks (`Tao Actor A -> BEAM -> Tao Actor A`):

```go
reqID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
	Target:  erlang.Registered("orders", "beam@host.example"),
	Request: etf.Tuple{etf.Atom("lookup"), int64(42)},
})
```

### Overload, Failure Outcomes & Migration

- **Overload**: Full Tao mailboxes return `{error, {tao, overloaded}}` (via `Schema.EncodeReply`) on `$gen_call`. Because `$gen_cast` and `!` have no reply path, mailbox exhaustion terminates that binding with `{tao, overloaded, DetailBinary}` and notifies monitors/links rather than silently dropping messages.
- **Delivery Outcomes**: `*erlang.Error` distinguishes `OutcomeNotSent` (rejected before socket write), `OutcomeUnknown` (timeout/disconnect after bytes may have reached the wire—never replayed), and `OutcomeUnsupported`, while preserving raw `DOWN` `Reason` terms.
- **`Replace` & `Handoff`**: Guardian `Monitor`s do not block `Node.Handoff`. Because bindings target concrete local `PID`s, stop new traffic, drain application state, call `binding.Close(ctx)`, perform `Handoff`, and re-export on the destination node. Direct `Handoff` without `binding.Close` succeeds and terminates the old proxy with `{tao, handed_off, DetailBinary}`.
