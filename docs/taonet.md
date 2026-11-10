# taonet Guide

English | [简体中文](taonet_zh.md)

`taonet` provides bidirectional `Request`, `Notify`, ordered message `Channel`, and byte `Stream` primitives over ordered connections. The module path is `github.com/sllt/tao/taonet`, requiring Go 1.26+ and depending solely on the Go standard library. See the [taonet README](../taonet/README.md) for quick-start commands.

## Connection & Session Model

A `Session` wraps a single physical connection and its multiplexing state. Both peers can register routes, initiate `Request`s, send `Notify` messages, and open `Channel`s or `Stream`s. `Initiator` and `Acceptor` distinguish handshake roles without restricting the direction of business calls.

`taonet` uses an internal `HN-MUX/1` multiplexer ([handshake.go](../taonet/handshake.go), [wire.go](../taonet/wire.go), and [internal/mux](../taonet/internal/mux)). Each connection carries a dedicated control stream while user operations run over their own multiplexed streams. A session becomes usable only after peer identity, feature flags, and flow-control quotas are negotiated.

## Peer Identity, Routing & Session Establishment

`SessionConfig.Local` specifies `Peer.ID`, `Peer.Instance`, and `Peer.Namespace`:

- `ID` identifies the logical peer; `Instance` distinguishes process incarnations (defaulting to a generated per-process ID when omitted).
- `Namespace` defaults to `"default"` and must match on both peers.
- `ID` and `Namespace` must be valid 1–255 byte UTF-8 strings without control characters.

Create a router with `NewMux()` and register handlers via `HandleRequest`, `HandleNotify`, `HandleChannel`, and `HandleStream`. Each mode maintains an independent routing table; route names must be 1–128 ASCII alphanumeric, `/`, `.`, `_`, or `-` characters. Once a `Mux` is bound to an `Endpoint` or `Session`, its routes are frozen.

| Entry Point | Ownership & Context Semantics |
| :--- | :--- |
| `Dial(ctx, address, cfg)` | Dials and performs the initiator handshake; the returned `Session` owns the connection |
| `Accept(ctx, conn, cfg)` | Performs the acceptor handshake on an accepted `net.Conn` |
| `NewSession(ctx, conn, role, cfg)` | Takes ownership of `conn` (closing it on handshake failure as well) |
| `Endpoint.Serve(ctx, listener)` | Takes ownership of `listener` and accepts sessions until `ctx` ends |

For `Dial`, `Accept`, and `NewSession`, `ctx` governs only connection establishment; canceling it afterward does not close the established `Session`. Always shut down sessions and endpoints explicitly via `Shutdown(ctx)` or `Close()`.

## `Request`: Synchronous RPC with Business Reply

`Session.Request(ctx, route, msg)` waits for the remote `RequestHandler` to return a complete `Message` or error. Handlers can call `request.Session()` to invoke reverse RPCs over the same connection ([reverse-rpc](../taonet/examples/reverse-rpc/main.go)).

Callers must keep `msg.Body` and `msg.Metadata` immutable until `Request` returns. When `ctx` has no deadline, `DefaultRequestTimeout` (5s) applies; remaining deadline budget is propagated across the wire to the remote handler's context.

Handlers may return `*StatusError` with a stable application code (`app.<name>`, 5–64 lowercase ASCII bytes). Plain errors map to `CodeApplication` and handler panics map to `CodeInternal`.

## `Notify`: One-Way Notifications

- `Notify` copies `Body` and `Metadata` into a bounded local send buffer; a `nil` return indicates local enqueueing.
- `NotifyConfirmed` additionally waits for the remote notification queue to accept the message (without waiting for `NotifyHandler` execution to finish).

Once accepted into the local send queue, the caller may immediately reuse its input slice, and canceling a confirmation wait does not retract the queued notification.

## `Channel`: Bidirectional Ordered Messaging

`OpenChannel(ctx, route, metadata)` opens a bidirectional ordered message channel whose lifetime is bound to `ctx`. Each direction preserves FIFO order and enforces local queue limits (up to 4,096 items), session byte budgets, and peer message/byte windows.

### Sending & Receipts

| Method | Meaning of Completion | When Input Buffer May Be Reused |
| :--- | :--- | :--- |
| `Send` | Bounded local copy enqueued | Immediately upon return |
| `SendConfirmed` | Peer called `Delivery.Accept()` | Immediately upon return (copied on enqueue) |
| `SendWithReceipt` | Returns `SendReceipt` for an enqueued copy | Immediately upon return |
| `SendOwnedWithReceipt` | Returns `SendReceipt` borrowing caller buffer | After `<-receipt.Released()` closes |
| `SendOwnedUnconfirmed` | Returns `WriteReceipt` reporting local socket write | After `<-receipt.Released()` closes |

`SendWithReceipt` lets callers pipeline a batch of sends and wait on receipts afterward without spawning a goroutine per message.

```go
func sendAccepted(ctx context.Context, channel *taonet.Channel, message taonet.Message) error {
	receipt, err := channel.SendWithReceipt(ctx, message)
	if err != nil {
		return err
	}
	return receipt.Wait(ctx)
}
```

### Receiving, Decisions & `Release`

`Receive(ctx)` returns `*Delivery` (or `io.EOF` when the peer closes its write direction). Receivers must call `Accept()` or `Reject(code)` in arrival order (`ErrOutOfOrder` if skipped, `ErrAlreadyResolved` if repeated), and **must call `Release()`** when finished reading `Delivery.Message().Body`.

`Accept()` sends the acceptance ACK, while `Release()` returns the message/byte window credit to the peer and recycles the buffer. Releasing a delivery without calling `Accept`/`Reject` automatically rejects it with `CodeAbandoned`.

### Borrowed Buffers & Unconfirmed Mode

For `SendOwnedWithReceipt` and `SendOwnedUnconfirmed`, the caller's `Body` backing array and `Metadata` remain borrowed by the transport until `receipt.Released()` closes—even if `Wait(ctx)` fails, is canceled, or the channel closes early.

`SendOwnedUnconfirmed` omits per-message acceptance ACKs for higher-level protocols (such as `actor`) that carry their own end-to-end replies, while still enforcing FIFO order and receive windows (`channel-unconfirmed-v1`). When `channel-inline-v1` is negotiated, metadata-free messages up to 16 KiB use compact binary framing and inline ACKs.

### Graceful Half-Close

`Channel.CloseWrite()` closes the local send direction while draining queued messages and keeping ACK/WINDOW control active. See [duplex-chat](../taonet/examples/duplex-chat/main.go).

## `Stream`: Bidirectional Byte Streams & Half-Close

`OpenStream(ctx, route, metadata)` returns an `io.Reader` / `io.Writer` byte stream supporting concurrent read/write, `SetReadDeadline` / `SetWriteDeadline`, `CloseWrite()` half-close, and vectored zero-copy writes via `WritevBorrowed(net.Buffers) (written, released, err)`. See [file-copy](../taonet/examples/file-copy/main.go).

## Error Outcomes & Cancellation

Use `errors.As(err, &opError)` to inspect `*OpError` fields (`Code`, `Outcome`, `Remote`, `Operation`):

| `Outcome` | Meaning |
| :--- | :--- |
| `NotSent` | Operation never left the local node (e.g., validation or admission failure) |
| `Rejected` | Peer explicitly rejected the operation before handler dispatch |
| `Unknown` | Send was attempted, but the remote outcome cannot be proven |
| `Responded` | Peer handler executed and returned an explicit business error |

`taonet` never automatically retries `Unknown` operations.

## `Endpoint`: Connection Management & Traffic Classes

`NewEndpoint(EndpointConfig{Session: cfg})` manages connection pooling, concurrent dial coalescing, and traffic-class isolation:

- **`Compact` (default)**: Multiplexes all operations to a peer over a single shared session.
- **`Isolated`**: Opens a primary `ClassControl` session plus on-demand physical sessions for `ClassInteractive`, `ClassReply`, and `ClassBulk` (even when the remote peer is an outbound-only client behind NAT, via reverse-connect coordination). See [isolated-peer](../taonet/examples/isolated-peer/main.go).

## Identity & TLS

By default, `taonet` requires TLS 1.3, mutual certificate verification (`RootCAs` / `ClientCAs`), `taonet/1` ALPN, and SAN verification matching `Peer.ID`. `AllowInsecureLoopback` permits plaintext TCP only when both socket endpoints are loopback addresses, intended for local examples and tests.

## Quotas & Backpressure

| Option | Default | Purpose |
| :--- | :--- | :--- |
| `Limits.MaxMessageSize` | 4 MiB | Maximum `Request` / `Notify` payload size (1 B – 64 MiB) |
| `Limits.MaxMetadataBytes` / `MaxMetadataEntries` | 8 KiB / 32 | Metadata size and entry limits |
| `Limits.MaxOperations` / `MaxHandlers` | 256 / `min(64, MaxOperations)` | Concurrent session operations and inbound handler slots |
| `Limits.ReceiveBytes` / `SendBytes` | 16 MiB / 8 MiB | Session receive and send memory budgets |
| `Limits.ChannelMaxMessage` | 256 KiB | Maximum single `Channel` message size (1 B – 4 MiB) |
| `Limits.ChannelWindowBytes` / `ChannelWindowMessages` | 1 MiB / 256 | Per-channel receive window |
| `SessionConfig.StreamReceiveWindow` | 64 KiB | Per-stream mux receive window (4 KiB – 1 MiB, multiple of 4 KiB) |
| `EndpointConfig.MaxSessions` / `ManagedBytes` | 8 / 256 MiB | Physical session cap and total endpoint reservation budget |

## Verification

Run from `taonet/`:

```sh
go test ./...
go test -race ./...
GOWORK=off go test ./...
go test ./internal/mux -run '^$' -fuzz '^FuzzHeader$' -fuzztime=10s
```
