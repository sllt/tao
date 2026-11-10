# taonet

English | [简体中文](README_zh.md)

`github.com/sllt/tao/taonet` is a zero-dependency bidirectional networking module built solely on the Go standard library, providing `Request`, `Notify`, ordered `Channel`, and byte `Stream` primitives. Both peers can initiate requests and open streams over a single multiplexed `Session`, while `Endpoint` manages connection pooling, traffic-class isolation, memory quotas, and graceful shutdown. `taonet` has no dependency on `actor`, `etcd`, or application message codecs.

## Requirements & Module

- **Minimum Go Version**: 1.26
- **Import Path**: `github.com/sllt/tao/taonet` (package `taonet`)
- **Workspace**: Linked via the root `go.work` alongside the `actor` modules (Go 1.27.3 toolchain).

## Quick Start

Run the bidirectional RPC example from `taonet/`:

```sh
go run ./examples/reverse-rpc
```

The server calls back into the client over the same physical connection and prints:

```text
desktop client: hello over one connection
```

Applications register routes on a `Mux` via `NewMux()`, configure the local `Peer` identity and routing table in `SessionConfig`, and establish connections via `Dial`, `Accept`, or `Endpoint`. Request handlers can invoke reverse RPCs on the caller via `Request.Session()`; see [reverse-rpc](examples/reverse-rpc/main.go) for the full code.

## Communication Primitives

| API | Purpose | Completion Boundary |
| :--- | :--- | :--- |
| `Session.Request` | Bounded request with business reply | Full reply returned by the remote handler |
| `Session.Notify` / `NotifyConfirmed` | One-way notification | Local enqueueing / Remote notification queue acceptance |
| `Session.OpenChannel` | Bidirectional ordered messages with per-message receipts | Ordered delivery; receipts distinguish local enqueueing, remote acceptance, or local socket write |
| `Session.OpenStream` | Bidirectional byte stream & half-close | Raw byte stream (`io.Reader` / `io.Writer`); application protocol defines completion |

Acceptance receipts are strictly separated from business execution completion. Zero-copy / borrowed-buffer sends must wait for `Released()` before reusing input slices; cancellation and `Unknown` outcomes never automatically replay business operations. For complete contracts, see the [taonet Guide](../docs/taonet.md).

By default, connections require TLS 1.3 with mutual certificate verification and the `taonet/1` ALPN protocol. Repository examples explicitly opt into plaintext loopback via `AllowInsecureLoopback` for convenience; see [Identity & TLS](../docs/taonet.md#identity--tls) for production TLS setup.

## Examples

Run from `taonet/`:

| Example | Command | Description |
| :--- | :--- | :--- |
| [Reverse RPC](examples/reverse-rpc/main.go) | `go run ./examples/reverse-rpc` | Server calls back into the client over the same connection |
| [Duplex Chat](examples/duplex-chat/main.go) | `go run ./examples/duplex-chat` | Bidirectional `Channel`, send receipts, `Accept`, and `Release` |
| [File Copy](examples/file-copy/main.go) | `go run ./examples/file-copy` | Fixed-buffer streaming upload, `CloseWrite` half-close, and digest reply |
| [Isolated Peer](examples/isolated-peer/main.go) | `go run ./examples/isolated-peer` | Four traffic-class physical connections (`Control`, `Interactive`, `Reply`, `Bulk`) and outbound-only client |
| [Mux Echo](examples/mux-echo/main.go) | `go run ./examples/mux-echo` | Low-level `HN-MUX/1` stream opening and half-close |

## Documentation

- [taonet Guide](../docs/taonet.md): Lifecycles, error outcomes, buffer ownership, `Endpoint`, TLS, and quotas.
- [Documentation Index](../docs/README.md): Overview of all repository modules.
- [Contributing Guide](../CONTRIBUTING.md): Development and verification workflow.

Public APIs reside in the root `taonet` package; the underlying `HN-MUX/1` multiplexer lives in [internal/mux](internal/mux).

## Testing

Run from `taonet/`:

```sh
go test ./...
go test -race ./...
GOWORK=off go test ./...
```

`GOWORK=off` verifies the module in isolation without the root workspace. All tests use loopback sockets and in-memory pipes with no external service dependencies.

## License

Licensed under the [MIT License](../LICENSE).
