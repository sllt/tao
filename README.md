# Tao

English | [简体中文](README_zh.md)

**Tao** is an OTP-style Actor runtime for Go, providing sequential message processing, request/reply, supervision trees, multi-node communication, and online state handoff.
Its underlying networking layer, **[taonet](taonet/README.md)**, is a standalone zero-dependency library supporting bidirectional RPC, ordered message channels, and byte streams over multiplexed connections.

## Highlights

- **Actor Runtime**: Sequential per-actor message processing, bounded mailboxes, synchronous `Call` and asynchronous `Request`, hierarchical supervision (`OneForOne`, `OneForAll`, `RestForOne`), and lifecycle observation (`Monitor` / `Link`).
- **Multi-Node & State Handoff**: Remote actor messaging over `taonet`, stable logical addressing (`Ref`), optional CAS-backed [etcd Directory](actor/etcd/README.md), in-place behavior hot-upgrade (`Replace`), and cross-node state migration (`Handoff`).
- **taonet**: Pure Go standard-library transport featuring a custom `HN-MUX/1` multiplexer, mutual TLS 1.3, traffic class isolation, credit-based backpressure, and explicit delivery receipts (`NotSent`, `Rejected`, `Responded`, `Unknown`).
- **Web Observer**: Optional embedded read-only [Web UI](actor/observer/README.md) for inspecting supervision trees, mailboxes, in-flight requests, and actor state.
- **Erlang/OTP Interoperability**: Standalone pure-Go [Erlang distribution v6](erlang/README.md) and [Tao Actor adapter](actor/erlang/README.md) for direct `gen_server` / `GenServer` messaging, monitors, and links without a BEAM sidecar.

## Modules

This repository uses a Go workspace (`go.work`) to manage multiple independent modules. `actor` and `taonet` can be used together or independently.

| Module | Import Path | Minimum Go Version | Description |
| :--- | :--- | :--- | :--- |
| [actor](actor/README.md) | `github.com/sllt/tao/actor` | 1.27 | Core Actor runtime, supervision, and remote adapter |
| [taonet](taonet/README.md) | `github.com/sllt/tao/taonet` | 1.26 | Zero-dependency multiplexed transport |
| [erlang](erlang/README.md) | `github.com/sllt/tao/erlang` | 1.26 | Zero-dependency Erlang distribution v6 & ETF |
| [actor/erlang](actor/erlang/README.md) | `github.com/sllt/tao/actor/erlang` | 1.27 | Optional Tao Actor adapter for Erlang `gen_server` interop |
| [actor/etcd](actor/etcd/README.md) | `github.com/sllt/tao/actor/etcd` | 1.27 | Optional etcd CAS directory for global `Ref` ownership |
| [actor/codec/protobuf](actor/codec/protobuf/README.md) | `github.com/sllt/tao/actor/codec/protobuf` | 1.27 | Optional Protobuf message codec for remote actors |

The workspace uses the Go 1.27.3 toolchain. The Actor package name is `actor` (e.g., `actor.NewNode(...)`).
By default, remote Actor messages are encoded with CBOR; etcd and Protobuf dependencies are isolated in their respective optional modules.

## Quick Start

Clone the repository and run the included examples from the workspace root:

```sh
git clone https://github.com/sllt/tao.git
cd tao
go run ./actor/examples/counter
```

Output:

```text
counter=42
```

Run the bidirectional RPC over a single connection example:

```sh
go run ./taonet/examples/reverse-rpc
```

Output:

```text
desktop client: hello over one connection
```

See the [Actor README](actor/README.md) for a complete walkthrough of spawning actors, sending messages, and typed request/reply calls.

## Examples

Run from the repository root:

| Scenario | Command / Guide |
| :--- | :--- |
| Supervision & Recovery | `go run ./actor/examples/recovery` |
| Two-Process Communication | `go run ./actor/examples/two-node` |
| In-Place Behavior Upgrade | `go run ./actor/examples/replace` |
| Cross-Node State Handoff | `go run ./actor/examples/handoff` |
| Embedded Web Observer | `go run ./actor/examples/observer` |
| Erlang Distribution `gen_server` | `go run ./erlang/examples/genserver-call` |
| Tao Actor <-> Erlang Bridge | `go run ./actor/erlang/examples/bridge` |
| HTTP Service + Observer | [HTTP API Example](actor/examples/http-api/README.md) |
| Soak & Fault Recovery | [Soak Example](actor/examples/soak/README.md) |

Local actors require only `Node`, `Behavior`, `Spawn`, and `Send` / `Call`. `Directory`, network codecs, and migration hooks can be adopted incrementally when needed.
`Replace` and `Handoff` transfer in-memory state and eligible mailbox messages across instances; they do not provide persistent event sourcing or crash recovery after sudden process loss.

## Documentation & Development

- [Documentation Index](docs/README.md): User guides and module references.
- [Actor Guide](docs/actor.md): Addressing, messaging, supervision, networking, and state handoff.
- [taonet Guide](docs/taonet.md): Transport primitives, buffer ownership, backpressure, and connection management.
- [Contributing Guide](CONTRIBUTING.md): Workspace layout, testing conventions, and design principles.

Run module test suites independently:

```sh
(cd actor && GOWORK=off go test ./...)
(cd taonet && GOWORK=off go test ./...)
```

## License

Licensed under the [MIT License](LICENSE).
