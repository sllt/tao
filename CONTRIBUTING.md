# Contributing Guide

English | [简体中文](CONTRIBUTING_zh.md)

## Development Environment

Use the Go 1.27.3 toolchain after cloning the repository. The root `go.work` file links the four modules (`actor`, `taonet`, `actor/etcd`, and `actor/codec/protobuf`) for unified local development.

```sh
git clone https://github.com/sllt/tao.git
cd tao
go run ./actor/examples/counter
```

Each module uses relative `replace` directives pointing to in-repo sibling modules during development. The repository root is a Go workspace rather than a monolithic root module.

## Repository Layout

| Directory | Contents |
| :--- | :--- |
| `actor/` | Actor runtime, default CBOR codec, lifecycle management, and network adapter |
| `actor/observer/` | Optional embedded Web Observer |
| `actor/etcd/` | Standalone etcd Directory module |
| `actor/codec/protobuf/` | Standalone Protobuf codec module |
| `taonet/` | Standard-library-only transport layer and `HN-MUX/1` multiplexer |
| `actor/examples/`, `taonet/examples/` | Runnable examples |
| `docs/` | User guides and reference documentation |

## Design & Modification Principles

- Prefer concrete types, plain functions, and explicit state ownership. Introduce abstractions or configuration knobs only when backed by concrete requirements.
- Validate inputs at configuration, network, and public API boundaries; preserve protocol, memory, and concurrency invariants while avoiding redundant internal checks or silent fallbacks.
- Keep module dependency boundaries strict: `etcd` and optional codecs stay in their respective modules and never leak into `taonet` or the `actor` core.
- Distinguish local enqueueing, remote queue acceptance, and business execution completion. Never silently replay operations with `Unknown` outcomes.
- Keep examples and documentation synchronized whenever public APIs or wire protocols change.

## Testing

Run all four modules from the workspace root:

```sh
go test ./actor/... ./taonet/... ./actor/etcd/... ./actor/codec/protobuf/...
```

Verify each module independently with `GOWORK=off` to ensure module boundaries and `go.mod` dependencies remain self-contained:

```sh
(cd actor && GOWORK=off go test ./...)
(cd taonet && GOWORK=off go test ./...)
(cd actor/etcd && GOWORK=off go test ./...)
(cd actor/codec/protobuf && GOWORK=off go test ./...)
```

Changes touching concurrency state or resource lifecycles should be verified with the race detector:

```sh
(cd actor && GOWORK=off go test -race ./...)
(cd taonet && GOWORK=off go test -race ./...)
```

Tests use temporary directories, ephemeral loopback ports, and embedded etcd instances—no external daemon setup is required.
Use explicit synchronization points in concurrent tests; timeouts exist to fail hung tests rather than assert wall-clock machine speed.

## Pull Requests & Commits

- Format all Go source files with `gofmt`.
- Describe the problem, behavior change, and verification steps in commit messages.
- Update relevant examples and documentation when changing public APIs.
