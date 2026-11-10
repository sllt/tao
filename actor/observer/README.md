# Tao Actor Web Observer

English | [简体中文](README_zh.md)

`github.com/sllt/tao/actor/observer` provides an optional, read-only Web UI and HTTP inspection server for Tao Actor nodes. Static assets are embedded directly into the Go binary—no Node.js toolchain or external frontend assets are required. Runtime snapshots and the `Inspector` hook live in the `actor` core, while HTTP serving, host memory sampling, and the web interface reside in this package.

## Quick Start

Run the interactive demo from `actor/`:

```sh
go run ./examples/observer -listen 127.0.0.1:9911
```

Open `http://127.0.0.1:9911` in your browser. The demo runs two TCP-connected nodes with supervised child actors, slow mailboxes, periodic crash/restart recovery, and remote `Call` traffic. Press `Ctrl-C` to shut down the HTTP server and nodes cleanly.

## Embedding in Applications

Pass one or more in-process `*actor.Node` instances to `observer.New`:

```go
import (
	"context"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/observer"
)

func observe(ctx context.Context, node *actor.Node) error {
	view, err := observer.New(node)
	if err != nil {
		return err
	}
	server, err := view.Listen("") // defaults to 127.0.0.1:9911
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case <-server.Done():
		return server.Err()
	}
}
```

`observer.New` accepts multiple nodes (with unique node names) and enables per-actor diagnostic metrics on them. `Observer` also implements `http.Handler` so it can be mounted behind an application router with custom authentication. Shutting down the Observer HTTP server does not stop the observed nodes; applications remain responsible for calling `Node.Stop`.

## Accessing a Remote Server via SSH Tunnel

When running the Observer on a remote machine bound to loopback (`127.0.0.1:9911`), forward the port over SSH:

```sh
ssh -N -L 19911:127.0.0.1:9911 user@server
```

Then open `http://127.0.0.1:19911` locally. The UI displays only the in-process nodes passed to `observer.New`; separate OS processes expose their own Observer endpoints.

## Views & Metrics

- **Overview**: Registered actors, active executors, mailbox depth, pending `Call`s, asynchronous `Request`s, frozen/preparing/unresponsive counts, and process-wide Go heap & goroutine stats.
- **Actors**: Supervision tree by parent `PID`, path/`Kind` filtering, cursor pagination, per-instance details, `Monitor`/`Link` edges, and `Epoch`/`MoveID`.
- **Metrics**: `Receive` count, failures, cumulative/max duration, active message type, mailbox high-water mark, rejected admissions, and restart count within the current supervision chain.
- **Calls & Network**: In-flight `Call` edges (caller, target, elapsed wait), candidate wait cycles, and `taonet` physical connection/quota summaries.

`Stats.AsyncRequests` and per-actor `AsyncRequests` count both in-flight outbound `Request` operations and completed `CallResult` envelopes awaiting `Receive`. Asynchronous requests carry the `Asynchronous` flag and are excluded from synchronous wait-cycle detection.

### Collection Overhead

Calling `observer.New` invokes `Node.EnableDiagnostics`, activating per-message timing for existing and newly spawned actors. The UI polls roughly once per second and does not automatically poll business `Inspector` hooks unless an actor is explicitly inspected.

## HTTP API

| Endpoint (`GET`) | Parameters & Payload |
| :--- | :--- |
| `/api/nodes` | Visible node names, versions, and summary `Stats` |
| `/api/snapshot` | `node`, numeric cursor `after`, `limit` (default 250, max 1000), `filter`; returns `Node` and process-wide `Host` |
| `/api/actor` | `node`, `pid` (full `pid://...`); returns instance metadata and relationships |
| `/api/inspect` | `node`, `pid`; 1-second timeout, returns serialized JSON `Value` |

`node` may be omitted when observing a single node. Snapshot responses sample up to 1,000 in-flight calls and up to 1,000 relationships per actor. Wait cycles reflect currently visible local synchronous edges (`DeferredReply` may resolve a wait outside the call graph, so a cycle is a diagnostic hint rather than a proven deadlock).

## Business State Inspection (`Inspector`)

Actors can optionally implement `Inspect(context.Context) (any, error)` to expose read-only business state:

```go
func (a *Counter) Inspect(ctx context.Context) (any, error) {
	return struct{ Value int64 }{Value: a.value}, nil
}
```

`Node.Inspect(ctx, pid)` runs sequentially at the actor's execution boundary (never concurrently with `Receive`) and marshals the returned value to JSON before resuming subsequent messages, preventing concurrent map/slice mutation races. It does not invoke migration `Snapshotting` hooks or reflect over private struct fields.

At most one inspection may be queued or running per actor (`ErrCapacity` on concurrent requests), and serialized output is bounded by `NodeOptions.MaxSnapshotBytes`. Actors that do not implement the interface return `ErrInspectUnsupported`.

## Testing

Run from `actor/`:

```sh
go test ./observer
go test -race ./observer
```

See the [Actor README](../README.md) and [Actor Guide](../../docs/actor.md) for more details.
