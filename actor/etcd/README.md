# Tao Actor etcd Directory

English | [简体中文](README_zh.md)

`github.com/sllt/tao/actor/etcd` provides a persistent, CAS-backed `Directory` implementation for Tao Actor clusters.
The Go package name is `etcddirectory`, requiring Go 1.27+. Neither the `actor` core nor `taonet` depends on `etcd`.

## Integration

Applications supply an initialized etcd v3 client and choose an isolated absolute key prefix per cluster. The example below creates a node backed by `etcddirectory` and shuts down both the node and client cleanly when the context is canceled:

```go
import (
	"context"
	"errors"
	"time"

	"github.com/sllt/tao/actor"
	etcddirectory "github.com/sllt/tao/actor/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func run(ctx context.Context) (err error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()

	directory, err := etcddirectory.New(client, "/my-cluster/actors/v1")
	if err != nil {
		return err
	}
	node, err := actor.NewNode(actor.NodeOptions{
		Name:      "worker",
		Directory: directory,
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return node.Stop(shutdown)
}
```

The etcd client lifecycle belongs to the application (`etcddirectory` does not close it). Multi-node services use `StartNode` to enable networking; see the [Actor Guide](../../docs/actor.md) for global `Ref` routing, `Replace`, and `Handoff`.

## Ownership & Migration Semantics

Each route record stores `Owner`, `BootID`, `PID`, `Epoch`, `Kind`/`Version`, and route state, using etcd `ModRevision` as the revision. `Claim`, `Release`, and `SwapInstance` execute via atomic CAS transactions. `Release` writes a tombstone so subsequent registrations increment `Epoch` monotonically and avoid ABA conflicts after deletion. Route ownership is intentionally not bound to an expiring lease: transient network partitions never automatically transfer ownership or create split-brain actors.

`PrepareMove` writes the source route state and `MoveID` record in a single etcd transaction. `CommitMove` atomically switches the route to the target instance and records `Committed`; `AbortMove` only rolls back uncommitted preparations. Retrying the same operation reads back the authoritative outcome. When a commit returns `DirectoryError{Unknown: true}`, callers must query `ReadMove` to settle the state and must never resume the source instance while commit status remains unknown. Terminal `MoveID` records are retained persistently without automatic GC.

The route's `MoveID` retains the source of the most recently committed handoff so in-flight `Call` cancellations can verify subsequent migrations; local `SwapInstance` and `AbortMove` preserve that provenance while `Release`/`Claim` resets it.

## Watch & Caching

`Watch` reads a complete snapshot at revision `R` and subscribes from `R+1` onward; history compaction, unexpected key deletion, or consumer overflow terminates the subscription. `actor.DirectoryCache` automatically invalidates its cache and resubscribes on stream termination.

Cached routes serve purely as routing hints and never decide whether a migration has committed. Because etcd `Watch` does not provide linearizable read semantics, all ownership transitions rely on KV CAS transactions.

## Testing

Run from `actor/etcd/`:

```sh
GOWORK=off go test -race ./...
```

Tests launch an embedded etcd server using temporary directories and random loopback ports—no local etcd installation is required. The suite covers CAS transitions, monotonic epochs, concurrent claim races, handoff transactions, `Watch` compaction, lost commit responses, and three-node runtime `Handoff` integration.

See the [Actor README](../README.md) and [Contributing Guide](../../CONTRIBUTING.md) for more details.
