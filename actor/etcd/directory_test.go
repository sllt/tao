package etcddirectory

import (
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/internal/directorytest"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func startEtcd(t testing.TB) (*clientv3.Client, *embed.Etcd) {
	t.Helper()
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	cfg.LogOutputs = []string{cfg.Dir + "/etcd.log"}
	u, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*u}, []url.URL{*u}
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*u}, []url.URL{*u}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd did not become ready")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{"http://" + server.Clients[0].Addr().String()}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, server
}

func TestDirectoryContract(t *testing.T) {
	client, _ := startEtcd(t)
	directorytest.Run(t, func(t *testing.T) actor.Directory {
		directory, err := New(client, "/test/"+strings.ReplaceAll(t.Name(), "/", "_"))
		if err != nil {
			t.Fatal(err)
		}
		return directory
	})
}

type loseReplyKV struct{ clientv3.KV }

func (kv loseReplyKV) Txn(ctx context.Context) clientv3.Txn {
	return &loseReplyTxn{Txn: kv.KV.Txn(ctx)}
}

type loseReplyTxn struct{ clientv3.Txn }

func (txn *loseReplyTxn) If(comparisons ...clientv3.Cmp) clientv3.Txn {
	txn.Txn = txn.Txn.If(comparisons...)
	return txn
}
func (txn *loseReplyTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Then(ops...)
	return txn
}
func (txn *loseReplyTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	txn.Txn = txn.Txn.Else(ops...)
	return txn
}
func (txn *loseReplyTxn) Commit() (*clientv3.TxnResponse, error) {
	response, err := txn.Txn.Commit()
	if err == nil && response.Succeeded {
		return nil, io.ErrUnexpectedEOF
	}
	return response, err
}

func TestCommittedWriteWithLostResponse(t *testing.T) {
	client, _ := startEtcd(t)
	d, err := New(client, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	a, err := d.Claim(ctx, directorytest.Route("counter", "source", 1))
	if err != nil {
		t.Fatal(err)
	}
	m, err := d.PrepareMove(ctx, a, directorytest.Route(a.Path, "target", 2), actor.MoveID{1})
	if err != nil {
		t.Fatal(err)
	}
	d.kv = loseReplyKV{KV: client.KV}
	_, err = d.CommitMove(ctx, m.ID)
	var failure *actor.DirectoryError
	if !errors.As(err, &failure) || !failure.Unknown {
		t.Fatalf("lost response must be Unknown: %v", err)
	}
	// The same client can still make an authoritative read after losing the reply.
	resolved, err := d.ReadMove(ctx, m.ID)
	if err != nil || resolved.State != actor.MoveCommitted {
		t.Fatalf("resolve: %+v %v", resolved, err)
	}
	route, err := d.Lookup(ctx, a.Path)
	if err != nil || route != resolved.Target {
		t.Fatalf("atomic route: %+v %v", route, err)
	}
	if _, err := d.AbortMove(ctx, m.ID); !errors.Is(err, actor.ErrMoveCommitted) {
		t.Fatalf("must not roll back: %v", err)
	}
	if again, err := d.CommitMove(ctx, m.ID); err != nil || again != resolved {
		t.Fatalf("idempotent readback: %+v %v", again, err)
	}
}

type compactOnWatch struct {
	clientv3.Watcher
	client  *clientv3.Client
	compact bool
}

func (w *compactOnWatch) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if w.compact {
		w.compact = false
		// Advance and compact after the snapshot, before installing its watch.
		_, _ = w.client.Put(ctx, "/unrelated", "first")
		response, err := w.client.Put(ctx, "/unrelated", "advance")
		if err == nil {
			_, _ = w.client.Compact(ctx, response.Header.Revision)
		}
	}
	return w.Watcher.Watch(ctx, key, opts...)
}

func TestWatchCompactionGap(t *testing.T) {
	client, _ := startEtcd(t)
	d, err := New(client, "")
	if err != nil {
		t.Fatal(err)
	}
	route, err := d.Claim(t.Context(), directorytest.Route("counter", "a", 1))
	if err != nil {
		t.Fatal(err)
	}
	d.watcher = &compactOnWatch{Watcher: client.Watcher, client: client, compact: true}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	events, err := d.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	first := <-events
	if !first.Snapshot || len(first.Routes) != 1 {
		t.Fatalf("snapshot: %+v", first)
	}
	select {
	case event, ok := <-events:
		if !ok || event.Err == nil {
			t.Fatalf("compaction must invalidate cache: %+v, open=%v", event, ok)
		}
	case <-ctx.Done():
		t.Fatal("watch did not report compaction")
	}
	resumed, err := d.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if event := <-resumed; !event.Snapshot || len(event.Routes) != 1 || event.Routes[0] != route {
		t.Fatalf("resnapshot: %+v", event)
	}
}
