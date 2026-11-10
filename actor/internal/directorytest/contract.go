// Package directorytest checks the ownership contract shared by Directory backends.
package directorytest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
)

func Route(path, owner string, id uint64) actor.RouteRecord {
	return actor.RouteRecord{Path: path, Owner: owner, BootID: [16]byte{1}, ProcessID: id, Kind: "counter", Version: "1"}
}

func Run(t *testing.T, create func(*testing.T) actor.Directory) {
	t.Helper()
	t.Run("claim_cas_and_tombstone", func(t *testing.T) {
		d := create(t)
		ctx := t.Context()
		a, err := d.Claim(ctx, Route("app/counter", "a", 1))
		if err != nil || a.Epoch != 1 || a.Revision <= 0 || a.State != actor.RouteActive {
			t.Fatalf("claim: %+v, %v", a, err)
		}
		if again, err := d.Claim(ctx, a); err != nil || again != a {
			t.Fatalf("idempotent claim: %+v, %v", again, err)
		}
		if _, err := d.Claim(ctx, Route(a.Path, "b", 2)); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("duplicate claim: %v", err)
		}
		b, err := d.SwapInstance(ctx, a, Route(a.Path, "a", 2))
		if err != nil || b.Epoch != a.Epoch+1 || b.Revision <= a.Revision {
			t.Fatalf("swap: %+v, %v", b, err)
		}
		if _, err := d.Release(ctx, a); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("stale release: %v", err)
		}
		if again, err := d.SwapInstance(ctx, a, b); err != nil || again != b {
			t.Fatalf("idempotent swap: %+v, %v", again, err)
		}
		dead, err := d.Release(ctx, b)
		if err != nil || dead.State != actor.RouteTombstone || dead.Epoch != b.Epoch+1 {
			t.Fatalf("release: %+v, %v", dead, err)
		}
		if found, err := d.Lookup(ctx, a.Path); !errors.Is(err, actor.ErrDirectoryNotFound) || found != dead {
			t.Fatalf("tombstone: %+v, %v", found, err)
		}
		if again, err := d.Release(ctx, b); err != nil || again != dead {
			t.Fatalf("idempotent release: %+v, %v", again, err)
		}
		c, err := d.Claim(ctx, Route(a.Path, "b", 3))
		if err != nil || c.Epoch != dead.Epoch+1 {
			t.Fatalf("reclaim: %+v, %v", c, err)
		}
		if _, err := d.SwapInstance(ctx, a, b); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("ABA swap: %v", err)
		}
	})
	t.Run("move_commit", func(t *testing.T) {
		d := create(t)
		ctx := t.Context()
		a, err := d.Claim(ctx, Route("app/counter", "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		id := actor.MoveID{1}
		m, err := d.PrepareMove(ctx, a, Route(a.Path, "b", 2), id)
		if err != nil || m.State != actor.MovePreparing {
			t.Fatalf("prepare: %+v, %v", m, err)
		}
		preparing, err := d.Lookup(ctx, a.Path)
		if err != nil || !actor.DirectoryMoveMatchesRoute(m, preparing) {
			t.Fatalf("prepared route: %+v, %v", preparing, err)
		}
		if _, err := d.Release(ctx, preparing); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("release during move: %v", err)
		}
		if _, err := d.PrepareMove(ctx, a, Route(a.Path, "c", 3), actor.MoveID{2}); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("concurrent move: %v", err)
		}
		committed, err := d.CommitMove(ctx, id)
		if err != nil || committed.State != actor.MoveCommitted {
			t.Fatalf("commit: %+v, %v", committed, err)
		}
		current, err := d.Lookup(ctx, a.Path)
		if err != nil || current != committed.Target || current.Owner != "b" || current.Epoch != a.Epoch+1 || current.Revision != committed.Revision {
			t.Fatalf("committed route: %+v, %v", current, err)
		}
		if read, err := d.ReadMove(ctx, id); err != nil || read != committed {
			t.Fatalf("read move: %+v, %v", read, err)
		}
		if again, err := d.CommitMove(ctx, id); err != nil || again != committed {
			t.Fatalf("repeat commit: %+v, %v", again, err)
		}
		if again, err := d.PrepareMove(ctx, a, Route(a.Path, "b", 2), id); err != nil || again != committed {
			t.Fatalf("repeat prepare: %+v, %v", again, err)
		}
		if aborted, err := d.AbortMove(ctx, id); !errors.Is(err, actor.ErrMoveCommitted) || aborted != committed {
			t.Fatalf("rollback after commit: %+v, %v", aborted, err)
		}
		if got, _ := d.Lookup(ctx, a.Path); got != current {
			t.Fatalf("abort changed committed owner: %+v", got)
		}
	})
	t.Run("move_abort", func(t *testing.T) {
		d := create(t)
		ctx := t.Context()
		a, err := d.Claim(ctx, Route("app/counter", "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		m, err := d.PrepareMove(ctx, a, Route(a.Path, "b", 2), actor.MoveID{1})
		if err != nil {
			t.Fatal(err)
		}
		aborted, err := d.AbortMove(ctx, m.ID)
		if err != nil || aborted.State != actor.MoveAborted {
			t.Fatalf("abort: %+v, %v", aborted, err)
		}
		current, err := d.Lookup(ctx, a.Path)
		if err != nil || current.PID() != a.PID() || current.Epoch != a.Epoch || current.State != actor.RouteActive || current.Revision <= m.Revision {
			t.Fatalf("restored source: %+v, %v", current, err)
		}
		if again, err := d.AbortMove(ctx, m.ID); err != nil || again != aborted {
			t.Fatalf("repeat abort: %+v, %v", again, err)
		}
		if _, err := d.CommitMove(ctx, m.ID); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("commit aborted move: %v", err)
		}
		if _, err := d.PrepareMove(ctx, current, Route(a.Path, "c", 3), m.ID); !errors.Is(err, actor.ErrDirectoryConflict) {
			t.Fatalf("reuse MoveID: %v", err)
		}
	})
	t.Run("competing_claims", func(t *testing.T) {
		d := create(t)
		ctx := t.Context()
		var wg sync.WaitGroup
		outcomes := make(chan error, 16)
		for i := range 16 {
			wg.Go(func() { _, err := d.Claim(ctx, Route("same", "a", uint64(i+1))); outcomes <- err })
		}
		wg.Wait()
		close(outcomes)
		won := 0
		for err := range outcomes {
			if err == nil {
				won++
			} else if !errors.Is(err, actor.ErrDirectoryConflict) {
				t.Fatal(err)
			}
		}
		if won != 1 {
			t.Fatalf("owners=%d", won)
		}
	})
	t.Run("commit_abort_race", func(t *testing.T) {
		d := create(t)
		ctx := t.Context()
		a, err := d.Claim(ctx, Route("race", "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		m, err := d.PrepareMove(ctx, a, Route(a.Path, "b", 2), actor.MoveID{1})
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			_, err := d.CommitMove(ctx, m.ID)
			if err != nil && !errors.Is(err, actor.ErrDirectoryConflict) {
				t.Error(err)
			}
		})
		wg.Go(func() {
			_, err := d.AbortMove(ctx, m.ID)
			if err != nil && !errors.Is(err, actor.ErrMoveCommitted) && !errors.Is(err, actor.ErrDirectoryConflict) {
				t.Error(err)
			}
		})
		wg.Wait()
		resolved, err := d.ReadMove(ctx, m.ID)
		if err != nil {
			t.Fatal(err)
		}
		route, err := d.Lookup(ctx, a.Path)
		if err != nil {
			t.Fatal(err)
		}
		if resolved.State == actor.MoveCommitted {
			if route != resolved.Target {
				t.Fatalf("split commit: %+v %+v", resolved, route)
			}
		} else if resolved.State != actor.MoveAborted || route.PID() != a.PID() || route.State != actor.RouteActive {
			t.Fatalf("split abort: %+v %+v", resolved, route)
		}
	})
	t.Run("watch_snapshot_and_changes", func(t *testing.T) {
		d := create(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		a, err := d.Claim(ctx, Route("app/a", "a", 1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = d.Claim(ctx, Route("apple/b", "a", 2))
		if err != nil {
			t.Fatal(err)
		}
		events, err := d.Watch(ctx, "app/")
		if err != nil {
			t.Fatal(err)
		}
		first := next(t, events)
		if !first.Snapshot || first.Err != nil || len(first.Routes) != 1 || first.Routes[0] != a || first.Revision < a.Revision {
			t.Fatalf("snapshot: %+v", first)
		}
		b, err := d.SwapInstance(ctx, a, Route(a.Path, "a", 3))
		if err != nil {
			t.Fatal(err)
		}
		change := next(t, events)
		if change.Snapshot || change.Err != nil || change.Revision != b.Revision || len(change.Routes) != 1 || change.Routes[0] != b {
			t.Fatalf("change: %+v", change)
		}
		dead, err := d.Release(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if event := next(t, events); event.Err != nil || len(event.Routes) != 1 || event.Routes[0] != dead {
			t.Fatalf("delete event: %+v", event)
		}
		cancel()
		select {
		case _, ok := <-events:
			if ok {
				t.Fatal("watch did not close")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("watch cancellation stalled")
		}
	})
}

func next(t *testing.T, events <-chan actor.DirectoryEvent) actor.DirectoryEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("watch closed early")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("watch stalled")
		return actor.DirectoryEvent{}
	}
}
