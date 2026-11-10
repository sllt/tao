// Package etcddirectory provides a persistent, CAS-based Actor Directory.
// Ownership is not attached to a lease: loss of connectivity cannot transfer it.
package etcddirectory

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/sllt/tao/actor"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Directory struct {
	kv      clientv3.KV
	watcher clientv3.Watcher
	routes  string
	moves   string
}

var _ actor.Directory = (*Directory)(nil)

// New uses an application-owned client, which the caller must eventually close.
// An empty prefix selects /hive/actors/v1. Use separate prefixes for separate clusters.
func New(client *clientv3.Client, prefix string) (*Directory, error) {
	if client == nil {
		return nil, errors.New("actor etcd: nil client")
	}
	if prefix == "" {
		prefix = "/hive/actors/v1"
	}
	prefix = strings.TrimRight(prefix, "/")
	if !strings.HasPrefix(prefix, "/") || prefix == "" {
		return nil, errors.New("actor etcd: prefix must be an absolute, non-root key prefix")
	}
	return &Directory{kv: client.KV, watcher: client.Watcher, routes: prefix + "/routes/", moves: prefix + "/moves/"}, nil
}

func storageError(operation string, unknown bool, err error) error {
	return &actor.DirectoryError{Operation: operation, Unknown: unknown, Cause: err}
}
func routeValue(route actor.RouteRecord) string {
	route.Revision = 0 // The authoritative revision comes from etcd's ModRevision.
	data, _ := json.Marshal(route)
	return string(data)
}
func moveValue(move actor.MoveRecord) string {
	move.Revision = 0
	move.Target.Revision = 0
	data, _ := json.Marshal(move)
	return string(data)
}
func decodeRoute(kv *mvccpb.KeyValue) (actor.RouteRecord, error) {
	var record actor.RouteRecord
	if err := json.Unmarshal(kv.Value, &record); err != nil {
		return record, err
	}
	if err := record.Validate(); err != nil {
		return record, err
	}
	if record.Epoch == 0 || record.State != actor.RouteActive && record.State != actor.RoutePreparing && record.State != actor.RouteTombstone {
		return record, errors.New("actor etcd: invalid stored route state")
	}
	record.Revision = kv.ModRevision
	return record, nil
}
func decodeMove(kv *mvccpb.KeyValue) (actor.MoveRecord, error) {
	var move actor.MoveRecord
	if err := json.Unmarshal(kv.Value, &move); err != nil {
		return move, err
	}
	if move.ID == (actor.MoveID{}) || move.Source.Path != move.Target.Path || move.State != actor.MovePreparing && move.State != actor.MoveCommitted && move.State != actor.MoveAborted {
		return move, errors.New("actor etcd: invalid stored move state")
	}
	if err := move.Source.Validate(); err != nil {
		return move, err
	}
	if err := move.Target.Validate(); err != nil {
		return move, err
	}
	move.Revision = kv.ModRevision
	if move.State == actor.MoveCommitted {
		move.Target.Revision = kv.ModRevision
	}
	return move, nil
}
func (d *Directory) route(ctx context.Context, path string) (actor.RouteRecord, bool, error) {
	if err := actor.ValidateDirectoryPath(path); err != nil {
		return actor.RouteRecord{}, false, err
	}
	response, err := d.kv.Get(ctx, d.routes+path)
	if err != nil {
		return actor.RouteRecord{}, false, storageError("lookup", false, err)
	}
	if len(response.Kvs) == 0 {
		return actor.RouteRecord{}, false, nil
	}
	route, err := decodeRoute(response.Kvs[0])
	if err == nil && route.Path != path {
		err = errors.New("actor etcd: route key and path differ")
	}
	if err != nil {
		return actor.RouteRecord{}, false, storageError("lookup", false, err)
	}
	return route, true, nil
}
func (d *Directory) Lookup(ctx context.Context, path string) (actor.RouteRecord, error) {
	route, exists, err := d.route(ctx, path)
	if err != nil {
		return route, err
	}
	if !exists || route.State == actor.RouteTombstone {
		return route, actor.ErrDirectoryNotFound
	}
	return route, nil
}
func (d *Directory) write(ctx context.Context, operation string, comparisons []clientv3.Cmp, writes ...clientv3.Op) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	response, err := d.kv.Txn(ctx).If(comparisons...).Then(writes...).Commit()
	if err != nil {
		return 0, storageError(operation, true, err)
	}
	if !response.Succeeded {
		return 0, actor.ErrDirectoryConflict
	}
	return response.Header.Revision, nil
}
func (d *Directory) writeRoute(ctx context.Context, operation string, previous, next actor.RouteRecord) (actor.RouteRecord, error) {
	revision, err := d.write(ctx, operation, []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(d.routes+next.Path), "=", previous.Revision)}, clientv3.OpPut(d.routes+next.Path, routeValue(next)))
	if err != nil {
		return actor.RouteRecord{}, err
	}
	next.Revision = revision
	return next, nil
}
func (d *Directory) Claim(ctx context.Context, wanted actor.RouteRecord) (actor.RouteRecord, error) {
	if err := wanted.Validate(); err != nil {
		return actor.RouteRecord{}, err
	}
	previous, exists, err := d.route(ctx, wanted.Path)
	if err != nil {
		return actor.RouteRecord{}, err
	}
	if exists && previous.State != actor.RouteTombstone {
		if previous.State == actor.RouteActive && previous.PID() == wanted.PID() && previous.Kind == wanted.Kind && previous.Version == wanted.Version {
			return previous, nil
		}
		return actor.RouteRecord{}, actor.ErrDirectoryConflict
	}
	if previous.Epoch == math.MaxUint64 {
		return actor.RouteRecord{}, actor.ErrCapacity
	}
	wanted.State, wanted.MoveID, wanted.Epoch = actor.RouteActive, actor.MoveID{}, previous.Epoch+1
	return d.writeRoute(ctx, "claim", previous, wanted)
}
func (d *Directory) Release(ctx context.Context, expected actor.RouteRecord) (actor.RouteRecord, error) {
	current, exists, err := d.route(ctx, expected.Path)
	if err != nil {
		return actor.RouteRecord{}, err
	}
	if exists && current.State == actor.RouteTombstone && current.PID() == expected.PID() && expected.Epoch < math.MaxUint64 && current.Epoch == expected.Epoch+1 {
		return current, nil
	}
	if !exists || current != expected || current.State != actor.RouteActive {
		return actor.RouteRecord{}, actor.ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 {
		return actor.RouteRecord{}, actor.ErrCapacity
	}
	current.State, current.MoveID, current.Epoch = actor.RouteTombstone, actor.MoveID{}, current.Epoch+1
	return d.writeRoute(ctx, "release", expected, current)
}
func (d *Directory) SwapInstance(ctx context.Context, expected, replacement actor.RouteRecord) (actor.RouteRecord, error) {
	if err := replacement.Validate(); err != nil {
		return actor.RouteRecord{}, err
	}
	if expected.Path != replacement.Path || expected.Owner != replacement.Owner || expected.BootID != replacement.BootID {
		return actor.RouteRecord{}, actor.ErrDirectoryConflict
	}
	current, exists, err := d.route(ctx, expected.Path)
	if err != nil {
		return actor.RouteRecord{}, err
	}
	if exists && current.State == actor.RouteActive && current.PID() == replacement.PID() && current.Kind == replacement.Kind && current.Version == replacement.Version && expected.Epoch < math.MaxUint64 && current.Epoch == expected.Epoch+1 {
		return current, nil
	}
	if !exists || current != expected || current.State != actor.RouteActive {
		return actor.RouteRecord{}, actor.ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 {
		return actor.RouteRecord{}, actor.ErrCapacity
	}
	replacement.State, replacement.MoveID, replacement.Epoch = actor.RouteActive, current.MoveID, current.Epoch+1
	return d.writeRoute(ctx, "swap", expected, replacement)
}
func (d *Directory) moveKey(id actor.MoveID) string { return d.moves + hex.EncodeToString(id[:]) }
func (d *Directory) ReadMove(ctx context.Context, id actor.MoveID) (actor.MoveRecord, error) {
	response, err := d.kv.Get(ctx, d.moveKey(id))
	if err != nil {
		return actor.MoveRecord{}, storageError("read_move", false, err)
	}
	if len(response.Kvs) == 0 {
		return actor.MoveRecord{}, actor.ErrDirectoryNotFound
	}
	move, err := decodeMove(response.Kvs[0])
	if err == nil && move.ID != id {
		err = errors.New("actor etcd: move key and ID differ")
	}
	if err != nil {
		return actor.MoveRecord{}, storageError("read_move", false, err)
	}
	return move, nil
}
func (d *Directory) PrepareMove(ctx context.Context, expected, target actor.RouteRecord, id actor.MoveID) (actor.MoveRecord, error) {
	if err := target.Validate(); err != nil {
		return actor.MoveRecord{}, err
	}
	if id == (actor.MoveID{}) || expected.Path != target.Path || expected.Owner == target.Owner {
		return actor.MoveRecord{}, actor.ErrDirectoryConflict
	}
	previous, err := d.ReadMove(ctx, id)
	if err == nil {
		if previous.Source == expected && previous.Target.PID() == target.PID() && previous.Target.Kind == target.Kind && previous.Target.Version == target.Version {
			return previous, nil
		}
		return actor.MoveRecord{}, actor.ErrDirectoryConflict
	}
	if !errors.Is(err, actor.ErrDirectoryNotFound) {
		return actor.MoveRecord{}, err
	}
	current, exists, err := d.route(ctx, expected.Path)
	if err != nil {
		return actor.MoveRecord{}, err
	}
	if !exists || current != expected || current.State != actor.RouteActive {
		return actor.MoveRecord{}, actor.ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 {
		return actor.MoveRecord{}, actor.ErrCapacity
	}
	target.State, target.MoveID, target.Epoch, target.Revision = actor.RouteActive, id, current.Epoch+1, 0
	move := actor.MoveRecord{ID: id, Source: current, Target: target, State: actor.MovePreparing}
	current.State, current.MoveID = actor.RoutePreparing, id
	revision, err := d.write(ctx, "prepare_move", []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(d.routes+current.Path), "=", expected.Revision),
		clientv3.Compare(clientv3.Version(d.moveKey(id)), "=", 0),
	}, clientv3.OpPut(d.routes+current.Path, routeValue(current)), clientv3.OpPut(d.moveKey(id), moveValue(move)))
	if err != nil {
		return actor.MoveRecord{}, err
	}
	move.Revision = revision
	return move, nil
}
func (d *Directory) CommitMove(ctx context.Context, id actor.MoveID) (actor.MoveRecord, error) {
	return d.finishMove(ctx, id, true)
}
func (d *Directory) AbortMove(ctx context.Context, id actor.MoveID) (actor.MoveRecord, error) {
	return d.finishMove(ctx, id, false)
}
func (d *Directory) finishMove(ctx context.Context, id actor.MoveID, commit bool) (actor.MoveRecord, error) {
	move, err := d.ReadMove(ctx, id)
	if err != nil {
		return actor.MoveRecord{}, err
	}
	if move.State == actor.MoveCommitted {
		if !commit {
			return move, actor.ErrMoveCommitted
		}
		return move, nil
	}
	if move.State == actor.MoveAborted {
		if commit {
			return actor.MoveRecord{}, actor.ErrDirectoryConflict
		}
		return move, nil
	}
	route, err := d.Lookup(ctx, move.Source.Path)
	if err != nil {
		return actor.MoveRecord{}, err
	}
	if !actor.DirectoryMoveMatchesRoute(move, route) {
		return actor.MoveRecord{}, actor.ErrDirectoryConflict
	}
	next, operation := move.Source, "abort_move"
	next.MoveID = move.Source.MoveID
	move.State = actor.MoveAborted
	if commit {
		next, operation, move.State = move.Target, "commit_move", actor.MoveCommitted
	}
	revision, err := d.write(ctx, operation, []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(d.routes+route.Path), "=", route.Revision),
		clientv3.Compare(clientv3.ModRevision(d.moveKey(id)), "=", move.Revision),
	}, clientv3.OpPut(d.routes+route.Path, routeValue(next)), clientv3.OpPut(d.moveKey(id), moveValue(move)))
	if err != nil {
		return actor.MoveRecord{}, err
	}
	move.Revision = revision
	if commit {
		move.Target.Revision = revision
	}
	return move, nil
}

func (d *Directory) Watch(ctx context.Context, prefix string) (<-chan actor.DirectoryEvent, error) {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" {
		if err := actor.ValidateDirectoryPath(prefix); err != nil {
			return nil, err
		}
	}
	// Snapshot at revision R and watch from R+1. Compaction in this gap closes
	// the watch with an error; consumers must acquire a new snapshot.
	key := d.routes + prefix
	response, err := d.kv.Get(ctx, key, clientv3.WithPrefix(), clientv3.WithLimit(100001))
	if err != nil {
		return nil, storageError("watch_snapshot", false, err)
	}
	if response.More {
		return nil, actor.ErrCapacity
	}
	var snapshot []actor.RouteRecord
	for _, kv := range response.Kvs {
		path := strings.TrimPrefix(string(kv.Key), d.routes)
		if !actor.DirectoryPathMatches(path, prefix) {
			continue
		}
		route, err := decodeRoute(kv)
		if err != nil || route.Path != path {
			return nil, storageError("watch_snapshot", false, fmt.Errorf("invalid route at %q: %v", kv.Key, err))
		}
		snapshot = append(snapshot, route)
	}
	events := make(chan actor.DirectoryEvent, 65)
	events <- actor.DirectoryEvent{Revision: response.Header.Revision, Snapshot: true, Routes: snapshot}
	watchCtx, cancel := context.WithCancel(ctx)
	watch := d.watcher.Watch(watchCtx, key, clientv3.WithPrefix(), clientv3.WithRev(response.Header.Revision+1))
	go func() {
		defer close(events)
		defer cancel()
		send := func(event actor.DirectoryEvent) bool {
			if len(events) >= cap(events)-1 {
				events <- actor.DirectoryEvent{Revision: event.Revision, Err: actor.ErrWatchOverflow}
				return false
			}
			events <- event
			return true
		}
		for {
			select {
			case <-watchCtx.Done():
				return
			case response, ok := <-watch:
				if !ok {
					return
				}
				if err := response.Err(); err != nil {
					send(actor.DirectoryEvent{Err: storageError("watch", false, err)})
					return
				}
				var batch actor.DirectoryEvent
				for _, event := range response.Events {
					path := strings.TrimPrefix(string(event.Kv.Key), d.routes)
					if !actor.DirectoryPathMatches(path, prefix) {
						continue
					}
					if event.Type != mvccpb.PUT {
						send(actor.DirectoryEvent{Err: errors.New("actor etcd: route was deleted outside Directory")})
						return
					}
					route, err := decodeRoute(event.Kv)
					if err != nil || route.Path != path {
						send(actor.DirectoryEvent{Err: storageError("watch", false, fmt.Errorf("invalid route at %q: %v", event.Kv.Key, err))})
						return
					}
					if batch.Revision != 0 && batch.Revision != route.Revision {
						if !send(batch) {
							return
						}
						batch = actor.DirectoryEvent{}
					}
					batch.Revision = route.Revision
					batch.Routes = append(batch.Routes, route)
				}
				if len(batch.Routes) != 0 && !send(batch) {
					return
				}
			}
		}
	}()
	return events, nil
}
