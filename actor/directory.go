package actor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

var (
	ErrDirectoryNotFound = errors.New("actor directory: route or move not found")
	ErrDirectoryConflict = errors.New("actor directory: compare-and-swap conflict")
	ErrMoveCommitted     = errors.New("actor directory: move already committed")
	ErrWatchOverflow     = errors.New("actor directory: watch consumer fell behind")
)

type RouteState string

const (
	RouteActive    RouteState = "active"
	RoutePreparing RouteState = "preparing"
	RouteTombstone RouteState = "tombstone"
)

type MoveID [16]byte

func NewMoveID() (MoveID, error) { var id MoveID; _, err := rand.Read(id[:]); return id, err }

type RouteRecord struct {
	Path      string     `json:"path"`
	Owner     string     `json:"owner"`
	BootID    [16]byte   `json:"boot_id"`
	ProcessID uint64     `json:"process_id"`
	Epoch     uint64     `json:"epoch"`
	Revision  int64      `json:"revision"`
	State     RouteState `json:"state"`
	MoveID    MoveID     `json:"move_id"`
	Kind      string     `json:"kind,omitempty"`
	Version   string     `json:"version,omitempty"`
}

func (record RouteRecord) PID() PID {
	return PID{Node: record.Owner, BootID: record.BootID, ID: record.ProcessID}
}

// Validate checks identity and path fields shared by all Directory backends.
// Epoch/revision and state transitions are assigned by the backend methods.
func (record RouteRecord) Validate() error {
	if !validPath(record.Path) || !validSegment(record.Owner) || record.BootID == ([16]byte{}) || record.ProcessID == 0 || record.Kind != "" && !validSegment(record.Kind) || len(record.Version) > 128 {
		return fmt.Errorf("actor directory: invalid route identity")
	}
	return nil
}

func ValidateDirectoryPath(path string) error {
	if !validPath(path) {
		return ErrInvalidMessage
	}
	return nil
}

type MoveState string

const (
	MovePreparing MoveState = "preparing"
	MoveCommitted MoveState = "committed"
	MoveAborted   MoveState = "aborted"
)

type MoveRecord struct {
	ID       MoveID      `json:"id"`
	Source   RouteRecord `json:"source"`
	Target   RouteRecord `json:"target"`
	State    MoveState   `json:"state"`
	Revision int64       `json:"revision"`
}
type DirectoryEvent struct {
	Revision int64
	Snapshot bool
	Routes   []RouteRecord
	Err      error
}

// Watch begins with one coherent snapshot, followed by changes. A consumer
// must discard its cache after Err or stream closure and start a fresh watch.
// Lookup and all CAS operations are authoritative; a watch is only a cache aid.
type Directory interface {
	Lookup(context.Context, string) (RouteRecord, error)
	Claim(context.Context, RouteRecord) (RouteRecord, error)
	Release(context.Context, RouteRecord) (RouteRecord, error)
	SwapInstance(context.Context, RouteRecord, RouteRecord) (RouteRecord, error)
	PrepareMove(context.Context, RouteRecord, RouteRecord, MoveID) (MoveRecord, error)
	CommitMove(context.Context, MoveID) (MoveRecord, error)
	AbortMove(context.Context, MoveID) (MoveRecord, error)
	ReadMove(context.Context, MoveID) (MoveRecord, error)
	Watch(context.Context, string) (<-chan DirectoryEvent, error)
}

// DirectoryError marks a storage failure. Unknown means a write may have
// committed; callers must read back the route or MoveID before choosing a side.
type DirectoryError struct {
	Operation string
	Unknown   bool
	Cause     error
}

func (err *DirectoryError) Error() string {
	return fmt.Sprintf("actor directory %s: %v", err.Operation, err.Cause)
}
func (err *DirectoryError) Unwrap() error { return err.Cause }

type memoryDirectoryWatch struct {
	prefix string
	events chan DirectoryEvent
	stop   func() bool
}

// MemoryDirectory is a local test/backend implementation, not a consensus store.
type MemoryDirectory struct {
	mu       sync.Mutex
	revision int64
	routes   map[string]RouteRecord
	moves    map[MoveID]MoveRecord
	watches  map[*memoryDirectoryWatch]struct{}
}

func NewMemoryDirectory() *MemoryDirectory {
	return &MemoryDirectory{routes: make(map[string]RouteRecord), moves: make(map[MoveID]MoveRecord), watches: make(map[*memoryDirectoryWatch]struct{})}
}

func (d *MemoryDirectory) Lookup(ctx context.Context, path string) (RouteRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	record, exists := d.routes[path]
	if !exists || record.State == RouteTombstone {
		return record, ErrDirectoryNotFound
	}
	return record, nil
}
func (d *MemoryDirectory) nextLocked() (int64, error) {
	if d.revision == math.MaxInt64 {
		return 0, ErrCapacity
	}
	d.revision++
	return d.revision, nil
}
func DirectoryPathMatches(path, prefix string) bool {
	return prefix == "" || path == prefix || strings.HasPrefix(path, prefix+"/")
}

func (d *MemoryDirectory) publishLocked(record RouteRecord) {
	d.routes[record.Path] = record
	for watch := range d.watches {
		if !DirectoryPathMatches(record.Path, watch.prefix) {
			continue
		}
		if len(watch.events) >= cap(watch.events)-1 {
			watch.events <- DirectoryEvent{Revision: d.revision, Err: ErrWatchOverflow}
			delete(d.watches, watch)
			watch.stop()
			close(watch.events)
			continue
		}
		watch.events <- DirectoryEvent{Revision: d.revision, Routes: []RouteRecord{record}}
	}
}
func (d *MemoryDirectory) Claim(ctx context.Context, wanted RouteRecord) (RouteRecord, error) {
	if err := wanted.Validate(); err != nil {
		return RouteRecord{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	previous, exists := d.routes[wanted.Path]
	if exists && previous.State != RouteTombstone {
		if previous.State == RouteActive && previous.PID() == wanted.PID() && previous.Kind == wanted.Kind && previous.Version == wanted.Version {
			return previous, nil
		}
		return RouteRecord{}, ErrDirectoryConflict
	}
	if previous.Epoch == math.MaxUint64 || !exists && len(d.routes) >= 100000 {
		return RouteRecord{}, ErrCapacity
	}
	revision, err := d.nextLocked()
	if err != nil {
		return RouteRecord{}, err
	}
	wanted.State = RouteActive
	wanted.Epoch = previous.Epoch + 1
	wanted.Revision = revision
	wanted.MoveID = MoveID{}
	d.publishLocked(wanted)
	return wanted, nil
}
func (d *MemoryDirectory) Release(ctx context.Context, expected RouteRecord) (RouteRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	current, exists := d.routes[expected.Path]
	if exists && current.State == RouteTombstone && current.PID() == expected.PID() && expected.Epoch < math.MaxUint64 && current.Epoch == expected.Epoch+1 {
		return current, nil
	}
	if !exists || current != expected || current.State != RouteActive {
		return RouteRecord{}, ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 {
		return RouteRecord{}, ErrCapacity
	}
	revision, err := d.nextLocked()
	if err != nil {
		return RouteRecord{}, err
	}
	current.Epoch++
	current.State = RouteTombstone
	current.MoveID = MoveID{}
	current.Revision = revision
	d.publishLocked(current)
	return current, nil
}
func (d *MemoryDirectory) SwapInstance(ctx context.Context, expected, replacement RouteRecord) (RouteRecord, error) {
	if err := replacement.Validate(); err != nil {
		return RouteRecord{}, err
	}
	if expected.Path != replacement.Path || expected.Owner != replacement.Owner || expected.BootID != replacement.BootID {
		return RouteRecord{}, ErrDirectoryConflict
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteRecord{}, err
	}
	current, exists := d.routes[expected.Path]
	if exists && current.State == RouteActive && current.PID() == replacement.PID() && current.Kind == replacement.Kind && current.Version == replacement.Version && expected.Epoch < math.MaxUint64 && current.Epoch == expected.Epoch+1 {
		return current, nil
	}
	if !exists || current != expected || current.State != RouteActive {
		return RouteRecord{}, ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 {
		return RouteRecord{}, ErrCapacity
	}
	revision, err := d.nextLocked()
	if err != nil {
		return RouteRecord{}, err
	}
	replacement.Epoch = current.Epoch + 1
	replacement.Revision = revision
	replacement.State = RouteActive
	replacement.MoveID = current.MoveID
	d.publishLocked(replacement)
	return replacement, nil
}
func (d *MemoryDirectory) PrepareMove(ctx context.Context, expected, target RouteRecord, id MoveID) (MoveRecord, error) {
	if err := target.Validate(); err != nil {
		return MoveRecord{}, err
	}
	if id == (MoveID{}) || expected.Path != target.Path || expected.Owner == target.Owner {
		return MoveRecord{}, ErrDirectoryConflict
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return MoveRecord{}, err
	}
	if previous, ok := d.moves[id]; ok {
		if previous.Source == expected && previous.Target.PID() == target.PID() && previous.Target.Kind == target.Kind && previous.Target.Version == target.Version {
			return previous, nil
		}
		return MoveRecord{}, ErrDirectoryConflict
	}
	current, exists := d.routes[expected.Path]
	if !exists || current != expected || current.State != RouteActive {
		return MoveRecord{}, ErrDirectoryConflict
	}
	if current.Epoch == math.MaxUint64 || len(d.moves) >= 100000 {
		return MoveRecord{}, ErrCapacity
	}
	revision, err := d.nextLocked()
	if err != nil {
		return MoveRecord{}, err
	}
	target.State = RouteActive
	target.Epoch = current.Epoch + 1
	target.Revision = 0
	target.MoveID = id
	move := MoveRecord{ID: id, Source: current, Target: target, State: MovePreparing, Revision: revision}
	current.State = RoutePreparing
	current.MoveID = id
	current.Revision = revision
	d.moves[id] = move
	d.publishLocked(current)
	return move, nil
}
func DirectoryMoveMatchesRoute(move MoveRecord, route RouteRecord) bool {
	return route.Path == move.Source.Path && route.State == RoutePreparing && route.MoveID == move.ID && route.PID() == move.Source.PID() && route.Epoch == move.Source.Epoch && route.Revision == move.Revision
}
func (d *MemoryDirectory) CommitMove(ctx context.Context, id MoveID) (MoveRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return MoveRecord{}, err
	}
	move, exists := d.moves[id]
	if !exists {
		return MoveRecord{}, ErrDirectoryNotFound
	}
	if move.State == MoveCommitted {
		return move, nil
	}
	if move.State != MovePreparing {
		return MoveRecord{}, ErrDirectoryConflict
	}
	if !DirectoryMoveMatchesRoute(move, d.routes[move.Source.Path]) {
		return MoveRecord{}, ErrDirectoryConflict
	}
	revision, err := d.nextLocked()
	if err != nil {
		return MoveRecord{}, err
	}
	move.State = MoveCommitted
	move.Revision = revision
	move.Target.Revision = revision
	d.moves[id] = move
	d.publishLocked(move.Target)
	return move, nil
}
func (d *MemoryDirectory) AbortMove(ctx context.Context, id MoveID) (MoveRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return MoveRecord{}, err
	}
	move, exists := d.moves[id]
	if !exists {
		return MoveRecord{}, ErrDirectoryNotFound
	}
	if move.State == MoveCommitted {
		return move, ErrMoveCommitted
	}
	if move.State == MoveAborted {
		return move, nil
	}
	if !DirectoryMoveMatchesRoute(move, d.routes[move.Source.Path]) {
		return MoveRecord{}, ErrDirectoryConflict
	}
	revision, err := d.nextLocked()
	if err != nil {
		return MoveRecord{}, err
	}
	move.State = MoveAborted
	move.Revision = revision
	d.moves[id] = move
	route := move.Source
	route.State = RouteActive
	route.MoveID = move.Source.MoveID
	route.Revision = revision
	d.publishLocked(route)
	return move, nil
}
func (d *MemoryDirectory) ReadMove(ctx context.Context, id MoveID) (MoveRecord, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return MoveRecord{}, err
	}
	move, exists := d.moves[id]
	if !exists {
		return MoveRecord{}, ErrDirectoryNotFound
	}
	return move, nil
}

func (d *MemoryDirectory) Watch(ctx context.Context, prefix string) (<-chan DirectoryEvent, error) {
	prefix = strings.TrimSuffix(prefix, "/")
	if prefix != "" && !validPath(prefix) {
		return nil, ErrInvalidMessage
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(d.watches) >= 64 {
		return nil, ErrCapacity
	}
	watch := &memoryDirectoryWatch{prefix: prefix, events: make(chan DirectoryEvent, 65)}
	var snapshot []RouteRecord
	for _, record := range d.routes {
		if DirectoryPathMatches(record.Path, prefix) {
			snapshot = append(snapshot, record)
		}
	}
	sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].Path < snapshot[j].Path })
	watch.events <- DirectoryEvent{Revision: d.revision, Snapshot: true, Routes: snapshot}
	d.watches[watch] = struct{}{}
	watch.stop = context.AfterFunc(ctx, func() {
		d.mu.Lock()
		if _, exists := d.watches[watch]; exists {
			delete(d.watches, watch)
			close(watch.events)
		}
		d.mu.Unlock()
	})
	return watch.events, nil
}
