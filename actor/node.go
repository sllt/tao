package actor

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type route struct {
	process *process
	err     error
}

func (r route) status() error {
	if (r.err == ErrStarting || r.err == ErrRestarting) && r.process.published.Load() {
		return nil
	}
	return r.err
}

type Node struct {
	executors      executorPool
	created        time.Time
	diagnostics    atomic.Bool
	kinds          map[kindKey]KindSpec
	directoryCache *DirectoryCache
	remoteMonitors map[MonitorRef]*remoteMonitor
	network        *network
	opts           NodeOptions
	bootID         [16]byte
	mu             sync.RWMutex
	stopping       atomic.Bool
	draining       bool
	nextPID        atomic.Uint64
	nextRequest    atomic.Uint64
	pendingCalls   atomic.Int64 // all local, incoming and outgoing requests share one quota
	asyncPending   atomic.Int64 // Request operations and queued results, independently bounded
	nextAsync      uint64       // mu; local correlation IDs never wrap
	asyncWorkers   sync.WaitGroup
	processes      map[uint64]*process
	pidIndex       pidRegistry // published PID IDs only; mutations also hold mu
	routes         map[string]route
	pending        map[uint64]*request
	callSignals    chan chan struct{}
	requestTimers  chan *requestTimer
	root           *process
	monitors       map[MonitorRef]*monitorRecord
	links          map[PID]map[PID]*linkRecord
	watchCounts    map[PID]int
	observations   int
	nextMonitor    uint64
}

func NewNode(options NodeOptions) (*Node, error) {
	options, err := options.normalized()
	if err != nil {
		return nil, err
	}
	n := &Node{opts: options, processes: make(map[uint64]*process), routes: make(map[string]route), pending: make(map[uint64]*request), monitors: make(map[MonitorRef]*monitorRecord), links: make(map[PID]map[PID]*linkRecord), watchCounts: make(map[PID]int)}
	n.callSignals = make(chan chan struct{}, min(requestCacheEntries, options.MaxPendingCalls))
	n.requestTimers = make(chan *requestTimer, min(requestCacheEntries, options.MaxPendingCalls))
	procs := runtime.GOMAXPROCS(0)
	// Retain a bounded working set across bursts to many Actors; retiring a
	// worker also discards its reusable stack and mailbox batch storage.
	n.executors.init(64, min(32, procs))
	n.created = time.Now()
	n.kinds = make(map[kindKey]KindSpec, len(options.Kinds))
	for _, kind := range options.Kinds {
		if err := kind.validate(); err != nil {
			return nil, err
		}
		key := kindKey{kind.Name, kind.Version}
		if _, exists := n.kinds[key]; exists {
			return nil, fmt.Errorf("actor: duplicate kind %s@%s", kind.Name, kind.Version)
		}
		n.kinds[key] = kind
	}
	if _, err = rand.Read(n.bootID[:]); err != nil {
		return nil, err
	}
	n.root = newProcess(n, nil, "", PID{}, nil, spawnOptions{shutdown: options.ShutdownTimeout})
	n.root.internal = true
	n.root.booted = true
	n.root.running = false
	close(n.root.started)
	if options.Directory != nil {
		n.directoryCache, err = NewDirectoryCache(n.root.ctx, options.Directory, "")
		if err != nil {
			n.root.cancel(err)
			return nil, err
		}
	}
	return n, nil
}
func (n *Node) Name() string          { return n.opts.Name }
func (n *Node) Done() <-chan struct{} { return n.root.done }
func (n *Node) Err() error            { n.root.mu.Lock(); defer n.root.mu.Unlock(); return n.root.stopReason }
func (n *Node) BootID() [16]byte      { return n.bootID }
func (n *Node) Log() *slog.Logger     { return n.opts.Logger }
func (n *Node) Ref(address string) Ref {
	if strings.HasPrefix(address, "node://") {
		u, err := url.Parse(address)
		if err == nil {
			return Ref{Node: u.Host, Path: strings.TrimPrefix(u.Path, "/")}
		}
	}
	return Ref{Path: address}
}
func (n *Node) Spawn(factory Factory, options ...SpawnOption) (PID, error) {
	return n.spawn(n.root, context.Background(), factory, options...)
}

func (n *Node) spawn(parent *process, ctx context.Context, factory Factory, options ...SpawnOption) (PID, error) {
	if factory == nil {
		return PID{}, fmt.Errorf("actor: nil factory")
	}
	opts := spawnOptions{mailbox: n.opts.MailboxSize, shutdown: n.opts.ShutdownTimeout}
	for _, option := range options {
		if err := option(&opts); err != nil {
			return PID{}, err
		}
	}
	parent.childrenMu.Lock()
	parent.mu.Lock()
	stopping := parent.stopRequested || parent.finished
	parent.mu.Unlock()
	if stopping {
		parent.childrenMu.Unlock()
		return PID{}, ErrStopped
	}
	if n.stopping.Load() {
		parent.childrenMu.Unlock()
		return PID{}, ErrClosed
	}
	pid, err := n.allocatePID()
	if err != nil {
		parent.childrenMu.Unlock()
		return PID{}, err
	}
	if opts.name == "" {
		opts.name = fmt.Sprintf("actor-%d", pid.ID)
		if n.opts.Directory != nil {
			opts.name = fmt.Sprintf("actor-%s-%d", n.Name(), pid.ID)
		}
	}
	path := opts.name
	if parent.path != "" {
		path = parent.path + "/" + path
	}
	if !validPath(path) {
		parent.childrenMu.Unlock()
		return PID{}, fmt.Errorf("actor: invalid process path")
	}
	// Construction allocates contexts, channels, names and logger state. Keep
	// it outside the node registry lock so existing actors can start and retire.
	// childrenMu keeps this child in the parent's shutdown snapshot on commit.
	p := newProcess(n, parent, path, pid, factory, opts)
	p.spec = &childSpec{factory: factory, opts: opts, path: path, process: p}
	n.mu.Lock()
	if n.stopping.Load() || n.draining {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		p.cancel(ErrClosed)
		return PID{}, ErrClosed
	}
	if len(n.processes) >= n.opts.MaxActors || len(n.routes) >= n.opts.MaxActors {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		p.cancel(ErrCapacity)
		return PID{}, ErrCapacity
	}
	if _, exists := n.routes[path]; exists {
		n.mu.Unlock()
		parent.childrenMu.Unlock()
		p.cancel(ErrNameTaken)
		return PID{}, ErrNameTaken
	}
	// Diagnostics may have toggled while construction ran outside mu. A new
	// registration must follow the same setting as actors already in the map.
	if n.diagnostics.Load() {
		if p.metrics.Load() == nil {
			p.metrics.Store(&ReceiveMetrics{Since: p.created})
		}
	} else {
		p.metrics.Store(nil)
	}
	n.processes[pid.ID] = p
	n.routes[path] = route{p, ErrStarting}
	n.mu.Unlock()
	parent.children = append(parent.children, p.spec)
	parent.childrenMu.Unlock()
	p.scheduleStart()
	if err := n.waitStarted(ctx, p); err != nil {
		return PID{}, err
	}
	return pid, p.startErr
}

// IDs are unique within this boot, including reservations that later fail.
// Capacity, draining and name checks still commit together under node.mu.
func (n *Node) allocatePID() (PID, error) {
	for {
		previous := n.nextPID.Load()
		if previous == math.MaxUint64 {
			return PID{}, ErrCapacity
		}
		if n.nextPID.CompareAndSwap(previous, previous+1) {
			return PID{Node: n.opts.Name, BootID: n.bootID, ID: previous + 1}, nil
		}
	}
}

var startupTimers sync.Pool

// Startup's timeout only bounds this wait; the actor owns a separate context.
// A reusable timer avoids deriving a cancelCtx and linking it into the parent
// for every Spawn. Cancellation still stops the child and revokes its route.
func (n *Node) waitStarted(ctx context.Context, p *process) error {
	var timer *time.Timer
	if cached := startupTimers.Get(); cached != nil {
		timer = cached.(*time.Timer)
		timer.Reset(n.opts.StartupTimeout)
	} else {
		timer = time.NewTimer(n.opts.StartupTimeout)
	}
	defer func() { timer.Stop(); startupTimers.Put(timer) }()
	var err error
	select {
	case <-p.started:
		return nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = context.DeadlineExceeded
	}
	select {
	case <-p.started:
		return nil
	default:
	}
	p.requestStop(err, true)
	n.markUnresponsive(p)
	return errors.Join(ErrUnresponsive, err)
}

func (n *Node) resolve(address Addr) (*process, bool, error) {
	if pid, ok := address.(PID); ok {
		p, err := n.resolvePID(pid)
		return p, false, err
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.resolveLocked(address)
}

func (n *Node) resolvePID(pid PID) (*process, error) {
	if n.stopping.Load() {
		return nil, ErrClosed
	}
	if pid.Node != n.opts.Name {
		return nil, ErrRemoteUnavailable
	}
	if pid.BootID != n.bootID {
		return nil, ErrStalePID
	}
	if p, ok := n.pidIndex.Load(pid.ID); ok && p.published.Load() {
		return p, nil
	}
	return nil, ErrNoProcess
}

// resolveLocked shares lookup with Call's pending-request registration, so a
// Call need not release a read lock only to acquire the write lock immediately.
func (n *Node) resolveLocked(address Addr) (*process, bool, error) {
	if n.stopping.Load() {
		return nil, false, ErrClosed
	}
	switch address := address.(type) {
	case routedRef:
		if address.record.Owner != n.Name() || address.record.BootID != n.bootID {
			return nil, true, ErrRedirect
		}
		p := n.processes[address.record.ProcessID]
		if p == nil || !p.published.Load() {
			return nil, true, ErrRedirect
		}
		return p, true, nil
	case PID:
		if address.Node != n.opts.Name {
			return nil, false, ErrRemoteUnavailable
		}
		if address.BootID != n.bootID {
			return nil, false, ErrStalePID
		}
		p := n.processes[address.ID]
		if p == nil || !p.published.Load() {
			return nil, false, ErrNoProcess
		}
		return p, false, nil
	case Ref:
		if address.Node != "" && address.Node != n.opts.Name {
			return nil, true, ErrRemoteUnavailable
		}
		if !validPath(address.Path) {
			return nil, true, ErrNoProcess
		}
		r, exists := n.routes[address.Path]
		if !exists {
			return nil, true, ErrNoProcess
		}
		if err := r.status(); err != nil {
			return nil, true, err
		}
		return r.process, true, nil
	default:
		return nil, false, ErrNoProcess
	}
}

func (n *Node) Send(to Addr, msg any) error { return n.send(to, msg, nil, nil) }
func (n *Node) SendConfirmed(ctx context.Context, to Addr, msg any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n.isGlobal(to) {
		return n.sendGlobal(ctx, to.(Ref), msg, nil, true)
	}
	if peer := remoteNode(to, n.opts.Name); peer != "" && n.network != nil {
		return n.network.send(ctx, to, msg, PID{}, true)
	}
	return n.Send(to, msg)
}
func (n *Node) send(to Addr, msg any, sender Addr, request *request) error {
	if pid, ok := to.(PID); ok && pid.Node == n.opts.Name {
		p, err := n.resolvePID(pid)
		if err != nil {
			return err
		}
		return p.enqueue(envelope{message: msg, sender: sender, request: request})
	}
	if n.isGlobal(to) {
		ctx, cancel := context.WithTimeout(context.Background(), n.opts.CallTimeout)
		defer cancel()
		return n.sendGlobal(ctx, to.(Ref), msg, sender, false)
	}
	if peer := remoteNode(to, n.opts.Name); peer != "" && n.network != nil {
		var from PID
		if sender != nil {
			from, _ = sender.(PID)
		}
		return n.network.send(context.Background(), to, msg, from, false)
	}
	p, toRef, err := n.resolve(to)
	if err != nil {
		return err
	}
	return p.enqueue(envelope{message: msg, sender: sender, request: request, toRef: toRef, global: globalRecord(to)})
}

func (n *Node) Call[T any](ctx context.Context, to Addr, msg any) (T, error) {
	var zero T
	value, err := n.call(ctx, to, msg, nil)
	if err != nil {
		return zero, err
	}
	if value == nil {
		switch reflect.TypeFor[T]().Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			return zero, nil
		}
	}
	result, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: got %T", ErrReplyType, value)
	}
	return result, nil
}
func Call[T any](ctx context.Context, node *Node, to Addr, msg any) (T, error) {
	return node.Call[T](ctx, to, msg)
}

func (n *Node) StopActor(ctx context.Context, pid PID) error {
	n.mu.Lock()
	p := n.processes[pid.ID]
	valid := pid.Node == n.opts.Name && pid.BootID == n.bootID
	n.mu.Unlock()
	if !valid || p == nil {
		return ErrNoProcess
	}
	return n.stopProcess(ctx, p, nil, true)
}

func (n *Node) stopProcess(ctx context.Context, p *process, reason error, suppress bool) error {
	ctx, cancel := context.WithTimeout(ctx, p.opts.shutdown)
	defer cancel()
	p.requestStop(reason, suppress)
	select {
	case <-p.done:
		return p.stopErr
	case <-ctx.Done():
		select {
		case <-p.done:
			return p.stopErr
		default:
		}
		n.markUnresponsive(p)
		return errors.Join(ErrUnresponsive, ctx.Err())
	}
}

func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	n.stopping.Store(true)
	n.mu.Unlock()
	err := n.stopProcess(ctx, n.root, nil, true)
	// Aborted preparations are not children of the active supervisor tree.
	// A user hook that ignored cancellation must still be accounted for at shutdown.
	n.mu.Lock()
	var preparations []*process
	for _, p := range n.processes {
		p.mu.Lock()
		preparing := p.preparing
		p.mu.Unlock()
		if preparing {
			preparations = append(preparations, p)
		}
	}
	n.mu.Unlock()
	for _, p := range preparations {
		err = errors.Join(err, n.stopProcess(ctx, p, nil, true))
	}
	if n.directoryCache != nil {
		n.directoryCache.Close()
	}
	if n.network != nil {
		err = errors.Join(err, n.network.close(ctx))
	}
	return errors.Join(err, n.waitAsyncWorkers(ctx))
}

func (n *Node) markUnresponsive(p *process) {
	n.mu.Lock()
	p.mu.Lock()
	if p.finished {
		p.mu.Unlock()
		n.mu.Unlock()
		return
	}
	p.unresponsive = true
	p.cancelAsyncRequestsLocked()
	p.suppressRestart = true
	n.publishPIDLocked(p, false)
	if p.path != "" && n.routes[p.path].process == p {
		n.routes[p.path] = route{p, ErrUnresponsive}
	}
	p.mu.Unlock()
	fatal := n.endObservationsLocked(p, ErrUnresponsive)
	n.mu.Unlock()
	n.failRequests(p, ErrUnresponsive)
	for _, action := range fatal {
		action.run()
	}
}

// Requires p.mu. The registry entry precedes initial startup; the atomic flag
// publishes PID and Ref readiness after the index exists, without reacquiring
// node.mu behind a burst of unrelated creation/retirement. Ownership changes
// and registry entry mutations additionally hold node.mu as before.
func (n *Node) publishPIDLocked(p *process, published bool) {
	if published {
		n.pidIndex.Store(p.pid.ID, p)
		p.published.Store(true)
	} else {
		p.published.Store(false)
		n.pidIndex.Delete(p.pid.ID)
	}
}
