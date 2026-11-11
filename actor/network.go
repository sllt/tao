package actor

import (
	"context"
	"fmt"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/taonet"
)

type pipeKey struct {
	peer  string
	class taonet.TrafficClass
}
type actorPipe struct {
	sequence  uint64
	key       pipeKey
	queue     []*networkJob
	queueHead int
	queueSize int
	wake      chan struct{}
	channel   *taonet.Channel
	info      taonet.SessionInfo // immutable, refreshed only when its stream opens
}
type networkJob struct {
	redirects    int
	token        *controlReservation
	onComplete   func(error)
	onScope      func(wireScope)
	packet       actorPacket
	buffer       *packetBuffer
	raw          []byte
	peer         string
	class        taonet.TrafficClass
	charge       int
	result       chan error
	call         *outgoingCall
	expectedBoot [16]byte
	deadline     time.Time
}
type outgoingCall struct {
	routing          *outgoingRoute // only global/relocated Calls need ownership records
	local            bool
	sequence         uint64
	request          *request
	peer             string
	boot             [16]byte
	issued, canceled bool
	scope            wireScope
}

// One allocation for a default outbound Call's private state. These records
// are never recycled: a late writer, timer or reply keeps its original state.
type defaultCallState struct {
	outgoingRequestState
	scope requestScope
}

type outgoingRequestState struct {
	request  request
	outgoing outgoingCall
}

// Incoming state has the same non-recycled lifetime as its request. Holding a
// DeferredReply or a late timer never aliases a subsequent Call's storage.
type incomingRequestState struct {
	request request
	scope   requestScope
}

type outgoingRoute struct {
	move       MoveID
	moveTarget RouteRecord
	route      RouteRecord
}
type incomingCall struct {
	move          MoveID
	sequence      uint64
	budget        *requestScope
	cancelContext context.CancelFunc // migrated Calls use a standard context
	scope         wireScope
}

func (in incomingCall) cancel() {
	if in.budget != nil {
		in.budget.cancel()
		return
	}
	in.cancelContext()
}

type canceledCall struct {
	scope    wireScope
	sequence uint64
}
type network struct {
	handoffs                         map[MoveID]*handoffReservation
	outgoingHandoffs                 map[MoveID]*handoffOperation
	inboxProgress                    map[wireScope]uint64
	currentBoot                      map[string][16]byte
	draining                         bool
	controlReservations, controlJobs int
	controlWake                      chan struct{}
	node                             *Node
	opts                             NetworkOptions
	registry                         *Registry
	packetBuffers                    chan *packetBuffer
	packetCacheBytes                 atomic.Int64 // actual cached inline and growth storage, including pending publications
	jobs                             chan *networkJob
	endpoint                         *taonet.Endpoint
	address                          string
	ctx                              context.Context
	cancel                           context.CancelFunc
	mu                               sync.Mutex
	wg                               sync.WaitGroup
	once                             sync.Once
	closed                           bool
	peers                            map[string]string
	pipes                            map[pipeKey]*actorPipe
	watched                          map[*taonet.Session]struct{}
	outgoing                         map[uint64]*outgoingCall
	incoming                         map[requestKey]incomingCall
	canceled                         map[requestKey]canceledCall
	bytes, messages                  int
	sendChanged                      chan struct{} // mu; allocated only while Calls await send admission
	events                           chan NetworkEvent
	dropped                          uint64
}

// StartNode starts an Actor node with static peer addressing and taonet.
// address can be "name@host:port" or just "host:port" with NodeOptions.Name.
// An empty address creates an outbound-only node.
func StartNode(address string, options NodeOptions) (*Node, error) {
	for i, b := range []byte(address) {
		if b == '@' {
			if options.Name != "" && options.Name != address[:i] {
				return nil, fmt.Errorf("actor: conflicting node names")
			}
			options.Name = address[:i]
			address = address[i+1:]
			break
		}
	}
	opts, err := options.Network.normalized()
	if err != nil {
		return nil, err
	}
	registry, err := newRegistry(options.Register, opts.MaxMessageBytes, options.Codec)
	if err != nil {
		return nil, err
	}
	n, err := NewNode(options)
	if err != nil {
		return nil, err
	}
	started := false
	defer func() {
		if !started {
			_ = n.Stop(context.Background())
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	w := &network{node: n, opts: opts, registry: registry, ctx: ctx, cancel: cancel, peers: maps.Clone(opts.Peers), pipes: make(map[pipeKey]*actorPipe), watched: make(map[*taonet.Session]struct{}), outgoing: make(map[uint64]*outgoingCall), incoming: make(map[requestKey]incomingCall), canceled: make(map[requestKey]canceledCall), inboxProgress: make(map[wireScope]uint64), events: make(chan NetworkEvent, 64)}
	w.handoffs = make(map[MoveID]*handoffReservation)
	w.packetBuffers = make(chan *packetBuffer, min(networkCacheEntries, opts.SendMessages, packetCacheBudget(opts.SendBytes)/packetInlineBytes))
	w.jobs = make(chan *networkJob, min(networkCacheEntries, opts.SendMessages))
	w.outgoingHandoffs = make(map[MoveID]*handoffOperation)
	if w.peers == nil {
		w.peers = make(map[string]string)
	}
	mux := taonet.NewMux()
	if err = mux.HandleChannel(inboxRoute, w.serveInbox); err != nil {
		return nil, err
	}
	if err = mux.HandleStream(replyRoute, w.serveReplies); err != nil {
		return nil, err
	}
	if err = mux.HandleChannel(controlRoute, w.serveControl); err != nil {
		return nil, err
	}
	if err = mux.HandleRequest(handoffControlRoute, w.serveHandoffControl); err != nil {
		return nil, err
	}
	if err = mux.HandleStream(handoffBulkRoute, w.serveHandoffBulk); err != nil {
		return nil, err
	}
	profile := taonet.Isolated
	if opts.Profile == CompactNetwork {
		profile = taonet.Compact
	}
	maxMessage := max(256<<10, opts.MaxMessageBytes+4096)
	w.endpoint, err = taonet.NewEndpoint(taonet.EndpointConfig{Profile: profile, MaxSessions: opts.MaxSessions, ManagedBytes: opts.ManagedBytes, Session: taonet.SessionConfig{Local: taonet.Peer{ID: taonet.PeerID(n.Name()), Instance: taonet.ID(n.BootID()), Namespace: opts.Namespace}, Mux: mux, StreamReceiveWindow: 256 << 10, Transport: taonet.TransportConfig{TLS: opts.TLS, AllowInsecureLoopback: opts.AllowInsecureLoopback}, Limits: taonet.Limits{ChannelMaxMessage: maxMessage, ChannelWindowBytes: max(1<<20, 2*(maxMessage+128))}}})
	if err != nil {
		cancel()
		return nil, err
	}
	w.controlWake = make(chan struct{}, 1)
	w.currentBoot = make(map[string][16]byte)
	n.network = w
	if address != "" {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			_ = n.Stop(context.Background())
			return nil, err
		}
		w.address = listener.Addr().String()
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := w.endpoint.Serve(w.ctx, listener); err != nil && w.ctx.Err() == nil {
				w.event(NetworkEvent{Kind: "listener_failed", Err: err})
			}
		}()
	}
	started = true
	return n, nil
}

func (n *Node) ListenAddr() string {
	if n.network == nil {
		return ""
	}
	return n.network.address
}
func (n *Node) NetworkEvents() <-chan NetworkEvent {
	if n.network == nil {
		return nil
	}
	return n.network.events
}
func (n *Node) Connect(ctx context.Context, peer, address string) error {
	if n.network == nil {
		return ErrRemoteUnavailable
	}
	if !validSegment(peer) {
		return ErrInvalidMessage
	}
	w := n.network
	w.mu.Lock()
	if address != "" {
		w.peers[peer] = address
	}
	w.mu.Unlock()
	_, err := w.connection(ctx, peer, taonet.ClassControl)
	return err
}
func (n *Node) NetworkStats() NetworkStats {
	if n.network == nil {
		return NetworkStats{}
	}
	w := n.network
	w.mu.Lock()
	stats := NetworkStats{QueuedBytes: w.bytes, QueuedMessages: w.messages, OutgoingCalls: len(w.outgoing), IncomingCalls: len(w.incoming), CancellationRecords: len(w.canceled), DroppedEvents: w.dropped}
	stats.ControlReservations = w.controlReservations
	stats.ControlJobs = w.controlJobs
	w.mu.Unlock()
	stats.Endpoint = w.endpoint.Stats()
	return stats
}

func (w *network) event(event NetworkEvent) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		w.dropped++
		return
	}
	select {
	case w.events <- event:
	default:
		w.dropped++
	}
}
func (w *network) connection(ctx context.Context, peer string, class taonet.TrafficClass) (*taonet.Session, error) {
	w.mu.Lock()
	address := w.peers[peer]
	w.mu.Unlock()
	target := taonet.Target{Peer: taonet.PeerID(peer), Address: address}
	main, err := w.endpoint.Connect(ctx, target)
	if err != nil {
		// An accepted primary can dispatch its first control message just before
		// Endpoint registration completes. It is already a usable Session.
		if address == "" && (class == taonet.ClassControl || w.opts.Profile == CompactNetwork) {
			w.mu.Lock()
			for existing := range w.watched {
				if string(existing.Info().Remote.ID) == peer && existing.Err() == nil {
					main = existing
					break
				}
			}
			w.mu.Unlock()
			if main != nil {
				return main, nil
			}
		}
		return nil, err
	}
	w.watch(main)
	if class == taonet.ClassControl || w.opts.Profile == CompactNetwork {
		return main, nil
	}
	return w.endpoint.ConnectClass(ctx, target, class)
}

func (w *network) watch(session *taonet.Session) {
	if session.Info().Class != taonet.ClassControl {
		return
	}
	w.mu.Lock()
	if w.closed || session.Err() != nil {
		w.mu.Unlock()
		return
	}
	if _, ok := w.watched[session]; ok {
		w.mu.Unlock()
		return
	}
	w.watched[session] = struct{}{}
	info := session.Info()
	peer := string(info.Remote.ID)
	boot := [16]byte(info.Remote.Instance)
	previous, known := w.currentBoot[peer]
	w.currentBoot[peer] = boot
	w.wg.Add(1)
	w.mu.Unlock()
	if known && previous != boot {
		w.failPeer(peer, previous)
		for _, old := range w.endpoint.Sessions(info.Remote.ID) {
			if [16]byte(old.Info().Remote.Instance) == previous {
				_ = old.Close()
			}
		}
		w.event(NetworkEvent{Peer: peer, Kind: "peer_instance_changed"})
	}
	go func() {
		defer w.wg.Done()
		<-session.Done()
		info := session.Info()
		w.mu.Lock()
		delete(w.watched, session)
		alive := false
		for other := range w.watched {
			otherInfo := other.Info()
			if otherInfo.Remote == info.Remote && other.Err() == nil {
				alive = true
				break
			}
		}
		w.mu.Unlock()
		w.mu.Lock()
		current := w.currentBoot[string(info.Remote.ID)]
		w.mu.Unlock()
		if !alive && current == [16]byte(info.Remote.Instance) {
			w.failPeer(string(info.Remote.ID), [16]byte(info.Remote.Instance))
		}
	}()
}
func (w *network) watchInbound(session *taonet.Session) {
	w.watch(session)
	peer := session.Info().Remote
	for _, main := range w.endpoint.Sessions(peer.ID) {
		if main.Info().Remote == peer {
			w.watch(main)
		}
	}
}
func (w *network) failPeer(peer string, boot [16]byte) {
	w.node.remoteObservationLost(peer, boot, nil)
	w.mu.Lock()
	var calls []*outgoingCall
	var cancel []context.CancelFunc
	for _, out := range w.outgoing {
		if out.peer == peer && out.boot == boot && out.issued {
			calls = append(calls, out)
		}
	}
	for key, in := range w.incoming {
		if key.Caller == peer && key.Boot == boot {
			cancel = append(cancel, in.cancel)
		}
	}
	for key := range w.canceled {
		if key.Caller == peer && key.Boot == boot {
			delete(w.canceled, key)
		}
	}
	w.mu.Unlock()
	resolveCtx, resolveCancel := context.WithTimeout(w.ctx, w.node.opts.CallTimeout)
	defer resolveCancel()
	for _, out := range calls {
		if w.keepMovedCall(resolveCtx, out, peer, boot) {
			continue
		}
		r := out.request
		_ = w.node.reply(r, r.owner, nil, &RemoteError{Code: "unreachable", Message: "control connection closed", Outcome: Unknown, Cause: ErrNodeUnreachable}, true)
	}
	for _, stop := range cancel {
		stop()
	}
	for _, s := range w.endpoint.Sessions(taonet.PeerID(peer)) {
		if [16]byte(s.Info().Remote.Instance) == boot && s.Info().Class != taonet.ClassControl {
			_ = s.Close()
		}
	}
	w.event(NetworkEvent{Peer: peer, Kind: "peer_unreachable", Err: ErrNodeUnreachable})
	w.recoverHandoffPeer(resolveCtx, peer, boot)
}

func (w *network) close(ctx context.Context) error {
	var err error
	w.once.Do(func() {
		w.mu.Lock()
		w.draining = true
		w.wakeSendersLocked()
		w.mu.Unlock()
		grace, cancel := context.WithTimeout(ctx, w.node.opts.ShutdownTimeout)
		err = w.waitControlDrain(grace)
		cancel()
		w.mu.Lock()
		w.closed = true
		var calls []*request
		for _, out := range w.outgoing {
			calls = append(calls, out.request)
		}
		var incoming []context.CancelFunc
		for _, call := range w.incoming {
			incoming = append(incoming, call.cancel)
		}
		w.mu.Unlock()
		w.cancel()
		for _, r := range calls {
			_ = w.node.reply(r, r.owner, nil, ErrNodeUnreachable, true)
		}
		for _, cancel := range incoming {
			cancel()
		}
		_ = w.endpoint.Close()
		w.wg.Wait()
		w.mu.Lock()
		close(w.events)
		w.mu.Unlock()
	})
	return err
}

func (w *network) validPeerPacket(peer taonet.Peer, packet actorPacket, call bool) bool {
	if packet.Sender != nil && (packet.Sender.Node != string(peer.ID) || packet.Sender.BootID != [16]byte(peer.Instance) || packet.Sender.ID == 0) {
		return false
	}
	return !call || packet.Key.ID != 0 && packet.Key.Caller == string(peer.ID) && packet.Key.Boot == [16]byte(peer.Instance)
}

func (w *network) checkClass(c *taonet.Channel, class taonet.TrafficClass) error {
	if w.opts.Profile == IsolatedNetwork && c.Session().Info().Class != class {
		return ErrInvalidMessage
	}
	w.watchInbound(c.Session())
	return nil
}

func (w *network) coreRequest(ctx context.Context, owner PID) (*request, error) {
	ctx, cancel := context.WithCancel(ctx)
	return w.ownedRequest(ctx, cancel, owner, false)
}

func (w *network) ownedRequest(ctx context.Context, cancel context.CancelFunc, owner PID, outbound bool) (*request, error) {
	scope, _ := ctx.(*requestScope)
	return w.registerRequest(ctx, cancel, owner, outbound, scope)
}

func (w *network) registerRequest(ctx context.Context, cancel context.CancelFunc, owner PID, outbound bool, incoming *requestScope) (*request, error) {
	return w.registerRequestInto(new(request), ctx, cancel, owner, outbound, incoming)
}

// r is fresh storage, either independent or embedded in defaultCallState.
func (w *network) registerRequestInto(r *request, ctx context.Context, cancel context.CancelFunc, owner PID, outbound bool, incoming *requestScope) (*request, error) {
	n := w.node
	// Allocate before taking the registry lock: allocator work and cache
	// contention must not stall every other request's registration/completion.
	single := outbound && owner.Node != n.Name()
	var done, callerDone chan struct{}
	if single {
		callerDone = n.takeCallSignal()
	} else if incoming == nil {
		done = make(chan struct{})
	}
	*r = request{node: n, owner: owner, ctx: ctx, cancel: cancel, done: done, callerDone: callerDone}
	if incoming != nil {
		incoming.request = r
		r.ctx = incoming
	}
	if n.diagnostics.Load() {
		r.trace = &requestTrace{started: time.Now()}
	}
	n.mu.Lock()
	stopping := n.stopping.Load()
	var err error
	if stopping {
		err = ErrClosed
	} else {
		r.id, err = n.reserveRequest()
	}
	if err != nil {
		n.mu.Unlock()
		r.cancelContext()
		if single {
			n.recycleCallSignal(callerDone)
		}
		return nil, err
	}
	if r.trace == nil && n.diagnostics.Load() {
		// EnableDiagnostics can win the lock between construction and commit.
		r.trace = &requestTrace{started: time.Now()}
	}
	n.pending[r.id] = r
	if p := n.processes[owner.ID]; p != nil && p.pid == owner {
		p.callsUsed.Store(true)
	}
	n.mu.Unlock()
	return r, nil
}

func (n *Node) expireRequest(r *request, err error) bool {
	if owner := r.actorRequests(); owner != nil {
		return owner.expire(r, err)
	}
	if r.kind == requestRemotePID {
		return n.network.expireRemote(r, err)
	}
	n.mu.Lock()
	if r.completed {
		n.mu.Unlock()
		return false
	}
	r.completed = true
	r.expired = true
	r.err = err
	delete(n.pending, r.id)
	n.pendingCalls.Add(-1)
	r.signalDoneLocked()
	n.mu.Unlock()
	r.finish()
	return true
}

func (w *network) send(ctx context.Context, to Addr, message any, sender PID, confirmed bool) error {
	if confirmed {
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, w.node.opts.CallTimeout)
			defer cancel()
		}
	}
	if err := ctx.Err(); err != nil {
		return deliveryError(err, NotSent)
	}
	addr, err := encodeAddress(to)
	if err != nil {
		return err
	}
	var result chan error
	if confirmed {
		result = make(chan error, 1)
	}
	err = w.queueMessage(remoteNode(to, w.node.Name()), taonet.ClassInteractive, actorPacket{Kind: "send", Target: addr}, message, sender, nil, result)
	if err != nil {
		return deliveryError(err, NotSent)
	}
	if !confirmed {
		return nil
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case err := <-result:
			return err
		default:
		}
		return deliveryError(ctx.Err(), Unknown)
	}
}

func (w *network) call(ctx context.Context, to Addr, message any, sender PID) (any, error) {
	ctx, cancel := context.WithCancel(ctx)
	return w.callOwned(ctx, cancel, to, message, sender, nil)
}

// A remote Call without a caller deadline or cancellation uses the same
// bounded timer cache as incoming Calls. Its private scope materializes a
// standard context only when a transport operation actually needs one.
func (w *network) callDefault(parent context.Context, to Addr, message any, sender PID) (any, error) {
	state := &defaultCallState{scope: requestScope{parent: parent, deadline: time.Now().Add(w.node.opts.CallTimeout), hasDeadline: true}}
	return w.callOwned(&state.scope, nil, to, message, sender, state)
}

// The public Call entry may already own a timeout context. Transfer that
// cancellation instead of attaching another child and allocating its waiters.
// Caller-owned contexts still enter through call and get their own child.
func (w *network) callOwned(ctx context.Context, cancel context.CancelFunc, to Addr, message any, sender PID, state *defaultCallState) (any, error) {
	if pid, ok := to.(PID); ok && pid.Node != w.node.Name() {
		return w.callPIDOwned(ctx, cancel, pid, message, sender, state)
	}
	peer := remoteNode(to, w.node.Name())
	owner := PID{Node: peer}
	if pid, ok := to.(PID); ok {
		owner = pid
	}
	var r *request
	var out *outgoingCall
	var err error
	if state != nil {
		r, err = w.registerRequestInto(&state.request, ctx, cancel, owner, true, &state.scope)
		out = &state.outgoing
	} else {
		r, err = w.ownedRequest(ctx, cancel, owner, true)
	}
	if err != nil {
		return nil, deliveryError(err, NotSent)
	}
	done := r.callerDone
	if done != nil {
		defer w.node.recycleCallSignal(done)
	} else {
		done = r.done
	}
	if out == nil {
		out = new(outgoingCall)
	}
	*out = outgoingCall{request: r, peer: peer}
	scope, managed := ctx.(*requestScope)
	if managed {
		scope.outgoing = out
	}
	if w.node.diagnostics.Load() {
		w.node.mu.Lock()
		trace := r.enableTraceLocked()
		trace.caller, trace.target = sender, diagnosticTarget(to)
		w.node.mu.Unlock()
	}
	if route := globalRecord(to); route != nil {
		out.routing = &outgoingRoute{route: *route}
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.node.expireRequest(r, ErrClosed)
		return nil, deliveryError(ErrClosed, NotSent)
	}
	w.outgoing[r.id] = out
	w.mu.Unlock()
	if managed {
		scope.start()
	}
	defer func() { w.mu.Lock(); delete(w.outgoing, r.id); w.mu.Unlock() }()
	var addr wireAddress
	if _, ok := to.(PID); ok {
		// A direct remote Call already owns this immutable target PID. Its
		// request record is never recycled, including after a late reply.
		addr = wireAddress{Kind: "pid", PID: &r.owner}
	} else {
		addr, err = encodeAddress(to)
	}
	if err != nil {
		w.node.expireRequest(r, err)
		return nil, err
	}
	packet := actorPacket{Kind: "call", Target: addr, Key: requestKey{w.node.Name(), w.node.BootID(), r.id}}
	err = w.queueMessage(peer, taonet.ClassInteractive, packet, message, sender, out, nil)
	if err != nil {
		w.node.expireRequest(r, err)
		return nil, deliveryError(err, NotSent)
	}
	if managed {
		<-done // the scope timer expires this request and propagates CANCEL
		return w.callResult(out)
	}
	select {
	case <-done:
		return w.callResult(out)
	case <-ctx.Done():
		return w.cancelCall(ctx, out, packet.Key)
	}
}

func (w *network) callResult(out *outgoingCall) (any, error) {
	r := out.request
	if r.kind == requestRemotePID {
		return r.value, r.err // outcome was committed with completion under network.mu
	}
	if r.err == nil {
		return r.value, nil
	}
	w.mu.Lock()
	issued := out.issued
	w.mu.Unlock()
	outcome := NotSent
	if issued {
		outcome = Unknown
	}
	return nil, deliveryError(r.err, outcome)
}

func (w *network) cancelCall(ctx context.Context, out *outgoingCall, key requestKey) (any, error) {
	if out.request.kind == requestRemotePID {
		return w.cancelRemoteCall(ctx, out, key)
	}
	// select may choose cancellation when a reply is already committed. Only
	// the winner of the request completion lock may change its outcome.
	if !w.node.expireRequest(out.request, ctx.Err()) {
		return w.callResult(out)
	}
	w.mu.Lock()
	issued, scope, boot, sequence, peer, local := out.issued, out.scope, out.boot, out.sequence, out.peer, out.local
	out.canceled = true
	w.mu.Unlock()
	if issued && !local && scope.Channel != 0 {
		w.queueControl(peer, actorPacket{Kind: "cancel", Key: key, Scope: scope, Sequence: sequence}, boot)
	}
	w.cancelRelocated(out, key)
	outcome := NotSent
	if issued {
		outcome = Unknown
	}
	return nil, deliveryError(ctx.Err(), outcome)
}
