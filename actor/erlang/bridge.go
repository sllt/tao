package erlangactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

const (
	defaultCompletionQueueSize = 4096
	defaultCompletionWorkers   = 4
	defaultOutboundPeerQueue   = 1024
)

type peerGenKey struct {
	peer string
	gen  uint64
}

type peerGenScope struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type completionKind uint8

const (
	completionBeginCall completionKind = iota + 1
	completionDirectError
)

type completionEvent struct {
	kind    completionKind
	binding *Binding
	entry   *inboundPendingCall
	comp    actor.CallCompletion
	replyTo erlang.ReplyTarget
	err     error
	release func()
}

type completionShard struct {
	bridge   *Bridge
	reserved atomic.Int32
	limit    int32
	ch       chan completionEvent
}

func (s *completionShard) tryReserve() bool {
	for {
		cur := s.reserved.Load()
		if cur >= s.limit {
			return false
		}
		if s.reserved.CompareAndSwap(cur, cur+1) {
			s.bridge.compReservedWG.Add(1)
			return true
		}
	}
}

func (s *completionShard) releaseReservation() {
	s.reserved.Add(-1)
	s.bridge.compReservedWG.Done()
}

func (s *completionShard) publishReserved(ev completionEvent) {
	s.ch <- ev
}

type inboundPendingCall struct {
	id                uint64
	binding           *Binding
	replyTo           erlang.ReplyTarget
	baseCtx           context.Context
	ctx               context.Context
	cancel            context.CancelFunc
	hasInboundTimeout bool
	releaseSlot       func()

	handleMu  sync.Mutex
	handle    *actor.CallHandle
	abortReq  bool
	aborted   atomic.Bool
	completed atomic.Bool
}

func (p *inboundPendingCall) bindHandle(h *actor.CallHandle) {
	p.handleMu.Lock()
	p.handle = h
	abort := p.abortReq
	p.handleMu.Unlock()
	if abort && h != nil {
		h.Cancel()
	}
}

func (p *inboundPendingCall) abort() {
	p.aborted.Store(true)
	if p.cancel != nil {
		p.cancel()
	}
	p.handleMu.Lock()
	p.abortReq = true
	h := p.handle
	p.handleMu.Unlock()
	if h != nil {
		h.Cancel()
	}
}

// Bridge connects a local actor.Node with an erlang.Node without taking ownership of either (§4.2).
type Bridge struct {
	taoNode    *actor.Node
	erlangNode *erlang.Node
	cfg        bridgeConfig

	client      *erlang.Client
	outboundPID actor.PID

	lifeCtx    context.Context
	lifeCancel context.CancelFunc
	doneCh     chan struct{}
	closed     atomic.Bool
	closeMu    sync.Mutex
	closeDone  bool
	closeErr   error

	nextBindingID atomic.Uint64
	bindingsMu    sync.RWMutex
	bindings      map[uint64]*Binding

	guardianOpMu sync.Mutex
	watchMu      sync.Mutex
	guardians    []*guardianGroup
	watches      map[actor.PID]*targetWatch
	refToTarget  map[actor.MonitorRef]actor.PID
	earlyDowns   map[actor.MonitorRef]error

	peerScopeMu sync.Mutex
	peerScopes  map[peerGenKey]*peerGenScope

	submittersMu sync.RWMutex
	submitters   map[string]*peerSubmitter

	compShards     []completionShard
	compStopCh     chan struct{}
	compReservedWG sync.WaitGroup
	compWG         sync.WaitGroup
	subWG          sync.WaitGroup

	inboundInFlight      atomic.Int64
	outboundSubmitted    atomic.Uint64
	outboundErrors       atomic.Uint64
	decodeErrors         atomic.Uint64
	overloadReplies      atomic.Uint64
	overloadTerminations atomic.Uint64
}

// Attach borrows taoNode and erlangNode and initializes a Bridge owning its own outbound Client,
// outbound connector Actor, adaptive guardian Monitor pool, and completion workers (§4.2).
func Attach(taoNode *actor.Node, erlangNode *erlang.Node, opts ...Option) (*Bridge, error) {
	if taoNode == nil || erlangNode == nil {
		return nil, errors.New("erlangactor: nil node")
	}
	select {
	case <-taoNode.Done():
		return nil, actor.ErrClosed
	case <-erlangNode.Done():
		return nil, erlang.ErrNodeClosed
	default:
	}

	cfg := bridgeConfig{
		completionQueueSize: defaultCompletionQueueSize,
		completionWorkers:   defaultCompletionWorkers,
		outboundPeerQueue:   defaultOutboundPeerQueue,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	client, err := erlangNode.NewClient()
	if err != nil {
		return nil, err
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	perShardCap := (cfg.completionQueueSize + cfg.completionWorkers - 1) / cfg.completionWorkers
	if perShardCap < 64 {
		perShardCap = 64
	}

	b := &Bridge{
		taoNode:     taoNode,
		erlangNode:  erlangNode,
		cfg:         cfg,
		client:      client,
		lifeCtx:     lifeCtx,
		lifeCancel:  lifeCancel,
		doneCh:      make(chan struct{}),
		bindings:    make(map[uint64]*Binding),
		watches:     make(map[actor.PID]*targetWatch),
		refToTarget: make(map[actor.MonitorRef]actor.PID),
		earlyDowns:  make(map[actor.MonitorRef]error),
		peerScopes:  make(map[peerGenKey]*peerGenScope),
		submitters:  make(map[string]*peerSubmitter),
		compShards:  make([]completionShard, cfg.completionWorkers),
		compStopCh:  make(chan struct{}),
	}
	for i := range b.compShards {
		b.compShards[i].bridge = b
		b.compShards[i].limit = int32(perShardCap)
		b.compShards[i].ch = make(chan completionEvent, perShardCap)
	}

	outPID, err := b.spawnOutboundActor()
	if err != nil {
		lifeCancel()
		_ = client.Close()
		return nil, err
	}
	b.outboundPID = outPID

	for i := range b.compShards {
		b.compWG.Add(1)
		go b.runCompletionShard(&b.compShards[i])
	}

	go b.watchUnderlyingNodes()

	return b, nil
}

func (b *Bridge) watchUnderlyingNodes() {
	select {
	case <-b.lifeCtx.Done():
		return
	case <-b.taoNode.Done():
	case <-b.erlangNode.Done():
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = b.Close(ctx)
}

// Outbound returns the local Tao Actor PID used to perform non-blocking Erlang Call, Cast,
// and Send operations via Actor.Request or actor.CallAs (§4.2).
func (b *Bridge) Outbound() actor.PID {
	return b.outboundPID
}

// Done returns a channel closed once the Bridge has finished closing.
func (b *Bridge) Done() <-chan struct{} {
	return b.doneCh
}

// Stats returns a point-in-time snapshot of Bridge and underlying Erlang distribution metrics (§12.3).
func (b *Bridge) Stats() Stats {
	b.bindingsMu.RLock()
	numBindings := len(b.bindings)
	b.bindingsMu.RUnlock()

	b.watchMu.Lock()
	numGuardians := len(b.guardians)
	numWatched := len(b.watches)
	b.watchMu.Unlock()

	return Stats{
		Bindings:             numBindings,
		GuardianActors:       numGuardians,
		WatchedActors:        numWatched,
		InboundInFlight:      b.inboundInFlight.Load(),
		OutboundSubmitted:    b.outboundSubmitted.Load(),
		OutboundErrors:       b.outboundErrors.Load(),
		DecodeErrors:         b.decodeErrors.Load(),
		OverloadReplies:      b.overloadReplies.Load(),
		OverloadTerminations: b.overloadTerminations.Load(),
		Erlang:               b.erlangNode.Stats(),
	}
}

// Export binds a concrete local Tao Actor PID to a new virtual Erlang Endpoint and
// optionally publishes name as an Erlang registered process (§4.2, §9.3).
// Pass name == "" to export an anonymous PID-only Endpoint.
func (b *Bridge) Export(name string, localPID actor.PID, schema Schema, opts ...ExportOption) (*Binding, error) {
	if b.closed.Load() {
		return nil, ErrBridgeClosed
	}
	if schema == nil {
		return nil, errors.New("erlangactor: nil Schema")
	}
	if localPID.Node != b.taoNode.Name() || localPID.BootID != b.taoNode.BootID() || localPID.ID == 0 {
		if localPID.Node != b.taoNode.Name() {
			return nil, actor.ErrRemoteUnavailable
		}
		if localPID.BootID != b.taoNode.BootID() {
			return nil, actor.ErrStalePID
		}
		return nil, actor.ErrNoProcess
	}

	var ecfg exportConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&ecfg)
		}
	}

	id := b.nextBindingID.Add(1)
	shard := &b.compShards[int(id)%len(b.compShards)]
	bd := &Binding{
		id:        id,
		bridge:    b,
		name:      name,
		localPID:  localPID,
		schema:    schema,
		cfg:       ecfg,
		compShard: shard,
		pending:   make(map[uint64]*inboundPendingCall),
		doneCh:    make(chan struct{}),
	}

	// Step 1 (§9.3): Create an unpublished Endpoint first (Name == "") so no peer can resolve
	// the registered name before guardian Monitor observation is installed!
	ep, err := b.erlangNode.Export(erlang.EndpointOptions{
		Name:         "",
		TrapExit:     ecfg.trapExit,
		AllowedPeers: ecfg.allowedPeers,
		OnDeliver:    bd.onDeliver,
		OnTerminate:  bd.onEndpointTerminated,
	})
	if err != nil {
		return nil, err
	}
	bd.ep = ep

	b.bindingsMu.Lock()
	if b.closed.Load() {
		b.bindingsMu.Unlock()
		ep.Terminate(etf.Atom("shutdown"))
		return nil, ErrBridgeClosed
	}
	b.bindings[id] = bd
	b.bindingsMu.Unlock()

	// Step 2 (§9.3): Install/share guardian Monitor on localPID BEFORE publishing the registered name.
	alreadyDown, downErr, watchErr := b.attachWatch(localPID, bd)
	if watchErr != nil {
		b.removeBindingRecord(id)
		ep.Terminate(etf.Atom("shutdown"))
		return nil, watchErr
	}
	if alreadyDown {
		b.removeBindingRecord(id)
		ep.Terminate(mapDownReason(downErr))
		if downErr == nil {
			downErr = actor.ErrNoProcess
		}
		return nil, downErr
	}

	// Step 3 (§9.3): Publish the Erlang registered name only after guardian observation is active.
	if name != "" {
		if pubErr := ep.PublishName(name); pubErr != nil {
			b.detachBindingWatch(bd)
			b.removeBindingRecord(id)
			ep.Terminate(etf.Atom("shutdown"))
			return nil, pubErr
		}
	}

	// Re-check whether target exited concurrently while PublishName was running.
	bd.mu.Lock()
	closedDuringPublish := bd.closing
	bd.published.Store(true)
	bd.mu.Unlock()
	if closedDuringPublish {
		ep.BeginShutdown()
	}

	return bd, nil
}

func (b *Bridge) removeBindingRecord(id uint64) {
	b.bindingsMu.Lock()
	delete(b.bindings, id)
	b.bindingsMu.Unlock()
}

// Close stops new admissions on all Bindings, drains or cancels in-flight calls within ctx,
// releases guardian Monitors, stops the Outbound connector Actor and guardian Actors,
// and closes the Bridge's Erlang Client without stopping the borrowed Nodes (§4.2).
func (b *Bridge) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.closeMu.Lock()
	if b.closeDone {
		err := b.closeErr
		b.closeMu.Unlock()
		return err
	}
	b.closed.Store(true)

	b.bindingsMu.RLock()
	bindings := make([]*Binding, 0, len(b.bindings))
	for _, bd := range b.bindings {
		bindings = append(bindings, bd)
	}
	b.bindingsMu.RUnlock()

	var firstErr error
	for _, bd := range bindings {
		if err := bd.closeInternal(ctx, bd.cfg.stopActor); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	b.lifeCancel()

	// Stop outbound actor and guardian actors if taoNode is still running.
	select {
	case <-b.taoNode.Done():
	default:
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = b.taoNode.StopActor(stopCtx, b.outboundPID)
		b.watchMu.Lock()
		guardians := append([]*guardianGroup(nil), b.guardians...)
		b.guardians = nil
		b.watches = make(map[actor.PID]*targetWatch)
		b.refToTarget = make(map[actor.MonitorRef]actor.PID)
		b.watchMu.Unlock()
		for _, g := range guardians {
			_ = b.taoNode.StopActor(stopCtx, g.pid)
		}
		stopCancel()
	}

	b.subWG.Wait()
	_ = b.client.Close()

	b.peerScopeMu.Lock()
	for k, sc := range b.peerScopes {
		sc.cancel()
		delete(b.peerScopes, k)
	}
	b.peerScopeMu.Unlock()

	b.compReservedWG.Wait()
	close(b.compStopCh)
	b.compWG.Wait()
	close(b.doneCh)

	b.closeErr = firstErr
	b.closeDone = true
	b.closeMu.Unlock()
	return firstErr
}

func (b *Bridge) peerContext(peer string, gen uint64, sessDone <-chan struct{}) context.Context {
	key := peerGenKey{peer: peer, gen: gen}
	b.peerScopeMu.Lock()
	if sc, ok := b.peerScopes[key]; ok {
		b.peerScopeMu.Unlock()
		return sc.ctx
	}
	if b.closed.Load() {
		b.peerScopeMu.Unlock()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	ctx, cancel := context.WithCancel(b.lifeCtx)
	sc := &peerGenScope{ctx: ctx, cancel: cancel}
	b.peerScopes[key] = sc
	b.peerScopeMu.Unlock()

	go func() {
		select {
		case <-sessDone:
		case <-b.lifeCtx.Done():
		}
		cancel()
		b.peerScopeMu.Lock()
		if b.peerScopes[key] == sc {
			delete(b.peerScopes, key)
		}
		b.peerScopeMu.Unlock()
	}()
	return ctx
}

func (b *Bridge) runCompletionShard(shard *completionShard) {
	defer b.compWG.Done()
	for {
		select {
		case ev := <-shard.ch:
			b.processCompletion(ev)
			shard.releaseReservation()
		case <-b.compStopCh:
			for {
				select {
				case ev := <-shard.ch:
					b.processCompletion(ev)
					shard.releaseReservation()
				default:
					return
				}
			}
		}
	}
}

func (b *Bridge) processCompletion(ev completionEvent) {
	bd := ev.binding
	switch ev.kind {
	case completionDirectError:
		replyTerm, encErr := bd.schema.EncodeReply(nil, ev.err)
		if encErr != nil || replyTerm == nil {
			replyTerm = etf.Tuple{
				etf.Atom("error"),
				erlang.TaoReason("encode_error", errText(encErr)),
			}
		}
		bd.sendPreExecErrorReply(ev.replyTo, replyTerm)
		if ev.release != nil {
			ev.release()
		}
		bd.finishInFlight(0)

	case completionBeginCall:
		entry := ev.entry
		comp := ev.comp
		switch comp.Origin {
		case actor.Reply:
			// Includes both normal Reply(value) and business ReplyError(err) (§9.2).
			replyTerm, encErr := bd.schema.EncodeReply(comp.Value, comp.Err)
			if encErr != nil || replyTerm == nil {
				replyTerm = etf.Tuple{
					etf.Atom("error"),
					erlang.TaoReason("encode_error", errText(encErr)),
				}
			}
			bd.sendPostExecReply(entry.replyTo, replyTerm)

		case actor.NoReply:
			bd.sendPostExecReply(entry.replyTo, etf.Tuple{
				etf.Atom("error"),
				erlang.TaoReason("no_reply", "actor handler returned without reply"),
			})

		case actor.Canceled:
			if entry.hasInboundTimeout &&
				!entry.aborted.Load() &&
				(entry.baseCtx == nil || entry.baseCtx.Err() == nil) &&
				(errors.Is(comp.Err, context.DeadlineExceeded) || errors.Is(context.Cause(entry.ctx), ErrInboundTimeout)) {
				bd.sendPostExecReply(entry.replyTo, etf.Tuple{
					etf.Atom("error"),
					erlang.TaoReason("timeout", "inbound call timeout exceeded"),
				})
			}

		case actor.TargetExit:
			// Target Actor stopped/crashed/handed off: NEVER pass to business EncodeReply;
			// watcher received/will receive terminal DOWN once pre-stop replies drain (§9.2, §9.3).
		}

		if entry.cancel != nil {
			entry.cancel()
		}
		if entry.releaseSlot != nil {
			entry.releaseSlot()
		}
		bd.finishInFlight(entry.id)
	}
}

func errText(err error) string {
	if err == nil {
		return "nil reply term"
	}
	return err.Error()
}

func overloadedErrorTuple() etf.Tuple {
	return etf.Tuple{
		etf.Atom("error"),
		etf.Tuple{etf.Atom("tao"), etf.Atom("overloaded")},
	}
}

func isErlangStage(err error, stage erlang.Stage) bool {
	var erlErr *erlang.Error
	return errors.As(err, &erlErr) && erlErr.Stage == stage
}

// sendPreExecErrorReply delivers an error reply for a call rejected before Actor execution.
// If the schema-encoded reply fails ETF encoding, a compact encode_error tuple is tried; if
// send quota prevents delivering even a compact error reply, the Binding is terminated (or
// the peer session disconnected if unmonitored) so callers never block forever (§11.1).
func (bd *Binding) sendPreExecErrorReply(replyTo erlang.ReplyTarget, replyTerm etf.Term) {
	err := bd.ep.SendReply(replyTo, replyTerm)
	if err == nil || isErlangStage(err, erlang.StageSend) {
		return
	}
	if isErlangStage(err, erlang.StageEncode) {
		fallback := etf.Tuple{
			etf.Atom("error"),
			erlang.TaoReason("encode_error", err.Error()),
		}
		err2 := bd.ep.SendReply(replyTo, fallback)
		if err2 == nil || isErlangStage(err2, erlang.StageSend) {
			return
		}
		code := "encode_error"
		if errors.Is(err2, erlang.ErrCapacity) {
			code = "overloaded"
		}
		bd.failBindingOnReplyError(replyTo, code, err2)
		return
	}
	if err2 := bd.ep.SendReply(replyTo, overloadedErrorTuple()); err2 == nil || isErlangStage(err2, erlang.StageSend) {
		return
	} else {
		bd.failBindingOnReplyError(replyTo, "overloaded", err2)
	}
}

// sendPostExecReply delivers a reply after the call has been admitted to the Actor.
// Encode errors fall back to a compact encode_error reply; send quota exhaustion immediately
// terminates the Binding (and disconnects unmonitored callers) rather than forging a
// pre-execution business error tuple (§11.1, §11.2).
func (bd *Binding) sendPostExecReply(replyTo erlang.ReplyTarget, replyTerm etf.Term) {
	err := bd.ep.SendReply(replyTo, replyTerm)
	if err == nil || isErlangStage(err, erlang.StageSend) {
		return
	}
	if isErlangStage(err, erlang.StageEncode) {
		fallback := etf.Tuple{
			etf.Atom("error"),
			erlang.TaoReason("encode_error", err.Error()),
		}
		err2 := bd.ep.SendReply(replyTo, fallback)
		if err2 == nil || isErlangStage(err2, erlang.StageSend) {
			return
		}
		code := "encode_error"
		if errors.Is(err2, erlang.ErrCapacity) {
			code = "overloaded"
		}
		bd.failBindingOnReplyError(replyTo, code, err2)
		return
	}
	bd.failBindingOnReplyError(replyTo, "overloaded", err)
}

func (bd *Binding) failBindingOnReplyError(replyTo erlang.ReplyTarget, code string, cause error) {
	bd.ep.BeginShutdown()

	reason := erlang.TaoReason(code, errText(cause))
	var firstTerm bool
	var toAbort []*inboundPendingCall
	bd.mu.Lock()
	bd.closing = true
	if !bd.termReq {
		bd.termReq = true
		bd.termReason = reason
	}
	if !bd.terminated {
		bd.terminated = true
		firstTerm = true
	}
	for _, p := range bd.pending {
		toAbort = append(toAbort, p)
	}
	bd.mu.Unlock()

	if firstTerm {
		if code == "overloaded" {
			bd.bridge.overloadTerminations.Add(1)
		}
		bd.bridge.detachBindingWatch(bd)
	}
	for _, p := range toAbort {
		p.abort()
	}
	bd.ep.TerminateForFailedReply(replyTo, reason, cause)
	bd.markDone()
	if firstTerm && bd.cfg.stopActor {
		go bd.stopBoundActor()
	}
}

// Binding represents one exported generation mapping an Erlang PID (and optional registered name)
// to a concrete local Tao Actor PID (§4.3, §9.3).
type Binding struct {
	id        uint64
	bridge    *Bridge
	name      string
	localPID  actor.PID
	schema    Schema
	cfg       exportConfig
	compShard *completionShard
	ep        *erlang.Endpoint
	published atomic.Bool

	nextCallID atomic.Uint64
	mu         sync.Mutex
	closing    bool
	terminated bool
	termReq    bool
	termReason etf.Term
	inFlight   int
	pending    map[uint64]*inboundPendingCall

	doneOnce sync.Once
	doneCh   chan struct{}
}

// PID returns the virtual Erlang PID assigned to this Binding generation.
func (bd *Binding) PID() erlang.PID { return bd.ep.PID() }

// Name returns the configured Erlang registered name (or "" if anonymous).
func (bd *Binding) Name() string { return bd.name }

// LocalPID returns the bound local Tao Actor PID.
func (bd *Binding) LocalPID() actor.PID { return bd.localPID }

// Done returns a channel closed when this Binding has finished draining and terminated.
func (bd *Binding) Done() <-chan struct{} { return bd.doneCh }

// Close detaches the registered name, stops new inbound deliveries, waits up to ctx for
// already-admitted calls to complete, emits shutdown DOWN/EXIT signals, and removes the
// guardian Monitor (§10.4). If ctx expires before in-flight calls drain, remaining calls
// are canceled and ctx.Err() is returned.
func (bd *Binding) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return bd.closeInternal(ctx, bd.cfg.stopActor)
}

func (bd *Binding) closeInternal(ctx context.Context, stopLocalActor bool) error {
	bd.bridge.detachBindingWatch(bd)
	bd.requestTerminate(etf.Atom("shutdown"), false)

	var drainErr error
	select {
	case <-bd.doneCh:
	case <-ctx.Done():
		drainErr = ctx.Err()
		bd.abortAllPending()
		bd.forceTerminate(etf.Atom("shutdown"))
	}

	if stopLocalActor {
		bd.stopBoundActor()
	}
	return drainErr
}

func (bd *Binding) stopBoundActor() {
	select {
	case <-bd.bridge.taoNode.Done():
		return
	default:
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = bd.bridge.taoNode.StopActor(stopCtx, bd.localPID)
}

func (bd *Binding) onDeliver(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
	switch msg.Kind {
	case erlang.InboundExit:
		bd.deliverOneWay(Exit{
			From:     msg.From,
			Reason:   msg.Term,
			FromLink: msg.ExitFromLink,
		})

	case erlang.InboundGenCast:
		decoded, err := bd.schema.DecodeCast(msg.Term)
		if err != nil {
			bd.bridge.decodeErrors.Add(1)
			return
		}
		bd.deliverOneWay(bd.maybeWrapSource(msg, decoded))

	case erlang.InboundInfo:
		decoded, err := bd.schema.DecodeInfo(msg.Term)
		if err != nil {
			bd.bridge.decodeErrors.Add(1)
			return
		}
		bd.deliverOneWay(bd.maybeWrapSource(msg, decoded))

	case erlang.InboundGenCall:
		bd.admitInboundCall(ep, msg)
	}
}

func (bd *Binding) maybeWrapSource(msg erlang.InboundDelivery, decoded any) any {
	if !bd.cfg.sourceEnvelope {
		return decoded
	}
	return Inbound{
		Peer:    msg.Peer,
		From:    msg.From,
		HasFrom: msg.HasFrom,
		Message: decoded,
	}
}

func (bd *Binding) deliverOneWay(payload any) {
	bd.mu.Lock()
	closing := bd.closing
	bd.mu.Unlock()
	if closing {
		return
	}

	err := bd.bridge.taoNode.Send(bd.localPID, payload)
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, actor.ErrMailboxFull), errors.Is(err, actor.ErrCapacity):
		// Bounded one-way cast/info overload must NOT silently drop and pretend healthy (§11.1).
		bd.bridge.overloadTerminations.Add(1)
		bd.bridge.detachBindingWatch(bd)
		bd.requestTerminate(erlang.TaoReason("overloaded", err.Error()), false)
		if bd.cfg.stopActor {
			go bd.stopBoundActor()
		}
	case errors.Is(err, actor.ErrMigrating):
		bd.bridge.detachBindingWatch(bd)
		bd.requestTerminate(erlang.TaoReason("migrating", err.Error()), false)
	}
}

func (bd *Binding) admitInboundCall(ep *erlang.Endpoint, msg erlang.InboundDelivery) {
	callID := bd.nextCallID.Add(1)

	bd.mu.Lock()
	if bd.closing {
		bd.mu.Unlock()
		return
	}
	bd.inFlight++
	bd.mu.Unlock()
	bd.bridge.inboundInFlight.Add(1)

	sessDone, releaseSlot, err := ep.ReserveInboundCall(msg.Peer, msg.PeerGen)
	if err != nil {
		if errors.Is(err, erlang.ErrCapacity) {
			bd.bridge.overloadReplies.Add(1)
			if bd.compShard.tryReserve() {
				bd.compShard.publishReserved(completionEvent{
					kind:    completionDirectError,
					binding: bd,
					replyTo: msg.ReplyTo,
					err:     ErrOverloaded,
				})
				return
			}
			bd.sendPreExecErrorReply(msg.ReplyTo, overloadedErrorTuple())
		}
		bd.finishInFlight(0)
		return
	}

	if !bd.compShard.tryReserve() {
		bd.bridge.overloadReplies.Add(1)
		bd.sendPreExecErrorReply(msg.ReplyTo, overloadedErrorTuple())
		releaseSlot()
		bd.finishInFlight(0)
		return
	}

	decoded, decErr := bd.schema.DecodeCall(msg.Term)
	if decErr != nil {
		bd.bridge.decodeErrors.Add(1)
		bd.compShard.publishReserved(completionEvent{
			kind:    completionDirectError,
			binding: bd,
			replyTo: msg.ReplyTo,
			err:     decErr,
			release: releaseSlot,
		})
		return
	}

	baseCtx := bd.bridge.peerContext(msg.Peer, msg.PeerGen, sessDone)
	var callCtx context.Context
	var cancel context.CancelFunc
	if bd.cfg.inboundTimeout > 0 {
		callCtx, cancel = context.WithTimeoutCause(baseCtx, bd.cfg.inboundTimeout, ErrInboundTimeout)
	} else {
		callCtx, cancel = context.WithCancel(baseCtx)
	}

	// Pre-build pending entry BEFORE BeginCall because completion may fire before BeginCall returns (§9.2).
	entry := &inboundPendingCall{
		id:                callID,
		binding:           bd,
		replyTo:           msg.ReplyTo,
		baseCtx:           baseCtx,
		ctx:               callCtx,
		cancel:            cancel,
		hasInboundTimeout: bd.cfg.inboundTimeout > 0,
		releaseSlot:       releaseSlot,
	}

	bd.mu.Lock()
	if bd.closing {
		bd.mu.Unlock()
		cancel()
		bd.compShard.releaseReservation()
		releaseSlot()
		bd.finishInFlight(0)
		return
	}
	bd.pending[callID] = entry
	bd.mu.Unlock()

	handle, admitErr := bd.bridge.taoNode.BeginCall(
		callCtx,
		bd.localPID,
		bd.maybeWrapSource(msg, decoded),
		func(comp actor.CallCompletion) {
			if !entry.completed.CompareAndSwap(false, true) {
				return
			}
			// Non-blocking publish to pre-reserved shard slot outside Actor runtime locks (§9.2).
			bd.compShard.publishReserved(completionEvent{
				kind:    completionBeginCall,
				binding: bd,
				entry:   entry,
				comp:    comp,
			})
		},
	)
	if admitErr != nil {
		// BeginCall guarantees complete callback is NEVER invoked on admission error (§9.2).
		bd.mu.Lock()
		delete(bd.pending, callID)
		bd.mu.Unlock()
		cancel()

		switch {
		case errors.Is(admitErr, actor.ErrMailboxFull), errors.Is(admitErr, actor.ErrCapacity):
			bd.bridge.overloadReplies.Add(1)
			bd.compShard.publishReserved(completionEvent{
				kind:    completionDirectError,
				binding: bd,
				replyTo: msg.ReplyTo,
				err:     ErrOverloaded,
				release: releaseSlot,
			})
			return
		case errors.Is(admitErr, actor.ErrMigrating):
			bd.compShard.publishReserved(completionEvent{
				kind:    completionDirectError,
				binding: bd,
				replyTo: msg.ReplyTo,
				err:     admitErr,
				release: releaseSlot,
			})
			return
		default:
			// Target already stopped/no_process: do not send a bogus business reply;
			// endpoint termination delivers native DOWN (§8.3, §9.3).
			bd.compShard.releaseReservation()
			releaseSlot()
			bd.finishInFlight(0)
			return
		}
	}

	entry.bindHandle(handle)
}

func (bd *Binding) finishInFlight(callID uint64) {
	bd.bridge.inboundInFlight.Add(-1)

	var doTerm bool
	var reason etf.Term
	bd.mu.Lock()
	if callID != 0 {
		delete(bd.pending, callID)
	}
	if bd.inFlight > 0 {
		bd.inFlight--
	}
	if bd.inFlight == 0 && bd.termReq && !bd.terminated {
		bd.terminated = true
		doTerm = true
		reason = bd.termReason
	}
	bd.mu.Unlock()

	if doTerm {
		bd.ep.Terminate(reason)
		bd.markDone()
	}
}

func (bd *Binding) requestTerminate(reason etf.Term, cancelActive bool) {
	bd.ep.BeginShutdown()

	var doTerm bool
	var toAbort []*inboundPendingCall
	bd.mu.Lock()
	bd.closing = true
	if !bd.termReq {
		bd.termReq = true
		bd.termReason = reason
	}
	if cancelActive {
		for _, p := range bd.pending {
			toAbort = append(toAbort, p)
		}
	}
	if bd.inFlight == 0 && !bd.terminated {
		bd.terminated = true
		doTerm = true
		reason = bd.termReason
	}
	bd.mu.Unlock()

	for _, p := range toAbort {
		p.abort()
	}
	if doTerm {
		bd.ep.Terminate(reason)
		bd.markDone()
	}
}

func (bd *Binding) abortAllPending() {
	bd.mu.Lock()
	toAbort := make([]*inboundPendingCall, 0, len(bd.pending))
	for _, p := range bd.pending {
		toAbort = append(toAbort, p)
	}
	bd.mu.Unlock()
	for _, p := range toAbort {
		p.abort()
	}
}

func (bd *Binding) forceTerminate(reason etf.Term) {
	var doTerm bool
	bd.mu.Lock()
	bd.closing = true
	if !bd.terminated {
		bd.terminated = true
		doTerm = true
	}
	bd.mu.Unlock()
	if doTerm {
		bd.ep.Terminate(reason)
		bd.markDone()
	}
}

func (bd *Binding) onEndpointTerminated(_ etf.Term, remoteExit bool) {
	bd.bridge.removeBindingRecord(bd.id)
	if remoteExit {
		// Remote exit (e.g. untrappable exit(Pid, kill)): detach guardian watch, cancel pending
		// bridge requests, and optionally request cooperative StopActor (§10.3).
		bd.bridge.detachBindingWatch(bd)
		bd.mu.Lock()
		bd.closing = true
		bd.terminated = true
		toAbort := make([]*inboundPendingCall, 0, len(bd.pending))
		for _, p := range bd.pending {
			toAbort = append(toAbort, p)
		}
		bd.mu.Unlock()
		for _, p := range toAbort {
			p.abort()
		}
		if bd.cfg.stopActor {
			go bd.stopBoundActor()
		}
	}
	bd.markDone()
}

func (bd *Binding) markDone() {
	bd.doneOnce.Do(func() {
		bd.bridge.removeBindingRecord(bd.id)
		close(bd.doneCh)
	})
}
