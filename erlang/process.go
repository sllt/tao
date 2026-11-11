package erlang

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/dist"
)

// InboundSignalKind classifies an inbound signal delivered to a registered Endpoint.
type InboundSignalKind uint8

const (
	InboundGenCall InboundSignalKind = iota + 1
	InboundGenCast
	InboundInfo
	InboundExit
)

// InboundDelivery carries one decoded, owned Erlang message or trappable exit signal.
type InboundDelivery struct {
	Kind         InboundSignalKind
	Peer         string
	PeerGen      uint64
	From         PID
	HasFrom      bool
	ReplyTo      ReplyTarget
	Term         etf.Term
	ExitFromLink bool
	WireBytes    int
	DecodedBytes int64
}

// DownEvent represents an asynchronous process DOWN notification observed by Client.Monitor.
type DownEvent struct {
	Ref     Ref
	Watched etf.Term // PID or {Name, Node} tuple matching Erlang's {'DOWN', Ref, process, Object, Reason}
	Reason  etf.Term
}

// ExitEvent represents a trappable exit signal received by a Client configured with TrapExit.
type ExitEvent struct {
	From     PID
	Reason   etf.Term
	FromLink bool
}

type linkRecord struct {
	peerGen  uint64
	active   bool
	unlinkID uint64
}

type inboundMonKey struct {
	peerGen uint64
	watcher PID
	ref     Ref
}

type inboundMonRecord struct {
	session *peerSession
	watcher PID
	watched etf.Term // preserved raw PID or Atom from MONITOR_P (§10.1)
	ref     Ref
}

type outboundMonRecord struct {
	ref     Ref
	peer    string
	peerGen uint64
	watched etf.Term // wire watched term (PID or Atom) for DEMONITOR_P
	object  etf.Term // user-facing object (PID or Tuple{Name, Node})
	ch      chan DownEvent
}

// EndpointOptions configures an exported virtual Erlang process on a Node.
type EndpointOptions struct {
	// Name optionally registers the endpoint under an Erlang atom name.
	Name string
	// TrapExit delivers trappable EXIT/EXIT2 signals as InboundExit instead of terminating the endpoint.
	// Explicit exit(Pid, kill) (EXIT2 with Reason == Atom("kill")) remains untrappable (§10.2–§10.3).
	TrapExit bool
	// AllowedPeers restricts access to this endpoint to the listed remote node names when non-empty.
	AllowedPeers []string
	// OnDeliver runs synchronously on the peer's single ordered inbound loop.
	// It must only perform bounded value decoding and local mailbox admission without blocking.
	OnDeliver func(ep *Endpoint, msg InboundDelivery)
	// OnTerminate runs outside locks when the endpoint terminates due to a remote exit signal
	// or explicit Terminate call.
	OnTerminate func(reason etf.Term, remoteExit bool)
}

// Endpoint is a concrete virtual Erlang process exported by a Node (used directly or via erlangactor).
type Endpoint struct {
	node      *Node
	pid       PID
	name      etf.Atom
	trapExit  bool
	allowPeer map[string]struct{}
	onDeliver func(ep *Endpoint, msg InboundDelivery)
	onTerm    func(reason etf.Term, remoteExit bool)

	mu         sync.Mutex
	draining   bool
	terminated bool
	relations  int
	monitors   map[inboundMonKey]inboundMonRecord
	links      map[PID]linkRecord
}

func (ep *Endpoint) PID() PID { return ep.pid }

func (ep *Endpoint) Name() etf.Atom {
	ep.mu.Lock()
	name := ep.name
	ep.mu.Unlock()
	return name
}

// PublishName publishes an initially unnamed Endpoint under an Erlang registered atom name (§9.3).
func (ep *Endpoint) PublishName(name string) error {
	if name == "" {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: etf.ErrInvalidTerm}
	}
	nameAtom := etf.Atom(name)
	if err := etf.ValidateAtom(nameAtom); err != nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: err}
	}
	if _, reserved := reservedNames[nameAtom]; reserved {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrReservedName}
	}

	ep.mu.Lock()
	if ep.draining || ep.terminated {
		ep.mu.Unlock()
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrEndpointClosed}
	}
	if ep.name != "" {
		ep.mu.Unlock()
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrDuplicateExport}
	}
	ep.name = nameAtom
	ep.mu.Unlock()

	if err := ep.node.publishEndpointName(ep, nameAtom); err != nil {
		ep.mu.Lock()
		if ep.name == nameAtom {
			ep.name = ""
		}
		ep.mu.Unlock()
		return err
	}

	ep.mu.Lock()
	stopped := ep.draining || ep.terminated
	ep.mu.Unlock()
	if stopped {
		ep.node.detachEndpointName(ep)
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrEndpointClosed}
	}
	return nil
}

func (ep *Endpoint) allowsPeer(peer string) bool {
	if len(ep.allowPeer) == 0 {
		return true
	}
	_, ok := ep.allowPeer[peer]
	return ok
}

// BeginShutdown stops new inbound message deliveries and detaches the registered name while
// keeping monitors/links intact until Terminate is called (enabling Reply-before-DOWN draining, §9.3).
func (ep *Endpoint) BeginShutdown() {
	ep.node.detachEndpointName(ep)
	ep.mu.Lock()
	ep.draining = true
	ep.mu.Unlock()
}

// Terminate idempotently closes the endpoint, detaches its name, and emits terminal
// MONITOR_P_EXIT and active link EXIT signals with reason.
func (ep *Endpoint) Terminate(reason etf.Term) {
	if reason != nil {
		if owned, _, err := ep.node.preflightOutboundTerm(reason); err == nil {
			reason = owned
		} else {
			reason = TaoReason("invalid_exit_reason", err.Error())
		}
	}
	ep.terminateInternal(reason, false)
}

// TerminateForFailedReply terminates ep with reason and ensures the caller identified by rt
// cannot remain blocked indefinitely when a call reply could not be delivered: if the call did
// not carry an inbound monitor on ep, the authorized peer session is closed (§11.1, §11.2).
func (ep *Endpoint) TerminateForFailedReply(rt ReplyTarget, reason etf.Term, disconnectErr error) {
	ep.Terminate(reason)
	if rt.callerMonitored {
		return
	}
	if sess := ep.node.lookupAuthorizedReplySession(string(rt.PeerNode())); sess != nil {
		if disconnectErr == nil {
			disconnectErr = ErrCapacity
		}
		sess.closeWithError(disconnectErr)
	}
}

func (ep *Endpoint) hasInboundCallMonitor(peerGen uint64, rt ReplyTarget) bool {
	callRef, ok := rt.callMonitorRef()
	if !ok || rt.To.Node == "" {
		return false
	}
	key := inboundMonKey{
		peerGen: peerGen,
		watcher: rt.To,
		ref:     callRef,
	}
	ep.mu.Lock()
	_, exists := ep.monitors[key]
	ep.mu.Unlock()
	return exists
}

func (ep *Endpoint) terminateInternal(reason etf.Term, remoteExit bool) {
	if reason == nil {
		reason = etf.Atom("normal")
	}
	ep.node.detachEndpointName(ep)

	ep.mu.Lock()
	if ep.terminated {
		ep.mu.Unlock()
		return
	}
	ep.draining = true
	ep.terminated = true

	var released int
	var monSnapshot []struct {
		key  inboundMonKey
		sess *peerSession
	}
	for k, mon := range ep.monitors {
		released++
		if mon.session != nil {
			monSnapshot = append(monSnapshot, struct {
				key  inboundMonKey
				sess *peerSession
			}{key: k, sess: mon.session})
			if !mon.session.isClosed() {
				mon.session.enqueueControl(dist.ControlMessage{
					Op:     dist.OpMonitorPExit,
					ToProc: mon.watched,
					ToPID:  mon.watcher,
					Ref:    mon.ref,
					Reason: reason,
				})
			}
		}
	}
	ep.monitors = nil

	for remotePID, lnk := range ep.links {
		released++
		if lnk.active {
			if sess := ep.node.lookupActiveSession(string(remotePID.Node)); sess != nil && sess.gen == lnk.peerGen {
				sess.enqueueControl(dist.ControlMessage{
					Op:      dist.OpExit,
					FromPID: ep.pid,
					HasFrom: true,
					ToPID:   remotePID,
					Reason:  reason,
				})
			}
		}
	}
	ep.links = nil
	ep.relations = 0
	ep.mu.Unlock()

	for _, item := range monSnapshot {
		item.sess.untrackInboundMonitor(item.key)
	}
	ep.node.unregisterEndpoint(ep, released)
	if ep.onTerm != nil {
		ep.onTerm(reason, remoteExit)
	}
}

// ReserveInboundCall reserves one inbound pending Call slot on both Node and peer session (§11.3),
// returning the peer session's lossless done channel alongside the slot release callback.
func (ep *Endpoint) ReserveInboundCall(peer string, peerGen uint64) (<-chan struct{}, func(), error) {
	sess := ep.node.lookupActiveSession(peer)
	if sess == nil || sess.gen != peerGen {
		return nil, nil, errSessionClosed
	}
	if !ep.node.tryAcquireInboundCall(sess) {
		return nil, nil, ErrCapacity
	}
	var once sync.Once
	return sess.done, func() {
		once.Do(func() {
			ep.node.releaseInboundCall(sess)
		})
	}, nil
}

// SendReply sends {rt.Tag, reply} from ep.PID() using an existing authorized peer session.
// It never auto-dials arbitrary unconfigured third-party nodes discovered inside payload terms (§8.2).
func (ep *Endpoint) SendReply(rt ReplyTarget, reply etf.Term) error {
	destNode := string(rt.PeerNode())
	sess := ep.node.lookupAuthorizedReplySession(destNode)
	if sess == nil {
		return &Error{Stage: StageSend, Outcome: OutcomeNotSent, Peer: destNode, Err: errSessionClosed}
	}
	payload := etf.Tuple{rt.Tag, reply}
	if rt.UseAlias {
		if sess.negotiatedFlags&dist.DFlagAlias == 0 {
			return nil
		}
		return sess.enqueueData(dist.ControlMessage{
			Op:      dist.OpAliasSend,
			FromPID: ep.pid,
			HasFrom: true,
			Ref:     rt.Alias,
		}, payload)
	}
	return sess.enqueueData(dist.ControlMessage{
		Op:      dist.OpSendSender,
		FromPID: ep.pid,
		HasFrom: true,
		ToPID:   rt.To,
	}, payload)
}

func (ep *Endpoint) onPeerDown(peerName string, peerGen uint64) {
	var linkExit bool
	ep.mu.Lock()
	if ep.terminated {
		ep.mu.Unlock()
		return
	}
	var released int
	for k := range ep.monitors {
		if k.peerGen == peerGen && string(k.watcher.Node) == peerName {
			delete(ep.monitors, k)
			ep.relations--
			released++
		}
	}
	var trapped []PID
	for remotePID, lnk := range ep.links {
		if lnk.peerGen == peerGen && string(remotePID.Node) == peerName {
			delete(ep.links, remotePID)
			ep.relations--
			released++
			if lnk.active {
				if ep.trapExit {
					trapped = append(trapped, remotePID)
				} else {
					linkExit = true
				}
			}
		}
	}
	ep.mu.Unlock()

	if released > 0 {
		ep.node.releaseRelations(released)
	}
	if linkExit {
		ep.terminateInternal(etf.Atom("noconnection"), true)
		return
	}
	if ep.onDeliver != nil {
		for _, rpid := range trapped {
			ep.onDeliver(ep, InboundDelivery{
				Kind:         InboundExit,
				Peer:         peerName,
				PeerGen:      peerGen,
				From:         rpid,
				HasFrom:      true,
				Term:         etf.Atom("noconnection"),
				ExitFromLink: true,
			})
		}
	}
}

type pendingCall struct {
	ref           Ref
	tag           etf.Term
	watched       etf.Term
	peer          string
	peerGen       uint64
	session       *peerSession
	bundle        *callBundleState
	onSettle      func(etf.Term, error)
	cancelTimeout context.CancelFunc
	stopWatch     atomic.Pointer[func() bool]
	settled       atomic.Bool
	reply         etf.Term
	err           error
	done          chan struct{}
}

func (pc *pendingCall) attachStopWatch(stop func() bool) {
	pc.stopWatch.Store(&stop)
	if pc.settled.Load() {
		stop()
	}
}

func (pc *pendingCall) settle(reply etf.Term, err error) bool {
	if !pc.settled.CompareAndSwap(false, true) {
		return false
	}
	pc.reply = reply
	pc.err = err
	if pc.done != nil {
		close(pc.done)
	}
	if stopPtr := pc.stopWatch.Load(); stopPtr != nil {
		(*stopPtr)()
	}
	if pc.cancelTimeout != nil {
		pc.cancelTimeout()
	}
	if pc.onSettle != nil {
		pc.onSettle(reply, err)
	}
	return true
}

// ClientOptions configures a standalone virtual Erlang Client process.
type ClientOptions struct {
	TrapExit bool
}

// Client is a virtual Erlang process capable of concurrent outbound Call, Cast, Send,
// Monitor, and Link operations (§4.1, §8.2).
type Client struct {
	node     *Node
	pid      PID
	trapExit bool
	exitCh   chan ExitEvent

	mu         sync.Mutex
	closed     bool
	closeCause etf.Term
	relations  int
	calls      map[Ref]*pendingCall
	monitors   map[Ref]*outboundMonRecord
	inMonitors map[inboundMonKey]inboundMonRecord
	links      map[PID]linkRecord
}

func (c *Client) PID() PID { return c.pid }

// Exits returns a bounded channel of trappable ExitEvents when ClientOptions.TrapExit is true.
func (c *Client) Exits() <-chan ExitEvent { return c.exitCh }

// Close terminates the virtual Client process, cancels pending calls, revokes aliases,
// sends DEMONITOR_P for active outbound monitors, and emits exit/DOWN for linked/watching peers.
func (c *Client) Close() error {
	c.terminate(etf.Atom("normal"), false)
	return nil
}

func (c *Client) terminate(reason etf.Term, remoteExit bool) {
	if reason == nil {
		reason = etf.Atom("normal")
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.closeCause = reason

	calls := make([]*pendingCall, 0, len(c.calls))
	for _, pc := range c.calls {
		calls = append(calls, pc)
	}
	c.calls = nil

	mons := make([]*outboundMonRecord, 0, len(c.monitors))
	for _, m := range c.monitors {
		mons = append(mons, m)
	}
	c.monitors = nil

	var released int
	released += len(mons)
	var inMonSnapshot []struct {
		key  inboundMonKey
		sess *peerSession
	}
	for k, im := range c.inMonitors {
		released++
		if im.session != nil {
			inMonSnapshot = append(inMonSnapshot, struct {
				key  inboundMonKey
				sess *peerSession
			}{key: k, sess: im.session})
			if !im.session.isClosed() {
				im.session.enqueueControl(dist.ControlMessage{
					Op:     dist.OpMonitorPExit,
					ToProc: im.watched,
					ToPID:  im.watcher,
					Ref:    im.ref,
					Reason: reason,
				})
			}
		}
	}
	c.inMonitors = nil

	for remotePID, lnk := range c.links {
		released++
		if lnk.active {
			if sess := c.node.lookupActiveSession(string(remotePID.Node)); sess != nil && sess.gen == lnk.peerGen {
				if remoteExit && reason == etf.Atom("normal") {
					// No-op
				} else {
					sess.enqueueControl(dist.ControlMessage{
						Op:      dist.OpExit,
						FromPID: c.pid,
						HasFrom: true,
						ToPID:   remotePID,
						Reason:  reason,
					})
				}
			}
		}
	}
	c.links = nil
	c.relations = 0
	c.mu.Unlock()

	for _, item := range inMonSnapshot {
		item.sess.untrackInboundMonitor(item.key)
	}

	for _, pc := range calls {
		c.node.revokeAlias(pc.ref)
		outcome := OutcomeUnknown
		if pc.bundle.tryAbort() {
			outcome = OutcomeNotSent
		} else {
			c.node.unknownOutcomes.Add(1)
			if pc.session != nil && !pc.session.isClosed() {
				pc.session.enqueueControl(dist.ControlMessage{
					Op:      dist.OpDemonitorP,
					FromPID: c.pid,
					HasFrom: true,
					ToProc:  pc.watched,
					Ref:     pc.ref,
				})
			}
		}
		if pc.settle(nil, &Error{
			Stage:   StageWait,
			Outcome: outcome,
			Peer:    pc.peer,
			Reason:  reason,
			Err:     ErrClientClosed,
		}) {
			c.node.releaseOutboundCall(pc.session)
		}
	}

	for _, m := range mons {
		if sess := c.node.lookupActiveSession(m.peer); sess != nil && sess.gen == m.peerGen {
			sess.enqueueControl(dist.ControlMessage{
				Op:      dist.OpDemonitorP,
				FromPID: c.pid,
				HasFrom: true,
				ToProc:  m.watched,
				Ref:     m.ref,
			})
		}
	}

	c.node.unregisterClient(c, released)
}

// Call executes an OTP '$gen_call' with ordered MONITOR_P + alias/tag routing (§8.2).
// If ctx carries no deadline, Config.DefaultCallTimeout (5s) is applied.
func (c *Client) Call(ctx context.Context, target Target, request etf.Term) (etf.Term, error) {
	callCtx, cancel, pc, err := c.admitCall(ctx, target, request, nil)
	if err != nil {
		return nil, err
	}
	if cancel != nil {
		defer cancel()
	}

	select {
	case <-pc.done:
		return pc.reply, pc.err
	case <-callCtx.Done():
		cancelErr := c.cancelAdmittedCall(pc, callCtx.Err())
		if cancelErr != nil {
			return nil, cancelErr
		}
		<-pc.done
		return pc.reply, pc.err
	}
}

// CallAsync synchronously admits an OTP '$gen_call' onto the target peer's ordered outbound
// stream and invokes complete(reply, err) exactly once outside locks when settled.
// If admission fails, complete is never invoked and the error is returned directly.
func (c *Client) CallAsync(
	ctx context.Context,
	target Target,
	request etf.Term,
	complete func(etf.Term, error),
) error {
	if complete == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil completion callback")}
	}
	callCtx, _, pc, err := c.admitCall(ctx, target, request, complete)
	if err != nil {
		return err
	}
	if !pc.settled.Load() && callCtx.Done() != nil {
		stop := context.AfterFunc(callCtx, func() {
			_ = c.cancelAdmittedCall(pc, callCtx.Err())
		})
		pc.attachStopWatch(stop)
	}
	return nil
}

func (c *Client) admitCall(
	ctx context.Context,
	target Target,
	request etf.Term,
	onSettle func(etf.Term, error),
) (context.Context, context.CancelFunc, *pendingCall, error) {
	if ctx == nil {
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	if err := target.Validate(); err != nil {
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: err}
	}
	if request == nil {
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: etf.ErrInvalidTerm}
	}

	var cancel context.CancelFunc
	if _, hasDL := ctx.Deadline(); !hasDL {
		ctx, cancel = context.WithTimeout(ctx, c.node.defaultCallTimeout)
	}
	if err := ctx.Err(); err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: err}
	}

	peerName := string(target.Node())
	if peerName == c.node.cfg.Name {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeUnsupported, Peer: peerName, Err: errors.New("erlang: local self-node Call is unsupported; use remote peer")}
	}

	sess, err := c.node.ensureSession(ctx, peerName)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, err
	}

	if !c.node.tryAcquireOutboundCall(sess) {
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrCapacity}
	}

	mref, err := c.node.ids.nextRef()
	if err != nil {
		c.node.releaseOutboundCall(sess)
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: err}
	}

	var watchedTerm etf.Term
	var callCtrl dist.ControlMessage
	if target.IsPID() {
		watchedTerm = target.PID()
		callCtrl = dist.ControlMessage{
			Op:      dist.OpSendSender,
			FromPID: c.pid,
			HasFrom: true,
			ToPID:   target.PID(),
		}
	} else {
		if sess.negotiatedFlags&dist.DFlagDistMonitorName == 0 {
			c.node.releaseOutboundCall(sess)
			if cancel != nil {
				cancel()
			}
			return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeUnsupported, Peer: peerName, Err: errors.New("peer does not support DFLAG_DIST_MONITOR_NAME")}
		}
		watchedTerm = target.Name()
		callCtrl = dist.ControlMessage{
			Op:      dist.OpRegSend,
			FromPID: c.pid,
			HasFrom: true,
			ToName:  target.Name(),
		}
	}

	var tag etf.Term = mref
	useAlias := sess.negotiatedFlags&dist.DFlagAlias != 0
	if useAlias {
		tag = BuildAliasTag(mref)
	}

	bundle := &callBundleState{}
	var doneCh chan struct{}
	var asyncCancel context.CancelFunc
	if onSettle == nil {
		doneCh = make(chan struct{})
	} else {
		asyncCancel = cancel
	}
	pc := &pendingCall{
		ref:           mref,
		tag:           tag,
		watched:       watchedTerm,
		peer:          peerName,
		peerGen:       sess.gen,
		session:       sess,
		bundle:        bundle,
		onSettle:      onSettle,
		cancelTimeout: asyncCancel,
		done:          doneCh,
	}

	monCtrl := dist.ControlMessage{
		Op:      dist.OpMonitorP,
		FromPID: c.pid,
		HasFrom: true,
		ToProc:  watchedTerm,
		Ref:     mref,
	}
	envelope := etf.Tuple{
		etf.Atom("$gen_call"),
		etf.Tuple{c.pid, tag},
		request,
	}
	ownedEnv, est, err := c.node.preflightOutboundTerm(envelope)
	if err != nil {
		c.node.releaseOutboundCall(sess)
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, err
	}

	if useAlias {
		c.node.registerAlias(mref, c, pc)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if useAlias {
			c.node.revokeAlias(mref)
		}
		c.node.releaseOutboundCall(sess)
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrClientClosed}
	}
	if err := sess.enqueueCallBundlePrevalidated(monCtrl, callCtrl, ownedEnv, est, bundle); err != nil {
		c.mu.Unlock()
		if useAlias {
			c.node.revokeAlias(mref)
		}
		c.node.releaseOutboundCall(sess)
		if cancel != nil {
			cancel()
		}
		return nil, nil, nil, err
	}
	c.calls[mref] = pc
	c.mu.Unlock()

	return ctx, cancel, pc, nil
}

func (c *Client) cancelAdmittedCall(pc *pendingCall, cause error) error {
	c.removePendingCall(pc.ref)
	c.node.revokeAlias(pc.ref)
	outcome := OutcomeUnknown
	if pc.bundle.tryAbort() {
		outcome = OutcomeNotSent
	} else {
		c.node.unknownOutcomes.Add(1)
		if pc.session != nil && !pc.session.isClosed() {
			pc.session.enqueueControl(dist.ControlMessage{
				Op:      dist.OpDemonitorP,
				FromPID: c.pid,
				HasFrom: true,
				ToProc:  pc.watched,
				Ref:     pc.ref,
			})
		}
	}
	cancelErr := &Error{
		Stage:   StageWait,
		Outcome: outcome,
		Peer:    pc.peer,
		Err:     cause,
	}
	if pc.settle(nil, cancelErr) {
		c.node.releaseOutboundCall(pc.session)
		return cancelErr
	}
	if pc.done != nil {
		<-pc.done
	}
	return nil
}

func (c *Client) removePendingCall(ref Ref) {
	c.mu.Lock()
	delete(c.calls, ref)
	c.mu.Unlock()
}

// Cast enqueues an asynchronous {'$gen_cast', request} message within local send quotas (§4.1).
func (c *Client) Cast(ctx context.Context, target Target, request etf.Term) error {
	if request == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: etf.ErrInvalidTerm}
	}
	return c.Send(ctx, target, etf.Tuple{etf.Atom("$gen_cast"), request})
}

// Send enqueues a raw Erlang '!' message to target within bounded local send quotas (§4.1).
func (c *Client) Send(ctx context.Context, target Target, message etf.Term) error {
	if ctx == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	if err := target.Validate(); err != nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: err}
	}
	if message == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: etf.ErrInvalidTerm}
	}
	if err := ctx.Err(); err != nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: err}
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: ErrClientClosed}
	}

	peerName := string(target.Node())
	sess, err := c.node.ensureSession(ctx, peerName)
	if err != nil {
		return err
	}

	var ctrl dist.ControlMessage
	if target.IsPID() {
		ctrl = dist.ControlMessage{
			Op:      dist.OpSendSender,
			FromPID: c.pid,
			HasFrom: true,
			ToPID:   target.PID(),
		}
	} else {
		ctrl = dist.ControlMessage{
			Op:      dist.OpRegSend,
			FromPID: c.pid,
			HasFrom: true,
			ToName:  target.Name(),
		}
	}
	return sess.enqueueData(ctrl, message)
}

// Monitor establishes a distributed process monitor on target (PID or Registered(Name, Node))
// and returns the monitor Ref and a 1-buffered terminal DownEvent channel (§10.1).
func (c *Client) Monitor(ctx context.Context, target Target) (Ref, <-chan DownEvent, error) {
	if ctx == nil {
		return Ref{}, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	if err := target.Validate(); err != nil {
		return Ref{}, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node()), Err: err}
	}
	peerName := string(target.Node())
	sess, err := c.node.ensureSession(ctx, peerName)
	if err != nil {
		return Ref{}, nil, err
	}

	var watched etf.Term
	var obj etf.Term
	if target.IsPID() {
		watched = target.PID()
		obj = target.PID()
	} else {
		if sess.negotiatedFlags&dist.DFlagDistMonitorName == 0 {
			return Ref{}, nil, &Error{Stage: StageValidate, Outcome: OutcomeUnsupported, Peer: peerName, Err: errors.New("peer does not support DFLAG_DIST_MONITOR_NAME")}
		}
		watched = target.Name()
		obj = etf.Tuple{target.Name(), target.Node()}
	}

	ref, err := c.node.ids.nextRef()
	if err != nil {
		return Ref{}, nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: err}
	}
	if !c.node.tryAcquireRelation() {
		return Ref{}, nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrCapacity}
	}

	ch := make(chan DownEvent, 1)
	rec := &outboundMonRecord{
		ref:     ref,
		peer:    peerName,
		peerGen: sess.gen,
		watched: watched,
		object:  obj,
		ch:      ch,
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.node.releaseRelations(1)
		return Ref{}, nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrClientClosed}
	}
	if c.relations >= c.node.limits.MaxProcessRelations {
		c.mu.Unlock()
		c.node.releaseRelations(1)
		return Ref{}, nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrCapacity}
	}
	c.relations++
	c.monitors[ref] = rec
	if !sess.enqueueControl(dist.ControlMessage{
		Op:      dist.OpMonitorP,
		FromPID: c.pid,
		HasFrom: true,
		ToProc:  watched,
		Ref:     ref,
	}) {
		delete(c.monitors, ref)
		c.relations--
		c.mu.Unlock()
		c.node.releaseRelations(1)
		return Ref{}, nil, &Error{Stage: StageSend, Outcome: OutcomeNotSent, Peer: peerName, Err: errSessionClosed}
	}
	c.mu.Unlock()

	return ref, ch, nil
}

// Demonitor removes an outbound monitor established by Monitor (§10.1).
func (c *Client) Demonitor(ctx context.Context, ref Ref) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: err}
		}
	}
	c.mu.Lock()
	rec, ok := c.monitors[ref]
	if ok {
		delete(c.monitors, ref)
		c.relations--
	}
	c.mu.Unlock()
	if !ok {
		return nil
	}
	c.node.releaseRelations(1)
	if sess := c.node.lookupActiveSession(rec.peer); sess != nil && sess.gen == rec.peerGen {
		sess.enqueueControl(dist.ControlMessage{
			Op:      dist.OpDemonitorP,
			FromPID: c.pid,
			HasFrom: true,
			ToProc:  rec.watched,
			Ref:     rec.ref,
		})
	}
	return nil
}

// Link establishes a bidirectional Erlang process link to target (§10.2).
func (c *Client) Link(ctx context.Context, target PID) error {
	if ctx == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	if err := target.Validate(); err != nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node), Err: err}
	}
	peerName := string(target.Node)
	sess, err := c.node.ensureSession(ctx, peerName)
	if err != nil {
		return err
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrClientClosed}
	}
	existing, exists := c.links[target]
	if exists && existing.active && existing.peerGen == sess.gen {
		c.mu.Unlock()
		return nil
	}
	needQuota := !exists
	if needQuota {
		if c.relations >= c.node.limits.MaxProcessRelations {
			c.mu.Unlock()
			return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrCapacity}
		}
		if !c.node.tryAcquireRelation() {
			c.mu.Unlock()
			return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrCapacity}
		}
		c.relations++
	}
	// Sending LINK activates the link and clears any outstanding unlinkID (§10.2).
	c.links[target] = linkRecord{
		peerGen:  sess.gen,
		active:   true,
		unlinkID: 0,
	}
	if !sess.enqueueControl(dist.ControlMessage{
		Op:      dist.OpLink,
		FromPID: c.pid,
		HasFrom: true,
		ToPID:   target,
	}) {
		if needQuota {
			delete(c.links, target)
			c.relations--
			c.node.releaseRelations(1)
		} else {
			c.links[target] = existing
		}
		c.mu.Unlock()
		return &Error{Stage: StageSend, Outcome: OutcomeNotSent, Peer: peerName, Err: errSessionClosed}
	}
	c.mu.Unlock()
	return nil
}

// Unlink initiates the OTP 26+ UNLINK_ID / UNLINK_ID_ACK handshake if an active link exists (§10.2).
func (c *Client) Unlink(ctx context.Context, target PID) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node), Err: err}
		}
	}
	id, err := c.node.ids.nextUnlinkID()
	if err != nil {
		return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: string(target.Node), Err: err}
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: string(target.Node), Err: ErrClientClosed}
	}
	lnk, exists := c.links[target]
	if !exists || !lnk.active {
		c.mu.Unlock()
		return nil
	}
	sess := c.node.lookupActiveSession(string(target.Node))
	if sess == nil || sess.gen != lnk.peerGen {
		delete(c.links, target)
		c.relations--
		c.mu.Unlock()
		c.node.releaseRelations(1)
		return nil
	}
	lnk.active = false
	lnk.unlinkID = id
	c.links[target] = lnk
	sess.enqueueControl(dist.ControlMessage{
		Op:       dist.OpUnlinkID,
		UnlinkID: id,
		FromPID:  c.pid,
		HasFrom:  true,
		ToPID:    target,
	})
	c.mu.Unlock()
	return nil
}

func (c *Client) handleAliasOrPIDMessage(payload etf.Term) {
	tup, ok := payload.(etf.Tuple)
	if !ok || len(tup) != 2 {
		return
	}
	tag := tup[0]
	reply := tup[1]

	var matched *pendingCall
	c.mu.Lock()
	if !c.closed {
		if ref, isRef := tag.(Ref); isRef {
			if pc, ok := c.calls[ref]; ok {
				matched = pc
				delete(c.calls, ref)
			}
		} else if aliasRef, ok := extractAliasFromTag(tag); ok {
			if pc, ok := c.calls[aliasRef]; ok && etf.TermEqual(pc.tag, tag) {
				matched = pc
				delete(c.calls, aliasRef)
			}
		} else {
			for ref, pc := range c.calls {
				if etf.TermEqual(pc.tag, tag) {
					matched = pc
					delete(c.calls, ref)
					break
				}
			}
		}
	}
	c.mu.Unlock()

	if matched == nil {
		c.node.lateReplies.Add(1)
		return
	}
	c.node.revokeAlias(matched.ref)
	if matched.settle(reply, nil) {
		c.node.releaseOutboundCall(matched.session)
		if matched.session != nil && !matched.session.isClosed() {
			matched.session.enqueueControl(dist.ControlMessage{
				Op:      dist.OpDemonitorP,
				FromPID: c.pid,
				HasFrom: true,
				ToProc:  matched.watched,
				Ref:     matched.ref,
			})
		}
	} else {
		c.node.lateReplies.Add(1)
	}
}

func (c *Client) handleMonitorExit(peer string, peerGen uint64, watched etf.Term, ref Ref, reason etf.Term) {
	c.mu.Lock()
	var pc *pendingCall
	if cand, ok := c.calls[ref]; ok && cand.peerGen == peerGen {
		pc = cand
		delete(c.calls, ref)
	}
	var mon *outboundMonRecord
	if cand, ok := c.monitors[ref]; ok && cand.peerGen == peerGen {
		mon = cand
		delete(c.monitors, ref)
		c.relations--
	}
	c.mu.Unlock()

	if pc != nil {
		c.node.revokeAlias(pc.ref)
		downErr := &Error{
			Stage:   StageProcess,
			Outcome: OutcomeUnknown,
			Peer:    peer,
			Reason:  reason,
			Err:     fmt.Errorf("remote process down: %v", reason),
		}
		if pc.settle(nil, downErr) {
			c.node.unknownOutcomes.Add(1)
			c.node.releaseOutboundCall(pc.session)
		}
	}
	if mon != nil {
		c.node.releaseRelations(1)
		obj := mon.object
		if a, ok := watched.(etf.Atom); ok {
			obj = etf.Tuple{a, etf.Atom(peer)}
		} else if p, ok := watched.(PID); ok {
			obj = p
		}
		mon.ch <- DownEvent{
			Ref:     ref,
			Watched: obj,
			Reason:  reason,
		}
	}
}

func (c *Client) onPeerDown(peerName string, peerGen uint64) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var failedCalls []*pendingCall
	for ref, pc := range c.calls {
		if pc.peerGen == peerGen && pc.peer == peerName {
			failedCalls = append(failedCalls, pc)
			delete(c.calls, ref)
		}
	}
	var downMons []*outboundMonRecord
	var released int
	for ref, m := range c.monitors {
		if m.peerGen == peerGen && m.peer == peerName {
			downMons = append(downMons, m)
			delete(c.monitors, ref)
			c.relations--
			released++
		}
	}
	for k := range c.inMonitors {
		if k.peerGen == peerGen && string(k.watcher.Node) == peerName {
			delete(c.inMonitors, k)
			c.relations--
			released++
		}
	}
	var trapped []PID
	linkExit := false
	for remotePID, lnk := range c.links {
		if lnk.peerGen == peerGen && string(remotePID.Node) == peerName {
			delete(c.links, remotePID)
			c.relations--
			released++
			if lnk.active {
				if c.trapExit {
					trapped = append(trapped, remotePID)
				} else {
					linkExit = true
				}
			}
		}
	}
	c.mu.Unlock()

	if released > 0 {
		c.node.releaseRelations(released)
	}
	for _, pc := range failedCalls {
		c.node.revokeAlias(pc.ref)
		outcome := OutcomeUnknown
		if pc.bundle.tryAbort() {
			outcome = OutcomeNotSent
		} else {
			c.node.unknownOutcomes.Add(1)
		}
		if pc.settle(nil, &Error{
			Stage:   StageWait,
			Outcome: outcome,
			Peer:    peerName,
			Reason:  etf.Atom("noconnection"),
			Err:     errSessionClosed,
		}) {
			c.node.releaseOutboundCall(pc.session)
		}
	}
	for _, m := range downMons {
		m.ch <- DownEvent{
			Ref:     m.ref,
			Watched: m.object,
			Reason:  etf.Atom("noconnection"),
		}
	}
	if linkExit {
		c.terminate(etf.Atom("noconnection"), true)
		return
	}
	if c.exitCh != nil {
		for _, rpid := range trapped {
			select {
			case c.exitCh <- ExitEvent{From: rpid, Reason: etf.Atom("noconnection"), FromLink: true}:
			default:
			}
		}
	}
}

func (ep *Endpoint) installInboundMonitor(sess *peerSession, watcher PID, watched etf.Term, ref Ref) (alive bool, quotaExceeded bool) {
	if !ep.allowsPeer(sess.peerName) {
		return false, false
	}
	key := inboundMonKey{
		peerGen: sess.gen,
		watcher: watcher,
		ref:     ref,
	}
	ep.mu.Lock()
	if ep.terminated {
		ep.mu.Unlock()
		return false, false
	}
	if _, dup := ep.monitors[key]; dup {
		ep.mu.Unlock()
		return true, false
	}
	if ep.relations >= ep.node.limits.MaxProcessRelations {
		ep.mu.Unlock()
		return true, true
	}
	if !ep.node.tryAcquireRelation() {
		ep.mu.Unlock()
		return true, true
	}
	ep.relations++
	ep.monitors[key] = inboundMonRecord{
		session: sess,
		watcher: watcher,
		watched: watched,
		ref:     ref,
	}
	ep.mu.Unlock()

	if !sess.trackInboundMonitor(key, ep) {
		ep.mu.Lock()
		if _, ok := ep.monitors[key]; ok {
			delete(ep.monitors, key)
			ep.relations--
			ep.mu.Unlock()
			ep.node.releaseRelations(1)
		} else {
			ep.mu.Unlock()
		}
	}
	return true, false
}

func (ep *Endpoint) handleDemonitor(key inboundMonKey, watched etf.Term) {
	var sess *peerSession
	var matched bool
	ep.mu.Lock()
	if rec, ok := ep.monitors[key]; ok && etf.TermEqual(rec.watched, watched) {
		sess = rec.session
		delete(ep.monitors, key)
		ep.relations--
		matched = true
	}
	ep.mu.Unlock()

	if matched {
		if sess != nil {
			sess.untrackInboundMonitor(key)
		}
		ep.node.releaseRelations(1)
	}
}

func (ep *Endpoint) handleInboundLink(sess *peerSession, from PID) (alive bool, quotaExceeded bool) {
	if !ep.allowsPeer(sess.peerName) {
		return false, false
	}
	ep.mu.Lock()
	if ep.terminated {
		ep.mu.Unlock()
		return false, false
	}
	if existing, exists := ep.links[from]; exists && existing.peerGen == sess.gen {
		// OTP link protocol: existing link info (active or unlinking) ignores inbound LINK (§10.2).
		ep.mu.Unlock()
		return true, false
	}
	if _, exists := ep.links[from]; !exists {
		if ep.relations >= ep.node.limits.MaxProcessRelations {
			ep.mu.Unlock()
			return true, true
		}
		if !ep.node.tryAcquireRelation() {
			ep.mu.Unlock()
			return true, true
		}
		ep.relations++
	}
	ep.links[from] = linkRecord{
		peerGen:  sess.gen,
		active:   true,
		unlinkID: 0,
	}
	ep.mu.Unlock()
	return true, false
}

func (ep *Endpoint) handleInboundUnlinkID(sess *peerSession, id uint64, from PID) {
	if !ep.allowsPeer(sess.peerName) {
		sess.enqueueControl(dist.ControlMessage{
			Op:       dist.OpUnlinkIDAck,
			UnlinkID: id,
			FromPID:  ep.pid,
			HasFrom:  true,
			ToPID:    from,
		})
		return
	}
	var released int
	ep.mu.Lock()
	if !ep.terminated {
		if lnk, exists := ep.links[from]; exists && lnk.peerGen == sess.gen && lnk.active {
			delete(ep.links, from)
			ep.relations--
			released = 1
		}
	}
	// Hold ep.mu across leaf sess.enqueueControl so UNLINK_ID_ACK precedes any later EXIT from ep (§10.2).
	sess.enqueueControl(dist.ControlMessage{
		Op:       dist.OpUnlinkIDAck,
		UnlinkID: id,
		FromPID:  ep.pid,
		HasFrom:  true,
		ToPID:    from,
	})
	ep.mu.Unlock()

	if released > 0 {
		ep.node.releaseRelations(released)
	}
}

func (ep *Endpoint) handleInboundUnlinkIDAck(sess *peerSession, id uint64, from PID) {
	var released int
	ep.mu.Lock()
	if !ep.terminated {
		if lnk, exists := ep.links[from]; exists && lnk.peerGen == sess.gen && !lnk.active && lnk.unlinkID == id {
			delete(ep.links, from)
			ep.relations--
			released = 1
		}
	}
	ep.mu.Unlock()

	if released > 0 {
		ep.node.releaseRelations(released)
	}
}

func (ep *Endpoint) handleInboundExitSignal(sess *peerSession, from PID, reason etf.Term, isExit2 bool) {
	if !ep.allowsPeer(sess.peerName) {
		return
	}
	var released int
	ignore := false
	ep.mu.Lock()
	if ep.terminated {
		ep.mu.Unlock()
		return
	}
	if !isExit2 {
		lnk, exists := ep.links[from]
		if !exists || lnk.peerGen != sess.gen {
			ignore = true
		} else {
			if !lnk.active {
				ignore = true
			}
			delete(ep.links, from)
			ep.relations--
			released = 1
		}
	}
	draining := ep.draining
	ep.mu.Unlock()

	if released > 0 {
		ep.node.releaseRelations(released)
	}
	if ignore {
		return
	}

	if !(isExit2 && reason == etf.Atom("kill")) && ep.trapExit {
		if !draining && ep.onDeliver != nil {
			ep.onDeliver(ep, InboundDelivery{
				Kind:         InboundExit,
				Peer:         sess.peerName,
				PeerGen:      sess.gen,
				From:         from,
				HasFrom:      true,
				Term:         reason,
				ExitFromLink: !isExit2,
			})
		}
		return
	}
	if reason == etf.Atom("normal") {
		return
	}
	outReason := reason
	if isExit2 && reason == etf.Atom("kill") {
		outReason = etf.Atom("killed")
	}
	ep.terminateInternal(outReason, true)
}

func (ep *Endpoint) deliverInboundMessage(sess *peerSession, msg dist.InboundMessage) {
	if !ep.allowsPeer(sess.peerName) {
		return
	}
	ep.mu.Lock()
	blocked := ep.draining || ep.terminated
	ep.mu.Unlock()
	if blocked {
		return
	}

	if rt, _, ok := ParseSystemRequest(msg.Payload); ok {
		rt.callerMonitored = ep.hasInboundCallMonitor(sess.gen, rt)
		if err := ep.SendReply(rt, etf.Tuple{
			etf.Atom("error"),
			etf.Tuple{etf.Atom("tao"), etf.Atom("unsupported")},
		}); err != nil && errors.Is(err, ErrCapacity) {
			ep.TerminateForFailedReply(rt, TaoReason("overloaded", err.Error()), err)
		}
		return
	}
	if ep.onDeliver == nil {
		return
	}

	if rt, req, ok := ParseGenCall(msg.Payload); ok {
		rt.callerMonitored = ep.hasInboundCallMonitor(sess.gen, rt)
		ep.onDeliver(ep, InboundDelivery{
			Kind:         InboundGenCall,
			Peer:         sess.peerName,
			PeerGen:      sess.gen,
			From:         msg.Control.FromPID,
			HasFrom:      msg.Control.HasFrom,
			ReplyTo:      rt,
			Term:         req,
			WireBytes:    msg.WireBytes,
			DecodedBytes: msg.DecodedBytes,
		})
		return
	}
	if req, ok := ParseGenCast(msg.Payload); ok {
		ep.onDeliver(ep, InboundDelivery{
			Kind:         InboundGenCast,
			Peer:         sess.peerName,
			PeerGen:      sess.gen,
			From:         msg.Control.FromPID,
			HasFrom:      msg.Control.HasFrom,
			Term:         req,
			WireBytes:    msg.WireBytes,
			DecodedBytes: msg.DecodedBytes,
		})
		return
	}
	ep.onDeliver(ep, InboundDelivery{
		Kind:         InboundInfo,
		Peer:         sess.peerName,
		PeerGen:      sess.gen,
		From:         msg.Control.FromPID,
		HasFrom:      msg.Control.HasFrom,
		Term:         msg.Payload,
		WireBytes:    msg.WireBytes,
		DecodedBytes: msg.DecodedBytes,
	})
}

func (c *Client) installInboundMonitor(sess *peerSession, watcher PID, watched etf.Term, ref Ref) (alive bool, quotaExceeded bool) {
	key := inboundMonKey{
		peerGen: sess.gen,
		watcher: watcher,
		ref:     ref,
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false, false
	}
	if _, dup := c.inMonitors[key]; dup {
		c.mu.Unlock()
		return true, false
	}
	if c.relations >= c.node.limits.MaxProcessRelations {
		c.mu.Unlock()
		return true, true
	}
	if !c.node.tryAcquireRelation() {
		c.mu.Unlock()
		return true, true
	}
	c.relations++
	c.inMonitors[key] = inboundMonRecord{
		session: sess,
		watcher: watcher,
		watched: watched,
		ref:     ref,
	}
	c.mu.Unlock()

	if !sess.trackInboundMonitor(key, c) {
		c.mu.Lock()
		if _, ok := c.inMonitors[key]; ok {
			delete(c.inMonitors, key)
			c.relations--
			c.mu.Unlock()
			c.node.releaseRelations(1)
		} else {
			c.mu.Unlock()
		}
	}
	return true, false
}

func (c *Client) handleDemonitor(key inboundMonKey, watched etf.Term) {
	var sess *peerSession
	var matched bool
	c.mu.Lock()
	if rec, ok := c.inMonitors[key]; ok && etf.TermEqual(rec.watched, watched) {
		sess = rec.session
		delete(c.inMonitors, key)
		c.relations--
		matched = true
	}
	c.mu.Unlock()

	if matched {
		if sess != nil {
			sess.untrackInboundMonitor(key)
		}
		c.node.releaseRelations(1)
	}
}

func (c *Client) handleInboundLink(sess *peerSession, from PID) (alive bool, quotaExceeded bool) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false, false
	}
	if existing, exists := c.links[from]; exists && existing.peerGen == sess.gen {
		c.mu.Unlock()
		return true, false
	}
	if _, exists := c.links[from]; !exists {
		if c.relations >= c.node.limits.MaxProcessRelations {
			c.mu.Unlock()
			return true, true
		}
		if !c.node.tryAcquireRelation() {
			c.mu.Unlock()
			return true, true
		}
		c.relations++
	}
	c.links[from] = linkRecord{
		peerGen:  sess.gen,
		active:   true,
		unlinkID: 0,
	}
	c.mu.Unlock()
	return true, false
}

func (c *Client) handleInboundUnlinkID(sess *peerSession, id uint64, from PID) {
	var released int
	c.mu.Lock()
	if !c.closed {
		if lnk, exists := c.links[from]; exists && lnk.peerGen == sess.gen && lnk.active {
			delete(c.links, from)
			c.relations--
			released = 1
		}
	}
	sess.enqueueControl(dist.ControlMessage{
		Op:       dist.OpUnlinkIDAck,
		UnlinkID: id,
		FromPID:  c.pid,
		HasFrom:  true,
		ToPID:    from,
	})
	c.mu.Unlock()

	if released > 0 {
		c.node.releaseRelations(released)
	}
}

func (c *Client) handleInboundUnlinkIDAck(sess *peerSession, id uint64, from PID) {
	var released int
	c.mu.Lock()
	if !c.closed {
		if lnk, exists := c.links[from]; exists && lnk.peerGen == sess.gen && !lnk.active && lnk.unlinkID == id {
			delete(c.links, from)
			c.relations--
			released = 1
		}
	}
	c.mu.Unlock()

	if released > 0 {
		c.node.releaseRelations(released)
	}
}

func (c *Client) handleInboundExitSignal(sess *peerSession, from PID, reason etf.Term, isExit2 bool) {
	var released int
	ignore := false
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if !isExit2 {
		lnk, exists := c.links[from]
		if !exists || lnk.peerGen != sess.gen {
			ignore = true
		} else {
			if !lnk.active {
				ignore = true
			}
			delete(c.links, from)
			c.relations--
			released = 1
		}
	}
	c.mu.Unlock()

	if released > 0 {
		c.node.releaseRelations(released)
	}
	if ignore {
		return
	}

	if !(isExit2 && reason == etf.Atom("kill")) && c.trapExit {
		if c.exitCh != nil {
			select {
			case c.exitCh <- ExitEvent{From: from, Reason: reason, FromLink: !isExit2}:
			default:
			}
		}
		return
	}
	if reason == etf.Atom("normal") {
		return
	}
	outReason := reason
	if isExit2 && reason == etf.Atom("kill") {
		outReason = etf.Atom("killed")
	}
	c.terminate(outReason, true)
}
