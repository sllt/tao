package erlang

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/dist"
	"github.com/sllt/tao/erlang/internal/epmd"
)

var (
	errSessionClosed = errors.New("erlang: peer session closed")
)

type callBundleState struct {
	state atomic.Uint32 // 0 = queued, 1 = committed to wire, 2 = aborted before encode
}

func (b *callBundleState) markCommitted() bool {
	if b == nil {
		return true
	}
	return b.state.CompareAndSwap(0, 1)
}

func (b *callBundleState) tryAbort() bool {
	if b == nil {
		return false
	}
	return b.state.CompareAndSwap(0, 2)
}

func (b *callBundleState) wasCommitted() bool {
	return b != nil && b.state.Load() == 1
}

type outboundItem struct {
	isControl bool
	ctrl      dist.ControlMessage
	payload   etf.Term
	estBytes  int64
	bundle    *callBundleState
}

type inboundMonOwner interface {
	handleDemonitor(key inboundMonKey, watched etf.Term)
}

// peerSession owns one authenticated v6 connection generation to a single remote Erlang node.
// Reader owns FrameReader + RecvAtomCache; Writer owns FrameEncoder + SendAtomCache.
type peerSession struct {
	node            *Node
	peerName        string
	gen             uint64
	remoteCreation  uint32
	negotiatedFlags uint64
	isTLS           bool
	conn            *dist.Conn

	lastReadNano  atomic.Int64
	lastWriteNano atomic.Int64
	outboundCalls atomic.Int32
	inboundCalls  atomic.Int32
	reassemblyB   atomic.Int64

	inMonMu sync.Mutex
	inMons  map[inboundMonKey]inboundMonOwner

	writeMu            sync.Mutex
	closed             bool
	flushing           bool
	closeReason        error
	queue              []outboundItem
	sendQueuedBytes    int64
	controlQueuedCount int
	wakeWriter         chan struct{}

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newPeerSession(
	n *Node,
	res *dist.HandshakeResult,
	gen uint64,
	isTLS bool,
) *peerSession {
	now := monotonicNano()
	s := &peerSession{
		node:            n,
		peerName:        res.PeerName,
		gen:             gen,
		remoteCreation:  res.PeerCreation,
		negotiatedFlags: res.NegotiatedFlags,
		isTLS:           isTLS,
		conn:            res.Conn,
		inMons:          make(map[inboundMonKey]inboundMonOwner),
		wakeWriter:      make(chan struct{}, 1),
		done:            make(chan struct{}),
	}
	s.lastReadNano.Store(now)
	s.lastWriteNano.Store(now)
	return s
}

func (s *peerSession) trackInboundMonitor(key inboundMonKey, owner inboundMonOwner) bool {
	s.inMonMu.Lock()
	defer s.inMonMu.Unlock()
	if s.inMons == nil || s.isClosed() {
		return false
	}
	s.inMons[key] = owner
	return true
}

func (s *peerSession) untrackInboundMonitor(key inboundMonKey) {
	s.inMonMu.Lock()
	delete(s.inMons, key)
	s.inMonMu.Unlock()
}

func (s *peerSession) lookupInboundMonitor(key inboundMonKey) inboundMonOwner {
	s.inMonMu.Lock()
	owner := s.inMons[key]
	s.inMonMu.Unlock()
	return owner
}

func (s *peerSession) hasPendingWrites() bool {
	s.writeMu.Lock()
	busy := !s.closed && (len(s.queue) > 0 || s.flushing)
	s.writeMu.Unlock()
	return busy
}

func (s *peerSession) start() {
	s.wg.Add(3)
	go s.readLoop()
	go s.writeLoop()
	go s.tickLoop()
}

func (s *peerSession) closeWithError(err error) {
	s.closeOnce.Do(func() {
		if err == nil {
			err = errSessionClosed
		}
		s.writeMu.Lock()
		s.closed = true
		s.closeReason = err
		// Drain uncommitted queued items; in-flight batches popped by writeLoop release
		// their own byte charges unconditionally when writeLoop finishes that batch.
		var released int64
		for i := range s.queue {
			if !s.queue[i].isControl {
				released += s.queue[i].estBytes
			}
			s.queue[i] = outboundItem{}
		}
		s.queue = nil
		s.sendQueuedBytes -= released
		s.controlQueuedCount = 0
		s.writeMu.Unlock()
		if released > 0 {
			s.node.totalSendQueued.Add(-released)
		}

		s.inMonMu.Lock()
		s.inMons = nil
		s.inMonMu.Unlock()

		_ = s.conn.Close()
		close(s.done)
		s.node.onSessionClosed(s, err)
	})
}

// enqueueControl appends a protocol control signal using the dedicated bounded control budget.
// writeMu is a strict leaf lock. If the control queue is exhausted, the session is closed (§11.1).
func (s *peerSession) enqueueControl(ctrl dist.ControlMessage) bool {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return false
	}
	if s.controlQueuedCount >= s.node.limits.MaxControlQueue {
		s.closed = true
		s.closeReason = ErrCapacity
		s.writeMu.Unlock()
		s.node.overloadDrops.Add(1)
		s.node.emitEvent(EventOverload, s.peerName, s.gen, "control queue exhausted; closing session")
		_ = s.conn.Close()
		go s.closeWithError(ErrCapacity)
		return false
	}
	s.controlQueuedCount++
	s.queue = append(s.queue, outboundItem{
		isControl: true,
		ctrl:      ctrl,
	})
	s.signalWriterLocked()
	s.writeMu.Unlock()
	return true
}

// enqueueData validates/estimates size, snapshots caller-mutable data, and appends a business
// message (Send, Cast, Reply) within the per-peer send byte budget (§5, §11.1).
func (s *peerSession) enqueueData(ctrl dist.ControlMessage, payload etf.Term) error {
	owned, est, err := s.node.preflightOutboundTerm(payload)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return &Error{Stage: StageSend, Outcome: OutcomeNotSent, Peer: s.peerName, Err: errSessionClosed}
	}
	if est > s.node.limits.MaxPeerSendBytes-s.sendQueuedBytes {
		s.writeMu.Unlock()
		s.node.overloadDrops.Add(1)
		return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: s.peerName, Err: ErrCapacity}
	}
	s.sendQueuedBytes += est
	s.node.totalSendQueued.Add(est)
	s.queue = append(s.queue, outboundItem{
		isControl: false,
		ctrl:      ctrl,
		payload:   owned,
		estBytes:  est,
	})
	s.signalWriterLocked()
	s.writeMu.Unlock()
	return nil
}

// enqueueCallBundlePrevalidated atomically queues MONITOR_P immediately followed by '$gen_call'
// on the single ordered outbound stream (§8.2). writeMu is a strict leaf lock.
func (s *peerSession) enqueueCallBundlePrevalidated(monCtrl, callCtrl dist.ControlMessage, callPayload etf.Term, est int64, bundle *callBundleState) error {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return &Error{Stage: StageSend, Outcome: OutcomeNotSent, Peer: s.peerName, Err: errSessionClosed}
	}
	if s.controlQueuedCount >= s.node.limits.MaxControlQueue {
		s.closed = true
		s.closeReason = ErrCapacity
		s.writeMu.Unlock()
		s.node.overloadDrops.Add(1)
		s.node.emitEvent(EventOverload, s.peerName, s.gen, "control queue exhausted; closing session")
		_ = s.conn.Close()
		go s.closeWithError(ErrCapacity)
		return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: s.peerName, Err: ErrCapacity}
	}
	if est > s.node.limits.MaxPeerSendBytes-s.sendQueuedBytes {
		s.writeMu.Unlock()
		s.node.overloadDrops.Add(1)
		return &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: s.peerName, Err: ErrCapacity}
	}
	s.controlQueuedCount++
	s.sendQueuedBytes += est
	s.node.totalSendQueued.Add(est)
	s.queue = append(s.queue,
		outboundItem{
			isControl: true,
			ctrl:      monCtrl,
			bundle:    bundle,
		},
		outboundItem{
			isControl: false,
			ctrl:      callCtrl,
			payload:   callPayload,
			estBytes:  est,
			bundle:    bundle,
		},
	)
	s.signalWriterLocked()
	s.writeMu.Unlock()
	return nil
}

func (s *peerSession) enqueueTick() {
	s.writeMu.Lock()
	if s.closed {
		s.writeMu.Unlock()
		return
	}
	s.signalWriterLocked()
	s.writeMu.Unlock()
}

func (s *peerSession) signalWriterLocked() {
	select {
	case s.wakeWriter <- struct{}{}:
	default:
	}
}

func (s *peerSession) writeLoop() {
	defer s.wg.Done()

	enc := dist.NewFrameEncoder(dist.FrameEncoderConfig{
		NegotiatedFlags: s.negotiatedFlags,
		FragmentSize:    s.node.cfg.FragmentSize,
		MaxPayloadBytes: s.node.limits.MaxPayloadBytes,
		EncodeLimits: etf.Limits{
			MaxFrameBytes:  s.node.limits.MaxPayloadBytes + dist.DefaultMaxControlOverhead,
			MaxBinaryBytes: s.node.limits.MaxPayloadBytes,
		},
	})

	const maxBatchItems = 64
	batch := make([]outboundItem, 0, maxBatchItems)
	tickInterval := s.node.tickTime / 4

	for {
		select {
		case <-s.done:
			return
		case <-s.wakeWriter:
		}

		for {
			s.writeMu.Lock()
			if s.closed {
				s.writeMu.Unlock()
				return
			}
			n := len(s.queue)
			if n == 0 {
				s.writeMu.Unlock()
				if monotonicNano()-s.lastWriteNano.Load() >= tickInterval.Nanoseconds() {
					var tick [4]byte
					if _, err := s.conn.Write(tick[:]); err != nil {
						s.closeWithError(err)
						return
					}
					s.lastWriteNano.Store(monotonicNano())
				}
				break
			}
			if n > maxBatchItems {
				n = maxBatchItems
			}
			batch = append(batch[:0], s.queue[:n]...)
			for i := 0; i < n; i++ {
				s.queue[i] = outboundItem{}
			}
			s.queue = s.queue[n:]
			if len(s.queue) == 0 {
				s.queue = s.queue[:0]
			}
			var controlPopped int
			var batchBytes int64
			for i := range batch {
				if batch[i].isControl {
					controlPopped++
				} else {
					batchBytes += batch[i].estBytes
				}
			}
			s.controlQueuedCount -= controlPopped
			s.flushing = true
			s.writeMu.Unlock()

			var frames []*[]byte
			var encodeErr error

			for i := range batch {
				item := batch[i]
				batch[i] = outboundItem{}
				if item.bundle != nil {
					if item.isControl {
						// First part of CallBundle (MONITOR_P): check if caller already canceled before encode.
						if !item.bundle.markCommitted() {
							continue
						}
					} else if !item.bundle.wasCommitted() {
						// Aborted before MONITOR_P was encoded; skip '$gen_call' too.
						continue
					}
				}

				itemFrames, err := enc.EncodeFrames(item.ctrl, item.payload)
				if err != nil {
					encodeErr = err
					for j := i + 1; j < len(batch); j++ {
						batch[j] = outboundItem{}
					}
					break
				}
				frames = append(frames, itemFrames...)
			}

			// If encoding failed AFTER earlier items in this batch already advanced SendAtomCache,
			// we MUST still flush the already-encoded frames or close the session — never drop a
			// frame mid-cache-state and keep running (§7.2).
			writeErr := s.flushFrames(frames)
			for _, fp := range frames {
				dist.ReleaseBuffer(fp)
			}

			s.writeMu.Lock()
			s.flushing = false
			if batchBytes > 0 {
				s.sendQueuedBytes -= batchBytes
			}
			s.writeMu.Unlock()
			if batchBytes > 0 {
				s.node.totalSendQueued.Add(-batchBytes)
			}

			if writeErr != nil {
				s.closeWithError(writeErr)
				return
			}
			if encodeErr != nil {
				s.node.protocolErrors.Add(1)
				s.node.emitEvent(EventProtocolError, s.peerName, s.gen, encodeErr.Error())
				s.closeWithError(encodeErr)
				return
			}
		}
	}
}

func (s *peerSession) isClosed() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *peerSession) flushFrames(frames []*[]byte) error {
	if len(frames) == 0 {
		return nil
	}
	var totalBytes uint64
	if !s.isTLS && len(frames) > 1 {
		bufs := make(net.Buffers, len(frames))
		for i, fp := range frames {
			bufs[i] = *fp
			totalBytes += uint64(len(*fp))
		}
		if _, err := bufs.WriteTo(s.conn.RawConn()); err != nil {
			return err
		}
	} else {
		for _, fp := range frames {
			totalBytes += uint64(len(*fp))
			if _, err := s.conn.Write(*fp); err != nil {
				return err
			}
		}
	}
	s.node.encodedBytesTotal.Add(totalBytes)
	s.lastWriteNano.Store(monotonicNano())
	return nil
}

func (s *peerSession) readLoop() {
	defer s.wg.Done()

	maxActiveFrags := max(
		dist.DefaultMaxActiveFragments,
		int(min(int64(s.node.limits.MaxPeerPendingCalls), s.node.limits.MaxPeerRecvBytes/(256<<10))),
	)
	rd := dist.NewFrameReader(dist.FrameReaderConfig{
		LocalNode:          s.node.nameAtom,
		RemoteNode:         etf.Atom(s.peerName),
		NegotiatedFlags:    s.negotiatedFlags,
		MaxPayloadBytes:    s.node.limits.MaxPayloadBytes,
		MaxReassemblyBytes: s.node.limits.MaxPeerRecvBytes,
		MaxActiveFragments: maxActiveFrags,
		FragmentTimeout:    maxDuration(s.node.tickTime/2, 5*time.Second),
		DecodeLimits: etf.Limits{
			MaxFrameBytes:   s.node.limits.MaxPayloadBytes + dist.DefaultMaxControlOverhead,
			MaxDecodedBytes: s.node.limits.MaxPeerDecodedBytes,
			MaxBinaryBytes:  s.node.limits.MaxPayloadBytes,
		},
	})

	for {
		kind, msg, err := rd.ReadPacket(s.conn)
		s.reassemblyB.Store(rd.ReassemblyBytes())
		if err != nil {
			if !s.isClosed() && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.node.protocolErrors.Add(1)
				s.node.emitEvent(EventProtocolError, s.peerName, s.gen, err.Error())
			}
			s.reassemblyB.Store(0)
			s.closeWithError(err)
			return
		}
		s.lastReadNano.Store(monotonicNano())
		if kind != dist.PacketMessage {
			continue
		}
		s.node.decodedBytesTotal.Add(uint64(msg.WireBytes))
		if err := s.node.dispatchInbound(s, msg); err != nil {
			s.node.protocolErrors.Add(1)
			s.node.emitEvent(EventProtocolError, s.peerName, s.gen, err.Error())
			s.reassemblyB.Store(0)
			s.closeWithError(err)
			return
		}
	}
}

func (s *peerSession) tickLoop() {
	defer s.wg.Done()

	interval := s.node.tickTime / 4
	if interval < 25*time.Millisecond {
		interval = 25 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	budgetNano := s.node.tickTime.Nanoseconds()
	writeIdleNano := interval.Nanoseconds()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			now := monotonicNano()
			if now-s.lastReadNano.Load() >= budgetNano {
				s.closeWithError(errors.New("erlang: net_ticktime timeout"))
				return
			}
			if now-s.lastWriteNano.Load() >= writeIdleNano {
				s.enqueueTick()
			}
		}
	}
}

var monotonicBase = time.Now()

func monotonicNano() int64 {
	return time.Since(monotonicBase).Nanoseconds()
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

type dialResult struct {
	session *peerSession
	err     error
}

// peerCoord serializes connection lifecycle, simultaneous-connect tie-breaking, and alive
// replacement for a single remote peer name (§6.2).
type peerCoord struct {
	node     *Node
	peerName string

	mu         sync.Mutex
	active     *peerSession
	dialing    bool
	hasSlot    bool
	dialCancel context.CancelFunc
	waiters    []chan dialResult
}

func (c *peerCoord) currentSession() *peerSession {
	c.mu.Lock()
	s := c.active
	c.mu.Unlock()
	return s
}

func (c *peerCoord) arbitrateInbound() dist.InboundDecision {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != nil && !c.active.isClosed() {
		return dist.DecisionAlive
	}
	if c.dialing {
		// Simultaneous connect: larger literal node name wins (§6.2).
		if c.node.cfg.Name > c.peerName {
			return dist.DecisionNOK
		}
		return dist.DecisionOKSimultaneous
	}
	return dist.DecisionOK
}

// commitInbound installs an authenticated inbound session, retiring any superseded
// active session or losing simultaneous outbound dial ONLY AFTER cookie verification (§6.2).
// transferredSlot reports whether the caller's temporary inbound handshake slot was transferred to c.
func (c *peerCoord) commitInbound(s *peerSession) (transferredSlot bool, err error) {
	if c.node.stopping.Load() {
		return false, ErrNodeClosed
	}
	c.mu.Lock()
	old := c.active
	if c.dialing && c.node.cfg.Name > c.peerName {
		// Outbound dial won tie-break concurrently.
		c.mu.Unlock()
		return false, dist.ErrSimultaneousReject
	}
	var cancelDial context.CancelFunc
	var waiters []chan dialResult
	if c.dialing {
		cancelDial = c.dialCancel
		c.dialCancel = nil
		c.dialing = false
		waiters = c.waiters
		c.waiters = nil
	}
	if !c.hasSlot {
		c.hasSlot = true
		transferredSlot = true
	}
	c.active = s
	c.mu.Unlock()

	if cancelDial != nil {
		cancelDial()
	}
	if old != nil && old != s {
		old.closeWithError(errors.New("erlang: superseded by authenticated alive connection"))
	}
	s.start()
	c.node.emitEvent(EventPeerUp, s.peerName, s.gen, "inbound")
	for _, w := range waiters {
		w <- dialResult{session: s}
	}
	return transferredSlot, nil
}

func (c *peerCoord) ensureConnected(ctx context.Context) (*peerSession, error) {
	if c.node.stopping.Load() {
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Peer: c.peerName, Err: ErrNodeClosed}
	}

	c.mu.Lock()
	if c.active != nil {
		if !c.active.isClosed() {
			s := c.active
			c.mu.Unlock()
			return s, nil
		}
		c.active = nil
	}
	if !c.dialing {
		if !c.hasSlot {
			if !c.node.tryAcquirePeerSlot() {
				c.mu.Unlock()
				c.node.overloadDrops.Add(1)
				c.node.emitEvent(EventOverload, c.peerName, 0, "max peers reached on outbound dial")
				return nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Peer: c.peerName, Err: ErrCapacity}
			}
			c.hasSlot = true
		}
		c.dialing = true
		c.node.handshakingOutbound.Add(1)
		dialCtx, cancel := context.WithCancel(c.node.lifeCtx)
		c.dialCancel = cancel
		go c.runDial(dialCtx)
	}
	waitCh := make(chan dialResult, 1)
	c.waiters = append(c.waiters, waitCh)
	c.mu.Unlock()

	select {
	case res := <-waitCh:
		return res.session, res.err
	case <-ctx.Done():
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Peer: c.peerName, Err: ctx.Err()}
	}
}

func (c *peerCoord) runDial(dialCtx context.Context) {
	defer c.node.handshakingOutbound.Add(-1)

	dialTimeoutCtx, cancelTimeout := context.WithTimeout(dialCtx, 10*time.Second)
	defer cancelTimeout()

	s, err := c.dialOnce(dialTimeoutCtx)

	var releaseSlot bool
	c.mu.Lock()
	if err == nil {
		if c.active != nil && !c.active.isClosed() {
			// Inbound simultaneous winner already installed an active session!
			_ = s.conn.Close()
			s = c.active
		} else {
			c.active = s
			s.start()
			c.node.emitEvent(EventPeerUp, s.peerName, s.gen, "outbound")
		}
	} else if c.active != nil && !c.active.isClosed() && errors.Is(err, dist.ErrSimultaneousReject) {
		// Peer rejected our simultaneous dial with "nok" because its inbound dial won!
		s = c.active
		err = nil
	}
	waiters := c.waiters
	c.waiters = nil
	c.dialing = false
	c.dialCancel = nil
	if (c.active == nil || c.active.isClosed()) && c.hasSlot {
		c.active = nil
		c.hasSlot = false
		releaseSlot = true
	}
	c.mu.Unlock()

	if releaseSlot {
		c.node.releasePeerSlot()
	}
	for _, w := range waiters {
		w <- dialResult{session: s, err: err}
	}
}

func (c *peerCoord) dialOnce(ctx context.Context) (*peerSession, error) {
	alive, host, err := epmd.SplitNodeName(c.peerName)
	if err != nil {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: c.peerName, Err: err}
	}
	if !c.node.isPeerAllowed(c.peerName) {
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Peer: c.peerName, Err: ErrPeerNotAllowed}
	}

	var addr string
	tlsCfg := c.node.outboundTLSFor(c.peerName, host)
	if route, ok := c.node.cfg.StaticRoutes[c.peerName]; ok {
		addr = route.Addr
		if route.TLS != nil {
			tlsCfg = cloneClientTLS(route.TLS, host)
		}
	} else {
		epmdAddr := c.node.epmdAddrForHost(host)
		info, err := epmd.LookupPort(ctx, epmdAddr, alive)
		if err != nil {
			return nil, &Error{Stage: StageDiscovery, Outcome: OutcomeNotSent, Peer: c.peerName, Err: err}
		}
		addr = net.JoinHostPort(host, strconv.Itoa(int(info.Port)))
	}

	var rawConn net.Conn
	var dialer net.Dialer
	if tlsCfg != nil {
		rawConn, err = (&tls.Dialer{NetDialer: &dialer, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		rawConn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Peer: c.peerName, Err: err}
	}

	inCookie, outCookie := c.node.cookiesForPeer(c.peerName)
	pktSize := dist.PacketSizeTCPHandshake
	if tlsCfg != nil {
		pktSize = dist.PacketSizeTLSHandshake
	}

	res, err := dist.HandshakeClient(ctx, rawConn, dist.ClientHandshakeConfig{
		LocalName:        c.node.cfg.Name,
		LocalCreation:    c.node.creation,
		LocalFlags:       dist.DefaultFlags(c.node.cfg.Published),
		ExpectedPeerName: c.peerName,
		InCookie:         inCookie,
		OutCookie:        outCookie,
		PacketSize:       pktSize,
		ConfirmAlive: func() bool {
			c.mu.Lock()
			hasLive := c.active != nil && !c.active.isClosed()
			c.mu.Unlock()
			return !hasLive
		},
	})
	if err != nil {
		_ = rawConn.Close()
		c.node.handshakeFailures.Add(1)
		c.node.emitEvent(EventHandshakeFailed, c.peerName, 0, err.Error())
		return nil, &Error{Stage: StageHandshake, Outcome: OutcomeNotSent, Peer: c.peerName, Err: err}
	}

	gen := c.node.nextSessionGen.Add(1)
	return newPeerSession(c.node, res, gen, tlsCfg != nil), nil
}

func (c *peerCoord) detachSession(s *peerSession) bool {
	var releaseSlot bool
	var detached bool
	c.mu.Lock()
	if c.active == s {
		c.active = nil
		detached = true
		if !c.dialing && c.hasSlot {
			c.hasSlot = false
			releaseSlot = true
		}
	}
	c.mu.Unlock()
	if releaseSlot {
		c.node.releasePeerSlot()
	}
	return detached
}

func cloneClientTLS(base *tls.Config, defaultHost string) *tls.Config {
	if base == nil {
		return nil
	}
	cfg := base.Clone()
	if cfg.ServerName == "" && !cfg.InsecureSkipVerify {
		cfg.ServerName = defaultHost
	}
	return cfg
}
