package erlang

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sllt/tao/erlang/etf"
	"github.com/sllt/tao/erlang/internal/dist"
	"github.com/sllt/tao/erlang/internal/epmd"
)

const (
	numProcShards  = 64
	numAliasShards = 64
)

var reservedNames = map[etf.Atom]struct{}{
	"net_kernel":         {},
	"rex":                {},
	"global_name_server": {},
	"init":               {},
	"erl_prim_loader":    {},
	"user":               {},
	"auth":               {},
	"code_server":        {},
	"kernel_sup":         {},
}

type procEntry struct {
	ep     *Endpoint
	client *Client
}

type procShard struct {
	mu    sync.RWMutex
	procs map[PID]procEntry
}

type aliasEntry struct {
	client *Client
	pc     *pendingCall
}

type aliasShard struct {
	mu      sync.RWMutex
	aliases map[Ref]aliasEntry
}

// Node is a standalone Erlang/OTP distribution v6 node incarnation (§4.1, §6).
type Node struct {
	cfg                Config
	limits             Limits
	nameAtom           etf.Atom
	aliveName          string
	hostName           string
	creation           uint32
	releaseCreation    func()
	tickTime           time.Duration
	defaultCallTimeout time.Duration
	ids                *idAllocator

	ln      net.Listener
	epmdReg *epmd.Registration

	lifeCtx      context.Context
	lifeCancel   context.CancelFunc
	doneCh       chan struct{}
	stopping     atomic.Bool
	shutdownMu   sync.Mutex
	shutdownErr  error
	shutdownDone bool

	allowPeers  map[string]struct{}
	peerCookies map[string]PeerCookie
	peerTLS     map[string]*tls.Config

	peersMu             sync.RWMutex
	coords              map[string]*peerCoord
	peerSlots           atomic.Int32
	handshakingInbound  atomic.Int32
	handshakingOutbound atomic.Int32
	nextSessionGen      atomic.Uint64

	procCount     atomic.Int32
	procShards    [numProcShards]procShard
	aliasCount    atomic.Int32
	aliasShards   [numAliasShards]aliasShard
	relationCount atomic.Int32
	outboundCalls atomic.Int32
	inboundCalls  atomic.Int32

	namesMu   sync.RWMutex
	names     map[etf.Atom]*Endpoint
	netKernel *Endpoint

	totalSendQueued   atomic.Int64
	encodedBytesTotal atomic.Uint64
	decodedBytesTotal atomic.Uint64
	handshakeFailures atomic.Uint64
	protocolErrors    atomic.Uint64
	lateReplies       atomic.Uint64
	overloadDrops     atomic.Uint64
	unknownOutcomes   atomic.Uint64
	droppedEvents     atomic.Uint64
	epmdLost          atomic.Bool

	eventsCh chan DiagnosticEvent
	acceptWG sync.WaitGroup
}

// Start opens the distribution listener, acquires an EPMD or generated Creation,
// exports the built-in net_kernel responder, and begins serving Erlang v6 connections (§4.1, §6.1).
func Start(ctx context.Context, cfg Config) (*Node, error) {
	if ctx == nil {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	alive, host, err := epmd.SplitNodeName(cfg.Name)
	if err != nil {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: err}
	}
	if cfg.Listen == "" {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("erlang: Listen address is required")}
	}
	if cfg.Cookie == "" {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("erlang: Cookie is required")}
	}

	var allowPeers map[string]struct{}
	if len(cfg.AllowedPeers) > 0 {
		allowPeers = make(map[string]struct{}, len(cfg.AllowedPeers))
		for _, p := range cfg.AllowedPeers {
			if _, _, err := epmd.SplitNodeName(p); err != nil {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: err}
			}
			allowPeers[p] = struct{}{}
		}
	}

	var peerCookies map[string]PeerCookie
	if len(cfg.PeerCookies) > 0 {
		peerCookies = make(map[string]PeerCookie, len(cfg.PeerCookies))
		for p, pc := range cfg.PeerCookies {
			if _, _, err := epmd.SplitNodeName(p); err != nil {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: err}
			}
			peerCookies[p] = pc
		}
	}

	var peerTLS map[string]*tls.Config
	if len(cfg.PeerTLS) > 0 {
		peerTLS = make(map[string]*tls.Config, len(cfg.PeerTLS))
		for p, tc := range cfg.PeerTLS {
			if _, _, err := epmd.SplitNodeName(p); err != nil {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: err}
			}
			if tc != nil {
				peerTLS[p] = tc.Clone()
			}
		}
	}

	if len(cfg.StaticRoutes) > 0 {
		routes := make(map[string]Route, len(cfg.StaticRoutes))
		for p, r := range cfg.StaticRoutes {
			if _, _, err := epmd.SplitNodeName(p); err != nil {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: err}
			}
			if r.Addr == "" {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: errors.New("erlang: empty static route address")}
			}
			if r.TLS != nil {
				r.TLS = r.TLS.Clone()
			}
			routes[p] = r
		}
		cfg.StaticRoutes = routes
	}
	if cfg.TLS != nil {
		cfg.TLS = cfg.TLS.Clone()
	}

	limits := cfg.Limits.normalize()
	tickTime := cfg.TickTime
	if tickTime <= 0 {
		tickTime = DefaultTickTime
	}
	callTimeout := cfg.DefaultCallTimeout
	if callTimeout <= 0 {
		callTimeout = DefaultCallTimeout
	}

	// Step 1: Open listener first so the actual port is bound before EPMD registration (§6.1).
	var lc net.ListenConfig
	rawLn, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Err: err}
	}
	tcpAddr, ok := rawLn.Addr().(*net.TCPAddr)
	if !ok || tcpAddr.Port <= 0 || tcpAddr.Port > 65535 {
		_ = rawLn.Close()
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Err: errors.New("erlang: listener did not bind a valid TCP port")}
	}
	ln := rawLn
	if cfg.TLS != nil {
		ln = tls.NewListener(rawLn, cfg.TLS)
	}

	// Step 2: Acquire Creation via external EPMD ALIVE2_REQ or DisableEPMD generator (§6.1).
	var creation uint32
	var releaseCreation func()
	var epmdReg *epmd.Registration
	if cfg.DisableEPMD {
		creation, releaseCreation, err = acquireGeneratedCreation(cfg.Name)
		if err != nil {
			_ = ln.Close()
			return nil, &Error{Stage: StageDiscovery, Outcome: OutcomeNotSent, Err: err}
		}
	} else {
		epmdAddr := cfg.EPMDAddr
		if epmdAddr == "" {
			epmdAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(epmd.DefaultPort))
		}
		epmdReg, err = epmd.Register(ctx, epmdAddr, alive, uint16(tcpAddr.Port), !cfg.Published, nil)
		if err != nil {
			_ = ln.Close()
			return nil, &Error{Stage: StageDiscovery, Outcome: OutcomeNotSent, Err: err}
		}
		creation = epmdReg.Creation()
	}

	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:                cfg,
		limits:             limits,
		nameAtom:           etf.Atom(cfg.Name),
		aliveName:          alive,
		hostName:           host,
		creation:           creation,
		releaseCreation:    releaseCreation,
		tickTime:           tickTime,
		defaultCallTimeout: callTimeout,
		ids:                newIDAllocator(etf.Atom(cfg.Name), creation),
		ln:                 ln,
		epmdReg:            epmdReg,
		lifeCtx:            lifeCtx,
		lifeCancel:         lifeCancel,
		doneCh:             make(chan struct{}),
		allowPeers:         allowPeers,
		peerCookies:        peerCookies,
		peerTLS:            peerTLS,
		coords:             make(map[string]*peerCoord),
		names:              make(map[etf.Atom]*Endpoint),
		eventsCh:           make(chan DiagnosticEvent, limits.MaxDiagnosticEvents),
	}
	for i := range n.procShards {
		n.procShards[i].procs = make(map[PID]procEntry)
	}
	for i := range n.aliasShards {
		n.aliasShards[i].aliases = make(map[Ref]aliasEntry)
	}

	n.initNetKernel()

	n.acceptWG.Add(1)
	go n.acceptLoop()

	if epmdReg != nil {
		go n.watchEPMDLoss()
	}

	return n, nil
}

func (n *Node) initNetKernel() {
	nk := &Endpoint{
		node:     n,
		pid:      n.ids.systemPID(),
		name:     "net_kernel",
		monitors: make(map[inboundMonKey]inboundMonRecord),
		links:    make(map[PID]linkRecord),
		onDeliver: func(ep *Endpoint, msg InboundDelivery) {
			if msg.Kind != InboundGenCall {
				return
			}
			// OTP net_adm:ping sends {'$gen_call', From, {is_auth, PeerNode}} -> reply 'yes' (§8.4).
			if tup, ok := msg.Term.(etf.Tuple); ok && len(tup) == 2 && tup[0] == etf.Atom("is_auth") {
				if _, isAtom := tup[1].(etf.Atom); isAtom {
					_ = ep.SendReply(msg.ReplyTo, etf.Atom("yes"))
					return
				}
			}
			_ = ep.SendReply(msg.ReplyTo, etf.Tuple{
				etf.Atom("error"),
				etf.Tuple{etf.Atom("tao"), etf.Atom("unsupported")},
			})
		},
	}
	n.netKernel = nk
	n.names["net_kernel"] = nk
	shard := &n.procShards[nk.pid.ID&(numProcShards-1)]
	shard.procs[nk.pid] = procEntry{ep: nk}
}

func (n *Node) watchEPMDLoss() {
	select {
	case <-n.doneCh:
		return
	case <-n.epmdReg.Lost():
		if n.stopping.Load() {
			return
		}
		n.epmdLost.Store(true)
		n.emitEvent(EventEPMDLost, "", 0, "epmd registration connection closed; shutting down incarnation")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = n.shutdownWithReason(ctx, etf.Atom("noconnection"))
	}
}

// Name returns the full Erlang node name ("alive@host").
func (n *Node) Name() string { return n.cfg.Name }

// Creation returns the 32-bit node Creation of this incarnation.
func (n *Node) Creation() uint32 { return n.creation }

// Addr returns the bound listener network address.
func (n *Node) Addr() net.Addr { return n.ln.Addr() }

// Done returns a channel that is closed once this Node incarnation has shut down
// (including automatic offline transition on EPMD registration loss).
func (n *Node) Done() <-chan struct{} { return n.doneCh }

// Events returns a bounded channel of non-sensitive operational diagnostic events (§12.3).
func (n *Node) Events() <-chan DiagnosticEvent { return n.eventsCh }

// Connect establishes (or reuses) an authenticated v6 connection to peerName.
func (n *Node) Connect(ctx context.Context, peerName string) error {
	if ctx == nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: errors.New("nil context")}
	}
	if _, _, err := epmd.SplitNodeName(peerName); err != nil {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: peerName, Err: err}
	}
	if peerName == n.cfg.Name {
		return &Error{Stage: StageValidate, Outcome: OutcomeUnsupported, Peer: peerName, Err: errors.New("erlang: cannot connect node to itself")}
	}
	_, err := n.ensureSession(ctx, peerName)
	return err
}

// NewClient allocates a new virtual Erlang Client process on n.
func (n *Node) NewClient() (*Client, error) {
	return n.NewClientWithOptions(ClientOptions{})
}

// NewClientWithOptions allocates a new virtual Erlang Client process with explicit options.
func (n *Node) NewClientWithOptions(opts ClientOptions) (*Client, error) {
	if n.stopping.Load() {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
	}
	if !n.tryAcquireProcess() {
		return nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Err: ErrCapacity}
	}
	pid, err := n.ids.nextPID()
	if err != nil {
		n.procCount.Add(-1)
		return nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Err: err}
	}

	var exitCh chan ExitEvent
	if opts.TrapExit {
		exitCh = make(chan ExitEvent, 64)
	}
	c := &Client{
		node:       n,
		pid:        pid,
		trapExit:   opts.TrapExit,
		exitCh:     exitCh,
		calls:      make(map[Ref]*pendingCall),
		monitors:   make(map[Ref]*outboundMonRecord),
		inMonitors: make(map[inboundMonKey]inboundMonRecord),
		links:      make(map[PID]linkRecord),
	}

	shard := &n.procShards[pid.ID&(numProcShards-1)]
	shard.mu.Lock()
	if n.stopping.Load() {
		shard.mu.Unlock()
		n.procCount.Add(-1)
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
	}
	shard.procs[pid] = procEntry{client: c}
	shard.mu.Unlock()
	return c, nil
}

// Export allocates a virtual Erlang Endpoint process and optionally registers it under opts.Name (§4.2, §8.3).
func (n *Node) Export(opts EndpointOptions) (*Endpoint, error) {
	if n.stopping.Load() {
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
	}
	var nameAtom etf.Atom
	if opts.Name != "" {
		nameAtom = etf.Atom(opts.Name)
		if err := etf.ValidateAtom(nameAtom); err != nil {
			return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: err}
		}
		if _, reserved := reservedNames[nameAtom]; reserved {
			return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrReservedName}
		}
	}

	var allowPeer map[string]struct{}
	if len(opts.AllowedPeers) > 0 {
		allowPeer = make(map[string]struct{}, len(opts.AllowedPeers))
		for _, p := range opts.AllowedPeers {
			if _, _, err := epmd.SplitNodeName(p); err != nil {
				return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Peer: p, Err: err}
			}
			allowPeer[p] = struct{}{}
		}
	}

	if !n.tryAcquireProcess() {
		return nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Err: ErrCapacity}
	}
	pid, err := n.ids.nextPID()
	if err != nil {
		n.procCount.Add(-1)
		return nil, &Error{Stage: StageQuota, Outcome: OutcomeNotSent, Err: err}
	}

	ep := &Endpoint{
		node:      n,
		pid:       pid,
		name:      nameAtom,
		trapExit:  opts.TrapExit,
		allowPeer: allowPeer,
		onDeliver: opts.OnDeliver,
		onTerm:    opts.OnTerminate,
		monitors:  make(map[inboundMonKey]inboundMonRecord),
		links:     make(map[PID]linkRecord),
	}

	if nameAtom != "" {
		n.namesMu.Lock()
		if n.stopping.Load() {
			n.namesMu.Unlock()
			n.procCount.Add(-1)
			return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
		}
		if _, exists := n.names[nameAtom]; exists {
			n.namesMu.Unlock()
			n.procCount.Add(-1)
			return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrDuplicateExport}
		}
		n.names[nameAtom] = ep
		n.namesMu.Unlock()
	}

	shard := &n.procShards[pid.ID&(numProcShards-1)]
	shard.mu.Lock()
	if n.stopping.Load() {
		shard.mu.Unlock()
		if nameAtom != "" {
			n.detachEndpointName(ep)
		}
		n.procCount.Add(-1)
		return nil, &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
	}
	shard.procs[pid] = procEntry{ep: ep}
	shard.mu.Unlock()
	return ep, nil
}

// Shutdown stops accepting new work, drains already-admitted replies/writes up to ctx,
// emits terminal signals for active endpoints/clients, closes peer sessions, and closes EPMD registration (§6.4).
func (n *Node) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return n.shutdownWithReason(ctx, etf.Atom("shutdown"))
}

// Close immediately tears down the Node incarnation without waiting to drain in-flight calls.
func (n *Node) Close() error {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = n.shutdownWithReason(ctx, etf.Atom("shutdown"))
	return nil
}

func (n *Node) shutdownWithReason(ctx context.Context, termReason etf.Term) error {
	n.shutdownMu.Lock()
	if n.shutdownDone {
		err := n.shutdownErr
		n.shutdownMu.Unlock()
		return err
	}
	n.stopping.Store(true)
	_ = n.ln.Close()

	// Step 1: Stop new inbound message admissions on exported endpoints while letting already-admitted
	// inbound calls finish sending replies before terminal DOWNs (§6.4, §9.3).
	endpoints, clients := n.snapshotUserProcesses()
	for _, ep := range endpoints {
		ep.BeginShutdown()
	}

	drainErr := n.waitForDrain(ctx, true)

	// Step 2: Terminate exported endpoints, built-in net_kernel, and clients so terminal
	// MONITOR_P_EXIT / EXIT signals are enqueued onto still-open peer sessions.
	for _, ep := range endpoints {
		ep.Terminate(termReason)
	}
	if n.netKernel != nil {
		n.netKernel.Terminate(termReason)
	}
	for _, cl := range clients {
		cl.terminate(termReason, false)
	}

	// Step 3: Flush terminal signals to sockets within any remaining ctx budget.
	if drainErr == nil {
		drainErr = n.waitForDrain(ctx, false)
	}

	// Step 4: Cancel dials, close all active peer sessions, wait for goroutines, close EPMD last.
	n.lifeCancel()
	sessions := n.activeSessionsSnapshot()
	for _, s := range sessions {
		s.closeWithError(ErrNodeClosed)
	}
	for _, s := range sessions {
		s.wg.Wait()
	}
	n.acceptWG.Wait()

	if n.epmdReg != nil {
		_ = n.epmdReg.Close()
	}
	if n.releaseCreation != nil {
		n.releaseCreation()
	}

	close(n.doneCh)
	n.shutdownErr = drainErr
	n.shutdownDone = true
	n.shutdownMu.Unlock()
	return drainErr
}

func (n *Node) waitForDrain(ctx context.Context, includeInboundCalls bool) error {
	for {
		busy := includeInboundCalls && n.inboundCalls.Load() > 0
		if !busy {
			for _, s := range n.activeSessionsSnapshot() {
				if s.hasPendingWrites() {
					busy = true
					break
				}
			}
		}
		if !busy {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		timer := time.NewTimer(2 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (n *Node) snapshotUserProcesses() ([]*Endpoint, []*Client) {
	var eps []*Endpoint
	var cls []*Client
	for i := range n.procShards {
		shard := &n.procShards[i]
		shard.mu.RLock()
		for _, entry := range shard.procs {
			if entry.ep != nil && entry.ep != n.netKernel {
				eps = append(eps, entry.ep)
			}
			if entry.client != nil {
				cls = append(cls, entry.client)
			}
		}
		shard.mu.RUnlock()
	}
	return eps, cls
}

// Stats returns a point-in-time snapshot of node, process, and peer metrics (§12.3).
func (n *Node) Stats() Stats {
	sessions := n.activeSessionsSnapshot()
	peers := make([]PeerStats, 0, len(sessions))
	var reassemblyTotal int64
	for _, s := range sessions {
		s.writeMu.Lock()
		sendQ := s.sendQueuedBytes
		s.writeMu.Unlock()
		reasm := s.reassemblyB.Load()
		reassemblyTotal += reasm
		peers = append(peers, PeerStats{
			Name:            s.peerName,
			Generation:      s.gen,
			Creation:        s.remoteCreation,
			NegotiatedFlags: s.negotiatedFlags,
			SendQueuedBytes: sendQ,
			ReassemblyBytes: reasm,
			OutboundCalls:   int(s.outboundCalls.Load()),
			InboundCalls:    int(s.inboundCalls.Load()),
		})
	}

	n.namesMu.RLock()
	exportedNames := len(n.names)
	if _, hasNK := n.names["net_kernel"]; hasNK && exportedNames > 0 {
		exportedNames--
	}
	n.namesMu.RUnlock()

	return Stats{
		NodeName:          n.cfg.Name,
		Creation:          n.creation,
		ActivePeers:       len(sessions),
		HandshakingPeers:  int(n.handshakingInbound.Load() + n.handshakingOutbound.Load()),
		Processes:         int(n.procCount.Load()),
		ExportedNames:     exportedNames,
		ActiveAliases:     int(n.aliasCount.Load()),
		Relations:         int(n.relationCount.Load()),
		OutboundPending:   int(n.outboundCalls.Load()),
		InboundPending:    int(n.inboundCalls.Load()),
		SendQueuedBytes:   n.totalSendQueued.Load(),
		ReassemblyBytes:   reassemblyTotal,
		EncodedBytesTotal: n.encodedBytesTotal.Load(),
		DecodedBytesTotal: n.decodedBytesTotal.Load(),
		HandshakeFailures: n.handshakeFailures.Load(),
		ProtocolErrors:    n.protocolErrors.Load(),
		LateReplies:       n.lateReplies.Load(),
		OverloadDrops:     n.overloadDrops.Load(),
		UnknownOutcomes:   n.unknownOutcomes.Load(),
		DroppedEvents:     n.droppedEvents.Load(),
		EPMDLost:          n.epmdLost.Load(),
		Peers:             peers,
	}
}

func (n *Node) acceptLoop() {
	defer n.acceptWG.Done()
	for {
		conn, err := n.ln.Accept()
		if err != nil {
			if n.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if n.stopping.Load() {
			_ = conn.Close()
			return
		}
		if !n.tryAcquirePeerSlot() {
			n.overloadDrops.Add(1)
			n.emitEvent(EventOverload, "", 0, "max peers reached on inbound accept")
			_ = conn.Close()
			continue
		}
		n.handshakingInbound.Add(1)
		n.acceptWG.Add(1)
		go n.handleInboundConn(conn)
	}
}

func (n *Node) handleInboundConn(rawConn net.Conn) {
	defer n.acceptWG.Done()
	defer n.handshakingInbound.Add(-1)

	transferredSlot := false
	defer func() {
		if !transferredSlot {
			n.releasePeerSlot()
		}
	}()

	hsCtx, cancel := context.WithTimeout(n.lifeCtx, 10*time.Second)
	defer cancel()

	pktSize := dist.PacketSizeTCPHandshake
	isTLS := n.cfg.TLS != nil
	if isTLS {
		pktSize = dist.PacketSizeTLSHandshake
	}

	var coord *peerCoord
	res, err := dist.HandshakeServer(hsCtx, rawConn, dist.ServerHandshakeConfig{
		LocalName:     n.cfg.Name,
		LocalCreation: n.creation,
		LocalFlags:    dist.DefaultFlags(n.cfg.Published),
		PacketSize:    pktSize,
		AllowPeer: func(peerName string) bool {
			return peerName != n.cfg.Name && n.isPeerAllowed(peerName)
		},
		CookiesFor: n.cookiesForPeer,
		Arbitrate: func(peerName string, _ uint32, _ uint64) dist.InboundDecision {
			coord = n.getOrCreateCoord(peerName)
			return coord.arbitrateInbound()
		},
		CommitAuthenticated: func(peerName string, _ uint32, _ uint64, _ bool) error {
			if n.stopping.Load() {
				return ErrNodeClosed
			}
			if coord == nil {
				coord = n.getOrCreateCoord(peerName)
			}
			return nil
		},
	})
	if err != nil {
		_ = rawConn.Close()
		if !errors.Is(err, dist.ErrAliveDeclined) {
			n.handshakeFailures.Add(1)
			peerName := ""
			if coord != nil {
				peerName = coord.peerName
			}
			n.emitEvent(EventHandshakeFailed, peerName, 0, err.Error())
		}
		return
	}

	if coord == nil {
		coord = n.getOrCreateCoord(res.PeerName)
	}
	gen := n.nextSessionGen.Add(1)
	sess := newPeerSession(n, res, gen, isTLS)
	transferred, commitErr := coord.commitInbound(sess)
	if commitErr != nil {
		_ = rawConn.Close()
		n.handshakeFailures.Add(1)
		n.emitEvent(EventHandshakeFailed, res.PeerName, gen, commitErr.Error())
		return
	}
	transferredSlot = transferred
}

func (n *Node) getOrCreateCoord(peerName string) *peerCoord {
	n.peersMu.RLock()
	c, ok := n.coords[peerName]
	n.peersMu.RUnlock()
	if ok {
		return c
	}
	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	if c, ok = n.coords[peerName]; ok {
		return c
	}
	c = &peerCoord{
		node:     n,
		peerName: peerName,
	}
	n.coords[peerName] = c
	return c
}

func (n *Node) ensureSession(ctx context.Context, peerName string) (*peerSession, error) {
	if !n.isPeerAllowed(peerName) {
		return nil, &Error{Stage: StageConnect, Outcome: OutcomeNotSent, Peer: peerName, Err: ErrPeerNotAllowed}
	}
	coord := n.getOrCreateCoord(peerName)
	return coord.ensureConnected(ctx)
}

func (n *Node) lookupActiveSession(peerName string) *peerSession {
	n.peersMu.RLock()
	coord := n.coords[peerName]
	n.peersMu.RUnlock()
	if coord == nil {
		return nil
	}
	s := coord.currentSession()
	if s != nil && s.isClosed() {
		return nil
	}
	return s
}

func (n *Node) lookupAuthorizedReplySession(peerName string) *peerSession {
	if !n.isPeerAllowed(peerName) {
		return nil
	}
	return n.lookupActiveSession(peerName)
}

func (n *Node) activeSessionsSnapshot() []*peerSession {
	n.peersMu.RLock()
	coords := make([]*peerCoord, 0, len(n.coords))
	for _, c := range n.coords {
		coords = append(coords, c)
	}
	n.peersMu.RUnlock()

	var out []*peerSession
	for _, c := range coords {
		if s := c.currentSession(); s != nil && !s.isClosed() {
			out = append(out, s)
		}
	}
	return out
}

func (n *Node) onSessionClosed(s *peerSession, err error) {
	n.peersMu.RLock()
	coord := n.coords[s.peerName]
	n.peersMu.RUnlock()
	if coord != nil {
		coord.detachSession(s)
	}

	detail := ""
	if err != nil {
		detail = err.Error()
	}
	n.emitEvent(EventPeerDown, s.peerName, s.gen, detail)

	// Notify all virtual processes holding relations or pending calls on (s.peerName, s.gen) outside locks.
	eps, cls := n.snapshotUserProcesses()
	for _, ep := range eps {
		ep.onPeerDown(s.peerName, s.gen)
	}
	if n.netKernel != nil {
		n.netKernel.onPeerDown(s.peerName, s.gen)
	}
	for _, cl := range cls {
		cl.onPeerDown(s.peerName, s.gen)
	}
}

func (n *Node) dispatchInbound(sess *peerSession, msg dist.InboundMessage) error {
	ctrl := msg.Control
	switch ctrl.Op {
	case dist.OpGroupLeader:
		// Recognized and safely ignored in v1 (§7.3).
		return nil

	case dist.OpLink:
		if ctrl.ToPID.Creation != n.creation {
			sess.enqueueControl(dist.ControlMessage{
				Op:      dist.OpExit,
				FromPID: ctrl.ToPID,
				HasFrom: true,
				ToPID:   ctrl.FromPID,
				Reason:  etf.Atom("noproc"),
			})
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			sess.enqueueControl(dist.ControlMessage{
				Op:      dist.OpExit,
				FromPID: ctrl.ToPID,
				HasFrom: true,
				ToPID:   ctrl.FromPID,
				Reason:  etf.Atom("noproc"),
			})
			return nil
		}
		var alive, quotaExceeded bool
		if entry.ep != nil {
			alive, quotaExceeded = entry.ep.handleInboundLink(sess, ctrl.FromPID)
		} else if entry.client != nil {
			alive, quotaExceeded = entry.client.handleInboundLink(sess, ctrl.FromPID)
		}
		if quotaExceeded {
			n.overloadDrops.Add(1)
			n.emitEvent(EventOverload, sess.peerName, sess.gen, "relation quota exhausted on LINK; closing session")
			sess.closeWithError(ErrCapacity)
			return nil
		}
		if !alive {
			sess.enqueueControl(dist.ControlMessage{
				Op:      dist.OpExit,
				FromPID: ctrl.ToPID,
				HasFrom: true,
				ToPID:   ctrl.FromPID,
				Reason:  etf.Atom("noproc"),
			})
		}
		return nil

	case dist.OpUnlinkID:
		// Stale Creation: explicit v1 tolerance policy drops without ACK and without closing peer (§7.3).
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			// Current Creation dead PID MUST still answer UNLINK_ID_ACK (§7.3, §10.2).
			sess.enqueueControl(dist.ControlMessage{
				Op:       dist.OpUnlinkIDAck,
				UnlinkID: ctrl.UnlinkID,
				FromPID:  ctrl.ToPID,
				HasFrom:  true,
				ToPID:    ctrl.FromPID,
			})
			return nil
		}
		if entry.ep != nil {
			entry.ep.handleInboundUnlinkID(sess, ctrl.UnlinkID, ctrl.FromPID)
		} else if entry.client != nil {
			entry.client.handleInboundUnlinkID(sess, ctrl.UnlinkID, ctrl.FromPID)
		}
		return nil

	case dist.OpUnlinkIDAck:
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			return nil
		}
		if entry.ep != nil {
			entry.ep.handleInboundUnlinkIDAck(sess, ctrl.UnlinkID, ctrl.FromPID)
		} else if entry.client != nil {
			entry.client.handleInboundUnlinkIDAck(sess, ctrl.UnlinkID, ctrl.FromPID)
		}
		return nil

	case dist.OpMonitorP:
		var entry procEntry
		var found bool
		switch watched := ctrl.ToProc.(type) {
		case PID:
			if watched.Creation == n.creation {
				entry, found = n.lookupProcess(watched)
			}
		case etf.Atom:
			if ep := n.lookupNamedEndpoint(watched); ep != nil {
				entry = procEntry{ep: ep}
				found = true
			}
		}
		if !found {
			// Non-existent target answers MONITOR_P_EXIT(noproc) preserving raw Watched term (§10.1).
			sess.enqueueControl(dist.ControlMessage{
				Op:     dist.OpMonitorPExit,
				ToProc: ctrl.ToProc,
				ToPID:  ctrl.FromPID,
				Ref:    ctrl.Ref,
				Reason: etf.Atom("noproc"),
			})
			return nil
		}
		var alive, quotaExceeded bool
		if entry.ep != nil {
			alive, quotaExceeded = entry.ep.installInboundMonitor(sess, ctrl.FromPID, ctrl.ToProc, ctrl.Ref)
		} else if entry.client != nil {
			alive, quotaExceeded = entry.client.installInboundMonitor(sess, ctrl.FromPID, ctrl.ToProc, ctrl.Ref)
		}
		if quotaExceeded {
			// Quota exhaustion must NOT masquerade as noproc; close session (§10.1).
			n.overloadDrops.Add(1)
			n.emitEvent(EventOverload, sess.peerName, sess.gen, "relation quota exhausted on MONITOR_P; closing session")
			sess.closeWithError(ErrCapacity)
			return nil
		}
		if !alive {
			sess.enqueueControl(dist.ControlMessage{
				Op:     dist.OpMonitorPExit,
				ToProc: ctrl.ToProc,
				ToPID:  ctrl.FromPID,
				Ref:    ctrl.Ref,
				Reason: etf.Atom("noproc"),
			})
		}
		return nil

	case dist.OpDemonitorP:
		if pid, isPID := ctrl.ToProc.(PID); isPID && pid.Creation != n.creation {
			return nil
		}
		key := inboundMonKey{
			peerGen: sess.gen,
			watcher: ctrl.FromPID,
			ref:     ctrl.Ref,
		}
		if owner := sess.lookupInboundMonitor(key); owner != nil {
			owner.handleDemonitor(key, ctrl.ToProc)
		}
		return nil

	case dist.OpMonitorPExit, dist.OpPayloadMonitorPExit:
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if ok && entry.client != nil {
			entry.client.handleMonitorExit(sess.peerName, sess.gen, ctrl.ToProc, ctrl.Ref, ctrl.Reason)
		}
		return nil

	case dist.OpExit, dist.OpExitTT, dist.OpPayloadExit, dist.OpPayloadExitTT:
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			return nil
		}
		if entry.ep != nil {
			entry.ep.handleInboundExitSignal(sess, ctrl.FromPID, ctrl.Reason, false)
		} else if entry.client != nil {
			entry.client.handleInboundExitSignal(sess, ctrl.FromPID, ctrl.Reason, false)
		}
		return nil

	case dist.OpExit2, dist.OpExit2TT, dist.OpPayloadExit2, dist.OpPayloadExit2TT:
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			return nil
		}
		if entry.ep != nil {
			entry.ep.handleInboundExitSignal(sess, ctrl.FromPID, ctrl.Reason, true)
		} else if entry.client != nil {
			entry.client.handleInboundExitSignal(sess, ctrl.FromPID, ctrl.Reason, true)
		}
		return nil

	case dist.OpSend, dist.OpSendTT, dist.OpSendSender, dist.OpSendSenderTT:
		if ctrl.ToPID.Creation != n.creation {
			return nil
		}
		entry, ok := n.lookupProcess(ctrl.ToPID)
		if !ok {
			return nil
		}
		if entry.ep != nil {
			entry.ep.deliverInboundMessage(sess, msg)
		} else if entry.client != nil {
			entry.client.handleAliasOrPIDMessage(msg.Payload)
		}
		return nil

	case dist.OpRegSend, dist.OpRegSendTT:
		ep := n.lookupNamedEndpoint(ctrl.ToName)
		if ep == nil {
			return nil
		}
		ep.deliverInboundMessage(sess, msg)
		return nil

	case dist.OpAliasSend, dist.OpAliasSendTT:
		if ctrl.Ref.Creation != n.creation {
			n.lateReplies.Add(1)
			return nil
		}
		ae, ok := n.lookupAlias(ctrl.Ref)
		if !ok || ae.client == nil {
			n.lateReplies.Add(1)
			return nil
		}
		ae.client.handleAliasOrPIDMessage(msg.Payload)
		return nil

	default:
		return fmt.Errorf("erlang: unhandled control opcode %d", ctrl.Op)
	}
}

func (n *Node) lookupProcess(pid PID) (procEntry, bool) {
	shard := &n.procShards[pid.ID&(numProcShards-1)]
	shard.mu.RLock()
	entry, ok := shard.procs[pid]
	shard.mu.RUnlock()
	return entry, ok
}

func (n *Node) lookupNamedEndpoint(name etf.Atom) *Endpoint {
	n.namesMu.RLock()
	ep := n.names[name]
	n.namesMu.RUnlock()
	return ep
}

func (n *Node) publishEndpointName(ep *Endpoint, nameAtom etf.Atom) error {
	n.namesMu.Lock()
	defer n.namesMu.Unlock()
	if n.stopping.Load() {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrNodeClosed}
	}
	if _, exists := n.names[nameAtom]; exists {
		return &Error{Stage: StageValidate, Outcome: OutcomeNotSent, Err: ErrDuplicateExport}
	}
	n.names[nameAtom] = ep
	return nil
}

func (n *Node) detachEndpointName(ep *Endpoint) {
	name := ep.Name()
	if name == "" {
		return
	}
	n.namesMu.Lock()
	if n.names[name] == ep {
		delete(n.names, name)
	}
	n.namesMu.Unlock()
}

func (n *Node) unregisterEndpoint(ep *Endpoint, releasedRelations int) {
	shard := &n.procShards[ep.pid.ID&(numProcShards-1)]
	var removed bool
	shard.mu.Lock()
	if cur, ok := shard.procs[ep.pid]; ok && cur.ep == ep {
		delete(shard.procs, ep.pid)
		removed = true
	}
	shard.mu.Unlock()
	if removed && ep != n.netKernel {
		n.procCount.Add(-1)
	}
	if releasedRelations > 0 {
		n.releaseRelations(releasedRelations)
	}
}

func (n *Node) unregisterClient(c *Client, releasedRelations int) {
	shard := &n.procShards[c.pid.ID&(numProcShards-1)]
	var removed bool
	shard.mu.Lock()
	if cur, ok := shard.procs[c.pid]; ok && cur.client == c {
		delete(shard.procs, c.pid)
		removed = true
	}
	shard.mu.Unlock()
	if removed {
		n.procCount.Add(-1)
	}
	if releasedRelations > 0 {
		n.releaseRelations(releasedRelations)
	}
}

func (n *Node) registerAlias(ref Ref, c *Client, pc *pendingCall) {
	shard := &n.aliasShards[ref.ID[0]&(numAliasShards-1)]
	shard.mu.Lock()
	_, existed := shard.aliases[ref]
	shard.aliases[ref] = aliasEntry{client: c, pc: pc}
	shard.mu.Unlock()
	if !existed {
		n.aliasCount.Add(1)
	}
}

func (n *Node) revokeAlias(ref Ref) {
	shard := &n.aliasShards[ref.ID[0]&(numAliasShards-1)]
	shard.mu.Lock()
	_, existed := shard.aliases[ref]
	if existed {
		delete(shard.aliases, ref)
	}
	shard.mu.Unlock()
	if existed {
		n.aliasCount.Add(-1)
	}
}

func (n *Node) lookupAlias(ref Ref) (aliasEntry, bool) {
	shard := &n.aliasShards[ref.ID[0]&(numAliasShards-1)]
	shard.mu.RLock()
	ae, ok := shard.aliases[ref]
	shard.mu.RUnlock()
	return ae, ok
}

func (n *Node) tryAcquirePeerSlot() bool {
	return casBoundedIncrement(&n.peerSlots, int32(n.limits.MaxPeers))
}

func (n *Node) releasePeerSlot() {
	n.peerSlots.Add(-1)
}

func (n *Node) tryAcquireProcess() bool {
	return casBoundedIncrement(&n.procCount, int32(n.limits.MaxProcesses))
}

func (n *Node) tryAcquireRelation() bool {
	return casBoundedIncrement(&n.relationCount, int32(n.limits.MaxRelations))
}

func (n *Node) releaseRelations(delta int) {
	if delta > 0 {
		n.relationCount.Add(-int32(delta))
	}
}

func (n *Node) tryAcquireOutboundCall(sess *peerSession) bool {
	if !casBoundedIncrement(&n.outboundCalls, int32(n.limits.MaxNodePendingCalls)) {
		n.overloadDrops.Add(1)
		return false
	}
	if !casBoundedIncrement(&sess.outboundCalls, int32(n.limits.MaxPeerPendingCalls)) {
		n.outboundCalls.Add(-1)
		n.overloadDrops.Add(1)
		return false
	}
	return true
}

func (n *Node) releaseOutboundCall(sess *peerSession) {
	n.outboundCalls.Add(-1)
	if sess != nil {
		sess.outboundCalls.Add(-1)
	}
}

func (n *Node) tryAcquireInboundCall(sess *peerSession) bool {
	if !casBoundedIncrement(&n.inboundCalls, int32(n.limits.MaxNodePendingCalls)) {
		n.overloadDrops.Add(1)
		return false
	}
	if !casBoundedIncrement(&sess.inboundCalls, int32(n.limits.MaxPeerPendingCalls)) {
		n.inboundCalls.Add(-1)
		n.overloadDrops.Add(1)
		return false
	}
	return true
}

func (n *Node) releaseInboundCall(sess *peerSession) {
	n.inboundCalls.Add(-1)
	if sess != nil {
		sess.inboundCalls.Add(-1)
	}
}

func casBoundedIncrement(counter *atomic.Int32, limit int32) bool {
	for {
		cur := counter.Load()
		if cur >= limit {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (n *Node) isPeerAllowed(peerName string) bool {
	if len(n.allowPeers) == 0 {
		return true
	}
	_, ok := n.allowPeers[peerName]
	return ok
}

func (n *Node) cookiesForPeer(peerName string) (inCookie, outCookie string) {
	inCookie = n.cfg.Cookie
	outCookie = n.cfg.Cookie
	if pc, ok := n.peerCookies[peerName]; ok {
		if pc.InCookie != "" {
			inCookie = pc.InCookie
		}
		if pc.OutCookie != "" {
			outCookie = pc.OutCookie
		}
	}
	return inCookie, outCookie
}

func (n *Node) outboundTLSFor(peerName, host string) *tls.Config {
	if tc, ok := n.peerTLS[peerName]; ok && tc != nil {
		return cloneClientTLS(tc, host)
	}
	if n.cfg.TLS != nil {
		return cloneClientTLS(n.cfg.TLS, host)
	}
	return nil
}

func (n *Node) epmdAddrForHost(host string) string {
	if n.cfg.EPMDAddr != "" {
		if _, portStr, err := net.SplitHostPort(n.cfg.EPMDAddr); err == nil && portStr != "" {
			if host == n.hostName {
				return n.cfg.EPMDAddr
			}
			return net.JoinHostPort(host, portStr)
		}
	}
	return net.JoinHostPort(host, strconv.Itoa(epmd.DefaultPort))
}

func (n *Node) preflightOutboundTerm(term etf.Term) (etf.Term, int64, error) {
	owned, est, err := etf.SnapshotWithLimits(term, etf.Limits{
		MaxFrameBytes:  n.limits.MaxPayloadBytes,
		MaxBinaryBytes: n.limits.MaxPayloadBytes,
	})
	if err != nil {
		outcome := OutcomeNotSent
		if errors.Is(err, etf.ErrUnsupportedTerm) {
			outcome = OutcomeUnsupported
		}
		return nil, 0, &Error{Stage: StageEncode, Outcome: outcome, Err: err}
	}
	if est > int64(n.limits.MaxPayloadBytes) {
		return nil, 0, &Error{Stage: StageEncode, Outcome: OutcomeNotSent, Err: etf.ErrLimitExceeded}
	}
	return owned, est, nil
}

func (n *Node) emitEvent(kind DiagnosticKind, peer string, gen uint64, detail string) {
	ev := DiagnosticEvent{
		Time:       time.Now(),
		Kind:       kind,
		Peer:       peer,
		Generation: gen,
		Detail:     detail,
	}
	select {
	case n.eventsCh <- ev:
	default:
		n.droppedEvents.Add(1)
	}
}
