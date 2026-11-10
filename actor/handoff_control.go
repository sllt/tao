package actor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/sllt/tao/taonet"
)

const handoffControlRoute = "hive.actor/handoff-control.v2"

type handoffControl struct {
	Codec      string         `json:"codec,omitempty"`
	Caller     requestKey     `json:"caller,omitempty"`
	Address    string         `json:"address,omitempty"`
	Kind       string         `json:"kind"`
	Move       MoveID         `json:"move"`
	Source     RouteRecord    `json:"source"`
	Version    string         `json:"version,omitempty"`
	Mailbox    int            `json:"mailbox,omitempty"`
	Restart    RestartPolicy  `json:"restart,omitempty"`
	MailPolicy RestartMailbox `json:"mail_policy,omitempty"`
}
type handoffControlResult struct {
	Target RouteRecord `json:"target"`
	State  MoveState   `json:"state,omitempty"`
}

// A reservation owns a target PID and local name, but not the Directory route.
// It must remain unpublished until the shared Directory proves commitment.
type handoffReservation struct {
	canceled                        map[requestKey]bool
	mu                              sync.Mutex
	receiving, prepared, activating bool
	last                            uint64
	activate                        chan struct{}
	activated                       chan struct{}
	activationErr                   error
	decision                        chan struct{}
	decisionSet, committed          bool
	id                              MoveID
	source                          RouteRecord
	target                          RouteRecord
	process                         *process
	request                         handoffControl
}

func (w *network) handoffRequest(ctx context.Context, peer string, packet handoffControl) (handoffControlResult, error) {
	if packet.Kind == "reserve" {
		packet.Codec = w.registry.encoding
	}
	session, err := w.connection(ctx, peer, taonet.ClassControl)
	if err != nil {
		return handoffControlResult{}, err
	}
	body, err := json.Marshal(packet)
	if err != nil {
		return handoffControlResult{}, err
	}
	reply, err := session.Request(ctx, handoffControlRoute, taonet.Message{Body: body})
	if err != nil {
		return handoffControlResult{}, deliveryError(err, Unknown)
	}
	if len(reply.Body) > 16<<10 {
		return handoffControlResult{}, ErrInvalidMessage
	}
	var result handoffControlResult
	if err := json.Unmarshal(reply.Body, &result); err != nil {
		return result, ErrInvalidMessage
	}
	return result, nil
}

func (w *network) serveHandoffControl(ctx context.Context, request *taonet.Request) (taonet.Message, error) {
	if w.opts.Profile == IsolatedNetwork && request.Session().Info().Class != taonet.ClassControl {
		return taonet.Message{}, &taonet.StatusError{Code: rejectCode(ErrInvalidMessage)}
	}
	w.watchInbound(request.Session())
	var packet handoffControl
	if len(request.Message.Body) > 16<<10 || json.Unmarshal(request.Message.Body, &packet) != nil {
		return taonet.Message{}, &taonet.StatusError{Code: rejectCode(ErrInvalidMessage)}
	}
	peer := request.Session().Info().Remote
	if packet.Move == (MoveID{}) || packet.Source.Owner != string(peer.ID) || packet.Source.BootID != [16]byte(peer.Instance) || packet.Source.Validate() != nil {
		return taonet.Message{}, &taonet.StatusError{Code: rejectCode(ErrInvalidMessage)}
	}
	var result handoffControlResult
	var err error
	switch packet.Kind {
	case "reserve":
		result, err = w.reserveHandoff(ctx, packet)
	case "abort":
		result, err = w.abortHandoff(ctx, packet)
	case "activate":
		result, err = w.activateHandoff(ctx, packet)
	case "connect":
		result, err = w.connectHandoffTarget(ctx, packet)
	default:
		err = ErrInvalidMessage
	}
	if err != nil {
		return taonet.Message{}, &taonet.StatusError{Code: rejectCode(err), Message: wireErrorText(err)}
	}
	body, err := json.Marshal(result)
	return taonet.Message{Body: body}, err
}

func (w *network) reserveHandoff(ctx context.Context, packet handoffControl) (handoffControlResult, error) {
	n := w.node
	if packet.Codec != w.registry.encoding {
		return handoffControlResult{}, ErrCodecMismatch
	}
	if n.opts.Directory == nil {
		return handoffControlResult{}, ErrDirectoryUnavailable
	}
	if packet.Mailbox < 1 || packet.Mailbox > n.opts.MailboxSize || packet.Restart > Temporary || packet.MailPolicy > PreserveRef {
		return handoffControlResult{}, ErrCapacity
	}
	version := packet.Version
	if version == "" {
		version = packet.Source.Version
	}
	kind, ok := n.kinds[kindKey{packet.Source.Kind, version}]
	if !ok {
		return handoffControlResult{}, ErrHandoffUnsupported
	}
	w.mu.Lock()
	previous := w.handoffs[packet.Move]
	w.mu.Unlock()
	if previous != nil {
		if previous.request != packet {
			return handoffControlResult{}, ErrDirectoryConflict
		}
		return handoffControlResult{Target: previous.target}, nil
	}
	current, err := n.opts.Directory.Lookup(ctx, packet.Source.Path)
	if err != nil {
		return handoffControlResult{}, err
	}
	if current != packet.Source || current.State != RouteActive {
		return handoffControlResult{}, ErrDirectoryConflict
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.draining {
		return handoffControlResult{}, ErrClosed
	}
	if previous = w.handoffs[packet.Move]; previous != nil {
		if previous.request != packet {
			return handoffControlResult{}, ErrDirectoryConflict
		}
		return handoffControlResult{Target: previous.target}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopping.Load() || n.draining {
		return handoffControlResult{}, ErrClosed
	}
	if _, exists := n.routes[current.Path]; exists {
		return handoffControlResult{}, ErrNameTaken
	}
	if len(n.processes) >= n.opts.MaxActors || len(n.routes) >= n.opts.MaxActors {
		return handoffControlResult{}, ErrCapacity
	}
	pid, err := n.allocatePID()
	if err != nil {
		return handoffControlResult{}, err
	}
	opts := spawnOptions{handoff: true, kind: kind.Name, version: kind.Version, mailbox: packet.Mailbox, shutdown: n.opts.ShutdownTimeout, restart: packet.Restart, mailPolicy: packet.MailPolicy}
	q := newProcess(n, n.root, current.Path, pid, kind.Factory, opts)
	q.preparing, q.booted, q.running = true, true, false
	q.spec = &childSpec{factory: kind.Factory, opts: opts, path: current.Path, process: q}
	target := RouteRecord{Path: current.Path, Owner: pid.Node, BootID: pid.BootID, ProcessID: pid.ID, Kind: kind.Name, Version: kind.Version}
	n.processes[pid.ID] = q
	n.routes[current.Path] = route{q, ErrStarting}
	w.handoffs[packet.Move] = &handoffReservation{id: packet.Move, source: current, target: target, process: q, request: packet, decision: make(chan struct{})}
	return handoffControlResult{Target: target}, nil
}

func (p *process) connectHandoffCaller(ctx context.Context, op *handoffOperation, key requestKey) error {
	w := p.node.network
	w.mu.Lock()
	address := w.peers[op.options.TargetNode]
	w.mu.Unlock()
	packet := handoffControl{Kind: "connect", Move: op.result.MoveID, Source: op.result.Source, Caller: key, Address: address}
	if key.Caller == p.node.Name() {
		_, err := w.connectHandoffTarget(ctx, packet)
		return err
	}
	_, err := w.handoffRequest(ctx, key.Caller, packet)
	return err
}

func (w *network) connectHandoffTarget(ctx context.Context, packet handoffControl) (handoffControlResult, error) {
	if w.node.opts.Directory == nil || packet.Caller.Caller != w.node.Name() || packet.Caller.Boot != w.node.BootID() || len(packet.Address) > 1024 {
		return handoffControlResult{}, ErrInvalidMessage
	}
	w.mu.Lock()
	out := w.outgoing[packet.Caller.ID]
	var origin RouteRecord
	if out != nil && out.routing != nil {
		origin = out.routing.route
	}
	w.mu.Unlock()
	if out == nil || out.routing == nil || origin.Path != packet.Source.Path || origin.Epoch > packet.Source.Epoch {
		return handoffControlResult{}, ErrInvalidMessage
	}
	if origin.Owner != packet.Source.Owner || origin.BootID != packet.Source.BootID {
		if err := w.verifyMovedReply(ctx, packet.Source.MoveID, origin, packet.Source.Owner, packet.Source.BootID); err != nil {
			return handoffControlResult{}, err
		}
	}
	move, err := w.node.opts.Directory.ReadMove(ctx, packet.Move)
	if err != nil {
		return handoffControlResult{}, err
	}
	if move.Source != packet.Source || move.State != MovePreparing {
		return handoffControlResult{}, ErrDirectoryConflict
	}
	if move.Target.Owner == w.node.Name() {
		if move.Target.BootID != w.node.BootID() {
			return handoffControlResult{}, ErrStalePID
		}
	} else {
		w.mu.Lock()
		if packet.Address != "" {
			w.peers[move.Target.Owner] = packet.Address
		}
		w.mu.Unlock()
		session, err := w.connection(ctx, move.Target.Owner, taonet.ClassControl)
		if err != nil {
			return handoffControlResult{}, err
		}
		if [16]byte(session.Info().Remote.Instance) != move.Target.BootID {
			return handoffControlResult{}, ErrStalePID
		}
	}
	w.mu.Lock()
	out.routing.move, out.routing.moveTarget = move.ID, move.Target
	w.mu.Unlock()
	if out.request.contextErr() != nil {
		w.cancelRelocated(out, packet.Caller)
	}
	return handoffControlResult{Target: move.Target}, nil
}

func (w *network) abortHandoff(ctx context.Context, packet handoffControl) (handoffControlResult, error) {
	if w.node.opts.Directory == nil {
		return handoffControlResult{}, ErrDirectoryUnavailable
	}
	w.mu.Lock()
	reservation := w.handoffs[packet.Move]
	w.mu.Unlock()
	if reservation != nil && reservation.source != packet.Source {
		return handoffControlResult{}, ErrDirectoryConflict
	}
	move, err := w.node.opts.Directory.ReadMove(ctx, packet.Move)
	if err == nil {
		if move.Source != packet.Source || move.Target.Owner != w.node.Name() || move.Target.BootID != w.node.BootID() {
			return handoffControlResult{}, ErrDirectoryConflict
		}
		move, err = w.node.opts.Directory.AbortMove(ctx, packet.Move)
		if errors.Is(err, ErrMoveCommitted) {
			return handoffControlResult{Target: move.Target, State: MoveCommitted}, ErrMoveCommitted
		}
	}
	if err != nil && !errors.Is(err, ErrDirectoryNotFound) {
		return handoffControlResult{}, err
	}
	if reservation != nil {
		reservation.decide(false)
		if err := w.node.stopProcess(ctx, reservation.process, ErrMigrating, true); err != nil {
			return handoffControlResult{}, err
		}
		w.mu.Lock()
		if w.handoffs[packet.Move] == reservation {
			delete(w.handoffs, packet.Move)
		}
		w.mu.Unlock()
	}
	return handoffControlResult{State: MoveAborted}, nil
}

func (reservation *handoffReservation) decide(committed bool) {
	reservation.mu.Lock()
	defer reservation.mu.Unlock()
	if !reservation.decisionSet {
		reservation.decisionSet, reservation.committed = true, committed
		close(reservation.decision)
	}
}

func (w *network) activateHandoff(ctx context.Context, packet handoffControl) (handoffControlResult, error) {
	if w.node.opts.Directory == nil {
		return handoffControlResult{}, ErrDirectoryUnavailable
	}
	move, err := w.node.opts.Directory.ReadMove(ctx, packet.Move)
	if err != nil {
		return handoffControlResult{}, err
	}
	if move.State != MoveCommitted || move.Source != packet.Source || move.Target.Owner != w.node.Name() || move.Target.BootID != w.node.BootID() {
		return handoffControlResult{}, ErrDirectoryConflict
	}
	w.mu.Lock()
	reservation := w.handoffs[packet.Move]
	w.mu.Unlock()
	if reservation == nil {
		return handoffControlResult{Target: move.Target, State: MoveCommitted}, nil
	}
	reservation.mu.Lock()
	if !reservation.prepared || reservation.target.PID() != move.Target.PID() {
		reservation.mu.Unlock()
		return handoffControlResult{}, ErrMigrating
	}
	if !reservation.activating {
		reservation.activating = true
		reservation.mu.Unlock()
		q, n := reservation.process, w.node
		n.root.childrenMu.Lock()
		n.mu.Lock()
		q.mu.Lock()
		if n.stopping.Load() || q.stopRequested || q.finished || n.routes[q.path].process != q {
			q.mu.Unlock()
			n.mu.Unlock()
			n.root.childrenMu.Unlock()
			reservation.mu.Lock()
			reservation.activating = false
			reservation.mu.Unlock()
			q.requestStop(ErrStopped, true)
			return handoffControlResult{}, ErrStopped
		}
		q.ownership = move.Target
		n.publishPIDLocked(q, true)
		n.routes[q.path] = route{process: q}
		n.root.children = append(n.root.children, q.spec)
		q.mu.Unlock()
		n.mu.Unlock()
		n.root.childrenMu.Unlock()
		reservation.mu.Lock()
		reservation.decisionSet, reservation.committed = true, true
		close(reservation.decision)
		close(reservation.activate)
	}
	reservation.mu.Unlock()
	select {
	case <-reservation.activated:
	case <-ctx.Done():
		return handoffControlResult{Target: move.Target, State: MoveCommitted}, ctx.Err()
	}
	reservation.mu.Lock()
	activationErr := reservation.activationErr
	reservation.mu.Unlock()
	w.mu.Lock()
	if w.handoffs[packet.Move] == reservation {
		delete(w.handoffs, packet.Move)
	}
	w.mu.Unlock()
	return handoffControlResult{Target: move.Target, State: MoveCommitted}, activationErr
}

func (w *network) recoverHandoffPeer(ctx context.Context, peer string, boot [16]byte) {
	w.mu.Lock()
	var reservations []*handoffReservation
	for _, reservation := range w.handoffs {
		if reservation.source.Owner == peer && reservation.source.BootID == boot {
			reservations = append(reservations, reservation)
		}
	}
	w.mu.Unlock()
	for _, reservation := range reservations {
		move, err := w.node.opts.Directory.ReadMove(ctx, reservation.id)
		packet := handoffControl{Kind: "abort", Move: reservation.id, Source: reservation.source}
		if err == nil && move.State == MoveCommitted {
			_, err = w.activateHandoff(ctx, packet)
		} else if err == nil || errors.Is(err, ErrDirectoryNotFound) {
			_, err = w.abortHandoff(ctx, packet)
		}
		if err != nil {
			w.event(NetworkEvent{Peer: peer, Kind: "handoff_recovery_failed", Err: err})
		}
	}
}
