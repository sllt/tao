package actor

import (
	"context"
	"sync"
	"time"

	"github.com/sllt/tao/taonet"
)

// A remote observation reserves its eventual notification slot at registration.
// The extra 64 slots keep cancellation and cleanup independent of that reserve.
type controlReservation struct {
	network *network
	once    sync.Once
}

func (token *controlReservation) release() {
	token.once.Do(func() {
		w := token.network
		w.mu.Lock()
		w.controlReservations--
		w.mu.Unlock()
		select {
		case w.controlWake <- struct{}{}:
		default:
		}
	})
}
func (w *network) reserveControl(future bool) (*controlReservation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrClosed
	}
	limit := w.opts.ControlMessages + 64
	if future {
		limit = w.opts.ControlMessages
	}
	if w.controlReservations >= limit {
		return nil, ErrCapacity
	}
	w.controlReservations++
	return &controlReservation{network: w}, nil
}
func (w *network) controlJob(peer string, packet actorPacket, boot [16]byte, token *controlReservation, complete func(error), scope func(wireScope)) (*networkJob, error) {
	var err error
	if token == nil {
		token, err = w.reserveControl(false)
		if err != nil {
			return nil, err
		}
	}
	payload, err := marshalPacket(packet)
	if err != nil || len(payload) > 4096 {
		token.release()
		return nil, ErrInvalidMessage
	}
	job := &networkJob{token: token, raw: payload, packet: packet, peer: peer, class: taonet.ClassControl, result: make(chan error, 1), expectedBoot: boot, deadline: time.Now().Add(w.opts.SendTimeout), onComplete: complete, onScope: scope}
	if err = w.enqueue(job); err != nil {
		token.release()
		return nil, err
	}
	return job, nil
}
func (w *network) sendObservationCleanup(target PID, packet actorPacket) {
	_, err := w.controlJob(target.Node, packet, target.BootID, nil, func(err error) {
		if err != nil {
			w.breakObservationTransport(target)
		}
	}, nil)
	if err != nil {
		w.breakObservationTransport(target)
	}
}
func (w *network) sendReservedObservation(token *controlReservation, target PID, packet actorPacket, complete func()) {
	_, err := w.controlJob(target.Node, packet, target.BootID, token, func(err error) {
		if complete != nil {
			complete()
		}
		if err != nil {
			w.breakObservationTransport(target)
		}
	}, nil)
	if err != nil {
		if complete != nil {
			complete()
		}
		w.breakObservationTransport(target)
	}
}
func (w *network) breakObservationTransport(target PID) {
	w.mu.Lock()
	var unregistered []*taonet.Session
	for session := range w.watched {
		info := session.Info()
		if string(info.Remote.ID) == target.Node && [16]byte(info.Remote.Instance) == target.BootID {
			unregistered = append(unregistered, session)
		}
	}
	w.mu.Unlock()
	for _, session := range unregistered {
		_ = session.Close()
	}
	for _, session := range w.endpoint.Sessions(taonet.PeerID(target.Node)) {
		if [16]byte(session.Info().Remote.Instance) == target.BootID && session.Info().Class == taonet.ClassControl {
			_ = session.Close()
		}
	}
}
func actorWireError(err error) *wireActorError {
	if err == nil {
		return nil
	}
	return &wireActorError{Code: codeForError(err), Message: wireErrorText(err)}
}
func actorWireReason(err *wireActorError) error {
	if err == nil {
		return nil
	}
	return &RemoteError{Code: err.Code, Message: err.Message, Outcome: Responded}
}

func (w *network) watchControlChannel(channel *taonet.Channel) {
	info := channel.Session().Info()
	scope := wireScope{info.ID, channel.ID()}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		select {
		case <-channel.Done():
		case <-w.ctx.Done():
		}
		w.node.remoteObservationLost(string(info.Remote.ID), [16]byte(info.Remote.Instance), &scope)
	}()
}

func (w *network) handleControl(session *taonet.Session, scope wireScope, packet actorPacket) error {
	peer := session.Info().Remote
	if packet.Error != nil && (len(packet.Error.Code) > 64 || len(packet.Error.Message) > 1024) {
		return ErrInvalidMessage
	}
	switch packet.Kind {
	case "cancel_moved":
		if !w.validPeerPacket(peer, packet, true) || packet.Move == (MoveID{}) {
			return ErrInvalidMessage
		}
		return w.recordMovedCancel(packet)
	case "cancel":
		if !w.validPeerPacket(peer, packet, true) || packet.Scope.Session == (taonet.ID{}) || packet.Scope.Channel == 0 || packet.Sequence == 0 {
			return ErrInvalidMessage
		}
		w.mu.Lock()
		incoming, active := w.incoming[packet.Key]
		if active && (incoming.scope != packet.Scope || incoming.sequence != packet.Sequence) {
			w.mu.Unlock()
			return ErrInvalidMessage
		}
		if !active {
			if seen, exists := w.inboxProgress[packet.Scope]; exists && packet.Sequence <= seen {
				w.mu.Unlock()
				return nil
			}
			if len(w.canceled) >= w.node.opts.MaxPendingCalls {
				w.mu.Unlock()
				return ErrCapacity
			}
			w.canceled[packet.Key] = canceledCall{packet.Scope, packet.Sequence}
		}
		w.mu.Unlock()
		if active {
			incoming.cancel()
		}
		return nil
	case "monitor", "demonitor":
		if packet.Monitor == nil || packet.Monitor.ID == 0 || packet.Monitor.Observer.Node != string(peer.ID) || packet.Monitor.Observer.BootID != [16]byte(peer.Instance) || packet.Monitor.Observer.ID == 0 {
			return ErrInvalidMessage
		}
		if packet.Kind == "demonitor" {
			w.removeRemoteMonitor(*packet.Monitor)
			return nil
		}
		if packet.Target.PID == nil || packet.Target.Kind != "pid" || packet.Target.PID.Node != w.node.Name() {
			return ErrInvalidMessage
		}
		return w.registerRemoteMonitor(*packet.Monitor, *packet.Target.PID, scope)
	case "down":
		if packet.Monitor == nil || packet.Target.PID == nil || packet.Target.PID.Node != string(peer.ID) || packet.Target.PID.BootID != [16]byte(peer.Instance) {
			return ErrInvalidMessage
		}
		return w.node.deliverRemoteDown(*packet.Monitor, *packet.Target.PID, actorWireReason(packet.Error))
	case "link", "unlink", "exit":
		if packet.From == nil || packet.From.Node != string(peer.ID) || packet.From.BootID != [16]byte(peer.Instance) || packet.From.ID == 0 || packet.Link == (LinkID{}) || packet.Target.Kind != "pid" || packet.Target.PID == nil || packet.Target.PID.Node != w.node.Name() {
			return ErrInvalidMessage
		}
		if packet.Kind == "link" {
			return w.registerRemoteLink(*packet.From, *packet.Target.PID, packet.Link, scope)
		}
		if packet.Kind == "unlink" {
			w.removeRemoteLink(*packet.Target.PID, *packet.From, packet.Link)
			return nil
		}
		return w.node.deliverRemoteExit(*packet.Target.PID, *packet.From, packet.Link, packet.Error)
	default:
		return ErrInvalidMessage
	}
}

func (w *network) waitControlDrain(ctx context.Context) error {
	for {
		w.mu.Lock()
		pending := w.controlJobs
		w.mu.Unlock()
		if pending == 0 {
			return nil
		}
		select {
		case <-w.controlWake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
