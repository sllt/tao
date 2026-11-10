package actor

func (w *network) cancelRelocated(out *outgoingCall, key requestKey) {
	w.mu.Lock()
	if out.routing == nil {
		w.mu.Unlock()
		return
	}
	move, target := out.routing.move, out.routing.moveTarget
	w.mu.Unlock()
	if move == (MoveID{}) {
		return
	}
	packet := actorPacket{Kind: "cancel_moved", Key: key, Move: move}
	if target.Owner == w.node.Name() && target.BootID == w.node.BootID() {
		_ = w.recordMovedCancel(packet)
		return
	}
	w.queueControl(target.Owner, packet, target.BootID)
}

func (w *network) recordMovedCancel(packet actorPacket) error {
	w.mu.Lock()
	if incoming, ok := w.incoming[packet.Key]; ok {
		w.mu.Unlock()
		if incoming.move != packet.Move {
			return ErrInvalidMessage
		}
		incoming.cancel()
		return nil
	}
	reservation := w.handoffs[packet.Move]
	if reservation == nil {
		w.mu.Unlock()
		return nil
	}
	// Holding the admission lock closes the gap between this check and staging
	// an incoming Call; stageHandoffEnvelope inspects the tombstone afterward.
	reservation.mu.Lock()
	if !reservation.prepared {
		if len(reservation.canceled) >= reservation.process.opts.mailbox {
			reservation.mu.Unlock()
			w.mu.Unlock()
			return ErrCapacity
		}
		if reservation.canceled == nil {
			reservation.canceled = make(map[requestKey]bool)
		}
		reservation.canceled[packet.Key] = true
	}
	reservation.mu.Unlock()
	w.mu.Unlock()
	return nil
}
