package actor

import "context"

// A committed MoveID authorizes the new node to answer an existing global Call.
// A node-bound Ref/PID has no global origin and cannot acquire this permission.
func (w *network) verifyMovedReply(ctx context.Context, id MoveID, origin RouteRecord, peer string, boot [16]byte) error {
	if id == (MoveID{}) || origin.Epoch == 0 || w.node.opts.Directory == nil {
		return ErrInvalidMessage
	}
	move, err := w.node.opts.Directory.ReadMove(ctx, id)
	if err != nil {
		return err
	}
	if move.State != MoveCommitted || move.Target.Owner != peer || move.Target.BootID != boot || move.Target.Path != origin.Path {
		return ErrInvalidMessage
	}
	// Follow committed provenance when a still-queued Call crossed more than one
	// node. Local replacements preserve this provenance; Release/Claim reset it.
	for range 16 {
		if move.Source.Path != origin.Path || move.Source.Epoch < origin.Epoch {
			return ErrInvalidMessage
		}
		if move.Source.Owner == origin.Owner && move.Source.BootID == origin.BootID {
			return nil
		}
		if move.Source.MoveID == (MoveID{}) {
			return ErrInvalidMessage
		}
		previous, err := w.node.opts.Directory.ReadMove(ctx, move.Source.MoveID)
		if err != nil {
			return err
		}
		if previous.State != MoveCommitted || previous.Target.Path != origin.Path || previous.Target.Owner != move.Source.Owner || previous.Target.BootID != move.Source.BootID || previous.Target.Epoch > move.Source.Epoch {
			return ErrInvalidMessage
		}
		move = previous
	}
	return ErrInvalidMessage
}

func (w *network) keepMovedCall(ctx context.Context, out *outgoingCall, peer string, boot [16]byte) bool {
	w.mu.Lock()
	var origin RouteRecord
	if out.routing != nil {
		origin = out.routing.route
	}
	unchanged := out.peer == peer && out.boot == boot
	w.mu.Unlock()
	if !unchanged {
		return true
	}
	if origin.Epoch == 0 || w.node.opts.Directory == nil {
		return false
	}
	current, err := w.node.opts.Directory.Lookup(ctx, origin.Path)
	if err != nil || current.MoveID == (MoveID{}) {
		return false
	}
	if current.State == RoutePreparing && current.Owner == peer && current.BootID == boot {
		return true
	}
	if err := w.verifyMovedReply(ctx, current.MoveID, origin, current.Owner, current.BootID); err != nil {
		return false
	}
	if current.Owner == peer && current.BootID == boot {
		return false
	}
	w.mu.Lock()
	if out.peer == peer && out.boot == boot {
		out.peer, out.boot = current.Owner, current.BootID
		out.scope, out.sequence = wireScope{}, 0
	}
	w.mu.Unlock()
	return true
}
