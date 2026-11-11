package actor

import "math"

func (n *Node) reservePendingCall() error {
	for {
		count := n.pendingCalls.Load()
		if count >= int64(n.opts.MaxPendingCalls) {
			return ErrCapacity
		}
		if n.pendingCalls.CompareAndSwap(count, count+1) {
			return nil
		}
	}
}

func (n *Node) allocateRequestID() (uint64, error) {
	for {
		id := n.nextRequest.Load()
		if id == math.MaxUint64 {
			return 0, ErrCapacity
		}
		if n.nextRequest.CompareAndSwap(id, id+1) {
			return id + 1, nil
		}
	}
}

// Called under the owning registry lock. IDs and the
// admission quota are node-wide even though request ownership is split.
func (n *Node) reserveRequest() (uint64, error) {
	if err := n.reservePendingCall(); err != nil {
		return 0, err
	}
	id, err := n.allocateRequestID()
	if err != nil {
		n.pendingCalls.Add(-1)
		return 0, err
	}
	return id, nil
}
