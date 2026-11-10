package actor

import "math"

// Called under the owning registry lock. IDs and the
// admission quota are node-wide even though request ownership is split.
func (n *Node) reserveRequest() (uint64, error) {
	for {
		count := n.pendingCalls.Load()
		if count >= int64(n.opts.MaxPendingCalls) {
			return 0, ErrCapacity
		}
		if n.pendingCalls.CompareAndSwap(count, count+1) {
			break
		}
	}
	for {
		id := n.nextRequest.Load()
		if id == math.MaxUint64 {
			n.pendingCalls.Add(-1)
			return 0, ErrCapacity
		}
		if n.nextRequest.CompareAndSwap(id, id+1) {
			return id + 1, nil
		}
	}
}
