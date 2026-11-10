package taonet

import "sync/atomic"

type receiptSignal struct{ channel chan struct{} }

var completedReceiptSignal = receiptSignal{channel: releasedInput}

// A receipt often finishes before anyone waits for its input release. Publish
// the shared closed signal in that case; allocate a channel only for an actual
// waiter. Once a waiter installs a channel its identity never changes.
type receiptNotification struct{ signal atomic.Pointer[receiptSignal] }

func (n *receiptNotification) ready() bool {
	signal := n.signal.Load()
	if signal == nil {
		return false
	}
	if signal == &completedReceiptSignal {
		return true
	}
	// An earlier waiter keeps its own channel even after completion.
	select {
	case <-signal.channel:
		return true
	default:
		return false
	}
}

func (n *receiptNotification) channel() <-chan struct{} {
	if signal := n.signal.Load(); signal != nil {
		return signal.channel
	}
	waiting := &receiptSignal{channel: make(chan struct{})}
	if n.signal.CompareAndSwap(nil, waiting) {
		return waiting.channel
	}
	return n.signal.Load().channel
}

// The Channel's existing resolved/bodyReleased state permits one publisher.
// Either the publisher installs completion first, or it closes the waiter's
// channel. Both paths publish the preceding receipt result to Wait.
func (n *receiptNotification) finish() {
	signal := n.signal.Load()
	if signal == nil {
		if n.signal.CompareAndSwap(nil, &completedReceiptSignal) {
			return
		}
		signal = n.signal.Load()
	}
	close(signal.channel)
}
