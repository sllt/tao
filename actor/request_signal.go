package actor

const requestCacheEntries = 256

func (n *Node) takeCallSignal() chan struct{} {
	select {
	case done := <-n.callSignals:
		return done
	default:
		return make(chan struct{}, 1)
	}
}

// Only the caller returns this private signal, after consuming the result or
// observing completion under its registry lock. The winning publisher sends before
// unlocking and never accesses the signal again. Request records and broadcast
// channels are not reused, so late replies/waiters retain the original state.
func (n *Node) recycleCallSignal(done chan struct{}) {
	select {
	case <-done:
	default:
	}
	select {
	case n.callSignals <- done:
	default:
	}
}
