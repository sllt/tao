package actor

import (
	"sync/atomic"
	"time"
)

// Only the timer is reused, never a request or its completion state. An
// AfterFunc whose Stop loses may still be executing; that timer is discarded.
type requestTimer struct {
	timer *time.Timer
	scope atomic.Pointer[requestScope]
}

func (t *requestTimer) fire() {
	if scope := t.scope.Load(); scope != nil {
		scope.timeout()
	}
}

func (n *Node) startRequestTimer(scope *requestScope, delay time.Duration) *requestTimer {
	select {
	case timer := <-n.requestTimers:
		timer.scope.Store(scope)
		timer.timer.Reset(delay)
		return timer
	default:
		timer := &requestTimer{}
		timer.scope.Store(scope)
		timer.timer = time.AfterFunc(delay, timer.fire)
		return timer
	}
}

// requestScope.mu gives one owner responsibility for removing its timer.
func (n *Node) stopRequestTimer(timer *requestTimer) {
	stopped := timer.timer.Stop()
	timer.scope.Store(nil)
	if stopped {
		select {
		case n.requestTimers <- timer:
		default:
		}
	}
}
