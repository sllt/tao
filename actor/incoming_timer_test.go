package actor

import (
	"testing"
	"time"
)

func TestIncomingTimerReuseDoesNotReuseRequestState(t *testing.T) {
	n := networkNode(t, "timer-reuse")
	first, firstScope := scopeRequest(t, n, t.Context(), time.Hour)
	firstScope.mu.Lock()
	oldTimer := firstScope.timer
	firstScope.mu.Unlock()
	if err := n.reply(first, first.owner, 1, nil, false); err != nil {
		t.Fatal(err)
	}
	second, secondScope := scopeRequest(t, n, t.Context(), time.Hour)
	secondScope.mu.Lock()
	reused := secondScope.timer == oldTimer
	secondScope.mu.Unlock()
	if !reused {
		t.Fatal("stopped timer was not reused")
	}
	// A late callback still holding the first scope cannot expire the next
	// request, even though their timer storage was reused after a winning Stop.
	firstScope.timeout()
	if second.settled.Load() {
		t.Fatal("late scope completion expired the next request")
	}
	if err := n.reply(second, second.owner, 2, nil, false); err != nil {
		t.Fatal(err)
	}
	if first.value != 1 || second.value != 2 {
		t.Fatal("timer reuse changed an immutable request result")
	}
}

func TestIncomingTimerAlreadyFiringIsNotReused(t *testing.T) {
	n := networkNode(t, "timer-firing")
	r, scope := scopeRequest(t, n, t.Context(), time.Hour)
	func() {
		// The callback can settle its scope, but must wait here before it can
		// expire the request. A new scope starts while the old callback lives.
		n.mu.Lock()
		defer n.mu.Unlock()
		scope.mu.Lock()
		firing := scope.timer
		firing.timer.Reset(0)
		scope.mu.Unlock()
		eventually(t, func() bool { return scope.reason.Load() == 2 })
		scope.mu.Lock()
		scope.mu.Unlock() // complete has finished removing the fired timer.
		next := newRequestScope(t.Context(), time.Now().Add(time.Hour))
		next.request = &request{node: n}
		next.start()
		defer next.finish()
		next.mu.Lock()
		reused := next.timer == firing
		next.mu.Unlock()
		if reused {
			t.Fatal("timer with a running callback was reused")
		}
	}()
	signal(t, r.waitDone())
}
