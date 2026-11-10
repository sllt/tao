package actor

import "errors"

type Stats struct {
	Preparing      int
	Frozen         int
	Actors         int
	RunningActors  int
	QueuedMessages int
	PendingCalls   int
	AsyncRequests  int // includes completed results that have not entered Receive
	Observations   int
	Starting       int
	Restarting     int
	Unresponsive   int
	Stopping       bool
}

func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	stats := Stats{Actors: len(n.processes), PendingCalls: int(n.pendingCalls.Load()), Observations: n.observations, Stopping: n.stopping.Load()}
	stats.AsyncRequests = int(n.asyncPending.Load())
	for _, p := range n.processes {
		p.mu.Lock()
		if p.running {
			stats.RunningActors++
		}
		stats.QueuedMessages += p.queuedLocked()
		if p.preparing {
			stats.Preparing++
		}
		if p.change != nil || p.handoff != nil {
			stats.Frozen++
		}
		if p.unresponsive && n.routes[p.path].process != p {
			stats.Unresponsive++
		}
		p.mu.Unlock()
	}
	for _, r := range n.routes {
		err := r.status()
		switch {
		case errors.Is(err, ErrStarting):
			stats.Starting++
		case errors.Is(err, ErrRestarting):
			stats.Restarting++
		case errors.Is(err, ErrUnresponsive):
			stats.Unresponsive++
		}
	}
	return stats
}
