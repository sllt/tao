package actor

import (
	"context"
	"errors"
	"sort"
	"strings"
)

type HandoffEntry struct {
	Path   string
	PID    PID
	Result HandoffResult
	Err    error
}
type HandoffReport struct {
	Committed, RolledBack, Unknown, Failed []HandoffEntry
	Stopped                                bool
}

// DrainAndHandoff commits actors individually. A partial failure leaves the
// node running and preserves committed destinations; a later call handles the
// remaining source actors. Actors without WithHandoff(true) stop at the end.
func (n *Node) DrainAndHandoff(ctx context.Context, options HandoffOptions) (report HandoffReport, resultErr error) {
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if n.opts.Directory == nil || n.network == nil {
		return report, ErrHandoffUnsupported
	}
	if !validSegment(options.TargetNode) || options.TargetNode == n.Name() || len(options.Version) > 128 {
		return report, ErrInvalidMessage
	}
	n.mu.Lock()
	if n.draining || n.stopping.Load() {
		n.mu.Unlock()
		return report, ErrMigrating
	}
	n.draining = true
	var processes []*process
	for _, p := range n.processes {
		if p.published.Load() && p.opts.handoff {
			processes = append(processes, p)
		}
	}
	n.mu.Unlock()
	defer func() {
		if !report.Stopped {
			n.mu.Lock()
			n.draining = false
			n.mu.Unlock()
		}
	}()
	sort.Slice(processes, func(i, j int) bool {
		a, b := processes[i].path, processes[j].path
		da, db := strings.Count(a, "/"), strings.Count(b, "/")
		if da != db {
			return da > db
		}
		return a < b
	})
	for _, p := range processes {
		entry := HandoffEntry{Path: p.path, PID: p.pid}
		if err := ctx.Err(); err != nil {
			entry.Err = err
		} else {
			p.mu.Lock()
			ongoing := p.handoff
			p.mu.Unlock()
			if ongoing != nil {
				entry.Result, entry.Err = n.ResolveHandoff(ctx, ongoing.result.MoveID)
			} else {
				entry.Result, entry.Err = n.Handoff(ctx, p.pid, options)
			}
		}
		switch {
		case entry.Result.Unknown:
			report.Unknown = append(report.Unknown, entry)
		case entry.Result.State == MoveCommitted:
			report.Committed = append(report.Committed, entry)
		case entry.Result.State == MoveAborted:
			report.RolledBack = append(report.RolledBack, entry)
		default:
			report.Failed = append(report.Failed, entry)
		}
		resultErr = errors.Join(resultErr, entry.Err)
	}
	if resultErr != nil {
		return report, resultErr
	}
	// Locally initiated Calls that moved away still need this node's reply path.
	n.mu.Lock()
	var pending []*request
	for _, r := range n.pending {
		if r.relocated {
			pending = append(pending, r)
		}
	}
	n.mu.Unlock()
	for _, r := range pending {
		select {
		case <-r.waitDone():
		case <-ctx.Done():
			return report, ctx.Err()
		}
	}
	resultErr = n.Stop(ctx)
	report.Stopped = resultErr == nil
	return report, resultErr
}
