package actor

import (
	"encoding/hex"
	"errors"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sllt/tao/taonet"
)

type ReceiveMetrics struct {
	Since                                                      time.Time
	Handled, Failed, ReceiveNanoseconds, MaxReceiveNanoseconds uint64
	MailboxHighWater, MailboxRejected                          int
	Behavior, CurrentMessage                                   string
	ReceiveStarted                                             time.Time
}
type ActorSnapshot struct {
	PID, Parent, Path, Kind, Version, State string
	Created                                 time.Time
	Published                               bool
	Mailbox, MailboxCapacity, SystemQueue   int
	AsyncRequests                           int
	Restarts                                uint64
	Epoch                                   uint64
	MoveID                                  string
	Metrics                                 *ReceiveMetrics
}
type CallSnapshot struct {
	ID, Caller, Owner, Target string
	Started, Deadline         time.Time
	Deferred, Relocated       bool
	Asynchronous              bool // the caller is not blocked waiting for this operation
}
type PeerSnapshot struct {
	Peer, Instance, Session, Class string
	Stats                          taonet.SessionStats
}
type SnapshotOptions struct {
	After  uint64
	Limit  int
	Filter string
}
type NodeSnapshot struct {
	Name, BootID, Version string
	Created, Sampled      time.Time
	Diagnostics, Draining bool
	Stats                 Stats
	Actors                []ActorSnapshot
	Calls                 []CallSnapshot
	Peers                 []PeerSnapshot
	Network               NetworkStats
	Total, CallsTotal     int
	Next                  string
	Cycles                [][]string
}
type Relationship struct{ Kind, From, To string }
type ActorDetail struct {
	ActorSnapshot
	Relationships     []Relationship
	RelationshipTotal int
}

// EnableDiagnostics starts optional per-Receive accounting. Existing actors
// begin counting from attachment; snapshots never read business fields.
func (n *Node) EnableDiagnostics() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.diagnostics.Store(true)
	for _, p := range n.processes {
		if p.metrics.Load() != nil {
			continue
		}
		metrics := &ReceiveMetrics{Since: time.Now()}
		// Published behaviors are immutable; constructors for unpublished
		// processes set this name on their first Receive instead.
		if p.published.Load() {
			metrics.Behavior = messageType(p.behavior)
		}
		p.metrics.Store(metrics)
	}
}

func ParsePID(value string) (PID, error) {
	parts := strings.Split(strings.TrimPrefix(value, "pid://"), "/")
	if !strings.HasPrefix(value, "pid://") || len(parts) != 3 || !validSegment(parts[0]) {
		return PID{}, ErrInvalidMessage
	}
	boot, err := hex.DecodeString(parts[1])
	if err != nil || len(boot) != 16 {
		return PID{}, ErrInvalidMessage
	}
	id, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || id == 0 {
		return PID{}, ErrInvalidMessage
	}
	pid := PID{Node: parts[0], ID: id}
	copy(pid.BootID[:], boot)
	if pid.BootID == ([16]byte{}) {
		return PID{}, ErrInvalidMessage
	}
	return pid, nil
}
func diagnosticPID(pid PID) string {
	if pid.ID == 0 {
		return ""
	}
	return pid.String()
}

// Call routing admits PID, Ref and routedRef before formatting a target.
// Concrete dispatch keeps the caller's Addr box from escaping just because
// diagnostics might be enabled on another Call.
func diagnosticTarget(to Addr) string {
	switch address := to.(type) {
	case PID:
		return address.String()
	case Ref:
		return address.String()
	default:
		return to.(routedRef).String()
	}
}

func messageType(message any) string {
	if message == nil {
		return "<nil>"
	}
	return reflect.TypeOf(message).String()
}

func (p *process) beginReceive(metrics *ReceiveMetrics, message any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	metrics.Behavior = messageType(p.behavior)
	metrics.CurrentMessage = messageType(message)
	metrics.ReceiveStarted = time.Now()
	metrics.MailboxHighWater = max(metrics.MailboxHighWater, p.queuedLocked())
}
func (p *process) endReceive(metrics *ReceiveMetrics, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	elapsed := uint64(time.Since(metrics.ReceiveStarted))
	metrics.Handled++
	if err != nil {
		metrics.Failed++
	}
	metrics.ReceiveNanoseconds += elapsed
	metrics.MaxReceiveNanoseconds = max(metrics.MaxReceiveNanoseconds, elapsed)
	metrics.CurrentMessage = ""
	metrics.ReceiveStarted = time.Time{}
}

type snapshotCandidate struct {
	p         *process
	published bool
	routeErr  error
}

func (c snapshotCandidate) snapshot() ActorSnapshot {
	p := c.p
	p.mu.Lock()
	defer p.mu.Unlock()
	result := ActorSnapshot{PID: p.pid.String(), Path: p.path, Kind: p.opts.kind, Version: p.opts.version, Created: p.created, Published: c.published, Mailbox: p.queuedLocked(), MailboxCapacity: p.mailbox.limit, SystemQueue: len(p.system), Restarts: p.restarts, Epoch: p.ownership.Epoch, State: "idle"}
	result.AsyncRequests = len(p.asyncRequests)
	if p.parent != nil && !p.parent.internal {
		result.Parent = p.parent.pid.String()
	}
	if p.handoff != nil {
		result.MoveID = hex.EncodeToString(p.handoff.result.MoveID[:])
	}
	if metrics := p.metrics.Load(); metrics != nil {
		copy := *metrics
		copy.MailboxHighWater = max(copy.MailboxHighWater, p.queuedLocked())
		result.Metrics = &copy
	}
	switch {
	case p.unresponsive:
		result.State = "unresponsive"
	case errors.Is(c.routeErr, ErrRestarting):
		result.State = "restarting"
	case p.finished:
		result.State = "stopped"
	case p.stopRequested:
		result.State = "stopping"
	case p.handoff != nil:
		result.State = "handoff"
		if p.handoff.result.Unknown {
			result.State = "outcome_unknown"
		}
		result.MoveID = hex.EncodeToString(p.handoff.result.MoveID[:])
	case p.change != nil:
		result.State = "replacing"
		if p.change.phase == replaceUnknown {
			result.State = "outcome_unknown"
		}
	case p.preparing:
		result.State = "preparing"
	case !c.published:
		result.State = "starting"
	case p.inspection != nil && p.inspection.running:
		result.State = "inspecting"
	case p.running:
		result.State = "running"
	}
	return result
}

// Snapshot is a bounded observational sample, not a transaction or ownership
// proof. Metadata locks are released before sampling network connections.
func (n *Node) Snapshot(options SnapshotOptions) (NodeSnapshot, error) {
	if options.Limit == 0 {
		options.Limit = 250
	}
	if options.Limit < 1 || options.Limit > 1000 || len(options.Filter) > 1024 {
		return NodeSnapshot{}, ErrInvalidMessage
	}
	result := NodeSnapshot{Name: n.Name(), BootID: hex.EncodeToString(n.bootID[:]), Version: n.opts.Version, Created: n.created, Sampled: time.Now(), Diagnostics: n.diagnostics.Load(), Stats: n.Stats()}
	n.mu.Lock()
	result.Draining = n.draining
	var candidates []snapshotCandidate
	var owners []*actorRequests
	for _, p := range n.processes {
		if owner := p.incomingRequests.Load(); owner != nil && owner != closedActorRequests {
			owners = append(owners, owner)
		}
		if strings.Contains(p.path, options.Filter) || strings.Contains(p.opts.kind, options.Filter) {
			candidates = append(candidates, snapshotCandidate{p, p.published.Load(), n.routes[p.path].status()})
		}
	}
	for _, r := range n.routes {
		if r.process != nil && n.processes[r.process.pid.ID] == nil && (strings.Contains(r.process.path, options.Filter) || strings.Contains(r.process.opts.kind, options.Filter)) {
			candidates = append(candidates, snapshotCandidate{r.process, false, r.status()})
		}
	}
	result.CallsTotal = int(n.pendingCalls.Load())
	for _, r := range n.pending {
		if len(result.Calls) == 1000 {
			break
		}
		deadline, _ := r.deadline()
		var trace requestTrace
		if r.trace != nil {
			trace = *r.trace
		}
		result.Calls = append(result.Calls, CallSnapshot{ID: strconv.FormatUint(r.id, 10), Caller: diagnosticPID(trace.caller), Owner: diagnosticPID(r.owner), Target: trace.target, Started: trace.started, Deadline: deadline, Deferred: r.deferred, Relocated: r.relocated, Asynchronous: r.asynchronous()})
	}
	n.mu.Unlock()
	for _, owner := range owners {
		if len(result.Calls) == 1000 {
			break
		}
		owner.mu.Lock()
		for _, r := range owner.pending {
			if len(result.Calls) == 1000 {
				break
			}
			deadline, _ := r.deadline()
			var trace requestTrace
			if r.trace != nil {
				trace = *r.trace
			}
			result.Calls = append(result.Calls, CallSnapshot{ID: strconv.FormatUint(r.id, 10), Caller: diagnosticPID(trace.caller), Owner: diagnosticPID(r.owner), Target: trace.target, Started: trace.started, Deadline: deadline, Deferred: r.deferred, Asynchronous: r.asynchronous()})
		}
		owner.mu.Unlock()
	}
	// Direct remote PID requests have no node.pending entry. Never take the
	// network lock under node.mu: incoming admission still uses the reverse order.
	if w := n.network; w != nil {
		w.mu.Lock()
		for _, out := range w.outgoing {
			if len(result.Calls) == 1000 {
				break
			}
			r := out.request
			if r.kind != requestRemotePID {
				continue
			}
			deadline, _ := r.deadline()
			var trace requestTrace
			if r.trace != nil {
				trace = *r.trace
			}
			result.Calls = append(result.Calls, CallSnapshot{ID: strconv.FormatUint(r.id, 10), Caller: diagnosticPID(trace.caller), Owner: diagnosticPID(r.owner), Target: trace.target, Started: trace.started, Deadline: deadline, Asynchronous: r.asynchronous()})
		}
		w.mu.Unlock()
	}
	sort.Slice(result.Calls, func(i, j int) bool {
		a, b := result.Calls[i].ID, result.Calls[j].ID
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].p.pid.ID < candidates[j].p.pid.ID })
	result.Total = len(candidates)
	for _, candidate := range candidates {
		if candidate.p.pid.ID <= options.After {
			continue
		}
		if len(result.Actors) == options.Limit {
			result.Next = strconv.FormatUint(lastSnapshotID(result.Actors), 10)
			break
		}
		result.Actors = append(result.Actors, candidate.snapshot())
	}
	result.Cycles = callCycles(result.Calls)
	if n.network != nil {
		result.Network = n.NetworkStats()
		for _, session := range n.network.endpoint.Sessions("") {
			info := session.Info()
			result.Peers = append(result.Peers, PeerSnapshot{Peer: string(info.Remote.ID), Instance: hex.EncodeToString(info.Remote.Instance[:]), Session: hex.EncodeToString(info.ID[:]), Class: info.Class.String(), Stats: session.Stats()})
		}
	}
	return result, nil
}
func lastSnapshotID(actors []ActorSnapshot) uint64 {
	pid, _ := ParsePID(actors[len(actors)-1].PID)
	return pid.ID
}

func (n *Node) ActorSnapshot(pid PID) (ActorDetail, error) {
	if pid.Node != n.Name() || pid.BootID != n.BootID() {
		return ActorDetail{}, ErrStalePID
	}
	n.mu.Lock()
	p := n.processes[pid.ID]
	if p == nil {
		for _, r := range n.routes {
			if r.process != nil && r.process.pid == pid {
				p = r.process
				break
			}
		}
	}
	if p == nil {
		n.mu.Unlock()
		return ActorDetail{}, ErrNoProcess
	}
	candidate := snapshotCandidate{p, p.published.Load(), n.routes[p.path].status()}
	var relationships []Relationship
	count := 0
	add := func(kind string, from, to PID) {
		count++
		if len(relationships) < 1000 {
			relationships = append(relationships, Relationship{kind, from.String(), to.String()})
		}
	}
	for _, record := range n.monitors {
		if record.observer.pid == pid || record.target == pid {
			add("monitor", record.observer.pid, record.target)
		}
	}
	for other := range n.links[pid] {
		add("link", pid, other)
	}
	for _, record := range n.remoteMonitors {
		if record.target == pid {
			add("monitor", record.ref.Observer, pid)
		}
	}
	n.mu.Unlock()
	sort.Slice(relationships, func(i, j int) bool {
		a, b := relationships[i], relationships[j]
		return a.Kind+a.From+a.To < b.Kind+b.From+b.To
	})
	return ActorDetail{candidate.snapshot(), relationships, count}, nil
}

// Cycles are candidates for investigation: a deferred reply can break a cycle
// without another Receive. We do not label these as proven deadlocks.
func callCycles(calls []CallSnapshot) [][]string {
	edges := make(map[string]string)
	for _, call := range calls {
		if call.Caller != "" && call.Owner != "" && !call.Relocated && !call.Asynchronous {
			edges[call.Caller] = call.Owner
		}
	}
	done := make(map[string]bool)
	var cycles [][]string
	var starts []string
	for start := range edges {
		starts = append(starts, start)
	}
	sort.Strings(starts)
	for _, start := range starts {
		var path []string
		seen := make(map[string]int)
		for at := start; at != "" && !done[at]; at = edges[at] {
			if index, ok := seen[at]; ok {
				cycles = append(cycles, append([]string(nil), path[index:]...))
				break
			}
			seen[at] = len(path)
			path = append(path, at)
		}
		for _, at := range path {
			done[at] = true
		}
	}
	return cycles
}
