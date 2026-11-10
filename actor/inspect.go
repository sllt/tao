package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrInspectUnsupported = errors.New("actor: behavior does not implement Inspector")

// Inspector is optional. Inspect runs serially with Receive and must be quick
// and read-only. JSON is encoded before the actor resumes, so mutable state
// never escapes to the HTTP or console goroutine.
type Inspector interface {
	Inspect(context.Context) (any, error)
}
type inspectionResult struct {
	value json.RawMessage
	err   error
}
type inspectionRequest struct {
	ctx     context.Context
	done    chan inspectionResult
	running bool
}

func (n *Node) Inspect(ctx context.Context, pid PID) (json.RawMessage, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.opts.CallTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, _, err := n.resolve(pid)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(p.ctx, cancel)
	defer stop()
	request := &inspectionRequest{ctx: ctx, done: make(chan inspectionResult, 1)}
	p.mu.Lock()
	if p.stopRequested || p.finished {
		p.mu.Unlock()
		return nil, ErrStopped
	}
	if p.inspection != nil {
		p.mu.Unlock()
		return nil, ErrCapacity
	}
	p.inspection = request
	p.system = append(p.system, request)
	p.interruptBatchLocked()
	start := !p.running
	if start {
		p.running = true
	}
	p.mu.Unlock()
	if start {
		p.schedule()
	}
	select {
	case result := <-request.done:
		return result.value, result.err
	case <-ctx.Done():
		p.mu.Lock()
		if p.inspection == request && !request.running {
			for i, message := range p.system {
				if message == request {
					copy(p.system[i:], p.system[i+1:])
					p.system[len(p.system)-1] = nil
					p.system = p.system[:len(p.system)-1]
					break
				}
			}
			p.inspection = nil
		}
		p.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (p *process) runInspection(request *inspectionRequest) {
	p.mu.Lock()
	if p.inspection != request {
		p.mu.Unlock()
		return
	}
	request.running = true
	p.mu.Unlock()
	result := p.inspectBehavior(request.ctx)
	p.mu.Lock()
	p.inspection = nil
	p.mu.Unlock()
	request.done <- result
}
func (p *process) inspectBehavior(ctx context.Context) (result inspectionResult) {
	defer func() {
		if value := recover(); value != nil {
			result = inspectionResult{err: fmt.Errorf("Inspector panic: %v", value)}
		}
	}()
	if err := ctx.Err(); err != nil {
		return inspectionResult{err: err}
	}
	inspector, ok := p.behavior.(Inspector)
	if !ok {
		return inspectionResult{err: ErrInspectUnsupported}
	}
	value, err := inspector.Inspect(ctx)
	if err != nil {
		return inspectionResult{err: err}
	}
	data, err := json.Marshal(value)
	if err == nil && len(data) > p.node.opts.MaxSnapshotBytes {
		err = ErrMessageTooLarge
		data = nil
	}
	if err == nil {
		err = ctx.Err()
	}
	return inspectionResult{data, err}
}
