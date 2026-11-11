package erlangactor

import (
	"context"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/erlang"
	"github.com/sllt/tao/erlang/etf"
)

type outboundTaskKind uint8

const (
	outboundTaskCall outboundTaskKind = iota + 1
	outboundTaskCast
	outboundTaskSend
)

type outboundTask struct {
	kind      outboundTaskKind
	ctx       context.Context
	target    erlang.Target
	payload   etf.Term
	ticket    actor.DeferredReply
	hasTicket bool
}

type peerSubmitter struct {
	bridge *Bridge
	peer   string
	ch     chan outboundTask
}

func (b *Bridge) spawnOutboundActor() (actor.PID, error) {
	return b.taoNode.Spawn(actor.FromFunc(func(a *actor.Actor, msg any) error {
		switch m := msg.(type) {
		case actor.Started, actor.Stopped:
			return nil

		case Call:
			if err := m.Target.Validate(); err != nil {
				b.outboundErrors.Add(1)
				_ = a.ReplyError(&erlang.Error{
					Stage:   erlang.StageValidate,
					Outcome: erlang.OutcomeNotSent,
					Peer:    string(m.Target.Node()),
					Err:     err,
				})
				return nil
			}
			ticket := a.Defer()
			if ticket == (actor.DeferredReply{}) {
				b.outboundErrors.Add(1)
				return nil
			}
			b.submitOutbound(a.Context(), outboundTask{
				kind:      outboundTaskCall,
				ctx:       a.Context(),
				target:    m.Target,
				payload:   m.Request,
				ticket:    ticket,
				hasTicket: true,
			})
			return nil

		case Cast:
			if err := m.Target.Validate(); err != nil {
				b.outboundErrors.Add(1)
				_ = a.ReplyError(&erlang.Error{
					Stage:   erlang.StageValidate,
					Outcome: erlang.OutcomeNotSent,
					Peer:    string(m.Target.Node()),
					Err:     err,
				})
				return nil
			}
			ticket := a.Defer()
			hasTicket := ticket != (actor.DeferredReply{})
			var reqCtx context.Context
			if hasTicket {
				reqCtx = a.Context()
			}
			b.submitOutbound(reqCtx, outboundTask{
				kind:      outboundTaskCast,
				ctx:       reqCtx,
				target:    m.Target,
				payload:   m.Request,
				ticket:    ticket,
				hasTicket: hasTicket,
			})
			return nil

		case Send:
			if err := m.Target.Validate(); err != nil {
				b.outboundErrors.Add(1)
				_ = a.ReplyError(&erlang.Error{
					Stage:   erlang.StageValidate,
					Outcome: erlang.OutcomeNotSent,
					Peer:    string(m.Target.Node()),
					Err:     err,
				})
				return nil
			}
			ticket := a.Defer()
			hasTicket := ticket != (actor.DeferredReply{})
			var reqCtx context.Context
			if hasTicket {
				reqCtx = a.Context()
			}
			b.submitOutbound(reqCtx, outboundTask{
				kind:      outboundTaskSend,
				ctx:       reqCtx,
				target:    m.Target,
				payload:   m.Message,
				ticket:    ticket,
				hasTicket: hasTicket,
			})
			return nil

		default:
			_ = a.ReplyError(ErrUnsupportedSignal)
			return nil
		}
	}))
}

func (b *Bridge) submitOutbound(_ context.Context, task outboundTask) {
	if b.closed.Load() {
		b.outboundErrors.Add(1)
		if task.hasTicket {
			_ = task.ticket.ReplyError(ErrBridgeClosed)
		}
		return
	}
	sub := b.getOrCreateSubmitter(string(task.target.Node()))
	if sub == nil {
		b.outboundErrors.Add(1)
		if task.hasTicket {
			_ = task.ticket.ReplyError(ErrBridgeClosed)
		}
		return
	}
	select {
	case sub.ch <- task:
		b.outboundSubmitted.Add(1)
	default:
		b.outboundErrors.Add(1)
		if task.hasTicket {
			_ = task.ticket.ReplyError(actor.ErrCapacity)
		}
	}
}

func (b *Bridge) getOrCreateSubmitter(peer string) *peerSubmitter {
	b.submittersMu.RLock()
	sub, ok := b.submitters[peer]
	b.submittersMu.RUnlock()
	if ok {
		return sub
	}

	b.submittersMu.Lock()
	defer b.submittersMu.Unlock()
	if b.closed.Load() {
		return nil
	}
	if sub, ok = b.submitters[peer]; ok {
		return sub
	}
	sub = &peerSubmitter{
		bridge: b,
		peer:   peer,
		ch:     make(chan outboundTask, b.cfg.outboundPeerQueue),
	}
	b.submitters[peer] = sub
	b.subWG.Add(1)
	go sub.run()
	return sub
}

func (s *peerSubmitter) run() {
	defer s.bridge.subWG.Done()
	for {
		select {
		case <-s.bridge.lifeCtx.Done():
			for {
				select {
				case task := <-s.ch:
					if task.hasTicket {
						_ = task.ticket.ReplyError(ErrBridgeClosed)
					}
				default:
					return
				}
			}
		case task := <-s.ch:
			s.execute(task)
		}
	}
}

func (s *peerSubmitter) execute(task outboundTask) {
	switch task.kind {
	case outboundTaskCall:
		err := s.bridge.client.CallAsync(task.ctx, task.target, task.payload, func(reply etf.Term, callErr error) {
			if callErr != nil {
				s.bridge.outboundErrors.Add(1)
				_ = task.ticket.ReplyError(callErr)
				return
			}
			_ = task.ticket.Reply(reply)
		})
		if err != nil {
			s.bridge.outboundErrors.Add(1)
			_ = task.ticket.ReplyError(err)
		}

	case outboundTaskCast:
		ctx := task.ctx
		var cancel context.CancelFunc
		if ctx == nil {
			ctx, cancel = context.WithTimeout(s.bridge.lifeCtx, 5*time.Second)
			defer cancel()
		}
		err := s.bridge.client.Cast(ctx, task.target, task.payload)
		if err != nil {
			s.bridge.outboundErrors.Add(1)
			if task.hasTicket {
				_ = task.ticket.ReplyError(err)
			}
			return
		}
		if task.hasTicket {
			_ = task.ticket.Reply(etf.Atom("ok"))
		}

	case outboundTaskSend:
		ctx := task.ctx
		var cancel context.CancelFunc
		if ctx == nil {
			ctx, cancel = context.WithTimeout(s.bridge.lifeCtx, 5*time.Second)
			defer cancel()
		}
		err := s.bridge.client.Send(ctx, task.target, task.payload)
		if err != nil {
			s.bridge.outboundErrors.Add(1)
			if task.hasTicket {
				_ = task.ticket.ReplyError(err)
			}
			return
		}
		if task.hasTicket {
			_ = task.ticket.Reply(etf.Atom("ok"))
		}
	}
}
