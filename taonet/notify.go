package taonet

import (
	"context"
	"maps"
	"slices"
)

// Events reports asynchronous send and handler failures. It never blocks the
// network path; overflow is counted by DroppedEvents. Close closes this channel.
func (s *Session) Events() <-chan Event  { return s.events }
func (s *Session) DroppedEvents() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.droppedEvents }

func (s *Session) emitEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asyncFailures++
	if s.err != nil {
		s.droppedEvents++
		return
	}
	select {
	case s.events <- event:
	default:
		s.droppedEvents++
	}
}

// Notify copies a message into a bounded local send operation. Cancellation of
// ctx after successful admission does not withdraw that message.
func (s *Session) Notify(ctx context.Context, route string, msg Message) error {
	_, err := s.queueNotification(ctx, route, msg)
	return err
}

// NotifyConfirmed waits for remote queue admission, not handler completion.
// A canceled confirmation wait does not retract the already admitted send.
func (s *Session) NotifyConfirmed(ctx context.Context, route string, msg Message) error {
	ctx, cancel := withDefaultTimeout(ctx, s.cfg.DefaultRequestTimeout)
	defer cancel()
	result, err := s.queueNotification(ctx, route, msg)
	if err != nil {
		return err
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		select {
		case err := <-result:
			return err
		default:
		}
		return operationError(ctx, ctx.Err(), Unknown, 0)
	}
}

func (s *Session) queueNotification(ctx context.Context, route string, msg Message) (<-chan error, error) {
	fail := func(err error) (<-chan error, error) { return nil, operationError(ctx, err, NotSent, 0) }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if !slices.Contains(s.info.Features, "notify") {
		return fail(&StatusError{Code: CodeNotFound, Message: "peer does not support notify"})
	}
	sendCtx, cancel := context.WithTimeout(s.ctx, s.cfg.DefaultWriteTimeout)
	op, err := s.prepareUnary(sendCtx, route, msg)
	if err != nil {
		cancel()
		return fail(err)
	}
	// Body was charged by prepareUnary; account for the copied metadata too.
	metadataBytes := 128
	for key, value := range msg.Metadata {
		metadataBytes += len(key) + len(value)
	}
	if err = s.reserve(op, 0, metadataBytes, false); err != nil {
		s.finish(op)
		cancel()
		return fail(err)
	}
	copyMessage := Message{Metadata: maps.Clone(msg.Metadata), Body: append([]byte(nil), msg.Body...)}
	if err = ctx.Err(); err != nil {
		s.finish(op)
		cancel()
		return fail(err)
	}
	result := make(chan error, 1)
	go func() {
		defer cancel()
		defer s.finish(op)
		_, err := s.exchangeUnary(op, "notify", route, copyMessage)
		if err != nil {
			var id uint64
			if op.stream != nil {
				id = op.stream.ID()
			}
			s.emitEvent(Event{Kind: NotifyFailed, Operation: id, Route: route, Err: err})
		}
		result <- err
	}()
	return result, nil
}

type notification struct {
	op      *operation
	route   string
	message Message
	handler NotifyHandler
	done    chan struct{}
}

func (s *Session) serveNotify(op *operation, handler NotifyHandler, open wireOpen, body []byte) {
	job := notification{op: op, route: open.Route, message: Message{Metadata: open.Metadata, Body: body}, handler: handler, done: make(chan struct{})}
	s.mu.Lock()
	if s.err != nil {
		s.mu.Unlock()
		return
	}
	// A handler slot was reserved before OPEN_OK, so the bounded queue has room.
	s.notifyQueue <- job
	s.mu.Unlock()
	if writeRecord(op.stream, recordAck, 0, nil) == nil {
		finishStream(op.stream)
	}
	// The job owns the body/handler reservations until its callback finishes.
	// Sender cancellation and an ACK write failure do not retract queue admission.
	<-job.done
}

func (s *Session) dispatchNotifications() {
	defer s.wg.Done()
	for {
		select {
		case job := <-s.notifyQueue:
			go s.runNotification(job)
		case <-s.done:
			for {
				select {
				case job := <-s.notifyQueue:
					close(job.done)
				default:
					return
				}
			}
		}
	}
}

func (s *Session) runNotification(job notification) {
	defer close(job.done)
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.DefaultRequestTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	if err := callNotify(job.handler, ctx, job.message); err != nil {
		s.emitEvent(Event{Kind: HandlerFailed, Operation: job.op.stream.ID(), Route: job.route, Err: handlerStatus(err)})
	}
}

func callNotify(handler NotifyHandler, ctx context.Context, msg Message) (err error) {
	defer func() {
		if recover() != nil {
			err = &StatusError{Code: CodeInternal, Message: "notify handler panicked"}
		}
	}()
	return handler(ctx, msg)
}
