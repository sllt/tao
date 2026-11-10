package taonet

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	native "github.com/sllt/tao/taonet/internal/mux"
)

type wireOpen struct {
	ID        uint64            `json:"id"`
	Mode      string            `json:"mode"`
	Route     string            `json:"route"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	TimeoutMS uint64            `json:"timeout_ms"`
	Size      int               `json:"size"`
	Channel   *channelOptions   `json:"channel,omitempty"`
}

type wireResult struct {
	Status   *StatusError      `json:"status,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Size     int               `json:"size"`
}

func checkMessage(limits Limits, size int, metadata map[string]string) error {
	if size < 0 {
		return protocolError("negative message size")
	}
	if size > limits.MaxMessageSize {
		return &StatusError{Code: CodeMessageTooLarge, Message: "message exceeds negotiated limit"}
	}
	return checkMetadata(metadata, limits)
}

func errorCode(err error) Code {
	var status *StatusError
	var op *OpError
	var stream *native.StreamError
	switch {
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return CodeDeadlineExceeded
	case errors.As(err, &status):
		return status.Code
	case errors.As(err, &op):
		return op.Code
	case errors.As(err, &stream):
		switch stream.Code {
		case native.Canceled:
			return CodeCanceled
		case native.DeadlineExceeded:
			return CodeDeadlineExceeded
		case native.ResourceExhausted:
			return CodeResourceExhausted
		case native.ProtocolError:
			return CodeProtocolError
		default:
			return CodeUnavailable
		}
	case errors.Is(err, ErrProtocol), errors.Is(err, native.ErrProtocol):
		return CodeProtocolError
	case errors.Is(err, native.ErrCapacity):
		return CodeResourceExhausted
	default:
		return CodeUnavailable
	}
}

func operationError(ctx context.Context, err error, outcome Outcome, id uint64) error {
	if cause := context.Cause(ctx); cause != nil {
		err = cause
	} else if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		// I/O and peer timers can finish before the local context callback.
		// An elapsed request budget is still a deadline, not a peer cancel.
		err = context.DeadlineExceeded
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		err = context.DeadlineExceeded
	}
	return &OpError{Code: errorCode(err), Outcome: outcome, Operation: id, Message: err.Error(), Cause: err}
}

func validStatus(s *StatusError) bool {
	if s == nil || len(s.Message) > 1024 {
		return false
	}
	switch s.Code {
	case CodeCanceled, CodeDeadlineExceeded, CodeUnavailable, CodeUnauthenticated, CodePermissionDenied,
		CodeNotFound, CodeResourceExhausted, CodeMessageTooLarge, CodeProtocolError, CodeInternal, CodeApplication, CodeAbandoned:
		return true
	default:
		code := string(s.Code)
		if !strings.HasPrefix(code, "app.") || len(code) > 64 || len(code) == 4 {
			return false
		}
		for _, ch := range []byte(code) {
			if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '.' || ch == '_' || ch == '-') {
				return false
			}
		}
		return true
	}
}

func handlerStatus(err error) *StatusError {
	if err == nil {
		return nil
	}
	code := errorCode(err)
	if code == CodeUnavailable {
		code = CodeApplication
	}
	status := &StatusError{Code: code, Message: err.Error()}
	var supplied *StatusError
	if errors.As(err, &supplied) {
		status.Code = supplied.Code
		status.Message = supplied.Message
	}
	var operation *OpError
	if errors.As(err, &operation) {
		status.Code = operation.Code
	}
	status.Message = strings.ToValidUTF8(status.Message, "\uFFFD")
	if len(status.Message) > 1024 {
		status.Message = status.Message[:1024]
		for !utf8.ValidString(status.Message) {
			status.Message = status.Message[:len(status.Message)-1]
		}
	}
	if !validStatus(status) {
		status.Code = CodeApplication
	}
	return status
}

func remoteError(status *StatusError, outcome Outcome, id uint64) error {
	return &OpError{Code: status.Code, Outcome: outcome, Remote: true, Operation: id, Message: status.Message, Cause: status}
}

func admissionOutcome(err error) Outcome {
	var reset *native.StreamError
	if errors.As(err, &reset) && (reset.Code == native.ResourceExhausted || reset.Code == native.Draining) {
		return Rejected
	}
	return Unknown
}

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

// Request invokes a route on the peer. The caller owns msg until Request returns.
// Transport failures after OPEN may have Outcome Unknown and are never retried.
func (s *Session) Request(ctx context.Context, route string, msg Message) (response Message, resultErr error) {
	defer func() {
		if resultErr != nil {
			s.mu.Lock()
			s.requestFailures++
			var op *OpError
			if errors.As(resultErr, &op) && op.Outcome == Unknown {
				s.unknownResults++
			}
			s.mu.Unlock()
		}
	}()
	ctx, cancel := withDefaultTimeout(ctx, s.cfg.DefaultRequestTimeout)
	defer cancel()
	op, err := s.prepareUnary(ctx, route, msg)
	if err != nil {
		return Message{}, operationError(ctx, err, NotSent, 0)
	}
	defer s.finish(op)
	return s.exchangeUnary(op, "request", route, msg)
}

func (s *Session) prepareUnary(ctx context.Context, route string, msg Message) (*operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validRoute(route) {
		return nil, &StatusError{Code: CodeNotFound, Message: "invalid route"}
	}
	if err := checkMessage(s.cfg.Limits, len(msg.Body), msg.Metadata); err != nil {
		return nil, err
	}
	if err := checkMessage(s.peerLimits, len(msg.Body), msg.Metadata); err != nil {
		return nil, err
	}
	op, err := s.begin(ctx, false)
	if err != nil {
		return nil, err
	}
	if err = s.reserve(op, 0, len(msg.Body), false); err != nil {
		s.finish(op)
		return nil, err
	}
	return op, nil
}

func (s *Session) exchangeUnary(op *operation, mode, route string, msg Message) (Message, error) {
	ctx := op.ctx
	fail := func(err error, outcome Outcome, id uint64) (Message, error) {
		return Message{}, operationError(ctx, err, outcome, id)
	}
	stream, err := s.raw.OpenStream(op.ctx)
	if err != nil {
		return fail(err, NotSent, 0)
	}
	op.stream = stream
	defer stream.Close()
	stop := context.AfterFunc(op.ctx, func() { _ = stream.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = stream.SetReadDeadline(deadline)
	_ = stream.SetWriteDeadline(deadline)
	id := stream.ID()
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return fail(context.DeadlineExceeded, NotSent, id)
	}
	timeoutMS := uint64(remaining / time.Millisecond)
	if remaining%time.Millisecond != 0 {
		timeoutMS++
	}
	open := wireOpen{ID: id, Mode: mode, Route: route, Metadata: msg.Metadata, Size: len(msg.Body), TimeoutMS: timeoutMS}
	if err = writeJSON(stream, recordOpen, open); err != nil {
		return fail(err, admissionOutcome(err), id)
	}
	h, err := readRecordHeader(stream)
	if err != nil {
		return fail(err, admissionOutcome(err), id)
	}
	if h.kind == recordOpenError {
		var status StatusError
		if err = readJSONBody(stream, h, &status); err != nil {
			return fail(err, Unknown, id)
		}
		if !validStatus(&status) {
			return fail(protocolError("invalid OPEN_ERR"), Unknown, id)
		}
		finishStream(stream)
		return Message{}, remoteError(&status, Rejected, id)
	}
	if h.kind != recordOpenOK || h.tag != 0 || h.length != 0 {
		return fail(protocolError("expected empty OPEN_OK"), Unknown, id)
	}
	if err = writeBody(stream, msg.Body); err != nil {
		return fail(err, Unknown, id)
	}
	if mode == "notify" {
		if err = stream.CloseWrite(); err != nil {
			return fail(err, Unknown, id)
		}
		h, err := readRecordHeader(stream)
		if err != nil {
			return fail(err, Unknown, id)
		}
		if h.kind != recordAck || h.tag != 0 || h.length != 0 {
			return fail(protocolError("expected notification ACK"), Unknown, id)
		}
		s.touch()
		finishStream(stream)
		return Message{}, nil
	}
	var result wireResult
	if err = readJSON(stream, recordResult, &result); err != nil {
		return fail(err, Unknown, id)
	}
	if result.Status != nil && !validStatus(result.Status) {
		return fail(protocolError("invalid RESULT status"), Unknown, id)
	}
	if err = checkMessage(s.cfg.Limits, result.Size, result.Metadata); err != nil {
		return fail(err, Unknown, id)
	}
	if err = s.reserve(op, result.Size, 0, false); err != nil {
		return fail(err, Unknown, id)
	}
	body, err := readBody(stream, result.Size)
	if err != nil {
		return fail(err, Unknown, id)
	}
	s.touch()
	// The complete response is authoritative even if cancellation races with FIN.
	finishStream(stream)
	if result.Status != nil {
		return Message{}, remoteError(result.Status, Responded, id)
	}
	return Message{Metadata: result.Metadata, Body: body}, nil
}

func finishStream(stream *native.Stream) {
	if stream.CloseWrite() == nil {
		var extra [1]byte
		_, _ = stream.Read(extra[:])
	}
}

func (s *Session) rejectRequest(op *operation, err error) {
	status := handlerStatus(err)
	if errors.Is(err, ErrDraining) {
		status.Code = CodeUnavailable
	}
	_ = op.stream.SetWriteDeadline(time.Now().Add(s.cfg.DefaultWriteTimeout))
	if writeJSON(op.stream, recordOpenError, status) == nil {
		finishStream(op.stream)
	}
}

func callRequest(handler RequestHandler, ctx context.Context, request *Request) (msg Message, err error) {
	defer func() {
		if recover() != nil {
			err = &StatusError{Code: CodeInternal, Message: "request handler panicked"}
		}
	}()
	return handler(ctx, request)
}

func (s *Session) serveOperation(op *operation) {
	defer s.finish(op)
	stream := op.stream
	defer stream.Close()
	stop := context.AfterFunc(op.ctx, func() { _ = stream.Close() })
	defer stop()
	loadDeadline := time.Now().Add(s.cfg.DefaultRequestTimeout)
	_ = stream.SetReadDeadline(loadDeadline)
	var open wireOpen
	if err := readJSON(stream, recordOpen, &open); err != nil {
		return
	}
	s.touch()
	longLived := open.Mode == "stream" || open.Mode == "channel"
	if open.ID != stream.ID() || (open.Mode != "request" && open.Mode != "notify" && !longLived) || !validRoute(open.Route) || (!longLived && open.TimeoutMS == 0) || open.TimeoutMS > uint64(math.MaxInt64/int64(time.Millisecond)) || longLived && open.Size != 0 {
		s.rejectRequest(op, protocolError("invalid request OPEN"))
		return
	}
	ctx, cancel := context.WithCancel(op.ctx)
	if open.TimeoutMS != 0 {
		cancel()
		ctx, cancel = context.WithTimeout(op.ctx, time.Duration(open.TimeoutMS)*time.Millisecond)
	}
	defer cancel()
	stopBudget := context.AfterFunc(ctx, func() {
		code := native.Canceled
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = native.DeadlineExceeded
		}
		_ = stream.Reset(code)
	})
	defer stopBudget()
	deadline, _ := ctx.Deadline()
	_ = stream.SetReadDeadline(minTime(loadDeadline, deadline))
	_ = stream.SetWriteDeadline(minTime(time.Now().Add(s.cfg.DefaultWriteTimeout), deadline))
	handler := s.routes.requests[open.Route]
	notifyHandler := s.routes.notifies[open.Route]
	streamHandler := s.routes.streams[open.Route]
	channelHandler := s.routes.channels[open.Route]
	if open.Mode == "request" && handler == nil || open.Mode == "notify" && notifyHandler == nil || open.Mode == "stream" && streamHandler == nil || open.Mode == "channel" && channelHandler == nil {
		s.rejectRequest(op, &StatusError{Code: CodeNotFound, Message: "unknown route or mode"})
		return
	}
	if err := checkMessage(s.cfg.Limits, open.Size, open.Metadata); err != nil {
		s.rejectRequest(op, err)
		return
	}
	if authorize := s.cfg.AuthorizeOperation; authorize != nil {
		if err := authorize(ctx, s.info.Remote, OperationInfo{ID: open.ID, Mode: open.Mode, Route: open.Route}); err != nil {
			s.rejectRequest(op, &StatusError{Code: CodePermissionDenied, Message: "operation denied"})
			return
		}
	}
	receive := open.Size
	if open.Mode == "channel" {
		if open.Channel == nil || *open.Channel != channelLimits(s.peerLimits) {
			s.rejectRequest(op, protocolError("invalid channel window"))
			return
		}
		receive = s.cfg.Limits.ChannelWindowBytes
	}
	if err := s.reserve(op, receive, 0, true); err != nil {
		s.rejectRequest(op, err)
		return
	}
	if open.Mode == "channel" {
		if writeJSON(stream, recordOpenOK, channelLimits(s.cfg.Limits)) != nil {
			return
		}
		_ = stream.SetReadDeadline(time.Time{})
		channel := newChannel(s, op, ctx, open.Route, open.Metadata, *open.Channel, false)
		err := callChannel(channelHandler, ctx, channel)
		if err != nil {
			s.emitEvent(Event{Kind: HandlerFailed, Operation: open.ID, Route: open.Route, Err: handlerStatus(err)})
		}
		if err == nil {
			channel.finishHandler()
		}
		_ = channel.Close()
		channel.wg.Wait()
		return
	}
	if err := writeRecord(stream, recordOpenOK, 0, nil); err != nil {
		return
	}
	if open.Mode == "stream" {
		_ = stream.SetReadDeadline(time.Time{})
		st := newStream(s, op, ctx, open.Route, open.Metadata, false)
		err := callStream(streamHandler, ctx, st)
		if err != nil {
			s.emitEvent(Event{Kind: HandlerFailed, Operation: open.ID, Route: open.Route, Err: handlerStatus(err)})
		} else {
			_ = st.CloseWrite()
		}
		_ = st.Close()
		return
	}
	body, err := readBody(stream, open.Size)
	if err != nil {
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.touch()
	_ = stream.SetReadDeadline(deadline)
	if open.Mode == "notify" {
		s.serveNotify(op, notifyHandler, open, body)
		return
	}
	var responding atomic.Bool
	tail := make(chan error, 1)
	go func() {
		h, err := readRecordHeader(stream)
		if err == nil {
			err = protocolError("unexpected trailing record %d", h.kind)
		}
		if err != io.EOF || !responding.Load() {
			op.cancel(err)
		}
		tail <- err
	}()
	result, err := callRequest(handler, ctx, &Request{Route: open.Route, Message: Message{Metadata: open.Metadata, Body: body}, session: s})
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = checkMessage(s.cfg.Limits, len(result.Body), result.Metadata)
	}
	if err == nil {
		err = checkMessage(s.peerLimits, len(result.Body), result.Metadata)
	}
	if err == nil {
		err = s.reserve(op, 0, len(result.Body), false)
	}
	status := handlerStatus(err)
	if status != nil {
		result = Message{}
	}
	responding.Store(true)
	_ = stream.SetWriteDeadline(minTime(time.Now().Add(s.cfg.DefaultWriteTimeout), deadline))
	if writeJSON(stream, recordResult, wireResult{Status: status, Metadata: result.Metadata, Size: len(result.Body)}) != nil {
		return
	}
	if writeBody(stream, result.Body) != nil {
		return
	}
	if stream.CloseWrite() != nil {
		return
	}
	<-tail
}

func minTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	}
	if b.IsZero() {
		return a
	}
	if a.Before(b) {
		return a
	}
	return b
}
