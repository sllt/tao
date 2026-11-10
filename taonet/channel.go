package taonet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	native "github.com/sllt/tao/taonet/internal/mux"
)

type channelOptions struct {
	Bytes      uint64 `json:"bytes"`
	Messages   uint64 `json:"messages"`
	MaxMessage int    `json:"max_message"`
}

func channelLimits(l Limits) channelOptions {
	return channelOptions{uint64(l.ChannelWindowBytes), uint64(l.ChannelWindowMessages), l.ChannelMaxMessage}
}

type channelMessage struct {
	Size     int               `json:"size"`
	Metadata map[string]string `json:"metadata,omitempty"`
}
type channelAck struct {
	Status *StatusError `json:"status,omitempty"`
}
type queuedAck struct {
	sequence uint64
	status   *StatusError
}
type channelSend struct {
	message      Message
	charge       int
	sequence     uint64
	offset       int
	started      bool
	confirmed    bool
	unconfirmed  bool
	owned        bool
	bodyReleased bool
	resolved     bool
	receipt      SendReceipt
}

// SendReceipt records the peer's acceptance or rejection of one Channel
// message. It is not a business-completion result. Receipt state is immutable
// after Done closes; a receipt can be waited on by multiple goroutines.
type SendReceipt struct {
	done      receiptNotification
	released  receiptNotification
	err       error
	operation uint64
}

func (r *SendReceipt) Done() <-chan struct{} { return r.done.channel() }

// IsDone queries completion without allocating a waiting notification.
// Completion does not release owned input; Released remains its lifetime barrier.
func (r *SendReceipt) IsDone() bool { return r.done.ready() }

// Released closes when the Channel no longer reads the submitted Body or
// Metadata. It is independent of remote acceptance and waiting cancellation.
// Copying sends release their input on admission; owned sends may reuse it only
// after this signal, including when Done has already reported an error.
func (r *SendReceipt) Released() <-chan struct{} { return r.released.channel() }

// WriteReceipt reports physical stream-write completion, without a peer
// acceptance decision. Successful Wait does not prove delivery or execution.
// Released remains the independent input-ownership barrier, including on error.
type WriteReceipt SendReceipt

func (r *WriteReceipt) Done() <-chan struct{}          { return r.done.channel() }
func (r *WriteReceipt) IsDone() bool                   { return r.done.ready() }
func (r *WriteReceipt) Released() <-chan struct{}      { return r.released.channel() }
func (r *WriteReceipt) Wait(ctx context.Context) error { return (*SendReceipt)(r).Wait(ctx) }

var releasedInput = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Wait stops waiting when ctx ends without retracting the queued message.
// A confirmed decision wins over concurrent cancellation; otherwise the result
// is Unknown. The Channel owns and bounds receipt bookkeeping until settlement.
func (r *SendReceipt) Wait(ctx context.Context) error {
	done := r.Done()
	select {
	case <-done:
		return r.err
	default:
	}
	select {
	case <-done:
		return r.err
	case <-ctx.Done():
		select {
		case <-done:
			return r.err
		default:
		}
		return operationError(ctx, ctx.Err(), Unknown, r.operation)
	}
}

type Channel struct {
	Route                                   string
	Metadata                                map[string]string
	session                                 *Session
	op                                      *operation
	raw                                     *native.Stream
	ctx                                     context.Context
	local, peer                             channelOptions
	inline                                  bool // negotiated once, before reader/writer start
	unconfirmed                             bool
	mu                                      sync.Mutex
	wg                                      sync.WaitGroup
	done                                    chan struct{}
	changed                                 chan struct{}
	writerWake                              chan struct{}
	writerWaiting                           bool
	receiveWake                             chan struct{}
	closed                                  bool
	err                                     error
	writeClosing                            bool
	txEOF, rxEOF                            bool
	finSent, finReceived                    bool
	queue                                   fifo[*channelSend]
	active                                  *channelSend
	pending                                 map[uint64]*channelSend
	expectedAcks                            fifo[uint64]
	sentBytes, sentMessages                 uint64
	endIssued, ackReceived                  uint64
	peerReleasedBytes, peerReleasedMessages uint64
	receivedBytes, receivedMessages         uint64
	releasedBytes, releasedMessages         uint64
	advertisedBytes, advertisedMessages     uint64
	acks                                    []queuedAck
	nextDecision                            uint64
	deliveries                              map[uint64]*Delivery
	inbox                                   fifo[*Delivery]
	receiveCache                            [][]byte
	receiveCacheBytes                       uint64
}

func (s *Session) OpenChannel(ctx context.Context, route string, metadata map[string]string) (*Channel, error) {
	op, err := s.openDuplex(ctx, "channel", route, metadata)
	if err != nil {
		return nil, err
	}
	return newChannel(s, op, op.ctx, route, maps.Clone(metadata), channelLimits(s.peerLimits), true), nil
}

func newChannel(s *Session, op *operation, ctx context.Context, route string, metadata map[string]string, peer channelOptions, owner bool) *Channel {
	c := &Channel{Route: route, Metadata: metadata, session: s, op: op, raw: op.stream, ctx: ctx, local: channelLimits(s.cfg.Limits), peer: peer, inline: slices.Contains(s.info.Features, channelInlineFeature),
		unconfirmed: slices.Contains(s.info.Features, channelUnconfirmedFeature),
		done:        make(chan struct{}), writerWake: make(chan struct{}, 1), pending: make(map[uint64]*channelSend), deliveries: make(map[uint64]*Delivery), nextDecision: 1}
	c.wg.Add(2)
	go c.readLoop()
	go c.writeLoop()
	go func() {
		select {
		case <-ctx.Done():
			c.end(context.Cause(ctx))
		case <-c.done:
		}
		c.wg.Wait()
		if owner {
			s.finish(op)
		}
	}()
	return c
}

func (c *Channel) Session() *Session     { return c.session }
func (c *Channel) ID() uint64            { return c.raw.ID() }
func (c *Channel) Done() <-chan struct{} { return c.done }
func (c *Channel) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *Channel) changedLocked() <-chan struct{} {
	if c.changed == nil {
		c.changed = make(chan struct{})
	}
	return c.changed
}
func (c *Channel) notifyLocked() {
	if c.writerWaiting {
		c.writerWaiting = false
		select {
		case c.writerWake <- struct{}{}:
		default:
		}
	}
	c.notifyChangedLocked()
}

func (c *Channel) notifyChangedLocked() {
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
}

func (c *Channel) receiveWakeLocked() <-chan struct{} {
	if c.receiveWake == nil {
		c.receiveWake = make(chan struct{}, 1)
	}
	return c.receiveWake
}

func (c *Channel) notifyReceiveLocked() {
	if c.rxEOF { // receiveWake is closed to broadcast the peer's half-close.
		return
	}
	select {
	case c.receiveWake <- struct{}{}:
	default:
	}
}

func (c *Channel) end(err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.err = err
	close(c.done)
	c.notifyLocked()
	if err != nil {
		for _, item := range c.pending {
			c.resolveSendLocked(item, operationError(c.ctx, err, Unknown, c.raw.ID()))
		}
		for c.queue.len() > 0 {
			item := c.queue.pop()
			c.resolveSendLocked(item, operationError(c.ctx, err, NotSent, c.raw.ID()))
			c.releaseBodyLocked(item)
		}
		if c.active != nil && !c.active.started {
			c.resolveSendLocked(c.active, operationError(c.ctx, err, NotSent, c.raw.ID()))
			c.releaseBodyLocked(c.active)
		}
	}
	c.queue = fifo[*channelSend]{}
	c.pending = nil
	c.expectedAcks = fifo[uint64]{}
	c.inbox = fifo[*Delivery]{}
	for _, d := range c.deliveries {
		d.message = Message{}
		d.released = true
	}
	c.deliveries = nil
	c.receiveCache = nil
	c.receiveCacheBytes = 0
	c.mu.Unlock()
	if err != nil {
		c.op.cancel(err)
		_ = c.raw.Close()
	}
}

func (c *Channel) Close() error { c.end(ErrClosed); return nil }
func (c *Channel) CloseWrite() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		if c.err != nil {
			return c.err
		}
		return nil
	}
	c.writeClosing = true
	c.notifyLocked()
	return nil
}

func messageCharge(size int, metadata map[string]string) (int, error) {
	if len(metadata) == 0 {
		return size + 128, nil
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return 0, err
	}
	return size + len(encoded) + 128, nil
}

func (c *Channel) Send(ctx context.Context, msg Message) error {
	_, err := c.enqueue(ctx, msg, false, false, false)
	return err
}
func (c *Channel) SendConfirmed(ctx context.Context, msg Message) error {
	ctx, cancel := withDefaultTimeout(ctx, c.session.cfg.DefaultRequestTimeout)
	defer cancel()
	receipt, err := c.SendWithReceipt(ctx, msg)
	if err != nil {
		return err
	}
	return receipt.Wait(ctx)
}

// SendWithReceipt makes the same bounded local copy as Send and returns without
// waiting for the peer. ctx controls enqueue admission; use receipt.Wait(ctx)
// to await acceptance within the same caller budget. Forgetting the receipt
// does not disable the channel's byte/message windows or queue limits.
func (c *Channel) SendWithReceipt(ctx context.Context, msg Message) (*SendReceipt, error) {
	item, err := c.enqueue(ctx, msg, true, false, false)
	if err != nil {
		return nil, err
	}
	return &item.receipt, nil
}

// SendOwnedWithReceipt transfers Body and Metadata to the channel on success.
// The caller must not access or reuse them until receipt.Released closes;
// cancellation or remote acceptance alone does not release the input.
// On admission failure ownership remains with the caller.
// Use SendWithReceipt when the caller needs to retain or reuse its buffers.
func (c *Channel) SendOwnedWithReceipt(ctx context.Context, msg Message) (*SendReceipt, error) {
	item, err := c.enqueue(ctx, msg, true, true, false)
	if err != nil {
		return nil, err
	}
	return &item.receipt, nil
}

// SendOwnedUnconfirmed transfers input ownership like SendOwnedWithReceipt,
// but suppresses peer acceptance ACKs. The receiver still resolves and releases
// deliveries in order, and both byte/message windows remain enforced. Use this
// when an upper protocol supplies its own reply, including rejection. It returns
// ErrUnsupported without taking ownership when the peer lacks this feature.
func (c *Channel) SendOwnedUnconfirmed(ctx context.Context, msg Message) (*WriteReceipt, error) {
	if !c.unconfirmed {
		return nil, ErrUnsupported
	}
	item, err := c.enqueue(ctx, msg, true, true, true)
	if err != nil {
		return nil, err
	}
	return (*WriteReceipt)(&item.receipt), nil
}

func (c *Channel) enqueue(ctx context.Context, msg Message, confirmed, owned, unconfirmed bool) (*channelSend, error) {
	fail := func(err error) (*channelSend, error) { return nil, operationError(ctx, err, NotSent, c.raw.ID()) }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if len(msg.Body) > min(c.local.MaxMessage, c.peer.MaxMessage) {
		return fail(&StatusError{Code: CodeMessageTooLarge, Message: "channel message too large"})
	}
	if err := checkMetadata(msg.Metadata, c.session.cfg.Limits); err != nil {
		return fail(err)
	}
	if err := checkMetadata(msg.Metadata, c.session.peerLimits); err != nil {
		return fail(err)
	}
	charge, err := messageCharge(len(msg.Body), msg.Metadata)
	if err != nil {
		return fail(err)
	}
	if uint64(charge) > c.peer.Bytes {
		return fail(&StatusError{Code: CodeMessageTooLarge, Message: "message exceeds channel window"})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		err := c.err
		if err == nil {
			err = io.ErrClosedPipe
		}
		return fail(err)
	}
	if c.writeClosing {
		return fail(io.ErrClosedPipe)
	}
	if c.queue.len() >= 4096 {
		return fail(&StatusError{Code: CodeResourceExhausted, Message: "channel send queue full"})
	}
	if err = c.session.reserve(c.op, 0, charge, false); err != nil {
		return fail(err)
	}
	if !owned {
		msg = Message{Body: append([]byte(nil), msg.Body...), Metadata: maps.Clone(msg.Metadata)}
	}
	item := &channelSend{message: msg, charge: charge, confirmed: confirmed, owned: owned, unconfirmed: unconfirmed, receipt: SendReceipt{operation: c.raw.ID()}}
	if !owned {
		item.receipt.released.finish()
	}
	c.queue.push(item)
	c.notifyLocked()
	return item, nil
}

func (c *Channel) releaseBodyLocked(item *channelSend) {
	if item.bodyReleased {
		return
	}
	item.bodyReleased = true
	item.message = Message{}
	if item.owned {
		item.receipt.released.finish()
	}
}

func (c *Channel) resolveSendLocked(item *channelSend, err error) {
	if item.resolved {
		return
	}
	item.resolved = true
	item.receipt.err = err
	item.receipt.done.finish()
	if err != nil && !item.confirmed {
		c.session.emitEvent(Event{Kind: SendFailed, Operation: c.raw.ID(), Route: c.Route, Err: err})
	}
}

func (c *Channel) Receive(ctx context.Context) (*Delivery, error) {
	for {
		if err := ctx.Err(); err != nil {
			// A canceled waiter may have consumed the coalesced wakeup on
			// its previous iteration. Keep queued data visible to other readers.
			c.mu.Lock()
			if c.inbox.len() > 0 {
				c.notifyReceiveLocked()
			}
			c.mu.Unlock()
			return nil, err
		}
		c.mu.Lock()
		if c.inbox.len() > 0 {
			d := c.inbox.pop()
			if c.inbox.len() > 0 {
				c.notifyReceiveLocked()
			}
			c.mu.Unlock()
			return d, nil
		}
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return nil, err
		}
		if c.rxEOF {
			c.mu.Unlock()
			return nil, io.EOF
		}
		changed := c.receiveWakeLocked()
		c.mu.Unlock()
		select {
		case <-changed:
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

type Delivery struct {
	channel                      *Channel
	sequence                     uint64
	charge                       uint64
	message                      Message
	decided, released, ackQueued bool
	unconfirmed                  bool
	status                       *StatusError
}

// Unconfirmed means Accept/Reject are local decisions: the sender did not ask
// for an acceptance ACK. The application must send any required response.
func (d *Delivery) Unconfirmed() bool { return d.unconfirmed }

// Message borrows Body until Release. Copy it to retain it beyond that point.
func (d *Delivery) Message() Message {
	d.channel.mu.Lock()
	defer d.channel.mu.Unlock()
	return d.message
}
func (d *Delivery) Accept() error { return d.resolve(nil) }
func (d *Delivery) Reject(code Code) error {
	status := &StatusError{Code: code, Message: "message rejected"}
	if !validStatus(status) {
		return fmt.Errorf("taonet: invalid rejection code")
	}
	return d.resolve(status)
}
func (d *Delivery) resolve(status *StatusError) error {
	c := d.channel
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.decided {
		return ErrAlreadyResolved
	}
	if c.closed {
		return ErrClosed
	}
	if d.sequence != c.nextDecision {
		return ErrOutOfOrder
	}
	d.decided = true
	d.status = status
	released := c.releasedMessages
	c.resolvePrefixLocked()
	// Unconfirmed acceptance alone creates no writer work. Resolving an
	// earlier decision may also release an already-abandoned suffix; keep
	// that credit notification and every confirmed ACK prompt.
	if len(c.acks) > 0 || c.releasedMessages != released {
		c.notifyLocked()
	} else {
		c.notifyChangedLocked()
	}
	return nil
}
func (d *Delivery) Release() {
	c := d.channel
	c.mu.Lock()
	defer c.mu.Unlock()
	if d.released {
		return
	}
	d.released = true
	body := d.message.Body
	d.message = Message{}
	if c.closed {
		return
	}
	if !d.decided {
		d.decided = true
		d.status = &StatusError{Code: CodeAbandoned, Message: "delivery released before acceptance"}
	}
	if d.ackQueued {
		c.releaseDeliveryLocked(d)
	} else {
		c.resolvePrefixLocked()
	}
	c.cacheReceiveBodyLocked(body)
	c.notifyLocked()
}
func (c *Channel) releaseDeliveryLocked(d *Delivery) {
	c.releasedBytes += d.charge
	c.releasedMessages++
	delete(c.deliveries, d.sequence)
}
func (c *Channel) resolvePrefixLocked() {
	for {
		d := c.deliveries[c.nextDecision]
		if d == nil || !d.decided {
			return
		}
		d.ackQueued = true
		if !d.unconfirmed {
			c.acks = append(c.acks, queuedAck{d.sequence, d.status})
		}
		c.nextDecision++
		if d.released {
			c.releaseDeliveryLocked(d)
		}
	}
}

func (c *Channel) readyForFINLocked() bool {
	return c.txEOF && c.rxEOF && len(c.deliveries) == 0 && len(c.pending) == 0 && len(c.acks) == 0 && c.advertisedBytes == c.releasedBytes && c.advertisedMessages == c.releasedMessages && c.peerReleasedBytes == c.sentBytes && c.peerReleasedMessages == c.sentMessages
}

func (c *Channel) finishHandler() {
	_ = c.CloseWrite()
	c.mu.Lock()
	drained := c.rxEOF && len(c.deliveries) == 0
	c.mu.Unlock()
	if drained {
		select {
		case <-c.done:
		case <-c.ctx.Done():
		}
	}
}

func callChannel(handler ChannelHandler, ctx context.Context, channel *Channel) (err error) {
	defer func() {
		if recover() != nil {
			err = &StatusError{Code: CodeInternal, Message: "channel handler panicked"}
		}
	}()
	return handler(ctx, channel)
}
