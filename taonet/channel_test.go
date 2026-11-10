package taonet

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

func channelPair(t *testing.T, limits Limits) (*Channel, *Channel, *Session, *Session) {
	t.Helper()
	accepted := make(chan *Channel, 1)
	m := NewMux()
	if err := m.HandleChannel("chat", func(ctx context.Context, c *Channel) error {
		accepted <- c
		select {
		case <-c.Done():
		case <-ctx.Done():
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("client", nil), localConfig("server", m)
	a.Limits = limits
	b.Limits = limits
	client, server := pair(t, a, b)
	channel, err := client.OpenChannel(t.Context(), "chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	select {
	case peer := <-accepted:
		return channel, peer, client, server
	case <-time.After(5 * time.Second):
		t.Fatal("channel not dispatched")
		return nil, nil, nil, nil
	}
}

func receive(t *testing.T, c *Channel) *Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	d, err := c.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func waitChannel(t *testing.T, c *Channel, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		c.mu.Lock()
		ready := condition()
		changed := c.changedLocked()
		c.mu.Unlock()
		if ready {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal("channel state did not advance")
		}
	}
}

func closeChannels(t *testing.T, a, b *Channel) {
	t.Helper()
	_ = a.CloseWrite()
	_ = b.CloseWrite()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, c := range []*Channel{a, b} {
		if _, err := c.Receive(ctx); err != io.EOF {
			t.Fatalf("expected message EOF: %v", err)
		}
	}
	for _, c := range []*Channel{a, b} {
		waitSignal(t, c.Done())
		if err := c.Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChannelReceiptsPipelineAndKeepDecisionAfterWaitCancellation(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{})
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	first, err := a.SendWithReceipt(ctx, Message{Body: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	d1 := receive(t, b)
	second, err := a.SendWithReceipt(ctx, Message{Body: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	d2 := receive(t, b) // Must arrive before deciding the first delivery.
	if string(d1.Message().Body) != "first" || string(d2.Message().Body) != "second" {
		t.Fatal("message order")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	var uncertain *OpError
	if err := first.Wait(canceled); !errors.As(err, &uncertain) || uncertain.Outcome != Unknown {
		t.Fatalf("canceled receipt: %v", err)
	}
	if err := d1.Reject(CodeResourceExhausted); err != nil {
		t.Fatal(err)
	}
	d1.Release()
	if err := d2.Accept(); err != nil {
		t.Fatal(err)
	}
	d2.Release()
	for range 2 { // Receipt results are repeatable, not consumed by the first waiter.
		var rejected *OpError
		if err := first.Wait(ctx); !errors.As(err, &rejected) || rejected.Outcome != Rejected {
			t.Fatalf("decision after cancellation: %v", err)
		}
		if err := second.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	closeChannels(t, a, b)
}

func TestChannelQueuedBurstCrossesMessageSizesAndWindows(t *testing.T) {
	a, b, client, server := channelPair(t, Limits{ChannelMaxMessage: 65536, ChannelWindowBytes: 128 << 10, ChannelWindowMessages: 4})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sizes := []int{0, 1, 1024, 16 << 10, 16<<10 + 1, 65000}
	var receipts []*SendReceipt
	for i := range 48 {
		body := bytes.Repeat([]byte{byte(i)}, sizes[i%len(sizes)])
		message := Message{Body: body, Metadata: map[string]string{"id": fmt.Sprint(i)}}
		if i%3 == 0 {
			message.Metadata = nil // Mix negotiated inline and legacy records.
		}
		var r *SendReceipt
		var err error
		if i%2 == 0 {
			r, err = a.SendOwnedWithReceipt(ctx, message)
		} else {
			r, err = a.SendWithReceipt(ctx, message)
			clear(body) // The copying API must own an independent copy.
		}
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, r)
	}
	if err := a.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	for i := range receipts {
		d := receive(t, b)
		msg := d.Message()
		wantID := fmt.Sprint(i)
		if i%3 == 0 {
			wantID = ""
		}
		if !bytes.Equal(msg.Body, bytes.Repeat([]byte{byte(i)}, sizes[i%len(sizes)])) || msg.Metadata["id"] != wantID {
			t.Fatalf("message %d changed or reordered", i)
		}
		var err error
		if i%7 == 0 {
			err = d.Reject(CodeApplication)
		} else {
			err = d.Accept()
		}
		if err != nil {
			t.Fatal(err)
		}
		d.Release()
	}
	for i, r := range receipts {
		err := r.Wait(ctx)
		if i%7 == 0 {
			requireOpError(t, err, CodeApplication, Rejected)
		} else if err != nil {
			t.Fatal(err)
		}
	}
	closeChannels(t, a, b)
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestChannelCloseSettlesOutstandingReceiptsWithoutReturningCredit(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{ChannelMaxMessage: 256, ChannelWindowBytes: 8192, ChannelWindowMessages: 16})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var receipts []*SendReceipt
	for range 40 {
		r, err := a.SendWithReceipt(ctx, Message{Body: []byte("pending")})
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, r)
	}
	for range 16 {
		_ = receive(t, b) // Neither decide nor release: the window stays full.
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	for i, r := range receipts {
		var op *OpError
		want := NotSent
		if i < 16 {
			want = Unknown
		}
		if err := r.Wait(ctx); !errors.As(err, &op) || op.Outcome != want {
			t.Fatalf("receipt %d: %v, want outcome %v", i, err, want)
		}
	}
}

func TestChannelBidirectionalChat(t *testing.T) {
	m := NewMux()
	if err := m.HandleChannel("chat", func(ctx context.Context, c *Channel) error {
		for {
			d, err := c.Receive(ctx)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err = d.Accept(); err != nil {
				return err
			}
			err = c.Send(ctx, d.Message())
			d.Release()
			if err != nil {
				return err
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("client", nil), localConfig("server", m)
	a.Limits = Limits{ChannelMaxMessage: 256, ChannelWindowBytes: 1024, ChannelWindowMessages: 2}
	b.Limits = a.Limits
	client, server := pair(t, a, b)
	c, err := client.OpenChannel(t.Context(), "chat", map[string]string{"room": "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	readDone := make(chan error, 1)
	go func() {
		for i := range 24 {
			d, err := c.Receive(t.Context())
			if err != nil {
				readDone <- err
				return
			}
			if string(d.Message().Body) != fmt.Sprint(i) {
				readDone <- fmt.Errorf("out of order message %d", i)
				return
			}
			err = d.Accept()
			d.Release()
			if err != nil {
				readDone <- err
				return
			}
		}
		_, err := c.Receive(t.Context())
		if err == io.EOF {
			err = nil
		}
		readDone <- err
	}()
	for i := range 24 {
		if err = c.SendConfirmed(t.Context(), Message{Body: []byte(fmt.Sprint(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("chat did not drain")
	}
	waitSignal(t, c.Done())
	if c.Err() != nil {
		t.Fatal(c.Err())
	}
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestChannelSlowConsumerKeepsReverseDirectionAlive(t *testing.T) {
	a, b, client, server := channelPair(t, Limits{ChannelMaxMessage: 256, ChannelWindowBytes: 1024, ChannelWindowMessages: 1})
	if err := a.Send(t.Context(), Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	first := receive(t, b)
	if err := first.Accept(); err != nil {
		t.Fatal(err)
	}
	forward := make(chan error, 1)
	go func() { forward <- a.SendConfirmed(t.Context(), Message{Body: []byte("second")}) }()
	waitChannel(t, a, func() bool { return a.queue.len() > 0 || a.active != nil && !a.active.started })
	reverse := make(chan error, 1)
	go func() { reverse <- b.SendConfirmed(t.Context(), Message{Body: []byte("reverse")}) }()
	d := receive(t, a)
	if string(d.Message().Body) != "reverse" {
		t.Fatal("bad reverse payload")
	}
	_ = d.Accept()
	d.Release()
	if err := <-reverse; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-forward:
		t.Fatalf("ACK incorrectly returned message credit: %v", err)
	default:
	}
	first.Release()
	first.Release()
	d = receive(t, b)
	if string(d.Message().Body) != "second" {
		t.Fatal("bad forward payload")
	}
	_ = d.Accept()
	d.Release()
	if err := <-forward; err != nil {
		t.Fatal(err)
	}
	closeChannels(t, a, b)
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestChannelDecisionOrderAbandonAndTailWindow(t *testing.T) {
	a, b, client, _ := channelPair(t, Limits{ChannelMaxMessage: 256, ChannelWindowBytes: 4096, ChannelWindowMessages: 16})
	for range 2 {
		if err := a.Send(t.Context(), Message{Metadata: map[string]string{}}); err != nil {
			t.Fatal(err)
		}
	}
	first, second := receive(t, b), receive(t, b)
	if err := second.Accept(); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("decision order: %v", err)
	}
	if err := first.Accept(); err != nil {
		t.Fatal(err)
	}
	if err := first.Reject(CodeApplication); !errors.Is(err, ErrAlreadyResolved) {
		t.Fatalf("duplicate decision: %v", err)
	}
	first.Release()
	first.Release()
	second.Release()
	event := nextEvent(t, client)
	if event.Kind != SendFailed {
		t.Fatal(event)
	}
	requireOpError(t, event.Err, CodeAbandoned, Rejected)
	// Two small releases are below both batching thresholds and need tail flush.
	waitChannel(t, a, func() bool { return a.peerReleasedMessages == 2 })
	a.mu.Lock()
	credit := a.peerReleasedBytes
	a.mu.Unlock()
	if credit != 256 {
		t.Fatalf("duplicate release or empty metadata charge: %d", credit)
	}
	closeChannels(t, a, b)
}

func TestChannelSendBudgetAndCancel(t *testing.T) {
	limits := Limits{MaxMessageSize: 256, SendBytes: muxSendBudget + 1024, ChannelMaxMessage: 256, ChannelWindowBytes: 512, ChannelWindowMessages: 1}
	a, b, client, server := channelPair(t, limits)
	msg := Message{Body: make([]byte, 256)}
	receipt, err := a.SendOwnedWithReceipt(t.Context(), msg)
	if err != nil {
		t.Fatal(err)
	}
	first := receive(t, b)
	// The local write released its budget; keep the receive credit occupied.
	// active is nil while a batch is still writing and cannot prove release.
	waitSignal(t, receipt.Released())
	for range 2 {
		if err := a.Send(t.Context(), msg); err != nil {
			t.Fatal(err)
		}
	}
	requireOpError(t, a.Send(t.Context(), msg), CodeResourceExhausted, NotSent)
	_ = a.Close()
	first.Release()
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestChannelRejectsForgedCumulativeWindow(t *testing.T) {
	a, b, client, _ := channelPair(t, Limits{})
	var forged [16]byte
	binary.BigEndian.PutUint64(forged[:8], 1)
	// The peer can reject the forged record and close the connection before
	// its sender observes local write completion. Check the protocol decision.
	writeErr := writeRecord(b.raw, recordWindow, 0, forged[:])
	waitSignal(t, client.Done())
	waitSignal(t, a.Done())
	if !errors.Is(client.Err(), ErrProtocol) {
		t.Fatalf("bad window was not rejected: %v (write: %v)", client.Err(), writeErr)
	}
}

func TestChannelMessagesLargerThanTransportWindow(t *testing.T) {
	a, b, client, server := channelPair(t, Limits{ChannelMaxMessage: 512 << 10, ChannelWindowBytes: 1 << 20, ChannelWindowMessages: 2})
	payload := bytes.Repeat([]byte("duplex"), 64<<10)
	left, right := make(chan error, 1), make(chan error, 1)
	go func() { left <- a.SendConfirmed(t.Context(), Message{Body: payload}) }()
	go func() { right <- b.SendConfirmed(t.Context(), Message{Body: payload}) }()
	for _, c := range []*Channel{a, b} {
		d := receive(t, c)
		if !bytes.Equal(d.Message().Body, payload) {
			t.Fatal("payload corrupted")
		}
		if err := d.Accept(); err != nil {
			t.Fatal(err)
		}
		d.Release()
	}
	if err := <-left; err != nil {
		t.Fatal(err)
	}
	if err := <-right; err != nil {
		t.Fatal(err)
	}
	closeChannels(t, a, b)
	waitIdle(t, client)
	waitIdle(t, server)
}
