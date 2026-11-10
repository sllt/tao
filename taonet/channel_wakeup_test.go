package taonet

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

func TestUnconfirmedAcceptDoesNotWakeIdleWriter(t *testing.T) {
	c := &Channel{
		local:      channelOptions{Bytes: 4096, Messages: 8},
		writerWake: make(chan struct{}, 1), writerWaiting: true,
		nextDecision: 1, deliveries: make(map[uint64]*Delivery),
	}
	first := &Delivery{channel: c, sequence: 1, charge: 128, unconfirmed: true}
	second := &Delivery{channel: c, sequence: 2, charge: 128, unconfirmed: true}
	c.deliveries[1], c.deliveries[2] = first, second
	changed := c.changedLocked()
	if err := first.Accept(); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, changed)
	select {
	case <-c.writerWake:
		t.Fatal("unconfirmed acceptance without credit woke an idle writer")
	default:
	}
	first.Release()
	waitSignal(t, c.writerWake) // Release retains its original notification.
	c.writerWaiting = true
	if err := second.Accept(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.writerWake:
		t.Fatal("acceptance notified unchanged credit")
	default:
	}
	// Two released messages cross the existing quarter-window threshold.
	second.Release()
	waitSignal(t, c.writerWake)
	c.writerWaiting = true
	c.advertisedBytes, c.advertisedMessages = c.releasedBytes, c.releasedMessages
	third := &Delivery{channel: c, sequence: 3, charge: 128, unconfirmed: true}
	c.deliveries[3] = third
	if err := third.Accept(); err != nil {
		t.Fatal(err)
	}
	c.txEOF, c.rxEOF = true, true
	third.Release()
	waitSignal(t, c.writerWake) // The final WINDOW/FIN must not wait for a tick.
}

func TestChannelConcurrentReceiversDrainBeforeHalfClose(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{})
	var received atomic.Int32
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for {
				d, err := b.Receive(t.Context())
				if err == io.EOF {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				received.Add(1)
				d.Release()
			}
		})
	}
	for range 32 {
		if err := a.Send(t.Context(), Message{Body: []byte("message")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { readers.Wait(); close(done) }()
	waitSignal(t, done)
	if received.Load() != 32 {
		t.Fatal("half-close overtook queued deliveries")
	}
	closeChannels(t, a, b)
}

func TestCanceledChannelReceiverRestoresConsumedWake(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{})
	b.mu.Lock()
	wake := b.receiveWakeLocked()
	b.mu.Unlock()
	if err := a.Send(t.Context(), Message{Body: []byte("still queued")}); err != nil {
		t.Fatal(err)
	}
	// Replay a waiter that consumed DATA's wake and was canceled before its
	// next queue check. Other receivers must retain a notification to proceed.
	waitSignal(t, wake)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := b.Receive(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	waitSignal(t, wake)
	d := receive(t, b)
	if string(d.Message().Body) != "still queued" {
		t.Fatal("cancellation consumed the delivery")
	}
	_ = d.Accept()
	d.Release()
	closeChannels(t, a, b)
}
