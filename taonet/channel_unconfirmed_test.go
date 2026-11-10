package taonet

import (
	"bytes"
	"testing"
)

func TestUnconfirmedMixPreservesOrderWindowsAndConfirmedRejections(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{ChannelWindowMessages: 1})
	for index, size := range []int{10, 20 << 10} {
		body := bytes.Repeat([]byte{byte(size)}, size)
		expected := append([]byte(nil), body...)
		written, err := a.SendOwnedUnconfirmed(t.Context(), Message{Body: body})
		if err != nil {
			t.Fatal(err)
		}
		first := receive(t, b)
		if !first.Unconfirmed() || !bytes.Equal(first.Message().Body, expected) {
			t.Fatal("unconfirmed delivery changed")
		}
		// Write completion does not wait for this acceptance decision.
		if err := written.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		waitSignal(t, written.Released())
		clear(body)
		confirmed, err := a.SendOwnedWithReceipt(t.Context(), Message{Body: []byte("confirmed")})
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Reject(CodeApplication); err != nil {
			t.Fatal(err)
		}
		a.mu.Lock()
		if a.sentMessages != uint64(2*index+1) {
			t.Error("unreleased unconfirmed delivery did not hold the message window")
		}
		a.mu.Unlock()
		first.Release()
		second := receive(t, b)
		if second.Unconfirmed() || string(second.Message().Body) != "confirmed" {
			t.Fatal("mixed send order/mode changed")
		}
		if err := second.Reject(CodeNotFound); err != nil {
			t.Fatal(err)
		}
		second.Release()
		requireOpError(t, confirmed.Wait(t.Context()), CodeNotFound, Rejected)
		if err := written.Wait(t.Context()); err != nil {
			t.Fatalf("peer decision rewrote local write result: %v", err)
		}
	}
	closeChannels(t, a, b)
}

func TestUnconfirmedCloseReleasesQueuedInputWithoutClaimingWrite(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{ChannelWindowMessages: 1})
	first, err := a.SendOwnedUnconfirmed(t.Context(), Message{Body: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	d := receive(t, b)
	defer d.Release()
	second, err := a.SendOwnedUnconfirmed(t.Context(), Message{Body: []byte("queued")})
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Close()
	requireOpError(t, second.Wait(t.Context()), CodeUnavailable, NotSent)
	waitSignal(t, second.Released())
}

func TestUnconfirmedPartialWindowFlushesWhileOtherwiseIdle(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{ChannelWindowMessages: 8})
	written, err := a.SendOwnedUnconfirmed(t.Context(), Message{Body: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	d := receive(t, b)
	if err := d.Accept(); err != nil {
		t.Fatal(err)
	}
	if err := written.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	d.Release()
	// One message is below both WINDOW thresholds. Credit must still return
	// without another Send, ACK, or half-close waking the receiver's writer.
	waitChannel(t, a, func() bool { return a.peerReleasedMessages == 1 })
	closeChannels(t, a, b)
}
