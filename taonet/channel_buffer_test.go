package taonet

import (
	"bytes"
	"strconv"
	"testing"
)

func TestReceiveCachePreservesHeldDeliveryAndWindowBudget(t *testing.T) {
	for _, scale := range []int{1, 256} {
		t.Run(strconv.Itoa(scale), func(t *testing.T) {
			a, b, _, _ := channelPair(t, Limits{ChannelMaxMessage: 512 * scale, ChannelWindowBytes: 1024 * scale, ChannelWindowMessages: 8})
			if err := a.Send(t.Context(), Message{Body: bytes.Repeat([]byte{42}, 128*scale)}); err != nil {
				t.Fatal(err)
			}
			held := receive(t, b)
			if err := held.Accept(); err != nil {
				t.Fatal(err)
			}
			view := held.Message().Body
			for i := range 40 {
				body := bytes.Repeat([]byte{byte(i)}, []int{96, 128, 256, 384}[i%4]*scale)
				if err := a.Send(t.Context(), Message{Body: body}); err != nil {
					t.Fatal(err)
				}
				d := receive(t, b)
				if !bytes.Equal(d.Message().Body, body) || !bytes.Equal(view, bytes.Repeat([]byte{42}, 128*scale)) {
					t.Fatal("cache reused an unreleased delivery or exposed stale bytes")
				}
				if err := d.Accept(); err != nil {
					t.Fatal(err)
				}
				d.Release()
				b.mu.Lock()
				charged := b.receivedBytes - b.releasedBytes + b.receiveCacheBytes
				b.mu.Unlock()
				if charged > b.local.Bytes {
					t.Fatalf("cache exceeded the receive reservation: %d > %d", charged, b.local.Bytes)
				}
			}
			held.Release()
			closeChannels(t, a, b)
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.receiveCacheBytes != 0 || len(b.receiveCache) != 0 {
				t.Fatal("closed channel retained its receive cache")
			}
		})
	}
}

func TestReceiveCacheDoesNotSpendUnsettledReleaseCredit(t *testing.T) {
	a, b, _, _ := channelPair(t, Limits{ChannelMaxMessage: 256, ChannelWindowBytes: 768, ChannelWindowMessages: 2})
	for range 2 {
		if err := a.Send(t.Context(), Message{Body: make([]byte, 256)}); err != nil {
			t.Fatal(err)
		}
	}
	first, second := receive(t, b), receive(t, b)
	second.Release()
	b.mu.Lock()
	retained, credit := b.receiveCacheBytes, b.releasedBytes
	b.mu.Unlock()
	if retained != 0 || credit != 0 {
		t.Fatalf("out-of-order release reused unsettled credit: cached=%d credit=%d", retained, credit)
	}
	if err := first.Accept(); err != nil {
		t.Fatal(err)
	}
	first.Release()
	closeChannels(t, a, b)
}
