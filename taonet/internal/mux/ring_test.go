package mux

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
)

func retainedBytes(r *ring) int {
	if r.buf != nil {
		return len(r.buf)
	}
	total := 0
	for index := 0; index < len(r.blocks); {
		if r.blocks[index] == nil {
			index++
			continue
		}
		length := len(r.blocks[index])
		total += length
		index += length / receiveBlockSize
	}
	return total
}

func TestRingPromotesLargeDataOnlyAfterUnreadBytesDrain(t *testing.T) {
	r := ring{limit: 64 << 10}
	prefix := bytes.Repeat([]byte{1}, 1000)
	large := bytes.Repeat([]byte{2}, 32<<10)
	r.write(prefix)
	r.write(large)
	if r.buf != nil {
		t.Fatal("promoted with unread bytes")
	}
	buffer := make([]byte, r.limit)
	n := r.read(buffer)
	if !bytes.Equal(buffer[:n], append(prefix, large...)) {
		t.Fatal("pending data changed during block growth")
	}
	r.write(large)
	if r.blocks != nil || retainedBytes(&r) != r.limit {
		t.Fatal("large flow did not use one reserved contiguous buffer")
	}
	n = r.read(buffer)
	if !bytes.Equal(buffer[:n], large) {
		t.Fatal("promoted receive data changed")
	}
}

func TestRingMatchesFIFOAcrossBlockAndWindowBoundaries(t *testing.T) {
	for _, limit := range []int{4096, 12288, 65536, 262144} {
		r := ring{limit: limit}
		var want bytes.Buffer
		random := rand.New(rand.NewPCG(1, uint64(limit)))
		buffer := make([]byte, limit)
		// Keep unread data while moving the head around the full logical window;
		// exercise partial blocks, full windows and non-power-of-two windows.
		for step := range 1000 {
			if want.Len() < limit && (want.Len() == 0 || random.IntN(2) == 0) {
				n := 1 + random.IntN(limit-want.Len())
				for i := range n {
					buffer[i] = byte(step + i)
				}
				r.write(buffer[:n])
				want.Write(buffer[:n])
			} else {
				n := 1 + random.IntN(limit)
				got := r.read(buffer[:n])
				expected := want.Next(n)
				if !bytes.Equal(buffer[:got], expected) {
					t.Fatalf("limit=%d step=%d: FIFO changed", limit, step)
				}
			}
			if r.size != want.Len() || retainedBytes(&r) > limit {
				t.Fatalf("limit=%d step=%d: size/budget changed", limit, step)
			}
		}
		n := r.read(buffer)
		if !bytes.Equal(buffer[:n], want.Bytes()) {
			t.Fatalf("limit=%d: final drain changed", limit)
		}
	}
}

func TestRingIdleAndRepeatedSmallMessagesAllocateOnlyOneBlock(t *testing.T) {
	r := ring{limit: 256 << 10}
	if retainedBytes(&r) != 0 || r.blocks != nil {
		t.Fatal("idle ring allocated payload storage")
	}
	message := bytes.Repeat([]byte{42}, 1024)
	buffer := make([]byte, len(message))
	for range 1024 {
		r.write(message)
		if r.read(buffer) != len(message) || !bytes.Equal(buffer, message) {
			t.Fatal("small message changed")
		}
	}
	if got := retainedBytes(&r); got != receiveBlockSize {
		t.Fatalf("small traffic retained %d bytes", got)
	}
}

func TestLazyReceiveStorageStillReservesTheFullWindow(t *testing.T) {
	cfg := Config{ReceiveWindow: 12288, ReceiveBudget: 12288}
	a, b := pipePair(t, cfg, cfg)
	x, y := openPair(t, a, b)
	if a.Stats().ReceiveReserved != 12288 || b.Stats().ReceiveReserved != 12288 {
		t.Fatal("idle stream did not reserve its protocol window")
	}
	a.mu.Lock()
	empty := x.rx.blocks == nil
	a.mu.Unlock()
	if !empty {
		t.Fatal("opening a stream eagerly allocated receive blocks")
	}
	if _, err := a.OpenStream(testContext(t)); !errors.Is(err, ErrCapacity) {
		t.Fatalf("unused physical storage bypassed admission budget: %v", err)
	}
	payload := bytes.Repeat([]byte{7}, 12288)
	if _, err := x.Write(payload); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, len(payload))
	if _, err := io.ReadFull(y, buffer); err != nil || !bytes.Equal(buffer, payload) {
		t.Fatalf("full window read: %v", err)
	}
	b.mu.Lock()
	retained := retainedBytes(&y.rx)
	b.mu.Unlock()
	if retained != len(payload) {
		t.Fatalf("full window retained %d bytes", retained)
	}
	_ = x.Close()
	waitEmpty(t, a)
	waitEmpty(t, b)
	b.mu.Lock()
	retained = retainedBytes(&y.rx)
	b.mu.Unlock()
	if retained != 0 {
		t.Fatal("reset barrier did not release receive blocks")
	}
}
