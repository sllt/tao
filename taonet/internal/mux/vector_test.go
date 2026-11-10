package mux

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestBorrowedVectorUsesNegotiatedFrameLimit(t *testing.T) {
	for _, peerLimit := range []uint32{32 << 10, 64 << 10} {
		t.Run(fmt.Sprint(peerLimit), func(t *testing.T) {
			a, b := pipePair(t,
				Config{ReceiveWindow: 128 << 10, MaxDataPayload: 64 << 10},
				Config{ReceiveWindow: 128 << 10, MaxDataPayload: peerLimit})
			if a.maxData != peerLimit || b.maxData != peerLimit {
				t.Fatalf("negotiated limits: %d/%d want %d", a.maxData, b.maxData, peerLimit)
			}
			x, y := openPair(t, a, b)
			body := bytes.Repeat([]byte{0, 1, 2, 255}, (64<<10)/4+1)
			read := make(chan error, 1)
			go func() {
				got := make([]byte, len(body))
				_, err := io.ReadFull(y, got)
				if err == nil && !bytes.Equal(got, body) {
					err = fmt.Errorf("corrupted negotiated-frame payload")
				}
				read <- err
			}()
			n, released, err := x.WritevBorrowed(net.Buffers{body[:17], body[17:]})
			if err != nil || n != len(body) {
				t.Fatalf("write: %d %v", n, err)
			}
			select {
			case <-released:
			default:
				t.Fatal("successful write retained input")
			}
			if err := awaitError(t, testContext(t), read); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBorrowedVectorFragmentsAcrossPartsAndWindows(t *testing.T) {
	a, b := pipePair(t, Config{ReceiveWindow: 4096, MaxDataPayload: 1024}, Config{ReceiveWindow: 4096, MaxDataPayload: 1024})
	x, y := openPair(t, a, b)
	var parts net.Buffers
	var expected []byte
	for i := range 150 {
		p := bytes.Repeat([]byte{byte(i)}, i%93+1)
		parts = append(parts, nil, p)
		expected = append(expected, p...)
	}
	read := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(y)
		if err == nil && !bytes.Equal(got, expected) {
			t.Error("vectored payload reordered or corrupted")
		}
		read <- err
	}()
	n, released, err := x.WritevBorrowed(parts)
	if err != nil || n != len(expected) {
		t.Fatalf("vector write: %d %v", n, err)
	}
	select {
	case <-released:
	default:
		t.Fatal("successful write retained input")
	}
	for _, p := range parts {
		clear(p)
	}
	if err := x.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := awaitError(t, testContext(t), read); err != nil {
		t.Fatal(err)
	}
}

func TestBorrowedVectorResetWaitsForPhysicalIOBeforeRelease(t *testing.T) {
	t.Run("single-frame", func(t *testing.T) { testBorrowedVectorReset(t, 16) })
	t.Run("queued-and-active", func(t *testing.T) { testBorrowedVectorReset(t, 8192) })
}

func TestBorrowedVectorQueuesTailWithTwoFullFramesAndRetainsItOnReset(t *testing.T) {
	ca, cb := net.Pipe()
	paused := &pausedFrameConn{Conn: cb, kind: typeData, entered: make(chan struct{}), release: make(chan struct{})}
	defer paused.unblock()
	cfg := Config{ReceiveWindow: 128 << 10, MaxDataPayload: 32 << 10}
	a, b := makePair(t, ca, paused, cfg, cfg, true)
	x, y := openPair(t, a, b)
	paused.armed.Store(true)
	body := bytes.Repeat([]byte{42}, (64<<10)+64)
	type result struct {
		released <-chan struct{}
		err      error
	}
	returned := make(chan result, 1)
	go func() {
		_, released, err := y.WritevBorrowed(net.Buffers{body[:32<<10], body[32<<10:]})
		returned <- result{released, err}
	}()
	ctx := testContext(t)
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	b.mu.Lock()
	queued := len(y.queue)
	b.mu.Unlock()
	if queued != 2 {
		t.Errorf("queued behind first physical write: %d, want full frame and tail", queued)
	}
	_ = x.Close()
	var got result
	select {
	case got = <-returned:
	case <-ctx.Done():
		t.Fatal("reset failed to interrupt queued tail")
	}
	if got.err == nil {
		t.Fatal("reset write reported success")
	}
	select {
	case <-got.released:
		t.Fatal("queued tail cancellation released active physical input")
	default:
	}
	paused.unblock()
	select {
	case <-got.released:
	case <-ctx.Done():
		t.Fatal("physical completion leaked input lease")
	}
	clear(body)
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func testBorrowedVectorReset(t *testing.T, size int) {
	ca, cb := net.Pipe()
	paused := &pausedFrameConn{Conn: cb, kind: typeData, entered: make(chan struct{}), release: make(chan struct{})}
	defer paused.unblock()
	cfg := Config{ReceiveWindow: 4096, MaxDataPayload: 1024}
	a, b := makePair(t, ca, paused, cfg, cfg, true)
	x, y := openPair(t, a, b)
	paused.armed.Store(true)
	type result struct {
		released <-chan struct{}
		err      error
	}
	returned := make(chan result, 1)
	parts := net.Buffers{[]byte("header"), bytes.Repeat([]byte("x"), size)}
	go func() { _, released, err := y.WritevBorrowed(parts); returned <- result{released, err} }()
	ctx := testContext(t)
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if size > 1024 {
		b.mu.Lock()
		queued := len(y.queue)
		b.mu.Unlock()
		if queued == 0 {
			t.Fatal("test did not retain a second frame behind the paused I/O")
		}
	}
	_ = x.Close()
	var got result
	select {
	case got = <-returned:
	case <-ctx.Done():
		t.Fatal("reset did not interrupt the write")
	}
	if got.err == nil {
		t.Fatal("canceled write succeeded")
	}
	select {
	case <-got.released:
		t.Fatal("input released while socket Write still holds it")
	default:
	}
	_, nextReleased, err := y.WritevBorrowed(net.Buffers{[]byte("another write")})
	if err == nil {
		t.Fatal("reset stream accepted another write")
	}
	select {
	case <-nextReleased:
	default:
		t.Fatal("rejected write retained unused input")
	}
	paused.unblock()
	select {
	case <-got.released:
	case <-ctx.Done():
		t.Fatal("completed I/O retained the borrowed input")
	}
	for _, p := range parts {
		clear(p)
	}
	waitEmpty(t, a)
	waitEmpty(t, b)
}
