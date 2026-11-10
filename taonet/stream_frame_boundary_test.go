package taonet

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
)

type streamFrameCounter struct {
	net.Conn
	stream atomic.Uint64
	frames atomic.Int32
}

func (c *streamFrameCounter) Write(p []byte) (int, error) {
	// The wrapper uses the non-TCP gather path, whose native frame header is
	// one Write. Payload bytes in this test cannot resemble this header.
	if len(p) == 24 && p[0] == 1 && p[1] == 5 && binary.BigEndian.Uint64(p[8:16]) == c.stream.Load() {
		c.frames.Add(1)
	}
	return c.Conn.Write(p)
}

func TestBorrowedStreamAvoidsHeaderOnlyTailFrame(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	left, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	counted := &streamFrameCounter{Conn: left}
	type readResult struct {
		body []byte
		err  error
	}
	reads := make(chan readResult, 1)
	mux := NewMux()
	if err := mux.HandleStream("boundary", func(_ context.Context, stream *Stream) error {
		body, err := io.ReadAll(stream)
		reads <- readResult{body, err}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("client", nil), localConfig("server", mux)
	a.StreamReceiveWindow, b.StreamReceiveWindow = 256<<10, 256<<10
	type sessionResult struct {
		session *Session
		err     error
	}
	accepted := make(chan sessionResult, 1)
	go func() {
		s, err := NewSession(t.Context(), right, Acceptor, b)
		accepted <- sessionResult{s, err}
	}()
	client, err := NewSession(t.Context(), counted, Initiator, a)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer server.session.Close()
	for _, size := range []int{(64 << 10) - 16, 64 << 10, (64 << 10) + 100, (128 << 10) + 100} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			stream, err := client.OpenStream(t.Context(), "boundary", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			counted.frames.Store(0)
			counted.stream.Store(stream.raw.ID())
			input := bytes.Repeat([]byte{42}, size)
			n, released, err := stream.WritevBorrowed(net.Buffers{input[:100], input[100:]})
			if err != nil || n != size {
				t.Fatalf("write: %d %v", n, err)
			}
			waitSignal(t, released)
			// One extra small frame is needed for payload beyond a 64KiB pair;
			// an additional 16-byte header-only tail must not precede it.
			want := int32(2)
			if size >= 64<<10 {
				want = 3
			}
			if size > 128<<10 {
				want = 5
			}
			if got := counted.frames.Load(); got != want {
				t.Errorf("native DATA frames=%d want=%d", got, want)
			}
			clear(input)
			if err := stream.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			result := <-reads
			if result.err != nil || !bytes.Equal(result.body, bytes.Repeat([]byte{42}, size)) {
				t.Fatalf("record boundary or input lifetime corrupted: bytes=%d err=%v", len(result.body), result.err)
			}
		})
	}
}
