package taonet

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBorrowedStreamVectorsAcrossRecordsAndTLS(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "tcp"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			result := make(chan []byte, 1)
			m := NewMux()
			if err := m.HandleStream("vectors", func(_ context.Context, st *Stream) error {
				body, err := io.ReadAll(st)
				result <- body
				return err
			}); err != nil {
				t.Fatal(err)
			}
			a, b := localConfig("client", nil), localConfig("server", m)
			if secure {
				clientTLS, serverTLS := tlsConfigs(t)
				a.Transport = TransportConfig{TLS: clientTLS, ExpectedPeer: "server"}
				b.Transport = TransportConfig{TLS: serverTLS, ExpectedPeer: "client"}
			}
			client, _ := pair(t, a, b)
			st, err := client.OpenStream(t.Context(), "vectors", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			var parts net.Buffers
			var expected []byte
			for i := range 150 {
				part := bytes.Repeat([]byte{byte(i)}, 1+i*12)
				parts = append(parts, nil, part)
				expected = append(expected, part...)
			}
			large := bytes.Repeat([]byte("large"), maxDataRecord)
			parts = append(parts, large)
			expected = append(expected, large...)
			n, released, err := st.WritevBorrowed(parts)
			if err != nil || n != len(expected) {
				t.Fatalf("vectored write: %d %v", n, err)
			}
			waitSignal(t, released)
			for _, part := range parts {
				clear(part)
			}
			if _, _, err := st.WritevBorrowed(net.Buffers{[]byte("tail")}); err != nil {
				t.Fatal(err)
			}
			expected = append(expected, "tail"...)
			if err := st.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if got := <-result; !bytes.Equal(got, expected) {
				t.Fatalf("released input or record boundaries corrupted the stream: len=%d want=%d", len(got), len(expected))
			}
		})
	}
}

type pausedStreamWrite struct {
	net.Conn
	stream           uint64
	armed            atomic.Bool
	entered, release chan struct{}
	unblock          sync.Once
}

func (c *pausedStreamWrite) Write(p []byte) (int, error) {
	if c.armed.Load() && len(p) == 24 && p[0] == 1 && p[1] == 5 && binary.BigEndian.Uint64(p[8:16]) == c.stream && c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(p)
}

func TestBorrowedStreamResetRetainsInFlightInput(t *testing.T) {
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
	paused := &pausedStreamWrite{Conn: right, entered: make(chan struct{}), release: make(chan struct{})}
	unblock := func() { paused.unblock.Do(func() { close(paused.release) }) }
	defer unblock()
	incoming := make(chan *Stream, 1)
	m := NewMux()
	if err := m.HandleStream("paused", func(_ context.Context, st *Stream) error {
		incoming <- st
		<-st.Done()
		return st.Err()
	}); err != nil {
		t.Fatal(err)
	}
	type sessionResult struct {
		session *Session
		err     error
	}
	accepted := make(chan sessionResult, 1)
	go func() {
		s, err := NewSession(t.Context(), paused, Acceptor, localConfig("server", m))
		accepted <- sessionResult{s, err}
	}()
	client, err := NewSession(t.Context(), left, Initiator, localConfig("client", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer func() { unblock(); _ = server.session.Close() }()
	peer, err := client.OpenStream(t.Context(), "paused", nil)
	if err != nil {
		t.Fatal(err)
	}
	st := <-incoming
	paused.stream = st.raw.ID()
	paused.armed.Store(true)
	input := []byte("borrowed input")
	type writeResult struct {
		released <-chan struct{}
		err      error
	}
	written := make(chan writeResult, 1)
	go func() {
		_, released, err := st.WritevBorrowed(net.Buffers{input})
		written <- writeResult{released, err}
	}()
	waitSignal(t, paused.entered)
	_ = peer.Close()
	var result writeResult
	select {
	case result = <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not interrupt the borrowed write")
	}
	if result.err == nil {
		t.Fatal("reset write succeeded")
	}
	select {
	case <-result.released:
		t.Fatal("released input before physical Write completed")
	default:
	}
	if _, _, err := st.WritevBorrowed(net.Buffers{[]byte("later write")}); err == nil {
		t.Fatal("reset stream accepted another write")
	}
	unblock()
	waitSignal(t, result.released)
	clear(input)
}

type pauseFourthStreamFrame struct {
	*pausedStreamWrite
	target atomic.Uint64
	frames atomic.Int32
}

func (c *pauseFourthStreamFrame) Write(p []byte) (int, error) {
	if len(p) == 24 && p[0] == 1 && p[1] == 5 && binary.BigEndian.Uint64(p[8:16]) == c.target.Load() && c.frames.Add(1) == 4 {
		c.armed.Store(true)
	}
	return c.pausedStreamWrite.Write(p)
}

func TestBorrowedStreamPartialWriteExcludesEveryRecordHeader(t *testing.T) {
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
	paused := &pauseFourthStreamFrame{pausedStreamWrite: &pausedStreamWrite{Conn: right, entered: make(chan struct{}), release: make(chan struct{})}}
	unblock := func() { paused.unblock.Do(func() { close(paused.release) }) }
	defer unblock()
	incoming := make(chan *Stream, 1)
	m := NewMux()
	if err := m.HandleStream("partial", func(_ context.Context, st *Stream) error {
		incoming <- st
		<-st.Done()
		return st.Err()
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("client", nil), localConfig("server", m)
	a.StreamReceiveWindow, b.StreamReceiveWindow = 256<<10, 256<<10
	type sessionResult struct {
		session *Session
		err     error
	}
	accepted := make(chan sessionResult, 1)
	go func() {
		s, err := NewSession(t.Context(), paused, Acceptor, b)
		accepted <- sessionResult{s, err}
	}()
	client, err := NewSession(t.Context(), left, Initiator, a)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	if server.err != nil {
		t.Fatal(server.err)
	}
	defer func() { unblock(); _ = server.session.Close() }()
	peer, err := client.OpenStream(t.Context(), "partial", nil)
	if err != nil {
		t.Fatal(err)
	}
	stream := <-incoming
	paused.stream = stream.raw.ID()
	paused.target.Store(stream.raw.ID())
	input := bytes.Repeat([]byte{42}, (128<<10)+100)
	type writeResult struct {
		n        int
		released <-chan struct{}
		err      error
	}
	written := make(chan writeResult, 1)
	go func() {
		n, released, err := stream.WritevBorrowed(net.Buffers{input[:17], input[17:]})
		written <- writeResult{n, released, err}
	}()
	waitSignal(t, paused.entered)
	_ = peer.Close()
	var result writeResult
	select {
	case result = <-written:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not interrupt the fourth frame")
	}
	// Three native 32KiB frames completed. Their byte stream contains the
	// headers of two taonet DATA records, neither of which is user payload.
	if result.err == nil || result.n != (96<<10)-32 {
		t.Fatalf("partial payload count: %d, err=%v", result.n, result.err)
	}
	select {
	case <-result.released:
		t.Fatal("partial write released the still-paused input")
	default:
	}
	unblock()
	waitSignal(t, result.released)
	clear(input)
}
