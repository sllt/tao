package mux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func makePair(t *testing.T, aConn, bConn net.Conn, aCfg, bCfg Config, ready bool) (*Session, *Session) {
	t.Helper()
	ctx := testContext(t)
	type result struct {
		s   *Session
		err error
	}
	server := make(chan result, 1)
	go func() { s, err := New(ctx, bConn, Acceptor, bCfg); server <- result{s, err} }()
	a, err := New(ctx, aConn, Initiator, aCfg)
	if err != nil {
		_ = bConn.Close()
		t.Fatal(err)
	}
	var b *Session
	select {
	case r := <-server:
		if r.err != nil {
			_ = a.Close()
			t.Fatal(r.err)
		}
		b = r.s
	case <-ctx.Done():
		_ = a.Close()
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	if ready {
		a.MarkReady()
		b.MarkReady()
	}
	return a, b
}

func pipePair(t *testing.T, aCfg, bCfg Config) (*Session, *Session) {
	t.Helper()
	a, b := net.Pipe()
	return makePair(t, a, b, aCfg, bCfg, true)
}

func openPair(t *testing.T, a, b *Session) (*Stream, *Stream) {
	t.Helper()
	ctx := testContext(t)
	x, err := a.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return x, y
}

func waitEmpty(t *testing.T, s *Session) {
	t.Helper()
	ctx := testContext(t)
	for {
		s.mu.Lock()
		empty := len(s.streams) == 0 && s.receiveReserved == 0 && s.controlBytes == 0 && s.controlDataBytes == 0 && s.dataBytes == 0 && s.dataFrames == 0 && len(s.runnable) == 0 && s.active == nil
		ch := s.changedLocked()
		s.mu.Unlock()
		if empty {
			return
		}
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("stream resources did not drain")
		}
	}
}

func awaitError(t *testing.T, ctx context.Context, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return ctx.Err()
	}
}

func TestBidirectionalStreams(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	ctx := testContext(t)
	results := make(chan error, 4)
	payload := bytes.Repeat([]byte("duplex payload\n"), 12000)
	for _, s := range []*Session{a, b} {
		go func() {
			st, err := s.AcceptStream(ctx)
			if err == nil {
				var p []byte
				p, err = io.ReadAll(st)
				if err == nil && !bytes.Equal(p, payload) {
					err = errors.New("request payload mismatch")
				}
				if err == nil {
					_, err = st.Write(bytes.ToUpper(p))
				}
				if err == nil {
					err = st.CloseWrite()
				}
			}
			results <- err
		}()
		go func() {
			st, err := s.OpenStream(ctx)
			if err == nil {
				_, err = st.Write(payload)
			}
			if err == nil {
				err = st.CloseWrite()
			}
			if err == nil {
				var p []byte
				p, err = io.ReadAll(st)
				if err == nil && !bytes.Equal(p, bytes.ToUpper(payload)) {
					err = errors.New("reply payload mismatch")
				}
			}
			results <- err
		}()
	}
	for range 4 {
		if err := awaitError(t, ctx, results); err != nil {
			t.Fatal(err)
		}
	}
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestFINPreservesUnreadDataAndReadDeadline(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	x, y := openPair(t, a, b)
	payload := []byte("unread bytes survive two FINs")
	if _, err := x.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := x.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := y.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-y.Done():
		t.Fatal("stream ended before buffered data was read")
	default:
	}
	_ = y.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := y.Read(make([]byte, 8)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read error: %v", err)
	}
	_ = y.SetReadDeadline(time.Time{})
	got, err := io.ReadAll(y)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read = %q, %v", got, err)
	}
	if _, err = io.ReadAll(x); err != nil {
		t.Fatal(err)
	}
	waitEmpty(t, a)
	waitEmpty(t, b)
	if x.Err() != nil || y.Err() != nil {
		t.Fatal("normal FIN reported an error")
	}
}

func TestWindowBackpressureAndTailUpdate(t *testing.T) {
	cfg := Config{ReceiveWindow: 4096, ReceiveBudget: 4096, MaxDataPayload: 1024}
	a, b := pipePair(t, cfg, cfg)
	x, y := openPair(t, a, b)
	if _, err := x.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	_ = x.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := x.Write([]byte("x")); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write error: %v", err)
	}
	if x.Err() != nil {
		t.Fatal("deadline before admission reset the stream")
	}
	_ = x.SetWriteDeadline(time.Time{})
	written := make(chan error, 1)
	go func() { _, err := x.Write([]byte("x")); written <- err }()
	if _, err := io.ReadFull(y, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err := awaitError(t, testContext(t), written); err != nil {
		t.Fatal(err)
	}
	// One byte is below the batching threshold: only the bounded tail flush
	// can restore enough credit for the extra byte.
	_ = x.Close()
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestAcceptBacklogRejectsWithoutBlockingExistingStream(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{MaxOpenBacklog: 1})
	ctx := testContext(t)
	x, err := a.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.OpenStream(ctx)
	var remote *StreamError
	if !errors.As(err, &remote) || remote.Code != ResourceExhausted {
		t.Fatalf("rejection = %v", err)
	}
	y, err := b.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = x.Write([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 2)
	if _, err = io.ReadFull(y, got); err != nil || string(got) != "ok" {
		t.Fatalf("existing stream: %q %v", got, err)
	}
	_ = x.Close()
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestControlWindowIsSeparate(t *testing.T) {
	ca, cb := net.Pipe()
	cfg := Config{ReceiveWindow: 4096, ReceiveBudget: 4096, MaxDataPayload: 1024}
	a, b := makePair(t, ca, cb, cfg, cfg, false)
	ctx := testContext(t)
	if _, err := a.OpenStream(ctx); !errors.Is(err, ErrNotReady) {
		t.Fatalf("early OPEN = %v", err)
	}
	x, err := a.OpenControl(ctx)
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.AcceptControl(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a.MarkReady()
	b.MarkReady()
	ordinary, _ := openPair(t, a, b)
	if _, err = ordinary.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err = a.OpenStream(ctx); !errors.Is(err, ErrCapacity) {
		t.Fatalf("budget overflow = %v", err)
	}
	if _, err = x.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4)
	if _, err = io.ReadFull(y, got); err != nil || string(got) != "ping" {
		t.Fatalf("control = %q %v", got, err)
	}
}

type pausedFrameConn struct {
	net.Conn
	kind        frameType
	armed       atomic.Bool
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (c *pausedFrameConn) Write(p []byte) (int, error) {
	if c.armed.Load() && len(p) == headerSize && p[0] == 1 && frameType(p[1]) == c.kind {
		c.enterOnce.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.Conn.Write(p)
}
func (c *pausedFrameConn) unblock()     { c.releaseOnce.Do(func() { close(c.release) }) }
func (c *pausedFrameConn) Close() error { c.unblock(); return c.Conn.Close() }

func TestResetWaitsForPeerBarrier(t *testing.T) {
	for _, kind := range []frameType{typeData, typeResetAck} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			ca, cb := net.Pipe()
			paused := &pausedFrameConn{Conn: cb, kind: kind, entered: make(chan struct{}), release: make(chan struct{})}
			defer paused.unblock()
			cfg := Config{ReceiveWindow: 4096, ReceiveBudget: 4096, MaxDataPayload: 1024}
			a, b := makePair(t, ca, paused, cfg, cfg, true)
			x, y := openPair(t, a, b)
			paused.armed.Store(true)
			ctx := testContext(t)
			writeDone := make(chan error, 1)
			if kind == typeData {
				go func() { _, err := y.Write([]byte("in flight")); writeDone <- err }()
				select {
				case <-paused.entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			_ = x.Close()
			select {
			case <-y.Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if kind == typeResetAck {
				select {
				case <-paused.entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if _, err := a.OpenStream(ctx); !errors.Is(err, ErrCapacity) {
				t.Fatalf("window reused before barrier: %v", err)
			}
			paused.unblock()
			if kind == typeData {
				if err := awaitError(t, ctx, writeDone); err == nil {
					t.Fatal("in-flight canceled write succeeded")
				}
			}
			waitEmpty(t, a)
			waitEmpty(t, b)
			next, _ := openPair(t, a, b)
			_ = next.Close()
			waitEmpty(t, a)
			waitEmpty(t, b)
		})
	}
}

func TestResetDoesNotCloseOtherStream(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	x, y := openPair(t, a, b)
	keep, peer := openPair(t, a, b)
	_ = x.Close()
	if _, err := y.Read(make([]byte, 1)); err == nil {
		t.Fatal("reset returned no error")
	}
	if _, err := keep.Write([]byte("alive")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(peer, got); err != nil || string(got) != "alive" {
		t.Fatalf("peer = %q %v", got, err)
	}
	_ = keep.Close()
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestOpeningCancellation(t *testing.T) {
	ca, cb := net.Pipe()
	paused := &pausedFrameConn{Conn: cb, kind: typeOpenAck, entered: make(chan struct{}), release: make(chan struct{})}
	defer paused.unblock()
	a, b := makePair(t, ca, paused, Config{}, Config{}, true)
	paused.armed.Store(true)
	ctx, cancel := context.WithCancel(testContext(t))
	result := make(chan error, 1)
	go func() { _, err := a.OpenStream(ctx); result <- err }()
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	if err := awaitError(t, testContext(t), result); !errors.Is(err, context.Canceled) {
		t.Fatalf("open cancellation = %v", err)
	}
	paused.unblock()
	waitEmpty(t, a)
	waitEmpty(t, b)
}

type fragmentConn struct{ net.Conn }

func (c fragmentConn) Read(p []byte) (int, error)  { return c.Conn.Read(p[:min(3, len(p))]) }
func (c fragmentConn) Write(p []byte) (int, error) { return c.Conn.Write(p[:min(7, len(p))]) }

func TestTCPWithShortReadsAndWrites(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			accepted <- c
		}
	}()
	ca, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var cb net.Conn
	select {
	case cb = <-accepted:
	case <-testContext(t).Done():
		_ = ca.Close()
		t.Fatal("accept timed out")
	}
	a, b := makePair(t, fragmentConn{ca}, fragmentConn{cb}, Config{}, Config{}, true)
	x, y := openPair(t, a, b)
	payload := bytes.Repeat([]byte("fragment"), 100)
	if _, err = x.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err = x.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(y)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("fragmented transfer: %v", err)
	}
	if err = y.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadAll(x); err != nil {
		t.Fatal(err)
	}
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestSessionCloseWakesBlockedOperations(t *testing.T) {
	a, b := pipePair(t, Config{ReceiveWindow: 4096}, Config{ReceiveWindow: 4096})
	x, y := openPair(t, a, b)
	if _, err := x.Write(make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { _, err := x.Write([]byte("blocked")); results <- err }()
	go func() { _, err := a.AcceptStream(testContext(t)); results <- err }()
	_ = a.Close()
	for range 2 {
		if err := awaitError(t, testContext(t), results); err == nil {
			t.Fatal("blocked operation returned success")
		}
	}
	_ = b.Close()
	if y.Err() == nil {
		t.Fatal("peer stream did not terminate")
	}
	waitEmpty(t, a)
	waitEmpty(t, b)
}

func TestDrainKeepsAcceptedStreamsAlive(t *testing.T) {
	a, b := pipePair(t, Config{}, Config{})
	x, y := openPair(t, a, b)
	if err := b.Drain(); err != nil {
		t.Fatal(err)
	}
	ctx := testContext(t)
	for {
		a.mu.Lock()
		draining, ch := a.peerDraining, a.changedLocked()
		a.mu.Unlock()
		if draining {
			break
		}
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if _, err := a.OpenStream(ctx); !errors.Is(err, ErrDraining) {
		t.Fatalf("OPEN after GOAWAY: %v", err)
	}
	if _, err := x.Write([]byte("finish")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 6)
	if _, err := io.ReadFull(y, got); err != nil || string(got) != "finish" {
		t.Fatalf("accepted stream: %q %v", got, err)
	}
	_ = x.Close()
	waitEmpty(t, a)
	waitEmpty(t, b)
}
