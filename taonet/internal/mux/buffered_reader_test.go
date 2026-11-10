package mux

import (
	"errors"
	"io"
	"net"
	"testing"
)

func TestBufferedReaderRejectsTruncatedFrameBody(t *testing.T) {
	for _, body := range [][]byte{nil, []byte("short")} {
		// A TCP half-close delivers EOF while deadline operations remain
		// valid. Closing net.Pipe can instead fail SetReadDeadline with
		// ErrClosedPipe before the reader reaches the truncated body.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		ca, err := (&net.Dialer{}).DialContext(testContext(t), "tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		cb, err := listener.Accept()
		if err != nil {
			_ = ca.Close()
			t.Fatal(err)
		}
		a, b := makePair(t, ca, cb, Config{}, Config{}, true)
		x, _ := openPair(t, a, b)
		header := (header{kind: typeData, stream: x.ID(), length: 10}).encode()
		if err := writeFull(ca, header[:]); err != nil {
			t.Fatal(err)
		}
		if err := writeFull(ca, body); err != nil {
			t.Fatal(err)
		}
		if err := ca.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-b.Done():
		case <-testContext(t).Done():
			t.Fatal("truncated body did not close session")
		}
		if !errors.Is(b.Err(), io.ErrUnexpectedEOF) {
			t.Fatalf("truncated frame became a normal EOF: %v", b.Err())
		}
	}
}
