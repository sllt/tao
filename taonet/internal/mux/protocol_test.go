package mux

import (
	"errors"
	"io"
	"net"
	"testing"
)

func rawConnection(t *testing.T, role Role) (*Session, net.Conn) {
	t.Helper()
	ctx := testContext(t)
	a, b := net.Pipe()
	result := make(chan error, 1)
	cfg, _ := (Config{}).normalized()
	peerRole := Initiator
	if role == Initiator {
		peerRole = Acceptor
	}
	go func() { _, err := handshake(ctx, b, peerRole, cfg); result <- err }()
	s, err := New(ctx, a, role, Config{})
	if err != nil {
		_ = b.Close()
		t.Fatal(err)
	}
	if err = awaitError(t, ctx, result); err != nil {
		_ = s.Close()
		_ = b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(); _ = s.Close() })
	return s, b
}

func rawPeer(t *testing.T) (*Session, net.Conn) {
	t.Helper()
	s, b := rawConnection(t, Initiator)
	s.MarkReady()
	if err := writeFrame(b, header{kind: typeOpen, stream: 2, value: 4096}, nil); err != nil {
		t.Fatal(err)
	}
	ack, err := readHeader(b, 65536)
	if err != nil || ack.kind != typeOpenAck {
		t.Fatalf("OPEN: %+v %v", ack, err)
	}
	return s, b
}

func TestControlOpenDoesNotLowerStreamHighWater(t *testing.T) {
	s, peer := rawConnection(t, Acceptor)
	if err := writeFrame(peer, header{kind: typeOpen, stream: 3, value: 4096}, nil); err != nil {
		t.Fatal(err)
	}
	nack, err := readHeader(peer, 65536)
	if err != nil || nack.kind != typeOpenNack || Code(nack.value) != NotReady {
		t.Fatalf("early OPEN: %+v %v", nack, err)
	}
	if err = writeFrame(peer, header{kind: typeOpen, stream: 1, value: controlWindow}, nil); err != nil {
		t.Fatal(err)
	}
	ack, err := readHeader(peer, 65536)
	if err != nil || ack.kind != typeOpenAck {
		t.Fatalf("control OPEN: %+v %v", ack, err)
	}
	s.mu.Lock()
	high := s.peerHigh
	s.mu.Unlock()
	if high != 3 {
		t.Fatalf("control OPEN lowered high water to %d", high)
	}
	s.MarkReady()
	if err = writeFrame(peer, header{kind: typeOpen, stream: 3, value: 4096}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Done():
	case <-testContext(t).Done():
		t.Fatal("reused stream ID accepted")
	}
	if !errors.Is(s.Err(), ErrProtocol) {
		t.Fatalf("error: %v", s.Err())
	}
}

func TestProtocolViolationClosesSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		h    header
	}{
		{"oversized frame", header{kind: typeData, length: 65537, stream: 2}},
		{"wrong offset", header{kind: typeData, length: 1, stream: 2, value: 1}},
		{"unearned credit", header{kind: typeWindow, stream: 2, value: 1}},
		{"premature fin", header{kind: typeFin, stream: 2, value: 1}},
		{"duplicate open", header{kind: typeOpen, stream: 2, value: 4096}},
		{"wrong parity", header{kind: typeOpen, stream: 3, value: 4096}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, peer := rawPeer(t)
			b := tc.h.encode()
			if err := writeFull(peer, b[:]); err != nil {
				t.Fatal(err)
			}
			select {
			case <-s.Done():
			case <-testContext(t).Done():
				t.Fatal("invalid frame did not close session")
			}
			if !errors.Is(s.Err(), ErrProtocol) {
				t.Fatalf("error: %v", s.Err())
			}
		})
	}
}

func TestTruncatedPayloadTerminatesStream(t *testing.T) {
	s, peer := rawPeer(t)
	st, err := s.AcceptStream(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = writeFrame(peer, header{kind: typeData, length: 10, stream: 2}, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	_, err = st.Read(make([]byte, 10))
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated payload: %v", err)
	}
	_ = s.Close()
	waitEmpty(t, s)
}
