package mux

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestTCPFramesAcrossStreams(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
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
	cfg := Config{ReceiveWindow: 4096, MaxDataPayload: 1024}
	a, b := makePair(t, ca, cb, cfg, cfg, true)
	var group sync.WaitGroup
	for i := range 8 {
		x, y := openPair(t, a, b)
		for side, stream := range []*Stream{x, y} {
			payload := bytes.Repeat([]byte{byte(i), byte(side)}, 32<<10)
			peerPayload := bytes.Repeat([]byte{byte(i), byte(1 - side)}, 32<<10)
			group.Go(func() {
				if n, err := stream.WriteOwned(payload); err != nil || n != len(payload) {
					t.Errorf("stream %d queued %d: %v", stream.ID(), n, err)
					return
				}
				if err := stream.CloseWrite(); err != nil {
					t.Error(err)
				}
			})
			group.Go(func() {
				got, err := io.ReadAll(stream)
				if err != nil || !bytes.Equal(got, peerPayload) {
					t.Errorf("stream %d: payload/EOF mismatch: %v", stream.ID(), err)
				}
			})
		}
	}
	group.Wait()
	waitEmpty(t, a)
	waitEmpty(t, b)
}
