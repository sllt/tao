package taonet

import (
	"bytes"
	"context"
	"io"
	"testing"
)

func TestStreamAsymmetricWindowsPreserveFlowAndReservations(t *testing.T) {
	incoming := make(chan *Stream, 1)
	m := NewMux()
	if err := m.HandleStream("windows", func(_ context.Context, st *Stream) error {
		incoming <- st
		<-st.Done()
		return st.Err()
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("small", nil), localConfig("large", m)
	a.StreamReceiveWindow, b.StreamReceiveWindow = 4096, 256<<10
	client, server := pair(t, a, b)
	x, err := client.OpenStream(t.Context(), "windows", nil)
	if err != nil {
		t.Fatal(err)
	}
	y := <-incoming
	if got := client.raw.Stats().ReceiveReserved; got != 4096 {
		t.Fatalf("client reserved %d", got)
	}
	if got := server.raw.Stats().ReceiveReserved; got != 256<<10 {
		t.Fatalf("server reserved %d", got)
	}
	for _, endpoints := range [][2]*Stream{{x, y}, {y, x}} {
		src, dst := endpoints[0], endpoints[1]
		body := bytes.Repeat([]byte("flow"), 128<<10)
		written := make(chan error, 1)
		go func() {
			_, err := src.Write(body)
			if err == nil {
				err = src.CloseWrite()
			}
			written <- err
		}()
		got, err := io.ReadAll(dst)
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("window fragmented stream: bytes=%d err=%v", len(got), err)
		}
		if err := <-written; err != nil {
			t.Fatal(err)
		}
	}
	waitIdle(t, client)
	waitIdle(t, server)
}

func TestStreamWindowConfigurationRejectsUnrepresentableGrant(t *testing.T) {
	for _, size := range []int{-4096, 1, 4097, (1 << 20) + 4096} {
		cfg := localConfig("invalid", nil)
		cfg.StreamReceiveWindow = size
		if _, err := cfg.normalized(); err == nil {
			t.Fatalf("accepted window %d", size)
		}
	}
}
