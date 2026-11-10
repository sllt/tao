package mux

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestHeaderWireVector(t *testing.T) {
	want, _ := hex.DecodeString("010500000000000300000000000000030000000000000100")
	h := header{typeData, 3, 3, 256}
	b := h.encode()
	if !bytes.Equal(b[:], want) {
		t.Fatalf("header = %x, want %x", b, want)
	}
	got, err := readHeader(bytes.NewReader(want), 1024)
	if err != nil || got != h {
		t.Fatalf("decode = %#v, %v", got, err)
	}
	for i := 0; i < headerSize; i++ {
		if _, err := readHeader(bytes.NewReader(want[:i]), 1024); err == nil {
			t.Fatalf("accepted %d-byte header", i)
		}
	}
}

func TestInvalidHeaders(t *testing.T) {
	valid := header{typeData, 3, 3, 0}.encode()
	for _, tc := range []struct {
		name   string
		change func(*[headerSize]byte)
	}{
		{"version", func(b *[headerSize]byte) { b[0] = 2 }},
		{"flags", func(b *[headerSize]byte) { b[3] = 1 }},
		{"type", func(b *[headerSize]byte) { b[1] = 255 }},
		{"stream", func(b *[headerSize]byte) { b[15] = 0 }},
		{"length", func(b *[headerSize]byte) { b[4] = 1 }},
		{"offset overflow", func(b *[headerSize]byte) {
			for i := 16; i < headerSize; i++ {
				b[i] = 255
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := valid
			tc.change(&b)
			if _, err := decodeHeader(b, 1024); !errors.Is(err, ErrProtocol) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSettingsAndConfig(t *testing.T) {
	c, err := (Config{}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	b := c.settings().encode()
	if err := writeFrame(&wire, header{kind: typeSettings, length: 24}, b[:]); err != nil {
		t.Fatal(err)
	}
	got, err := readSettings(&wire)
	if err != nil || got != c.settings() {
		t.Fatalf("settings = %#v, %v", got, err)
	}
	for _, c := range []Config{{MaxDataPayload: 65537}, {ReceiveWindow: 4097}, {MaxOpenBacklog: 2, MaxPeerStreams: 1}, {SendBudget: 1}, {MaxStreamsTotal: -1}} {
		if _, err := c.normalized(); err == nil {
			t.Fatalf("accepted invalid config: %+v", c)
		}
	}
}

type smallWriter struct{ bytes.Buffer }

func (w *smallWriter) Write(p []byte) (int, error) { return w.Buffer.Write(p[:min(2, len(p))]) }

type stalledWriter struct{}

func (stalledWriter) Write([]byte) (int, error) { return 0, nil }

func TestWriteFull(t *testing.T) {
	w := new(smallWriter)
	if err := writeFull(w, []byte("partial writes")); err != nil || w.String() != "partial writes" {
		t.Fatalf("write: %q %v", w.String(), err)
	}
	if err := writeFull(stalledWriter{}, []byte("x")); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("error = %v", err)
	}
}

func FuzzHeader(f *testing.F) {
	for _, h := range []header{{typeData, 3, 3, 0}, {kind: typeOpen, stream: 3, value: 4096}, {kind: typeSettings, length: 24}} {
		b := h.encode()
		f.Add(b[:])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) != headerSize {
			return
		}
		var raw [headerSize]byte
		copy(raw[:], b)
		h, err := decodeHeader(raw, 65536)
		if err == nil && h.encode() != raw {
			t.Fatal("accepted header does not round trip")
		}
	})
}
