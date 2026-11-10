package actor

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultCodecBoundsAndOwnsDecodedBytes(t *testing.T) {
	r, err := NewRegistry([]MessageType{{"small", 1, int8(0)}, {"state", 1, codecMessage{}}, {"blob", 1, []byte(nil)}, {"numbers", 1, []uint64(nil)}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []EncodedMessage{
		{Type: "small", Version: 1, Codec: "cbor.v1", Data: []byte{0x19, 0x01, 0x2c}},
		{Type: "small", Version: 1, Codec: "cbor.v1", Data: []byte{1, 2}},
		{Type: "numbers", Version: 1, Codec: "cbor.v1", Data: []byte{0x9a, 0, 1, 0, 0}},
		{Type: "state", Version: 1, Codec: "cbor.v1", Data: []byte{0xa1, 0x67, 'u', 'n', 'k', 'n', 'o', 'w', 'n', 0xf5}},
		{Type: "state", Version: 1, Codec: "cbor.v1", Data: []byte{0xa2, 0x65, 'c', 'o', 'u', 'n', 't', 1, 0x65, 'c', 'o', 'u', 'n', 't', 2}},
	} {
		if _, err := r.Decode(bad); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("accepted malformed %s: %x, %v", bad.Type, bad.Data, err)
		}
	}
	p, err := r.Encode([]byte{1, 2, 3})
	if err != nil || p.Codec != "cbor.v1" {
		t.Fatalf("default codec: %+v, %v", p, err)
	}
	got, err := r.Decode(p)
	if err != nil {
		t.Fatal(err)
	}
	clear(p.Data)
	if !bytes.Equal(got.([]byte), []byte{1, 2, 3}) {
		t.Fatal("decoded value aliases released packet")
	}
	p.Codec = "json.v1"
	if _, err := r.Decode(p); !errors.Is(err, ErrCodecMismatch) {
		t.Fatal(err)
	}
}

func TestCodecMismatchIsRejectedBeforeMailboxAdmission(t *testing.T) {
	a := networkNode(t, "a")
	b, err := StartNode("b@127.0.0.1:0", NodeOptions{Codec: JSONCodec{}, Network: NetworkOptions{AllowInsecureLoopback: true}, Register: []MessageType{{"command", 1, remoteCommand{}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	connectNodes(t, a, b)
	pid := spawn(t, b, FromFunc(func(_ *Actor, message any) error {
		if _, ok := message.(remoteCommand); ok {
			t.Error("mismatched message reached mailbox")
		}
		return nil
	}))
	remoteError(t, a.SendConfirmed(t.Context(), pid, remoteCommand{}), ErrCodecMismatch, Rejected)
}

func TestDefaultCodecPreservesTimestampNanoseconds(t *testing.T) {
	r, err := NewRegistry([]MessageType{{"time", 1, time.Time{}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 11, 7, 12, 34, 56, 123456789, time.FixedZone("offset", 8*3600))
	encoded, err := r.Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	value, err := r.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	got := value.(time.Time)
	_, offset := got.Zone()
	if !got.Equal(want) || got.Nanosecond() != want.Nanosecond() || offset != 8*3600 {
		t.Fatalf("timestamp changed: %v -> %v", want, got)
	}
}
