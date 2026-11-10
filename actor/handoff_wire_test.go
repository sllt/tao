package actor

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

type shortHandoffWriter struct{ bytes.Buffer }

func (w *shortHandoffWriter) Write(p []byte) (int, error) { return w.Buffer.Write(p[:min(3, len(p))]) }

func TestHandoffBinaryPreservesOpaquePayloadAndShortWrites(t *testing.T) {
	r, err := NewRegistry([]MessageType{{"blob", 1, []byte(nil)}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := r.Encode([]byte{0xff, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []handoffFrame{
		{Kind: "begin", Move: MoveID{1}, Snapshot: []byte(`{"count":42}`)},
		{Kind: "message", Sequence: 1, Envelope: &handoffEnvelope{Message: encoded, Sender: &PID{Node: "a", BootID: [16]byte{2}, ID: 1}, Call: requestKey{"a", [16]byte{2}, 3}, BudgetMS: 100}},
		{Kind: "skip", Sequence: 2},
		{Kind: "barrier", Sequence: 3, Digest: [32]byte{4}},
		{Kind: "prepared", Move: MoveID{5}, Sequence: 3, Digest: [32]byte{4}},
	} {
		var w shortHandoffWriter
		payload, err := writeHandoffFrame(&w, want)
		if err != nil {
			t.Fatal(err)
		}
		got, received, err := readHandoffFrame(&w.Buffer, 4096)
		if err != nil || !bytes.Equal(payload, received) || !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %+v, %v", want.Kind, got, err)
		}
		for n := 0; n < len(payload); n++ {
			if _, err := unmarshalHandoffFrame(payload[:n]); err == nil {
				t.Fatalf("accepted truncated %s at %d", want.Kind, n)
			}
		}
	}
	if _, _, err := readHandoffFrame(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff}), 4096); err != ErrMessageTooLarge {
		t.Fatal(err)
	}
	if _, _, err := readHandoffFrame(bytes.NewReader([]byte{0, 0, 0, 30}), 4096); err != io.EOF {
		t.Fatal(err)
	}
}
