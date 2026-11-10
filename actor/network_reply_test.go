package actor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestReplyFrameBoundsAndTruncation(t *testing.T) {
	registry, err := NewRegistry([]MessageType{{Name: "bytes", Version: 1, Sample: []byte{}}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	message, err := registry.Encode([]byte{0, 1, 2, 255})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := marshalPacket(actorPacket{Kind: "reply", Message: message})
	if err != nil {
		t.Fatal(err)
	}
	frame := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	frame = append(frame, payload...)
	for i := 1; i < len(frame); i++ {
		if _, err := newReplyFrameReader(bytes.NewReader(frame[:i])).next(1024); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated at %d: %v", i, err)
		}
	}
	for _, size := range []uint32{0, 5, 1025, ^uint32(0)} {
		_, err := newReplyFrameReader(bytes.NewReader(binary.BigEndian.AppendUint32(nil, size))).next(1024)
		if !errors.Is(err, ErrInvalidMessage) && !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("length %d was not rejected before reading body: %v", size, err)
		}
	}
	// Consecutive frames reuse storage, while the decoded business value owns
	// its bytes independently of the stream's next read.
	reader := newReplyFrameReader(bytes.NewReader(append(append([]byte(nil), frame...), frame...)))
	buffer, err := reader.next(1024)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := unmarshalPacket(buffer)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := registry.Decode(packet.Message)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err = reader.next(1024)
	if err != nil {
		t.Fatal(err)
	}
	clear(buffer)
	if !bytes.Equal(decoded.([]byte), []byte{0, 1, 2, 255}) {
		t.Fatal("reply retained the reusable input buffer")
	}
}

func TestReplyFramesAlternateBorrowedAndLargeStorage(t *testing.T) {
	registry, err := NewRegistry([]MessageType{{Name: "bytes", Version: 1, Sample: []byte{}}}, 128<<10)
	if err != nil {
		t.Fatal(err)
	}
	sizes := []int{64, 1024, 4000, 4096, 5000, 64 << 10, 32, 64 << 10, 1024}
	var wire []byte
	for i, size := range sizes {
		message, err := registry.Encode(bytes.Repeat([]byte{byte(i)}, size))
		if err != nil {
			t.Fatal(err)
		}
		packet, err := marshalPacket(actorPacket{Kind: "reply", Message: message})
		if err != nil {
			t.Fatal(err)
		}
		wire = binary.BigEndian.AppendUint32(wire, uint32(len(packet)))
		wire = append(wire, packet...)
	}
	reader := newReplyFrameReader(bytes.NewReader(wire))
	var values [][]byte
	for range sizes {
		frame, err := reader.next(132 << 10)
		if err != nil {
			t.Fatal(err)
		}
		packet, err := unmarshalPacket(frame)
		if err != nil {
			t.Fatal(err)
		}
		value, err := registry.Decode(packet.Message)
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value.([]byte))
	}
	if _, err := reader.next(132 << 10); err != io.EOF {
		t.Fatalf("trailing frame: %v", err)
	}
	for i, value := range values {
		if !bytes.Equal(value, bytes.Repeat([]byte{byte(i)}, sizes[i])) {
			t.Fatalf("frame %d lost ownership or framing across buffer reuse", i)
		}
	}
}
