package taonet

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestRecordVectorAndBodyTruncation(t *testing.T) {
	var wire bytes.Buffer
	if err := writeRecord(&wire, recordData, 9, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(wire.Bytes()); got != "05000000000000030000000000000009616263" {
		t.Fatal(got)
	}
	if _, err := readBody(&wire, 3); !errors.Is(err, ErrProtocol) {
		t.Fatalf("offset mismatch: %v", err)
	}
	wire.Reset()
	_ = writeRecord(&wire, recordEnd, 0, nil)
	if _, err := readBody(&wire, 1); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncation: %v", err)
	}
	var header [16]byte
	header[0] = recordData
	binary.BigEndian.PutUint32(header[4:8], maxDataRecord+1)
	if _, err := readRecordHeader(bytes.NewReader(header[:])); !errors.Is(err, ErrProtocol) {
		t.Fatalf("accepted oversized record: %v", err)
	}
}
