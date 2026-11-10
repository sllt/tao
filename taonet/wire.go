package taonet

import (
	"encoding/binary"
	"encoding/json"
	"io"
)

const (
	recordOpen               byte = 0x01
	recordOpenOK             byte = 0x02
	recordOpenError          byte = 0x03
	recordResult             byte = 0x04
	recordData               byte = 0x05
	recordEnd                byte = 0x06
	recordReset              byte = 0x07
	recordAck                byte = 0x11
	recordMessage            byte = 0x10
	recordWindow             byte = 0x12
	recordInline             byte = 0x13
	recordAckBinary          byte = 0x14
	recordInlineUnconfirmed  byte = 0x15
	recordMessageUnconfirmed byte = 0x16
	recordPing               byte = 0x20
	recordPong               byte = 0x21
	recordGoAway             byte = 0x22
	recordHello              byte = 0x23
	recordWelcome            byte = 0x24
	recordReady              byte = 0x25
	recordReadyAck           byte = 0x26
	maxDataRecord                 = 64 << 10
)

type recordHeader struct {
	kind   byte
	length uint32
	tag    uint64
}

func appendRecord(dst []byte, kind byte, tag uint64, body []byte) []byte {
	var header [16]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	binary.BigEndian.PutUint64(header[8:16], tag)
	dst = append(dst, header[:]...)
	return append(dst, body...)
}

func recordLimit(kind byte) int {
	switch kind {
	case recordOpen, recordResult, recordHello, recordWelcome, recordMessage, recordMessageUnconfirmed:
		return 16 << 10
	case recordReady, recordReadyAck:
		return 1024
	case recordOpenOK, recordOpenError, recordReset, recordGoAway, recordAck, recordAckBinary:
		return 8 << 10
	case recordInline, recordInlineUnconfirmed:
		return 16 << 10
	case recordData:
		return maxDataRecord
	case recordWindow:
		return 16
	case recordEnd, recordPing, recordPong:
		return 0
	default:
		return -1
	}
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		p = p[n:]
	}
	return nil
}

func writeRecord(w io.Writer, kind byte, tag uint64, p []byte) error {
	if len(p) > recordLimit(kind) {
		return protocolError("record %d too large", kind)
	}
	var header [16]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[4:8], uint32(len(p)))
	binary.BigEndian.PutUint64(header[8:16], tag)
	// Small records should enter the byte stream in one write. Splitting a
	// 16-byte header from its body otherwise creates two mux scheduling/I/O
	// cycles. Keep large DATA on the existing path to avoid another large copy.
	if len(p) > 0 && len(p) <= 4096 {
		record := make([]byte, len(header)+len(p))
		copy(record, header[:])
		copy(record[len(header):], p)
		return writeAll(w, record)
	}
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, p)
}

func readRecordHeader(r io.Reader) (recordHeader, error) {
	var header [16]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return recordHeader{}, err
	}
	return decodeRecordHeader(header)
}

func decodeRecordHeader(header [16]byte) (recordHeader, error) {
	h := recordHeader{header[0], binary.BigEndian.Uint32(header[4:8]), binary.BigEndian.Uint64(header[8:16])}
	limit := recordLimit(h.kind)
	if header[1] != 0 || header[2] != 0 || header[3] != 0 || limit < 0 || uint64(h.length) > uint64(limit) || h.kind == recordData && h.length == 0 {
		return recordHeader{}, protocolError("invalid record header")
	}
	return h, nil
}

func writeJSON(w io.Writer, kind byte, v any) error {
	p, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeRecord(w, kind, 0, p)
}

func readJSONBody(r io.Reader, h recordHeader, v any) error {
	if h.tag != 0 {
		return protocolError("unexpected JSON record tag")
	}
	p := make([]byte, int(h.length))
	if _, err := io.ReadFull(r, p); err != nil {
		return err
	}
	if err := json.Unmarshal(p, v); err != nil {
		return protocolError("invalid JSON: %v", err)
	}
	return nil
}

func readJSON(r io.Reader, kind byte, v any) error {
	h, err := readRecordHeader(r)
	if err != nil {
		return err
	}
	if h.kind != kind {
		return protocolError("expected record %d, got %d", kind, h.kind)
	}
	return readJSONBody(r, h, v)
}

func writeBody(w io.Writer, p []byte) error {
	var offset uint64
	for len(p) > 0 {
		n := min(maxDataRecord, len(p))
		if err := writeRecord(w, recordData, offset, p[:n]); err != nil {
			return err
		}
		offset += uint64(n)
		p = p[n:]
	}
	return writeRecord(w, recordEnd, offset, nil)
}

func readBody(r io.Reader, size int) ([]byte, error) {
	p := make([]byte, size)
	offset := 0
	for {
		h, err := readRecordHeader(r)
		if err != nil {
			return nil, err
		}
		if h.tag != uint64(offset) {
			return nil, protocolError("body offset mismatch")
		}
		if h.kind == recordEnd {
			if offset != size {
				return nil, io.ErrUnexpectedEOF
			}
			return p, nil
		}
		if h.kind != recordData || uint64(h.length) > uint64(size-offset) {
			return nil, protocolError("unexpected body record or length")
		}
		if _, err = io.ReadFull(r, p[offset:offset+int(h.length)]); err != nil {
			return nil, err
		}
		offset += int(h.length)
	}
}
