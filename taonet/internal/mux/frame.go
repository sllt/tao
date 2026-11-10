package mux

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const headerSize = 24

type frameType byte

const (
	typeSettings frameType = iota + 1
	typeOpen
	typeOpenAck
	typeOpenNack
	typeData
	typeWindow
	typeFin
	typeReset
	typeResetAck
	typeGoAway
)

type header struct {
	kind   frameType
	length uint32
	stream uint64
	value  uint64
}

func (h header) encode() [headerSize]byte {
	var b [headerSize]byte
	b[0], b[1] = 1, byte(h.kind)
	binary.BigEndian.PutUint32(b[4:8], h.length)
	binary.BigEndian.PutUint64(b[8:16], h.stream)
	binary.BigEndian.PutUint64(b[16:24], h.value)
	return b
}

func decodeHeader(b [headerSize]byte, maxData uint32) (header, error) {
	h := header{frameType(b[1]), binary.BigEndian.Uint32(b[4:8]), binary.BigEndian.Uint64(b[8:16]), binary.BigEndian.Uint64(b[16:24])}
	if b[0] != 1 || binary.BigEndian.Uint16(b[2:4]) != 0 {
		return header{}, protocolError("unsupported version or flags")
	}
	connectionFrame := h.kind == typeSettings || h.kind == typeGoAway
	if connectionFrame != (h.stream == 0) {
		return header{}, protocolError("invalid stream ID for frame %d", h.kind)
	}
	switch h.kind {
	case typeSettings:
		if h.length != 24 || h.value != 0 {
			return header{}, protocolError("invalid SETTINGS")
		}
	case typeGoAway:
		if h.length != 4 {
			return header{}, protocolError("invalid GOAWAY")
		}
	case typeData:
		if h.length == 0 || h.length > maxData || h.value > math.MaxUint64-uint64(h.length) {
			return header{}, protocolError("invalid DATA length or offset")
		}
	case typeOpen, typeOpenAck, typeWindow, typeFin:
		if h.length != 0 {
			return header{}, protocolError("unexpected payload")
		}
	case typeOpenNack, typeReset:
		if h.length != 0 || h.value > math.MaxUint32 || (h.kind == typeOpenNack && h.value == 0) {
			return header{}, protocolError("invalid error code")
		}
	case typeResetAck:
		if h.length != 0 || h.value != 0 {
			return header{}, protocolError("invalid RST_ACK")
		}
	default:
		return header{}, protocolError("unknown frame %d", h.kind)
	}
	return h, nil
}

func readHeader(r io.Reader, maxData uint32) (header, error) {
	var b [headerSize]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return header{}, err
	}
	return decodeHeader(b, maxData)
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) != 0 {
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

func writeFrame(w io.Writer, h header, payload []byte) error {
	b := h.encode()
	if err := writeFull(w, b[:]); err != nil {
		return err
	}
	return writeFull(w, payload)
}

type settings struct {
	maxData        uint32
	maxPeerStreams uint32
	backlog        uint32
	maxWindow      uint32
	controlWindow  uint32
	flags          uint32
}

func (s settings) validate() error {
	if s.maxData < 1024 || s.maxData > 65536 || s.maxPeerStreams < 1 || s.maxPeerStreams > 4096 ||
		s.backlog < 1 || s.backlog > s.maxPeerStreams || !validWindow(uint64(s.maxWindow), 1<<20) ||
		s.controlWindow != controlWindow || s.flags != 0 {
		return fmt.Errorf("mux: invalid SETTINGS values")
	}
	return nil
}

func (s settings) encode() [24]byte {
	var b [24]byte
	values := [...]uint32{s.maxData, s.maxPeerStreams, s.backlog, s.maxWindow, s.controlWindow, s.flags}
	for i, value := range values {
		binary.BigEndian.PutUint32(b[i*4:], value)
	}
	return b
}

func readSettings(r io.Reader) (settings, error) {
	h, err := readHeader(r, 65536)
	if err != nil {
		return settings{}, err
	}
	if h.kind != typeSettings {
		return settings{}, protocolError("expected SETTINGS")
	}
	var b [24]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return settings{}, err
	}
	var values [6]uint32
	for i := range values {
		values[i] = binary.BigEndian.Uint32(b[i*4:])
	}
	s := settings{values[0], values[1], values[2], values[3], values[4], values[5]}
	if err := s.validate(); err != nil {
		return settings{}, protocolError("%v", err)
	}
	return s, nil
}
