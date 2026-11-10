package mux

import (
	"bufio"
	"encoding/binary"
	"io"
	"time"
)

func (s *Session) readLoop() {
	defer s.wg.Done()
	// One bounded buffer replaces the frame scratch. Small adjacent records
	// share a socket read; Peek avoids copying the body into another scratch.
	reader := bufio.NewReaderSize(s.conn, int(s.maxData)+headerSize)
	for {
		encoded, err := reader.Peek(headerSize)
		if err == io.EOF && len(encoded) != 0 {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			s.stop(err)
			return
		}
		h, err := decodeHeader([headerSize]byte(encoded), s.maxData)
		if err != nil {
			s.stop(err)
			return
		}
		_, _ = reader.Discard(headerSize)
		var st *Stream
		if h.kind == typeData {
			s.mu.Lock()
			st, err = s.validateDataLocked(h)
			s.mu.Unlock()
			if err != nil {
				s.stop(err)
				return
			}
		}
		var body []byte
		if h.length > 0 {
			needsRead := reader.Buffered() < int(h.length)
			if needsRead {
				err = s.conn.SetReadDeadline(time.Now().Add(s.cfg.IOTimeout))
			}
			if err == nil {
				body, err = reader.Peek(int(h.length))
				if err == io.EOF {
					err = io.ErrUnexpectedEOF
				}
			}
			if err == nil && needsRead {
				err = s.conn.SetReadDeadline(time.Time{})
			}
			if err != nil {
				s.stop(err)
				return
			}
		}
		s.mu.Lock()
		if s.err != nil {
			s.mu.Unlock()
			return
		}
		if h.kind == typeData {
			st.received += uint64(h.length)
			if st.err != nil {
				st.consumed = st.received
			} else {
				st.rx.write(body)
				st.notifyReadLocked()
			}
		} else {
			err = s.handleControlLocked(h, body)
			s.notifyWriterLocked()
			s.notifyLocked()
		}
		s.mu.Unlock()
		// ring.write and handleControlLocked consume/copy body before this
		// buffer can be advanced or refilled by the next header.
		_, _ = reader.Discard(len(body))
		if err != nil {
			s.stop(err)
			return
		}
	}
}

func (s *Session) validateDataLocked(h header) (*Stream, error) {
	if s.err != nil {
		return nil, s.err
	}
	st := s.streams[h.stream]
	if st == nil || st.remoteFin || st.remoteReset || st.resetAckReceived || st.nacked {
		return nil, protocolError("DATA for closed or unknown stream %d", h.stream)
	}
	if (st.local && !st.ackReceived) || (!st.local && !st.ackIssued) {
		return nil, protocolError("DATA before OPEN_ACK")
	}
	if h.value != st.received || st.received-st.advertised > st.window || uint64(h.length) > st.window-(st.received-st.advertised) {
		return nil, protocolError("DATA offset or receive window exceeded")
	}
	return st, nil
}

func (s *Session) incomingOpenLocked(h header) error {
	isControl := h.stream == 1
	if isControl {
		if s.role != Acceptor || s.controlUsed || s.ready || h.value != controlWindow {
			return protocolError("invalid control stream OPEN")
		}
	} else {
		peerOdd := s.role == Acceptor
		if (h.stream%2 == 1) != peerOdd || h.stream <= s.peerHigh || !validWindow(h.value, s.peer.maxWindow) {
			return protocolError("invalid stream OPEN")
		}
	}
	s.peerHigh = max(s.peerHigh, h.stream)
	var code Code
	if !isControl {
		switch {
		case s.draining:
			code = Draining
		case !s.ready:
			code = NotReady
		case s.peerCount >= int(s.cfg.MaxPeerStreams) || len(s.accepts) >= int(s.cfg.MaxOpenBacklog) || !s.capacityLocked():
			code = ResourceExhausted
		}
	}
	if code != Normal {
		_, err := s.enqueueControlLocked(header{kind: typeOpenNack, stream: h.stream, value: uint64(code)}, nil)
		return err
	}
	w := s.cfg.ReceiveWindow
	if isControl {
		w = controlWindow
		s.controlUsed = true
	}
	st := s.newStreamLocked(h.stream, w, false)
	st.peerWindow = h.value
	if _, err := s.enqueueControlLocked(header{kind: typeOpenAck, stream: st.id, value: uint64(w)}, st); err != nil {
		return err
	}
	s.acceptedHigh = max(s.acceptedHigh, h.stream)
	if isControl {
		s.control = st
	} else {
		s.accepts = append(s.accepts, st)
		s.notifyAcceptLocked()
	}
	return nil
}

func (s *Session) knownIDLocked(id uint64) bool {
	localOdd := s.role == Initiator
	if (id%2 == 1) == localOdd {
		return id <= s.localHigh
	}
	return id <= s.peerHigh
}

func (s *Session) handleControlLocked(h header, body []byte) error {
	if h.kind == typeSettings {
		return protocolError("duplicate SETTINGS")
	}
	if h.kind == typeOpen {
		return s.incomingOpenLocked(h)
	}
	if h.kind == typeGoAway {
		if h.value > s.localHigh || (h.value != 0 && (h.value%2 == 1) != (s.role == Initiator)) {
			return protocolError("invalid GOAWAY boundary")
		}
		_ = binary.BigEndian.Uint32(body) // Unknown reason codes retain the same draining semantics.
		s.peerDraining = true
		for _, st := range s.streams {
			if st.local && st.id > h.value {
				if st.ackReceived {
					return protocolError("GOAWAY excludes accepted stream")
				}
				if err := s.resetLocked(st, Draining, ErrDraining); err != nil {
					return err
				}
			}
		}
		return nil
	}
	st := s.streams[h.stream]
	if st == nil {
		if !s.knownIDLocked(h.stream) {
			return protocolError("control for unknown stream")
		}
		switch h.kind {
		case typeWindow, typeResetAck:
			return nil
		case typeReset:
			_, err := s.enqueueControlLocked(header{kind: typeResetAck, stream: h.stream}, nil)
			return err
		default:
			return protocolError("unexpected frame for retired stream")
		}
	}
	switch h.kind {
	case typeOpenAck:
		if !st.local || !st.openIssued || st.ackReceived || st.nacked {
			return protocolError("unexpected OPEN_ACK")
		}
		if (st.id == 1 && h.value != controlWindow) || (st.id != 1 && !validWindow(h.value, s.peer.maxWindow)) {
			return protocolError("invalid granted window")
		}
		st.ackReceived = true
		st.peerWindow = h.value
	case typeOpenNack:
		if !st.local || !st.openIssued || st.ackReceived || st.nacked {
			return protocolError("unexpected OPEN_NACK")
		}
		st.nacked = true
		st.endLocked(&StreamError{Code: Code(h.value)})
		s.reapLocked(st)
	case typeWindow:
		if h.value < st.peerConsumed || h.value > st.issued {
			return protocolError("invalid consumed offset")
		}
		st.peerConsumed = h.value
	case typeFin:
		if st.remoteFin || st.remoteReset || st.nacked || h.value != st.received {
			return protocolError("invalid FIN")
		}
		st.remoteFin = true
		st.notifyReadLocked()
		if st.windowRequest != nil {
			s.cancelControlLocked(st.windowRequest)
			st.windowRequest = nil
		}
		s.reapLocked(st)
	case typeReset:
		if st.remoteReset {
			return nil
		}
		st.remoteReset = true
		st.endLocked(&StreamError{Code: Code(h.value)})
		s.discardDataLocked(st)
		_, err := s.enqueueControlLocked(header{kind: typeResetAck, stream: st.id}, st)
		return err
	case typeResetAck:
		if !st.localReset {
			return protocolError("unsolicited RST_ACK")
		}
		st.resetAckReceived = true
		s.reapLocked(st)
	default:
		return protocolError("unexpected frame %d", h.kind)
	}
	return nil
}

// Drain rejects new streams while allowing already accepted streams to finish.
func (s *Session) Drain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.draining {
		return nil
	}
	q, err := s.enqueueControlLocked(header{kind: typeGoAway, length: 4, value: s.acceptedHigh}, nil)
	if err != nil {
		return err
	}
	q.payload = make([]byte, 4)
	s.draining = true
	return nil
}
