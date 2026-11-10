package mux

import (
	"net"
	"time"
)

type writeRequest struct {
	h         header
	payload   []byte
	vectors   net.Buffers
	lease     *writeLease
	st        *Stream
	dataQueue bool
	finished  bool
	err       error
}

func (s *Session) enqueueControlLocked(h header, st *Stream) (*writeRequest, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.controlBytes+headerSize+int(h.length) > controlBudget {
		return nil, ErrCapacity
	}
	q := &writeRequest{h: h, st: st}
	s.controls = append(s.controls, q)
	s.controlBytes += headerSize + int(h.length)
	if st != nil {
		st.pending++
	}
	s.notifyWriterLocked()
	return q, nil
}

func (s *Session) enqueueDataLocked(st *Stream, h header, payload []byte) *writeRequest {
	q := &writeRequest{h: h, payload: payload, st: st, dataQueue: true}
	s.appendDataLocked(q)
	return q
}

func (s *Session) appendDataLocked(q *writeRequest) {
	st := q.st
	st.queue = append(st.queue, q)
	st.pending++
	if st.id == 1 {
		s.controlDataBytes += headerSize + int(q.h.length)
	} else {
		s.dataBytes += headerSize + int(q.h.length)
	}
	s.dataFrames++
	if st.id != 1 && !st.scheduled {
		st.scheduled = true
		s.runnable = append(s.runnable, st)
	}
	s.notifyWriterLocked()
}

func (s *Session) finishLocked(q *writeRequest, err error) {
	q.finished = true
	q.err = err
	cost := headerSize + int(q.h.length)
	if q.dataQueue {
		if q.st.id == 1 {
			s.controlDataBytes -= cost
		} else {
			s.dataBytes -= cost
		}
		s.dataFrames--
	} else {
		s.controlBytes -= cost
	}
	if q.st != nil {
		q.st.pending--
		q.st.notifyWriteLocked()
	}
	q.payload = nil
	clear(q.vectors)
	q.vectors = nil
	if q.lease != nil {
		q.lease.releaseLocked()
		q.lease = nil
	}
}

func eligibleData(st *Stream) bool {
	return st != nil && len(st.queue) > 0 && st.err == nil && (st.local || st.ackSent)
}

func (s *Session) hasNormalDataLocked() bool {
	if eligibleData(s.active) {
		return true
	}
	for _, st := range s.runnable {
		if eligibleData(st) {
			return true
		}
	}
	return false
}

func (s *Session) popRunnableLocked(index int) *Stream {
	st := s.runnable[index]
	copy(s.runnable[index:], s.runnable[index+1:])
	s.runnable[len(s.runnable)-1] = nil
	s.runnable = s.runnable[:len(s.runnable)-1]
	st.scheduled = false
	return st
}

func (s *Session) removeRunnableLocked(st *Stream) {
	if s.active == st {
		s.active = nil
		st.scheduled = false
		st.deficit = 0
		return
	}
	if !st.scheduled {
		return
	}
	for i, v := range s.runnable {
		if v == st {
			s.popRunnableLocked(i)
			return
		}
	}
}

func (s *Session) takeDataLocked(st *Stream) *writeRequest {
	q := st.queue[0]
	// Serialized writes queue at most three DATA frames plus a concurrent FIN.
	// Keep the original capacity instead of losing one slot on every pop.
	copy(st.queue, st.queue[1:])
	st.queue[len(st.queue)-1] = nil
	st.queue = st.queue[:len(st.queue)-1]
	return q
}

// A DRR visit can issue several small frames while its byte quantum remains.
// Advancing to another stream after every frame would only be packet fairness.
func (s *Session) nextNormalDataLocked() *writeRequest {
	for {
		if st := s.active; st != nil {
			if !eligibleData(st) {
				s.active = nil
				st.scheduled = false
				st.deficit = 0
			} else {
				cost := headerSize + int(st.queue[0].h.length)
				if st.deficit >= cost {
					st.deficit -= cost
					return s.takeDataLocked(st)
				}
				s.active = nil
				s.runnable = append(s.runnable, st)
			}
		}
		st := s.popRunnableLocked(0)
		if len(st.queue) == 0 || st.err != nil {
			st.deficit = 0
			continue
		}
		st.scheduled = true
		if !eligibleData(st) {
			s.runnable = append(s.runnable, st)
			continue
		}
		st.deficit = min(st.deficit+(16<<10), 2*(int(s.maxData)+headerSize))
		s.active = st
	}
}

func (s *Session) nextLocked() *writeRequest {
	normal := s.hasNormalDataLocked()
	var q *writeRequest
	priority := s.controlBurst < 32 || !normal
	if len(s.controls) > 0 && priority {
		q = s.controls[0]
		s.controls[0] = nil
		if len(s.controls) == 1 {
			s.controls = s.controls[:0]
		} else {
			s.controls = s.controls[1:]
		}
		s.controlBurst++
	} else if eligibleData(s.control) && priority {
		q = s.takeDataLocked(s.control)
		s.controlBurst++
	} else if normal {
		q = s.nextNormalDataLocked()
		s.controlBurst = 0
	}
	if q == nil {
		return nil
	}
	if st := q.st; st != nil {
		switch q.h.kind {
		case typeOpen:
			st.openIssued = true
			s.localHigh = max(s.localHigh, st.id)
		case typeOpenAck:
			st.ackIssued = true
		case typeData:
			st.issued = q.h.value + uint64(q.h.length)
		case typeWindow:
			st.advertised = q.h.value
			st.windowRequest = nil
		}
	}
	return q
}

func (s *Session) writeLoop() {
	defer s.wg.Done()
	// A TCP writev submits header and payload together without a payload copy.
	// Keep descriptors owned by this writer; other net.Conn implementations
	// retain writeFull's short-write handling (including TLS and test wrappers).
	tcp, _ := s.conn.(*net.TCPConn)
	var encoded [8][headerSize]byte
	var requests [8]*writeRequest
	var parts [8 * (maxWriteParts + 1)][]byte
	var buffers net.Buffers
	var vectorBody []byte
	for {
		s.mu.Lock()
		if s.err != nil {
			s.mu.Unlock()
			return
		}
		q := s.nextLocked()
		if q == nil {
			s.mu.Unlock()
			select {
			case <-s.writerWake:
			case <-s.done:
				return
			}
			continue
		}
		requests[0] = q
		count, size := 1, int(q.h.length)
		// Gather only frames already runnable. DRR/control priority still pick
		// every frame; there is no timer or added delay waiting for a batch.
		if tcp != nil {
			for count < len(requests) && size < writeBatchTargetBytes {
				next := s.nextLocked()
				if next == nil {
					break
				}
				requests[count] = next
				count++
				size += int(next.h.length)
			}
		}
		h, payload, vectors := q.h, q.payload, q.vectors
		s.mu.Unlock()
		err := s.conn.SetWriteDeadline(time.Now().Add(s.cfg.IOTimeout))
		if err == nil {
			if tcp != nil {
				n := 0
				for i, frame := range requests[:count] {
					encoded[i] = frame.h.encode()
					parts[n] = encoded[i][:]
					n++
					if len(frame.vectors) > 0 {
						n += copy(parts[n:], frame.vectors)
					} else if len(frame.payload) > 0 {
						parts[n] = frame.payload
						n++
					}
				}
				buffers = parts[:n]
				_, err = buffers.WriteTo(tcp)
				clear(parts[:n])
			} else if len(vectors) > 0 {
				encoded[0] = h.encode()
				err = writeFull(s.conn, encoded[0][:])
				// TLS/custom connections cannot use TCP writev. Coalesce into
				// one bounded writer-owned frame, rather than issuing a TLS
				// record for every descriptor. No payload allocation per call.
				if err == nil {
					if cap(vectorBody) < int(h.length) {
						vectorBody = make([]byte, int(s.maxData))
					}
					body := vectorBody[:int(h.length)]
					offset := 0
					for _, part := range vectors {
						offset += copy(body[offset:], part)
					}
					err = writeFull(s.conn, body)
				}
			} else {
				err = writeFrame(s.conn, h, payload)
			}
		}
		s.mu.Lock()
		for i, q := range requests[:count] {
			if err == nil && q.st != nil {
				switch q.h.kind {
				case typeOpenAck:
					q.st.ackSent = true
				case typeFin:
					q.st.finSent = true
				case typeReset:
					q.st.resetSent = true
				case typeResetAck:
					q.st.resetAckSent = true
				}
			}
			// Completing physical I/O after a reset must not turn an unfinished
			// stream write back into success. Keep that result separate from the
			// connection error so one canceled stream cannot close the session.
			resultErr := err
			if resultErr == nil && q.dataQueue && q.st.err != nil {
				resultErr = q.st.err
			}
			s.finishLocked(q, resultErr)
			if q.st != nil {
				s.reapLocked(q.st)
			}
			requests[i] = nil
		}
		s.notifyLocked()
		s.mu.Unlock()
		if err != nil {
			s.stop(err)
			return
		}
	}
}
