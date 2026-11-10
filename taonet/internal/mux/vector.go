package mux

import (
	"io"
	"math"
	"net"
	"os"
	"time"
)

const maxWriteParts = 64
const writeBatchTargetBytes = 96 << 10

type vectorFrame struct {
	parts [maxWriteParts][]byte
	frame writeRequest
}

type vectorState struct {
	first  vectorFrame
	second *vectorFrame // only multi-frame writes need another descriptor set
	third  *vectorFrame
	lease  writeLease
}

var releasedWrite = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// The initiating call and each issued frame own one reference. All reference
// changes use Session.mu. A reset can return to the caller before socket I/O
// ends, so only the final reference releases the borrowed backing arrays.
type writeLease struct {
	refs int
	done chan struct{}
}

func (l *writeLease) releaseLocked() {
	l.refs--
	if l.refs == 0 && l.done != nil {
		close(l.done)
	}
}

// WritevBorrowed writes without copying payloads. The returned signal, rather
// than err or the stream's Done, determines when all input may be reused.
// It retains Write's deadlines, byte windows and serialized-write ordering.
func (st *Stream) WritevBorrowed(parts net.Buffers) (written int, released <-chan struct{}, err error) {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	s := st.s
	released = releasedWrite
	var lease *writeLease
	defer func() {
		if lease == nil {
			return
		}
		s.mu.Lock()
		lease.releaseLocked()
		if lease.refs != 0 {
			// Only interrupted physical I/O needs a future notification.
			// A successful synchronous write reuses the closed signal.
			lease.done = make(chan struct{})
			released = lease.done
		}
		s.mu.Unlock()
	}()
	index, offset := 0, 0
	for index < len(parts) {
		if offset == len(parts[index]) {
			index, offset = index+1, 0
			continue
		}
		s.mu.Lock()
		if st.err != nil {
			err = st.err
			s.mu.Unlock()
			return written, released, err
		}
		if st.writeClosed {
			s.mu.Unlock()
			return written, released, io.ErrClosedPipe
		}
		if expired(st.writeDeadline) {
			var fatal error
			if written > 0 {
				fatal = s.resetLocked(st, DeadlineExceeded, os.ErrDeadlineExceeded)
			}
			s.mu.Unlock()
			if fatal != nil {
				s.stop(fatal)
			}
			return written, released, os.ErrDeadlineExceeded
		}
		available := st.peerWindow - (st.reserved - st.peerConsumed)
		space := s.cfg.SendBudget - s.dataBytes - headerSize
		if st.id == 1 {
			space = controlBudget - s.controlDataBytes - headerSize
		}
		if available == 0 || space <= 0 || st.id != 1 && s.dataFrames >= 4096 {
			changed, deadline := s.changedLocked(), st.writeDeadline
			s.mu.Unlock()
			waitChange(changed, deadline)
			continue
		}
		if st.vectors == nil {
			st.vectors = &vectorState{}
		}
		if lease == nil {
			// writeMu serializes calls. An interrupted in-flight frame makes
			// st.err terminal, checked above before this storage can be reused.
			lease = &st.vectors.lease
			*lease = writeLease{refs: 1}
		}
		// Submit up to three frames before waiting. They consume the same
		// stream window and session budget, and return only after physical I/O.
		// This lets the connection writer gather adjacent frames from one call
		// without a round of goroutine handoffs for every 32KiB fragment.
		var frames [3]*writeRequest
		frameCount := 0
		for frameCount < len(frames) {
			for index < len(parts) && offset == len(parts[index]) {
				index, offset = index+1, 0
			}
			if index == len(parts) {
				break
			}
			available = st.peerWindow - (st.reserved - st.peerConsumed)
			space = s.cfg.SendBudget - s.dataBytes - headerSize
			if st.id == 1 {
				space = controlBudget - s.controlDataBytes - headerSize
			}
			if available == 0 || space <= 0 || st.id != 1 && s.dataFrames >= 4096 {
				break
			}
			frame := &st.vectors.first
			if frameCount == 1 {
				if st.vectors.second == nil {
					st.vectors.second = &vectorFrame{}
				}
				frame = st.vectors.second
			} else if frameCount == 2 {
				if st.vectors.third == nil {
					st.vectors.third = &vectorFrame{}
				}
				frame = st.vectors.third
			}
			remaining := min(int(s.maxData), int(available), space)
			n, count := 0, 0
			for index < len(parts) && remaining > 0 && count < maxWriteParts {
				p := parts[index][offset:]
				if len(p) == 0 {
					index, offset = index+1, 0
					continue
				}
				size := min(len(p), remaining)
				frame.parts[count] = p[:size]
				count++
				n += size
				remaining -= size
				offset += size
				if offset == len(parts[index]) {
					index, offset = index+1, 0
				}
			}
			if st.reserved > math.MaxUint64-uint64(n) {
				clear(frame.parts[:count])
				fatal := s.resetLocked(st, Internal, protocolError("send offset exhausted"))
				s.mu.Unlock()
				if fatal != nil {
					s.stop(fatal)
				}
				return written, released, protocolError("send offset exhausted")
			}
			lease.refs++
			q := &frame.frame
			*q = writeRequest{h: header{kind: typeData, length: uint32(n), stream: st.id, value: st.reserved}, st: st, dataQueue: true, vectors: frame.parts[:count], lease: lease}
			s.appendDataLocked(q)
			st.reserved += uint64(n)
			frames[frameCount] = q
			frameCount++
		}
		s.mu.Unlock()
		for _, q := range frames[:frameCount] {
			if err := st.waitWrite(q, time.Time{}); err != nil {
				return written, released, err
			}
			written += int(q.h.length)
		}
	}
	return written, released, nil
}
