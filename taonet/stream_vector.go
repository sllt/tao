package taonet

import (
	"encoding/binary"
	"math"
	"net"
)

type streamWriteVector struct {
	headers [2][16]byte
	parts   [64][]byte
}

const maxBorrowedBatchBytes = 96 << 10

// WritevBorrowed writes the concatenation of parts without copying the input.
// The caller must keep the input unchanged until released closes, including
// after an error, timeout or Close. On success it is already closed. This is a
// local buffer-lifetime signal, not remote acceptance or business completion.
// Writes retain Write's serialization, byte windows and deadline semantics.
func (st *Stream) WritevBorrowed(parts net.Buffers) (written int, released <-chan struct{}, err error) {
	st.writeMu.Lock()
	defer st.writeMu.Unlock()
	released = releasedInput
	if err = st.prepareWrite(); err != nil {
		return
	}
	index, offset := 0, 0
	for index < len(parts) {
		if offset == len(parts[index]) {
			index, offset = index+1, 0
			continue
		}
		if st.borrowed == nil {
			st.borrowed = &streamWriteVector{}
		}
		vector := st.borrowed
		count, payload, encoded, records := 0, 0, 0, 0
		var sizes [2]int
		for records < len(vector.headers) && count+1 < len(vector.parts) && encoded+16 < maxBorrowedBatchBytes && index < len(parts) {
			headerIndex, size := count, 0
			count++
			// Keep each record aligned to a 64KiB pair, but include a tail
			// record in this submission instead of another synchronous call.
			limit := min(maxDataRecord-16, maxBorrowedBatchBytes-encoded-16)
			for index < len(parts) && count < len(vector.parts) && size < limit {
				part := parts[index][offset:]
				if len(part) == 0 {
					index, offset = index+1, 0
					continue
				}
				n := min(len(part), limit-size)
				vector.parts[count] = part[:n]
				count++
				size += n
				offset += n
				if offset == len(parts[index]) {
					index, offset = index+1, 0
				}
			}
			if size == 0 {
				count-- // trailing empty input parts do not create an empty record
				break
			}
			if st.writeOffset > math.MaxUint64-uint64(payload+size) {
				clear(vector.parts[:count])
				err = protocolError("stream offset overflow")
				st.end(err)
				return
			}
			header := &vector.headers[records]
			*header = [16]byte{recordData}
			binary.BigEndian.PutUint32(header[4:8], uint32(size))
			binary.BigEndian.PutUint64(header[8:16], st.writeOffset+uint64(payload))
			vector.parts[headerIndex] = header[:]
			sizes[records] = size
			records++
			payload += size
			encoded += 16 + size
		}
		var n int
		n, released, err = st.raw.WritevBorrowed(net.Buffers(vector.parts[:count]))
		// Native I/O holds its own descriptors. The record header remains
		// immutable on error because end prevents a subsequent write.
		clear(vector.parts[:count])
		// A failed native write may finish only a prefix of these records.
		// Count payload bytes, excluding every fully or partly written header.
		payload = 0
		for _, size := range sizes[:records] {
			if n <= 16 {
				break
			}
			n -= 16
			body := min(size, n)
			payload += body
			n -= body
			if body < size {
				break
			}
		}
		written += payload
		st.writeOffset += uint64(payload)
		if err != nil {
			st.end(err)
			return
		}
	}
	return
}
