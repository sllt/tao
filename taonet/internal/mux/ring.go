package mux

const receiveBlockSize = 4096
const contiguousReceiveSize = 32 << 10

// ring is accessed only while holding its session's state lock. limit is the
// already-reserved protocol window, a multiple of receiveBlockSize. Blocks are
// allocated when first touched and retained until the stream is reclaimed.
// Adjacent unused slots are allocated together; each slot stores a suffix view
// of that allocation so large reads/writes can copy a whole contiguous run.
type ring struct {
	buf    []byte
	blocks [][]byte
	limit  int
	head   int
	size   int
}

func (r *ring) write(p []byte) {
	if len(p) == 0 {
		return
	}
	// Sustained large DATA uses the original contiguous fast path. Promote
	// only when empty: no unread bytes need copying and old blocks lose their
	// owner before the replacement allocation consumes the reserved window.
	if r.buf == nil && r.size == 0 && len(p) >= contiguousReceiveSize {
		r.blocks = nil
		r.buf = make([]byte, r.limit)
	}
	if r.buf != nil {
		tail := (r.head + r.size) % len(r.buf)
		n := copy(r.buf[tail:], p)
		copy(r.buf, p[n:])
		r.size += len(p)
		return
	}
	if r.blocks == nil {
		r.blocks = make([][]byte, r.limit/receiveBlockSize)
	}
	tail := (r.head + r.size) % r.limit
	r.size += len(p)
	for len(p) > 0 {
		index, offset := tail/receiveBlockSize, tail%receiveBlockSize
		if r.blocks[index] == nil {
			end := min(len(r.blocks), index+(offset+len(p)+receiveBlockSize-1)/receiveBlockSize)
			for next := index + 1; next < end; next++ {
				if r.blocks[next] != nil {
					end = next
					break
				}
			}
			storage := make([]byte, (end-index)*receiveBlockSize)
			for slot := index; slot < end; slot++ {
				r.blocks[slot] = storage[(slot-index)*receiveBlockSize:]
			}
		}
		n := copy(r.blocks[index][offset:], p)
		p = p[n:]
		tail = (tail + n) % r.limit
	}
}

func (r *ring) read(p []byte) int {
	n := min(len(p), r.size)
	p = p[:n]
	if r.buf != nil {
		first := copy(p, r.buf[r.head:min(r.head+n, len(r.buf))])
		copy(p[first:], r.buf[:n-first])
		r.head = (r.head + n) % len(r.buf)
		r.size -= n
		if r.size == 0 {
			r.head = 0
		}
		return n
	}
	for len(p) > 0 {
		index, offset := r.head/receiveBlockSize, r.head%receiveBlockSize
		copied := copy(p, r.blocks[index][offset:])
		p = p[copied:]
		r.head = (r.head + copied) % r.limit
	}
	r.size -= n
	// Reuse the first blocks for repeated small messages instead of eventually
	// touching the entire logical window as the cursor travels around it.
	if r.size == 0 {
		r.head = 0
	}
	return n
}
