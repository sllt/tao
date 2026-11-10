package taonet

import (
	"encoding/binary"
	"encoding/json"
	"net"
)

type channelLargeWrite struct {
	headers [4][16]byte
	parts   [8][]byte
}

func (b *channelLargeWrite) record(index int, kind byte, sequence uint64, body []byte) {
	header := &b.headers[index]
	*header = [16]byte{kind}
	binary.BigEndian.PutUint32(header[4:8], uint32(len(body)))
	binary.BigEndian.PutUint64(header[8:16], sequence)
	b.parts[index*2], b.parts[index*2+1] = header[:], body
}

// A chunk includes framing in its bounded submission. MESSAGE/DATA/END share a
// native vector write, while ACK/WINDOW still run between chunks. Body remains
// Channel-owned until physical I/O releases it, including after an early RST.
func (c *Channel) writeLargeChunk(item *channelSend, batch *channelLargeWrite, first bool) (bool, error) {
	count, prefix := 0, 0
	if first {
		body, err := json.Marshal(channelMessage{len(item.message.Body), item.message.Metadata})
		if err != nil {
			return false, err
		}
		if len(body) > recordLimit(recordMessage) {
			return false, protocolError("channel MESSAGE too large")
		}
		kind := byte(recordMessage)
		if item.unconfirmed {
			kind = recordMessageUnconfirmed
		}
		batch.record(count, kind, item.sequence, body)
		count++
		prefix = 16 + len(body)
	}
	// Reserve END, then fit up to two legal DATA records in the same write.
	// A 64KiB message plus its envelope need not yield just to send its tail.
	space, size := maxBorrowedBatchBytes-prefix-16, 0
	for item.offset+size < len(item.message.Body) && space > 16 {
		n := min(len(item.message.Body)-item.offset-size, maxDataRecord, space-16)
		batch.record(count, recordData, item.sequence, item.message.Body[item.offset+size:item.offset+size+n])
		count++
		size += n
		space -= 16 + n
	}
	complete := item.offset+size == len(item.message.Body)
	if complete {
		batch.record(count, recordEnd, item.sequence, nil)
		count++
		c.mu.Lock()
		c.endIssued = item.sequence
		c.mu.Unlock()
	}
	_, released, err := c.raw.WritevBorrowed(net.Buffers(batch.parts[:count*2]))
	if err != nil {
		select {
		case <-released:
			clear(batch.parts[:count*2])
		default:
			// Detach from writeLoop's deferred cleanup. Only the I/O release
			// signal can return this input; end reports the error promptly.
			c.mu.Lock()
			c.active = nil
			c.mu.Unlock()
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				<-released
				clear(batch.parts[:count*2])
				c.mu.Lock()
				c.releaseBodyLocked(item)
				c.mu.Unlock()
			}()
		}
		return false, err
	}
	clear(batch.parts[:count*2])
	item.offset += size
	return complete, nil
}
