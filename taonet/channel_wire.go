package taonet

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net"
	"time"
	"unicode/utf8"
)

func (c *Channel) readLoop() {
	defer c.wg.Done()
	var current *Delivery
	offset := 0
	var encoded [16]byte
	for {
		_, err := io.ReadFull(c.raw, encoded[:])
		var h recordHeader
		if err == nil {
			h, err = decodeRecordHeader(encoded)
		}
		if err == io.EOF {
			c.mu.Lock()
			valid := current == nil && c.readyForFINLocked()
			c.finReceived = valid
			done := valid && c.finSent
			c.mu.Unlock()
			if !valid {
				c.end(io.ErrUnexpectedEOF)
			} else if done {
				c.end(nil)
			}
			return
		}
		if err != nil {
			c.end(err)
			return
		}
		if current == nil {
			_ = c.raw.SetReadDeadline(time.Now().Add(c.session.cfg.DefaultWriteTimeout))
		}
		switch h.kind {
		case recordMessage, recordMessageUnconfirmed:
			if h.kind == recordMessageUnconfirmed && !c.unconfirmed {
				err = protocolError("unnegotiated unconfirmed message")
				break
			}
			if current != nil {
				err = protocolError("interleaved channel messages")
				break
			}
			var message channelMessage
			bodyHeader := h
			bodyHeader.tag = 0
			if err = readJSONBody(c.raw, bodyHeader, &message); err != nil {
				break
			}
			current, err = c.startDelivery(h.tag, message.Size, message.Metadata)
			if err == nil {
				current.unconfirmed = h.kind == recordMessageUnconfirmed
			}
			offset = 0
			_ = c.raw.SetReadDeadline(time.Now().Add(c.session.cfg.DefaultWriteTimeout))
		case recordInline, recordInlineUnconfirmed:
			if !c.inline || current != nil || h.kind == recordInlineUnconfirmed && !c.unconfirmed {
				err = protocolError("unexpected inline message")
				break
			}
			var d *Delivery
			d, err = c.startDelivery(h.tag, int(h.length), nil)
			if err == nil {
				d.unconfirmed = h.kind == recordInlineUnconfirmed
				_, err = io.ReadFull(c.raw, d.message.Body)
			}
			if err == nil {
				err = c.publishDelivery(d)
			}
		case recordData:
			if current == nil || h.tag != current.sequence || int(h.length) > len(current.message.Body)-offset {
				err = protocolError("invalid channel DATA")
				break
			}
			_, err = io.ReadFull(c.raw, current.message.Body[offset:offset+int(h.length)])
			offset += int(h.length)
		case recordEnd:
			if h.tag == 0 {
				c.mu.Lock()
				if current != nil || c.rxEOF {
					err = protocolError("unexpected channel half-close")
				} else {
					c.rxEOF = true
					c.receiveWakeLocked()
					close(c.receiveWake)
					c.notifyLocked()
				}
				c.mu.Unlock()
			} else {
				if current == nil || h.tag != current.sequence || offset != len(current.message.Body) {
					err = protocolError("incomplete channel message")
					break
				}
				err = c.publishDelivery(current)
				current = nil
				_ = c.raw.SetReadDeadline(time.Time{})
			}
		case recordAck, recordAckBinary:
			var ack channelAck
			if h.kind == recordAckBinary {
				if !c.inline {
					err = protocolError("unnegotiated binary ACK")
					break
				}
				ack.Status, err = readBinaryAck(c.raw, int(h.length))
			} else {
				bodyHeader := h
				bodyHeader.tag = 0
				err = readJSONBody(c.raw, bodyHeader, &ack)
			}
			if err != nil {
				break
			}
			if ack.Status != nil && !validStatus(ack.Status) {
				err = protocolError("invalid channel ACK status")
				break
			}
			c.mu.Lock()
			item := c.pending[h.tag]
			if c.expectedAcks.len() == 0 || h.tag != c.expectedAcks.peek() || h.tag > c.endIssued || item == nil {
				err = protocolError("invalid channel ACK sequence")
			} else {
				var response error
				if ack.Status != nil {
					response = remoteError(ack.Status, Rejected, c.raw.ID())
				}
				c.resolveSendLocked(item, response)
				delete(c.pending, h.tag)
				c.ackReceived = h.tag
				c.expectedAcks.pop()
				if c.writeClosing {
					c.notifyLocked()
				}
			}
			c.mu.Unlock()
		case recordWindow:
			if h.tag != 0 || h.length != 16 {
				err = protocolError("invalid channel WINDOW")
				break
			}
			var body [16]byte
			if _, err = io.ReadFull(c.raw, body[:]); err != nil {
				break
			}
			bytes, messages := binary.BigEndian.Uint64(body[:8]), binary.BigEndian.Uint64(body[8:])
			c.mu.Lock()
			if bytes < c.peerReleasedBytes || messages < c.peerReleasedMessages || bytes > c.sentBytes || messages > c.sentMessages {
				err = protocolError("invalid cumulative channel WINDOW")
			} else {
				c.peerReleasedBytes = bytes
				c.peerReleasedMessages = messages
				c.notifyLocked()
			}
			c.mu.Unlock()
		default:
			err = protocolError("unexpected channel record %d", h.kind)
		}
		if err != nil {
			if h.kind == recordWindow {
				c.session.stop(err)
			}
			c.end(err)
			return
		}
		c.session.touch()
		if current == nil {
			_ = c.raw.SetReadDeadline(time.Time{})
		}
	}
}

func (c *Channel) writeLoop() {
	defer c.wg.Done()
	defer func() {
		c.mu.Lock()
		if c.active != nil {
			c.releaseBodyLocked(c.active)
			c.active = nil
		}
		c.mu.Unlock()
	}()
	var batch *channelWriteBatch
	var large *channelLargeWrite
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	lastWindow := time.Now()
	for {
		c.mu.Lock()
		c.writerWaiting = false
		if c.closed {
			c.mu.Unlock()
			return
		}
		_ = c.raw.SetWriteDeadline(time.Now().Add(c.session.cfg.DefaultWriteTimeout))
		if len(c.acks) > 0 {
			n := min(32, len(c.acks))
			acks := c.acks[:n:n]
			c.acks = c.acks[n:]
			c.mu.Unlock()
			if err := c.writeAcknowledgments(acks); err != nil {
				c.end(err)
				return
			}
			continue
		}
		dirty := c.releasedMessages != c.advertisedMessages
		if dirty && (c.releasedBytes-c.advertisedBytes >= c.local.Bytes/4 || c.releasedMessages-c.advertisedMessages >= max(1, c.local.Messages/4) || time.Since(lastWindow) >= 10*time.Millisecond || c.txEOF && c.rxEOF) {
			var body [16]byte
			binary.BigEndian.PutUint64(body[:8], c.releasedBytes)
			binary.BigEndian.PutUint64(body[8:], c.releasedMessages)
			c.advertisedBytes = c.releasedBytes
			c.advertisedMessages = c.releasedMessages
			c.mu.Unlock()
			if err := writeRecord(c.raw, recordWindow, 0, body[:]); err != nil {
				c.end(err)
				return
			}
			lastWindow = time.Now()
			continue
		}
		if c.active == nil && c.queue.len() > 0 {
			c.active = c.queue.pop()
		}
		item := c.active
		if item != nil {
			if !item.started {
				if uint64(item.charge) <= c.peer.Bytes-(c.sentBytes-c.peerReleasedBytes) && c.sentMessages-c.peerReleasedMessages < c.peer.Messages {
					if c.sentMessages >= math.MaxUint64-1 || c.sentBytes > math.MaxUint64-uint64(item.charge) {
						c.mu.Unlock()
						c.end(protocolError("channel counters exhausted"))
						return
					}
					c.sentBytes += uint64(item.charge)
					c.sentMessages++
					item.sequence = c.sentMessages
					item.started = true
					c.pending[item.sequence] = item
					if !item.unconfirmed {
						c.expectedAcks.push(item.sequence)
					}
					c.mu.Unlock()
					if len(item.message.Body) <= 16<<10 {
						if batch == nil {
							batch = &channelWriteBatch{}
						}
						if err := c.writeSmallMessages(item, batch); err != nil {
							c.end(err)
							return
						}
						continue
					}
					if large == nil {
						large = &channelLargeWrite{}
					}
					complete, err := c.writeLargeChunk(item, large, true)
					if err != nil {
						c.end(err)
						return
					}
					if complete {
						c.sentMessage(item)
					}
					continue
				}
			} else {
				c.mu.Unlock()
				complete, err := c.writeLargeChunk(item, large, false)
				if err != nil {
					c.end(err)
					return
				}
				if complete {
					c.sentMessage(item)
				}
				continue
			}
		} else if c.writeClosing && !c.txEOF {
			c.txEOF = true
			c.mu.Unlock()
			if err := writeRecord(c.raw, recordEnd, 0, nil); err != nil {
				c.end(err)
				return
			}
			continue
		}
		if c.readyForFINLocked() {
			c.mu.Unlock()
			if err := c.raw.CloseWrite(); err != nil {
				c.end(err)
				return
			}
			c.mu.Lock()
			c.finSent = true
			done := c.finReceived
			c.mu.Unlock()
			if done {
				c.end(nil)
			}
			return
		}
		c.writerWaiting = true
		c.mu.Unlock()
		select {
		case <-c.done:
			return
		case <-c.writerWake:
		case <-ticker.C:
		}
	}
}

func (c *Channel) writeAcknowledgments(acks []queuedAck) error {
	packet := make([]byte, 0, len(acks)*18)
	for i, ack := range acks {
		if c.inline {
			var body []byte
			if ack.status != nil {
				body = binary.BigEndian.AppendUint16(body, uint16(len(ack.status.Code)))
				body = append(body, ack.status.Code...)
				body = append(body, ack.status.Message...)
			}
			packet = appendRecord(packet, recordAckBinary, ack.sequence, body)
			acks[i] = queuedAck{}
			continue
		}
		body := []byte("{}")
		if ack.status != nil {
			var err error
			body, err = json.Marshal(channelAck{ack.status})
			if err != nil {
				return err
			}
		}
		if len(body) > recordLimit(recordAck) {
			return protocolError("channel ACK too large")
		}
		packet = appendRecord(packet, recordAck, ack.sequence, body)
		acks[i] = queuedAck{}
	}
	_, err := c.raw.WriteOwned(packet)
	return err
}

// Submit the same MESSAGE/DATA/END records together. Small messages otherwise
// pay for three separate synchronous mux writes. Large messages retain their
// incremental path so ACK/WINDOW records can run between DATA chunks.
type channelWriteBatch struct {
	items   [32]*channelSend
	headers [32][16]byte
	parts   [64][]byte
}

func (c *Channel) writeSmallMessages(first *channelSend, state *channelWriteBatch) (resultErr error) {
	batch := state.items[:]
	batch[0] = first
	count, charge := 1, first.charge
	c.mu.Lock()
	if c.closed {
		c.releaseBodyLocked(first)
		c.active = nil
		batch[0] = nil
		c.mu.Unlock()
		return ErrClosed
	}
	// Only take messages already queued, within both peer windows. charge
	// includes metadata and 128 bytes of framing allowance per message.
	for c.queue.len() > 0 && count < len(batch) {
		item := c.queue.peek()
		if len(item.message.Body) > 16<<10 || charge+item.charge > 32<<10 || uint64(item.charge) > c.peer.Bytes-(c.sentBytes-c.peerReleasedBytes) || c.sentMessages-c.peerReleasedMessages >= c.peer.Messages || c.sentMessages >= math.MaxUint64-1 || c.sentBytes > math.MaxUint64-uint64(item.charge) {
			break
		}
		c.queue.pop()
		c.sentBytes += uint64(item.charge)
		c.sentMessages++
		item.sequence, item.started = c.sentMessages, true
		c.pending[item.sequence] = item
		if !item.unconfirmed {
			c.expectedAcks.push(item.sequence)
		}
		batch[count] = item
		count++
		charge += item.charge
	}
	// The batch now owns these inputs, including if the Channel closes while
	// native socket I/O is still using them. active is reserved for large sends.
	c.active = nil
	c.mu.Unlock()
	inline := c.inline
	for _, item := range batch[:count] {
		inline = inline && len(item.message.Metadata) == 0
	}
	if inline {
		for i, item := range batch[:count] {
			header := &state.headers[i]
			*header = [16]byte{recordInline}
			if item.unconfirmed {
				header[0] = recordInlineUnconfirmed
			}
			binary.BigEndian.PutUint32(header[4:8], uint32(len(item.message.Body)))
			binary.BigEndian.PutUint64(header[8:16], item.sequence)
			state.parts[2*i], state.parts[2*i+1] = header[:], item.message.Body
		}
		c.mu.Lock()
		c.endIssued = batch[count-1].sequence
		c.mu.Unlock()
		_, released, err := c.raw.WritevBorrowed(net.Buffers(state.parts[:2*count]))
		if err != nil {
			select {
			case <-released:
				c.releaseBatch(state, count, charge, err)
			default:
				// Only an interrupted physical write needs delayed reclamation.
				// Normal sends allocate no cleanup goroutine.
				c.wg.Add(1)
				go func() {
					defer c.wg.Done()
					<-released
					c.releaseBatch(state, count, charge, err)
				}()
			}
			return err
		}
		c.releaseBatch(state, count, charge, nil)
		return nil
	}
	defer func() { c.releaseBatch(state, count, charge, resultErr) }()
	packet := make([]byte, 0, charge)
	for _, item := range batch[:count] {
		if c.inline && len(item.message.Metadata) == 0 {
			kind := byte(recordInline)
			if item.unconfirmed {
				kind = recordInlineUnconfirmed
			}
			packet = appendRecord(packet, kind, item.sequence, item.message.Body)
			continue
		}
		header, err := json.Marshal(channelMessage{len(item.message.Body), item.message.Metadata})
		if err != nil {
			return err
		}
		if len(header) > recordLimit(recordMessage) {
			return protocolError("channel MESSAGE too large")
		}
		kind := byte(recordMessage)
		if item.unconfirmed {
			kind = recordMessageUnconfirmed
		}
		packet = appendRecord(packet, kind, item.sequence, header)
		if len(item.message.Body) > 0 {
			packet = appendRecord(packet, recordData, item.sequence, item.message.Body)
		}
		packet = appendRecord(packet, recordEnd, item.sequence, nil)
	}
	c.mu.Lock()
	// A complete record or END is part of this submission. Publish it
	// before Write: a peer can ACK before the local writer resumes. This is an
	// issuance watermark, not proof that the peer consumed any bytes.
	c.endIssued = batch[count-1].sequence
	c.mu.Unlock()
	if _, err := c.raw.WriteOwned(packet); err != nil {
		return err
	}
	return nil
}

func (c *Channel) releaseBatch(batch *channelWriteBatch, count, charge int, err error) {
	c.session.releaseSend(c.op, charge)
	c.mu.Lock()
	clear(batch.parts[:2*count])
	for i, item := range batch.items[:count] {
		c.completeWriteLocked(item, err)
		c.releaseBodyLocked(item)
		batch.items[i] = nil
	}
	c.notifyLocked()
	c.mu.Unlock()
}

func (c *Channel) startDelivery(sequence uint64, size int, metadata map[string]string) (*Delivery, error) {
	if size < 0 || size > c.local.MaxMessage {
		return nil, protocolError("invalid channel message size")
	}
	if err := checkMetadata(metadata, c.session.cfg.Limits); err != nil {
		return nil, err
	}
	charge, _ := messageCharge(size, metadata)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	if c.rxEOF || c.receivedMessages >= math.MaxUint64-1 || sequence != c.receivedMessages+1 || c.receivedMessages-c.releasedMessages >= c.local.Messages || uint64(charge) > c.local.Bytes-(c.receivedBytes-c.releasedBytes) || c.receivedBytes > math.MaxUint64-uint64(charge) {
		return nil, protocolError("channel receive window or sequence exceeded")
	}
	c.receivedBytes += uint64(charge)
	c.receivedMessages++
	return &Delivery{channel: c, sequence: sequence, charge: uint64(charge), message: Message{Metadata: metadata, Body: c.takeReceiveBodyLocked(size)}}, nil
}

func (c *Channel) publishDelivery(d *Delivery) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.deliveries[d.sequence] = d
	c.inbox.push(d)
	c.notifyReceiveLocked()
	return nil
}

func readBinaryAck(r io.Reader, size int) (*StatusError, error) {
	if size == 0 {
		return nil, nil
	}
	if size < 2 {
		return nil, protocolError("short binary ACK")
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(body[:2]))
	if n > len(body)-2 || !utf8.Valid(body[2:]) {
		return nil, protocolError("invalid binary ACK")
	}
	return &StatusError{Code: Code(body[2 : 2+n]), Message: string(body[2+n:])}, nil
}

func (c *Channel) sentMessage(item *channelSend) {
	c.session.releaseSend(c.op, item.charge)
	c.mu.Lock()
	c.completeWriteLocked(item, nil)
	c.releaseBodyLocked(item)
	c.active = nil
	c.notifyLocked()
	c.mu.Unlock()
}

func (c *Channel) completeWriteLocked(item *channelSend, err error) {
	if !item.unconfirmed {
		return
	}
	if err != nil {
		err = operationError(c.ctx, err, Unknown, c.raw.ID())
	}
	c.resolveSendLocked(item, err)
	delete(c.pending, item.sequence)
}
