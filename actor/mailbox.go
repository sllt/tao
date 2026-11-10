package actor

type envelope struct {
	global  *RouteRecord
	release func()
	message any
	sender  Addr
	request *request
	toRef   bool
}
type mailbox struct {
	buffer            []mailboxMessage
	metadata          []mailboxMetadata
	segments          *mailboxSegments // allocated only beyond the small ring
	head, size, limit int
}

// Ordinary PID sends need only the value and sender. Allocate parallel control
// storage only when this mailbox first receives a Call, Ref, or network lease.
// No metadata object is allocated per message, and both arrays share one FIFO.
type mailboxMessage struct {
	message any
	sender  Addr
}

type mailboxMetadata struct {
	global  *RouteRecord
	release func()
	request *request
	toRef   bool
}

func (m *mailbox) push(message envelope) error {
	return m.pushReserved(message, 0)
}

func (m *mailbox) pushReserved(message envelope, held int) error {
	if m.size+held >= m.limit {
		return ErrMailboxFull
	}
	if m.segments != nil {
		m.pushSegment(message)
		return nil
	}
	if m.size == len(m.buffer) {
		m.grow(m.size + 1)
	}
	if m.segments != nil {
		m.pushSegment(message)
		return nil
	}
	index := (m.head + m.size) % len(m.buffer)
	m.buffer[index] = mailboxMessage{message: message.message, sender: message.sender}
	if m.metadata == nil && (message.global != nil || message.release != nil || message.request != nil || message.toRef) {
		m.metadata = make([]mailboxMetadata, len(m.buffer))
	}
	if m.metadata != nil {
		m.metadata[index] = mailboxMetadata{global: message.global, release: message.release, request: message.request, toRef: message.toRef}
	}
	m.size++
	return nil
}

func (m *mailbox) grow(required int) {
	if required > mailboxSmallLimit {
		m.promote()
		return
	}
	buffer := make([]mailboxMessage, min(m.limit, mailboxSmallLimit, max(required, max(1, len(m.buffer)*2))))
	var metadata []mailboxMetadata
	if m.metadata != nil {
		metadata = make([]mailboxMetadata, len(buffer))
	}
	for i := 0; i < m.size; i++ {
		index := (m.head + i) % len(m.buffer)
		buffer[i] = m.buffer[index]
		if metadata != nil {
			metadata[i] = m.metadata[index]
		}
	}
	m.buffer, m.metadata, m.head = buffer, metadata, 0
}

// Find a contiguous pure-message prefix. Requests, leases and Ref/global
// messages remain in the shared mailbox and never enter the private batch.
func (m *mailbox) purePrefix(limit int) int {
	if m.segments != nil {
		return m.segmentPurePrefix(limit)
	}
	n := min(limit, m.size)
	if m.metadata != nil {
		for i := 0; i < n; i++ {
			meta := m.metadata[(m.head+i)%len(m.buffer)]
			if meta.global != nil || meta.release != nil || meta.request != nil || meta.toRef {
				n = i
				break
			}
		}
	}
	return n
}

func (m *mailbox) takePure(batch []mailboxMessage) int {
	n := m.purePrefix(len(batch))
	if n < 2 {
		return 0
	}
	if m.segments != nil {
		m.takeSegmentPure(batch[:n])
		return n
	}
	first := min(n, len(m.buffer)-m.head)
	copy(batch[:first], m.buffer[m.head:m.head+first])
	copy(batch[first:n], m.buffer[:n-first])
	clear(m.buffer[m.head : m.head+first])
	clear(m.buffer[:n-first])
	m.head = (m.head + n) % len(m.buffer)
	m.size -= n
	return n
}

// Restore only unclaimed pure messages, before any producer's later messages.
// Logical admission already reserved these slots; restoring does not recharge.
func (m *mailbox) prependPure(batch []mailboxMessage) {
	if m.segments == nil && m.size+len(batch) > len(m.buffer) {
		m.grow(m.size + len(batch))
	}
	if m.segments != nil {
		m.prependSegmentPure(batch)
		return
	}
	m.head = (m.head + len(m.buffer) - len(batch)) % len(m.buffer)
	for i, value := range batch {
		index := (m.head + i) % len(m.buffer)
		m.buffer[index] = value
		if m.metadata != nil {
			m.metadata[index] = mailboxMetadata{}
		}
	}
	m.size += len(batch)
}
func (m *mailbox) pop() envelope {
	if m.segments != nil {
		return m.popSegment()
	}
	value := &m.buffer[m.head]
	message := envelope{message: value.message, sender: value.sender}
	*value = mailboxMessage{}
	if m.metadata != nil {
		meta := &m.metadata[m.head]
		message.global, message.release, message.request, message.toRef = meta.global, meta.release, meta.request, meta.toRef
		*meta = mailboxMetadata{}
	}
	m.head = (m.head + 1) % len(m.buffer)
	m.size--
	return message
}

func (m *mailbox) at(index int) envelope {
	if m.segments != nil {
		position := m.head + index
		block := &m.segments.blocks[(m.segments.first+(position>>mailboxBlockShift))&(len(m.segments.blocks)-1)]
		return block.valueAt(position & (mailboxBlockSize - 1))
	}
	return m.valueAt((m.head + index) % len(m.buffer))
}

func (m *mailbox) valueAt(index int) envelope {
	value := m.buffer[index]
	message := envelope{message: value.message, sender: value.sender}
	if m.metadata != nil {
		meta := m.metadata[index]
		message.global, message.release, message.request, message.toRef = meta.global, meta.release, meta.request, meta.toRef
	}
	return message
}
