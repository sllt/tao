package actor

const (
	mailboxSmallLimit = 64
	mailboxBlockShift = 8
	mailboxBlockSize  = 1 << mailboxBlockShift
)

type mailboxDataBlock [mailboxBlockSize]mailboxMessage
type mailboxMetaBlock [mailboxBlockSize]mailboxMetadata

// A bounded cache, not a pool proportional to peak backlog. A returned block
// has no readers: batch dequeue copies values, and consumed slots are cleared.
var mailboxDataBlocks = make(chan *mailboxDataBlock, 32)
var mailboxMetaBlocks = make(chan *mailboxMetaBlock, 32)

type mailboxBlock struct {
	data *mailboxDataBlock
	meta *mailboxMetaBlock
}

type mailboxSegments struct {
	blocks []mailboxBlock // power-of-two directory; growth copies pointers only
	first  int
}

func takeMailboxData() *mailboxDataBlock {
	select {
	case block := <-mailboxDataBlocks:
		return block
	default:
		return new(mailboxDataBlock)
	}
}

func takeMailboxMeta() *mailboxMetaBlock {
	select {
	case block := <-mailboxMetaBlocks:
		return block
	default:
		return new(mailboxMetaBlock)
	}
}

func (block *mailboxBlock) release() {
	if block.data != nil {
		select {
		case mailboxDataBlocks <- block.data:
		default:
		}
	}
	if block.meta != nil {
		select {
		case mailboxMetaBlocks <- block.meta:
		default:
		}
	}
	*block = mailboxBlock{}
}

func (block *mailboxBlock) valueAt(index int) envelope {
	value := block.data[index]
	env := envelope{message: value.message, sender: value.sender}
	if block.meta != nil {
		meta := block.meta[index]
		env.global, env.release, env.request, env.toRef = meta.global, meta.release, meta.request, meta.toRef
	}
	return env
}

func (segments *mailboxSegments) ensure(required int) {
	if required <= len(segments.blocks) {
		return
	}
	blocks := make([]mailboxBlock, max(required, len(segments.blocks)*2))
	for i := range segments.blocks {
		blocks[i] = segments.blocks[(segments.first+i)&(len(segments.blocks)-1)]
	}
	segments.blocks, segments.first = blocks, 0
}

func (m *mailbox) promote() {
	block := mailboxBlock{data: takeMailboxData()}
	if m.metadata != nil {
		block.meta = takeMailboxMeta()
	}
	for i := 0; i < m.size; i++ {
		index := (m.head + i) % len(m.buffer)
		block.data[i] = m.buffer[index]
		if block.meta != nil {
			block.meta[i] = m.metadata[index]
		}
	}
	clear(m.buffer)
	clear(m.metadata)
	m.segments = &mailboxSegments{blocks: make([]mailboxBlock, 4)}
	m.segments.blocks[0] = block
	m.head = 0
}

func (m *mailbox) pushSegment(message envelope) {
	segments := m.segments
	position := m.head + m.size
	index := position >> mailboxBlockShift
	segments.ensure(index + 1)
	block := &segments.blocks[(segments.first+index)&(len(segments.blocks)-1)]
	if block.data == nil {
		block.data = takeMailboxData()
	}
	offset := position & (mailboxBlockSize - 1)
	block.data[offset] = mailboxMessage{message: message.message, sender: message.sender}
	if block.meta == nil && (message.global != nil || message.release != nil || message.request != nil || message.toRef) {
		block.meta = takeMailboxMeta()
	}
	if block.meta != nil {
		block.meta[offset] = mailboxMetadata{global: message.global, release: message.release, request: message.request, toRef: message.toRef}
	}
	m.size++
}

func (m *mailbox) segmentPurePrefix(limit int) int {
	n := min(limit, m.size)
	for examined := 0; examined < n; {
		position := m.head + examined
		block := &m.segments.blocks[(m.segments.first+(position>>mailboxBlockShift))&(len(m.segments.blocks)-1)]
		offset := position & (mailboxBlockSize - 1)
		count := min(n-examined, mailboxBlockSize-offset)
		if block.meta != nil {
			for i := range count {
				meta := block.meta[offset+i]
				if meta.global != nil || meta.release != nil || meta.request != nil || meta.toRef {
					return examined + i
				}
			}
		}
		examined += count
	}
	return n
}

func (m *mailbox) takeSegmentPure(batch []mailboxMessage) {
	for copied := 0; copied < len(batch); {
		position := m.head + copied
		block := &m.segments.blocks[(m.segments.first+(position>>mailboxBlockShift))&(len(m.segments.blocks)-1)]
		offset := position & (mailboxBlockSize - 1)
		count := min(len(batch)-copied, mailboxBlockSize-offset)
		copy(batch[copied:copied+count], block.data[offset:offset+count])
		clear(block.data[offset : offset+count])
		copied += count
	}
	m.advanceSegment(len(batch))
}

func (m *mailbox) popSegment() envelope {
	block := &m.segments.blocks[m.segments.first]
	message := block.valueAt(m.head)
	block.data[m.head] = mailboxMessage{}
	if block.meta != nil {
		block.meta[m.head] = mailboxMetadata{}
	}
	m.advanceSegment(1)
	return message
}

func (m *mailbox) advanceSegment(count int) {
	segments := m.segments
	m.size -= count
	for count > 0 {
		consumed := min(count, mailboxBlockSize-m.head)
		m.head += consumed
		count -= consumed
		if m.head == mailboxBlockSize {
			segments.blocks[segments.first].release()
			segments.first = (segments.first + 1) & (len(segments.blocks) - 1)
			m.head = 0
		}
	}
	if m.size == 0 {
		segments.blocks[segments.first].release()
		m.segments = nil
		m.head = 0
	}
}

func (m *mailbox) prependSegmentPure(batch []mailboxMessage) {
	segments := m.segments
	for i := len(batch) - 1; i >= 0; i-- {
		if m.head == 0 {
			occupied := (m.size + mailboxBlockSize - 1) >> mailboxBlockShift
			segments.ensure(occupied + 1)
			segments.first = (segments.first - 1) & (len(segments.blocks) - 1)
			segments.blocks[segments.first].data = takeMailboxData()
			m.head = mailboxBlockSize
		}
		m.head--
		segments.blocks[segments.first].data[m.head] = batch[i]
		m.size++
	}
}
