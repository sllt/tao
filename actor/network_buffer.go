package actor

// Bound idle storage independently of active send admission.
const networkCacheEntries = 256

const (
	packetInlineBytes       = 4096
	packetBufferRetainLimit = (128 << 10) + 4096
)

func packetCacheBudget(sendBytes int) int { return min(2<<20, sendBytes/4) }

func (w *network) takeJob() *networkJob {
	select {
	case job := <-w.jobs:
		return job
	default:
		return &networkJob{}
	}
}

// Data jobs are private to the writer. Recycle only after settlement and
// input release; control jobs remain externally referenced by their waiters.
func (w *network) recycleJob(job *networkJob) {
	if job.token != nil {
		return
	}
	*job = networkJob{}
	select {
	case w.jobs <- job:
	default:
	}
}

// Growth arrays are reused only after transport input release. Inline storage
// remains embedded, so both allocations must be charged while a job owns them.
type packetBuffer struct {
	data    []byte
	storage [packetInlineBytes]byte
}

func (b *packetBuffer) retainedBytes() int {
	bytes := len(b.storage)
	if cap(b.data) > 0 && &b.data[:1][0] != &b.storage[0] {
		bytes += cap(b.data)
	}
	return bytes
}

func (w *network) takePacketBuffer() *packetBuffer {
	select {
	case buffer := <-w.packetBuffers:
		w.packetCacheBytes.Add(-int64(buffer.retainedBytes()))
		return buffer
	default:
		buffer := &packetBuffer{}
		buffer.data = buffer.storage[:0]
		return buffer
	}
}

func (w *network) putPacketBuffer(buffer *packetBuffer) {
	if cap(buffer.data) <= len(buffer.storage) || cap(buffer.data) > packetBufferRetainLimit {
		buffer.data = buffer.storage[:0]
	} else {
		buffer.data = buffer.data[:0]
	}
	// Charge the actual idle storage, including the embedded array alongside
	// a growth allocation. Empty channel slots retain no packetBuffer storage.
	limit := int64(packetCacheBudget(w.opts.SendBytes))
	charge := int64(buffer.retainedBytes())
	for {
		used := w.packetCacheBytes.Load()
		if charge > limit-used {
			if charge == packetInlineBytes {
				return
			}
			buffer.data = buffer.storage[:0]
			charge = packetInlineBytes
			continue
		}
		if w.packetCacheBytes.CompareAndSwap(used, used+charge) {
			break
		}
	}
	select {
	case w.packetBuffers <- buffer:
	default:
		w.packetCacheBytes.Add(-charge)
	}
}

func (w *network) releaseJobBuffer(job *networkJob) {
	buffer := job.buffer
	job.raw, job.buffer, job.packet.Message.Data = nil, nil, nil
	if buffer != nil {
		w.putPacketBuffer(buffer)
	}
}
