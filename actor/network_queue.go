package actor

// These operations hold network.mu. A ring keeps reusable storage without
// allocating per pop/append or shifting a full queue during steady traffic.
func (p *actorPipe) queued() int { return p.queueSize }

func (p *actorPipe) jobAt(index int) *networkJob {
	return p.queue[(p.queueHead+index)&(len(p.queue)-1)]
}

func (p *actorPipe) appendJob(job *networkJob) {
	if p.queueSize == len(p.queue) {
		buffer := make([]*networkJob, max(1, 2*len(p.queue)))
		first := min(p.queueSize, len(p.queue)-p.queueHead)
		copy(buffer, p.queue[p.queueHead:p.queueHead+first])
		copy(buffer[first:], p.queue[:p.queueSize-first])
		p.queue = buffer
		p.queueHead = 0
	}
	p.queue[(p.queueHead+p.queueSize)&(len(p.queue)-1)] = job
	p.queueSize++
}

func (p *actorPipe) dropJobs(n int) {
	first := min(n, len(p.queue)-p.queueHead)
	clear(p.queue[p.queueHead : p.queueHead+first])
	clear(p.queue[:n-first])
	p.queueHead = (p.queueHead + n) & (len(p.queue) - 1)
	p.queueSize -= n
	if p.queueSize != 0 {
		return
	}
	if len(p.queue) > actorPipeWindow {
		p.queue = nil
	}
	p.queueHead = 0
}
