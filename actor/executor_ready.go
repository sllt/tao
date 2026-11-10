package actor

// executorReady is a FIFO of execution claims, protected by executorCache.mu.
// process.running admits one claim per Actor; taking or closing the queue
// transfers that claim, without publishing the Actor idle again.
type executorReady struct {
	items []*process
	head  int
	size  int
}

func (q *executorReady) push(p *process) {
	if q.size == len(q.items) {
		items := make([]*process, max(16, 2*len(q.items)))
		for i := range q.size {
			items[i] = q.items[(q.head+i)%len(q.items)]
		}
		q.items = items
		q.head = 0
	}
	q.items[(q.head+q.size)%len(q.items)] = p
	q.size++
}

func (q *executorReady) pop() *process {
	if q.size == 0 {
		return nil
	}
	p := q.items[q.head]
	q.items[q.head] = nil
	q.head = (q.head + 1) % len(q.items)
	q.size--
	return p
}
