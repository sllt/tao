package taonet

// fifo is protected by its Channel's mutex. Growth preserves order; steady
// enqueue/dequeue does not shift entries or allocate new slice backing arrays.
type fifo[T any] struct {
	buffer     []T
	head, size int
}

func (q *fifo[T]) len() int { return q.size }
func (q *fifo[T]) peek() T  { return q.buffer[q.head] }

func (q *fifo[T]) push(value T) {
	if q.size == len(q.buffer) {
		buffer := make([]T, max(1, 2*len(q.buffer)))
		first := min(q.size, len(q.buffer)-q.head)
		copy(buffer, q.buffer[q.head:q.head+first])
		copy(buffer[first:], q.buffer[:q.size-first])
		q.buffer, q.head = buffer, 0
	}
	q.buffer[(q.head+q.size)&(len(q.buffer)-1)] = value
	q.size++
}

func (q *fifo[T]) pop() T {
	value := q.buffer[q.head]
	var zero T
	q.buffer[q.head] = zero
	q.head = (q.head + 1) & (len(q.buffer) - 1)
	q.size--
	if q.size == 0 {
		q.head = 0
		if len(q.buffer) > 32 {
			q.buffer = nil
		}
	}
	return value
}
