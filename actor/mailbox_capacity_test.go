package actor

import (
	"errors"
	"fmt"
	"testing"
)

func TestMailboxAdmissionWithUnclaimedBatchBoundary(t *testing.T) {
	batch := len(mailboxBatch{})
	for _, capacity := range []int{batch - 1, batch, batch + 1, 3 * batch} {
		for _, interrupted := range []bool{false, true} {
			t.Run(fmt.Sprintf("capacity=%d/interrupted=%t", capacity, interrupted), func(t *testing.T) {
				// This is the state after detaching a batch under mu, before
				// the executor claims its first item. The producer can run now.
				p := &process{mailbox: mailbox{limit: capacity}, running: true}
				held := min(batch, capacity)
				state := uint32(held)
				if interrupted {
					state |= batchInterrupted
				}
				p.batchState.Store(state)
				for i := range capacity - held {
					if err := p.enqueue(envelope{message: i}); err != nil {
						t.Fatal(err)
					}
				}
				if err := p.enqueue(envelope{message: "full"}); !errors.Is(err, ErrMailboxFull) {
					t.Fatalf("unclaimed suffix escaped capacity: %v", err)
				}
				p.mu.Lock()
				queued := p.queuedLocked()
				p.mu.Unlock()
				if queued != capacity {
					t.Fatalf("queued=%d want %d", queued, capacity)
				}
				// Another legal snapshot has one fewer unclaimed message. If
				// interrupted is set, that claim preceded the interruption.
				p.batchState.Store(state - 1)
				if err := p.enqueue(envelope{message: "freed"}); err != nil {
					t.Fatal(err)
				}
				if err := p.enqueue(envelope{message: "overflow"}); !errors.Is(err, ErrMailboxFull) {
					t.Fatalf("one claim freed more than one slot: %v", err)
				}
			})
		}
	}
}
