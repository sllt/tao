package taonet

import (
	"errors"
	"sync"
	"testing"
)

func TestReceiptWaitersRaceWithCompletionAndKeepStableSignals(t *testing.T) {
	for _, early := range []bool{false, true} {
		var receipt SendReceipt
		var firstDone, firstReleased <-chan struct{}
		if early {
			firstDone, firstReleased = receipt.Done(), receipt.Released()
		}
		want := errors.New("write failed")
		start := make(chan struct{})
		var waiters sync.WaitGroup
		for range 32 {
			waiters.Go(func() {
				<-start
				done, released := receipt.Done(), receipt.Released()
				if err := receipt.Wait(t.Context()); err != want {
					t.Errorf("unpublished result: %v", err)
				}
				if !receipt.IsDone() {
					t.Error("completed waiter remained unready")
				}
				select {
				case <-released:
				case <-t.Context().Done():
					t.Error("input release did not wake waiter")
				}
				if receipt.Done() != done || receipt.Released() != released {
					t.Error("notification channel identity changed")
				}
			})
		}
		close(start)
		receipt.err = want
		receipt.done.finish()
		// A failed write may settle before the socket stops reading input.
		select {
		case <-receipt.Released():
			t.Fatal("completion incorrectly released input")
		default:
		}
		receipt.released.finish()
		waiters.Wait()
		if early && (receipt.Done() != firstDone || receipt.Released() != firstReleased) {
			t.Fatal("early waiter lost its completion signal")
		}
	}
}

func TestReceiptPollingDoesNotAllocateOrLoseExistingWaiterCompletion(t *testing.T) {
	var receipt SendReceipt
	if allocations := testing.AllocsPerRun(1000, func() {
		if receipt.IsDone() {
			t.Fatal("unsettled receipt appeared complete")
		}
	}); allocations != 0 {
		t.Fatalf("nonblocking polling allocated %g times", allocations)
	}
	if receipt.done.signal.Load() != nil {
		t.Fatal("polling materialized a waiting notification")
	}
	written := (*WriteReceipt)(&receipt)
	if written.IsDone() {
		t.Fatal("unsettled write appeared complete")
	}
	done := written.Done()
	if written.IsDone() {
		t.Fatal("creating a waiter completed the write")
	}
	want := errors.New("write failed")
	receipt.err = want
	receipt.done.finish()
	if !receipt.IsDone() || !written.IsDone() || written.Done() != done {
		t.Fatal("polling lost an existing waiter's completion or signal identity")
	}
	if err := written.Wait(t.Context()); err != want {
		t.Fatalf("completion result: %v", err)
	}
	if receipt.released.ready() {
		t.Fatal("polling completion released borrowed input")
	}
	var early WriteReceipt
	early.done.finish()
	if !early.IsDone() {
		t.Fatal("completion before any waiter was not visible")
	}
}

func TestReceiptCompletedBeforeWaitDoesNotRetainChannelOwner(t *testing.T) {
	var receipt SendReceipt
	receipt.done.finish()
	receipt.released.finish()
	if err := receipt.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-receipt.Released():
	default:
		t.Fatal("completed input remained borrowed")
	}
	// A late observer needs neither a private channel nor a reference back to
	// the Channel/Session. The receipt remains independently readable.
	if receipt.Done() != releasedInput || receipt.Released() != releasedInput {
		t.Fatal("late observer allocated another completion signal")
	}
}
