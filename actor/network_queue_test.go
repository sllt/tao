package actor

import "testing"

func TestPipeQueueKeepsFIFOAcrossWrapGrowthAndDrain(t *testing.T) {
	var pipe actorPipe
	next, want := uint64(1), uint64(1)
	push := func(count int) {
		for range count {
			pipe.appendJob(&networkJob{packet: actorPacket{Key: requestKey{ID: next}}})
			next++
		}
	}
	pop := func(count int) {
		for i := range count {
			if got := pipe.jobAt(i).packet.Key.ID; got != want {
				t.Fatalf("queue reordered: got %d want %d", got, want)
			}
			want++
		}
		pipe.dropJobs(count)
	}
	push(16)
	pop(7)
	push(7)
	pop(5)
	push(5)
	pop(14)
	push(65)
	pop(pipe.queued())
	if len(pipe.queue) != 0 {
		t.Fatal("drained large burst retained its queue storage")
	}
	for range 100 {
		push(4)
		pop(2)
		push(2)
		pop(4)
	}
	for _, job := range pipe.queue {
		if job != nil {
			t.Fatal("idle queue retained a job")
		}
	}
}
