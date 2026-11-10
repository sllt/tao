package actor

import (
	"context"
	"testing"
	"time"
)

func TestReplyBatchReturnsAllCreditAndWakesAdmission(t *testing.T) {
	w := &network{ctx: t.Context(), opts: NetworkOptions{SendBytes: 1 << 20, SendMessages: 3}, packetBuffers: make(chan *packetBuffer, 3), jobs: make(chan *networkJob, 3)}
	jobs := make([]*networkJob, 3)
	for i := range jobs {
		buffer := w.takePacketBuffer()
		buffer.data = buffer.storage[:16]
		charge := buffer.retainedBytes()
		if err := w.reserve(charge); err != nil {
			t.Fatal(err)
		}
		jobs[i] = &networkJob{buffer: buffer, raw: buffer.data, charge: charge}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	admitted := make(chan error, 1)
	go func() { admitted <- w.reserveCall(ctx, 1) }()
	eventually(t, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.sendChanged != nil
	})
	w.releaseReplyBatch(jobs)
	if err := <-admitted; err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	bytes, messages := w.bytes, w.messages
	w.mu.Unlock()
	if bytes != 1 || messages != 1 {
		t.Fatalf("batch credit: bytes=%d messages=%d", bytes, messages)
	}
	for _, job := range jobs {
		if job != nil {
			t.Fatal("batch retained an input after release")
		}
	}
	if len(w.packetBuffers) != 3 || len(w.jobs) != 3 {
		t.Fatal("batch did not recycle every input/job")
	}
	w.release(1)
}
