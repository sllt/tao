package mux

import "testing"

func TestSchedulerAccountsForBytes(t *testing.T) {
	cfg, err := (Config{SendBudget: 4 << 20}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	s := &Session{cfg: cfg, maxData: cfg.MaxDataPayload, changed: make(chan struct{}), streams: make(map[uint64]*Stream)}
	large := s.newStreamLocked(3, 1<<20, true)
	small := s.newStreamLocked(5, 1<<20, true)
	for _, st := range []*Stream{large, small} {
		size := 1024
		if st == large {
			size = 32 << 10
		}
		for offset := 0; offset < 1<<20; offset += size {
			s.enqueueDataLocked(st, header{kind: typeData, length: uint32(size), stream: st.id, value: uint64(offset)}, make([]byte, size))
		}
	}
	var largeBytes, smallBytes int
	for range 300 {
		q := s.nextLocked()
		if q == nil {
			t.Fatal("scheduler stopped with queued frames")
		}
		cost := headerSize + int(q.h.length)
		if q.st == large {
			largeBytes += cost
		} else {
			smallBytes += cost
		}
		s.finishLocked(q, nil)
		difference := largeBytes - smallBytes
		if difference < 0 {
			difference = -difference
		}
		if difference > (16<<10)+headerSize+int(s.maxData) {
			t.Fatalf("byte unfairness: large=%d small=%d", largeBytes, smallBytes)
		}
	}
	if largeBytes == 0 || smallBytes == 0 {
		t.Fatal("a stream was starved")
	}
}

func TestSchedulerKeepsFINBehindData(t *testing.T) {
	cfg, _ := (Config{}).normalized()
	s := &Session{cfg: cfg, maxData: cfg.MaxDataPayload, changed: make(chan struct{}), streams: make(map[uint64]*Stream)}
	st := s.newStreamLocked(3, 4096, true)
	s.enqueueDataLocked(st, header{kind: typeData, length: 3, stream: 3}, []byte("abc"))
	s.enqueueDataLocked(st, header{kind: typeFin, stream: 3, value: 3}, nil)
	_, err := s.enqueueControlLocked(header{kind: typeWindow, stream: 3}, st)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []frameType{typeWindow, typeData, typeFin} {
		q := s.nextLocked()
		if q == nil || q.h.kind != kind {
			t.Fatalf("wanted frame %d, got %+v", kind, q)
		}
		s.finishLocked(q, nil)
	}
}
