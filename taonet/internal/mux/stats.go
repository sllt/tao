package mux

type Stats struct {
	Streams         int
	ReceiveReserved uint64
	DataQueued      int
	ControlQueued   int
}

func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Streams: s.localCount + s.peerCount, ReceiveReserved: s.receiveReserved, DataQueued: s.dataBytes, ControlQueued: s.controlBytes + s.controlDataBytes}
}
