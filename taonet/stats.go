package taonet

type SessionStats struct {
	Operations             int
	Handlers               int
	MessageReceiveReserved int
	MessageSendReserved    int
	MuxStreams             int
	MuxReceiveReserved     uint64
	MuxSendQueued          int
	MuxControlQueued       int
	Draining               bool
	Closed                 bool
	RequestFailures        uint64
	UnknownResults         uint64
	AsyncFailures          uint64
	DroppedEvents          uint64
}

// Stats samples application and mux counters independently. Reservations are
// logical capacities, not an estimate of the process's total heap.
func (s *Session) Stats() SessionStats {
	s.mu.Lock()
	stats := SessionStats{Operations: len(s.operations), Handlers: s.handlers, MessageReceiveReserved: s.receivedBytes, MessageSendReserved: s.sentBytes, Draining: s.draining, Closed: s.err != nil, RequestFailures: s.requestFailures, UnknownResults: s.unknownResults, AsyncFailures: s.asyncFailures, DroppedEvents: s.droppedEvents}
	s.mu.Unlock()
	raw := s.raw.Stats()
	stats.MuxStreams = raw.Streams
	stats.MuxReceiveReserved = raw.ReceiveReserved
	stats.MuxSendQueued = raw.DataQueued
	stats.MuxControlQueued = raw.ControlQueued
	return stats
}

type EndpointStats struct {
	Sessions       int
	Handshakes     int
	ReservedBytes  int
	DialAttempts   uint64
	DialFailures   uint64
	ReusedSessions uint64
	DroppedEvents  uint64
}

func (e *Endpoint) Stats() EndpointStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return EndpointStats{Sessions: len(e.sessions), Handshakes: e.pending, ReservedBytes: e.reservedBytes, DialAttempts: e.dialAttempts, DialFailures: e.dialFailures, ReusedSessions: e.reused, DroppedEvents: e.droppedEvents}
}
