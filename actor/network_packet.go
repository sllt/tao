package actor

import (
	"encoding/binary"
	"math"
	"unicode/utf8"
)

// HA/2 encodes framework metadata, not arbitrary Go values. The business
// payload is an opaque length-delimited byte string; its codec is independent.
const (
	packetTarget uint16 = 1 << iota
	packetSender
	packetKey
	packetMessage
	packetError
	packetMove
	packetMonitor
	packetFrom
	packetLink
	packetScope
	packetSequence
	packetBudget
	packetKnownFlags = packetBudget*2 - 1
)

var packetKinds = [...]string{"", "send", "call", "reply", "cancel", "cancel_moved", "monitor", "demonitor", "down", "link", "unlink", "exit", "reject"}

type packetEncoder struct {
	data []byte
	err  error
}

func (e *packetEncoder) u64(v uint64) { e.data = binary.BigEndian.AppendUint64(e.data, v) }
func (e *packetEncoder) text(s string) {
	if len(s) > math.MaxUint16 || !utf8.ValidString(s) {
		e.err = ErrInvalidMessage
		return
	}
	e.data = binary.BigEndian.AppendUint16(e.data, uint16(len(s)))
	e.data = append(e.data, s...)
}
func (e *packetEncoder) blob(p []byte) {
	if uint64(len(p)) > math.MaxUint32 {
		e.err = ErrMessageTooLarge
		return
	}
	e.data = binary.BigEndian.AppendUint32(e.data, uint32(len(p)))
	e.data = append(e.data, p...)
}
func (e *packetEncoder) pid(p PID) {
	e.text(p.Node)
	e.data = append(e.data, p.BootID[:]...)
	e.u64(p.ID)
}
func (e *packetEncoder) message(m EncodedMessage) {
	e.text(m.Codec)
	e.text(m.Type)
	e.data = binary.BigEndian.AppendUint32(e.data, m.Version)
	e.blob(m.Data)
}
func (e *packetEncoder) route(r RouteRecord) {
	e.text(r.Path)
	e.pid(r.PID())
	e.u64(r.Epoch)
	e.u64(uint64(r.Revision))
	e.text(string(r.State))
	e.data = append(e.data, r.MoveID[:]...)
	e.text(r.Kind)
	e.text(r.Version)
}
func (e *packetEncoder) address(a wireAddress) {
	switch {
	case a.Kind == "pid" && a.PID != nil && a.Ref == nil && a.Route == nil:
		e.data = append(e.data, 1)
		e.pid(*a.PID)
	case a.Kind == "ref" && a.Ref != nil && a.PID == nil && a.Route == nil:
		e.data = append(e.data, 2)
		e.text(a.Ref.Node)
		e.text(a.Ref.Path)
	case a.Kind == "global" && a.Route != nil && a.PID == nil && a.Ref == nil:
		e.data = append(e.data, 3)
		e.route(*a.Route)
	default:
		e.err = ErrInvalidMessage
	}
}

func marshalPacket(p actorPacket) ([]byte, error) {
	return appendPacket(make([]byte, 0, 256+len(p.Message.Data)), p)
}

func appendPacket(dst []byte, p actorPacket) ([]byte, error) {
	return appendPacketValue(dst, p, nil)
}

func appendPacketValue(dst []byte, p actorPacket, value func([]byte) ([]byte, error)) ([]byte, error) {
	var kind byte
	for i := 1; i < len(packetKinds); i++ {
		if p.Kind == packetKinds[i] {
			kind = byte(i)
			break
		}
	}
	if kind == 0 {
		return nil, ErrInvalidMessage
	}
	start := len(dst)
	e := packetEncoder{data: append(dst, 'H', 'A', 2, kind, 0, 0)}
	var flags uint16
	if p.Target != (wireAddress{}) {
		flags |= packetTarget
		e.address(p.Target)
	}
	if p.Sender != nil {
		flags |= packetSender
		e.pid(*p.Sender)
	}
	if p.Key != (requestKey{}) {
		flags |= packetKey
		e.text(p.Key.Caller)
		e.data = append(e.data, p.Key.Boot[:]...)
		e.u64(p.Key.ID)
	}
	if p.Message.Codec != "" || p.Message.Type != "" || p.Message.Version != 0 || len(p.Message.Data) != 0 {
		flags |= packetMessage
		if value == nil {
			e.message(p.Message)
		} else {
			e.text(p.Message.Codec)
			e.text(p.Message.Type)
			e.data = binary.BigEndian.AppendUint32(e.data, p.Message.Version)
			lengthAt := len(e.data)
			e.data = binary.BigEndian.AppendUint32(e.data, 0)
			if e.err == nil {
				e.data, e.err = value(e.data)
				if e.err != nil {
					return nil, e.err
				}
				binary.BigEndian.PutUint32(e.data[lengthAt:lengthAt+4], uint32(len(e.data)-lengthAt-4))
			}
		}
	}
	if p.Error != nil {
		flags |= packetError
		e.text(p.Error.Code)
		e.text(p.Error.Message)
	}
	if p.Move != (MoveID{}) {
		flags |= packetMove
		e.data = append(e.data, p.Move[:]...)
	}
	if p.Monitor != nil {
		flags |= packetMonitor
		e.pid(p.Monitor.Observer)
		e.u64(p.Monitor.ID)
	}
	if p.From != nil {
		flags |= packetFrom
		e.pid(*p.From)
	}
	if p.Link != (LinkID{}) {
		flags |= packetLink
		e.data = append(e.data, p.Link[:]...)
	}
	if p.Scope != (wireScope{}) {
		flags |= packetScope
		e.data = append(e.data, p.Scope.Session[:]...)
		e.u64(p.Scope.Channel)
	}
	if p.Sequence != 0 {
		flags |= packetSequence
		e.u64(p.Sequence)
	}
	if p.BudgetMS != 0 {
		flags |= packetBudget
		e.u64(p.BudgetMS)
	}
	binary.BigEndian.PutUint16(e.data[start+4:start+6], flags)
	return e.data, e.err
}

type packetDecoder struct {
	targetPID     *PID // optional inbox-owned scratch; consumed before the next decode
	names         map[string]string
	local, remote string
	data          []byte
	err           error
}

// take checks lengths before slicing/allocation. Fields decoded after an error
// return zero values; the enclosing packet is discarded at the boundary.
func (d *packetDecoder) take(n uint64) []byte {
	if d.err != nil || n > uint64(len(d.data)) {
		d.err = ErrInvalidMessage
		return nil
	}
	p := d.data[:n]
	d.data = d.data[n:]
	return p
}
func (d *packetDecoder) u64() uint64 {
	p := d.take(8)
	if len(p) == 0 {
		return 0
	}
	return binary.BigEndian.Uint64(p)
}
func (d *packetDecoder) u32() uint32 {
	p := d.take(4)
	if len(p) == 0 {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}
func (d *packetDecoder) text() string {
	h := d.take(2)
	if len(h) == 0 {
		return ""
	}
	p := d.take(uint64(binary.BigEndian.Uint16(h)))
	// These strings were validated at registration/handshake and already have
	// independent lifetimes. A []byte map lookup/comparison does not allocate;
	// unknown names still receive an owned copy and the normal UTF-8 check.
	if string(p) == d.local {
		return d.local
	}
	if string(p) == d.remote {
		return d.remote
	}
	if name, ok := d.names[string(p)]; ok {
		return name
	}
	if !utf8.Valid(p) {
		d.err = ErrInvalidMessage
	}
	return string(p)
}
func (d *packetDecoder) pid() PID {
	p := PID{Node: d.text()}
	copy(p.BootID[:], d.take(16))
	p.ID = d.u64()
	return p
}
func (d *packetDecoder) message() EncodedMessage {
	m := EncodedMessage{Codec: d.text(), Type: d.text(), Version: d.u32()}
	m.Data = d.take(uint64(d.u32()))
	return m
}
func (d *packetDecoder) route() RouteRecord {
	r := RouteRecord{Path: d.text()}
	p := d.pid()
	r.Owner, r.BootID, r.ProcessID = p.Node, p.BootID, p.ID
	r.Epoch, r.Revision = d.u64(), int64(d.u64())
	r.State = RouteState(d.text())
	copy(r.MoveID[:], d.take(16))
	r.Kind, r.Version = d.text(), d.text()
	return r
}
func (d *packetDecoder) address() wireAddress {
	kind := d.take(1)
	if len(kind) == 0 {
		return wireAddress{}
	}
	switch kind[0] {
	case 1:
		if d.targetPID != nil {
			*d.targetPID = d.pid()
			return wireAddress{Kind: "pid", PID: d.targetPID}
		}
		p := d.pid()
		return wireAddress{Kind: "pid", PID: &p}
	case 2:
		return wireAddress{Kind: "ref", Ref: &Ref{Node: d.text(), Path: d.text()}}
	case 3:
		r := d.route()
		return wireAddress{Kind: "global", Route: &r}
	default:
		d.err = ErrInvalidMessage
		return wireAddress{}
	}
}

// Message.Data aliases input until the delivery is released. Registry.Decode
// must finish before Release, and codecs must return independently owned data.
func unmarshalPacket(data []byte) (actorPacket, error) {
	return unmarshalPacketNames(data, nil, "", "")
}

func unmarshalPacketNames(data []byte, names map[string]string, local, remote string) (actorPacket, error) {
	return unmarshalPacketInto(data, names, local, remote, nil)
}

func unmarshalPacketInto(data []byte, names map[string]string, local, remote string, targetPID *PID) (actorPacket, error) {
	var p actorPacket
	if len(data) < 6 || data[0] != 'H' || data[1] != 'A' || data[2] != 2 || data[3] == 0 || int(data[3]) >= len(packetKinds) {
		return p, ErrInvalidMessage
	}
	p.Kind = packetKinds[data[3]]
	flags := binary.BigEndian.Uint16(data[4:6])
	if flags & ^uint16(packetKnownFlags) != 0 {
		return p, ErrInvalidMessage
	}
	d := packetDecoder{data: data[6:], names: names, local: local, remote: remote, targetPID: targetPID}
	if flags&packetTarget != 0 {
		p.Target = d.address()
	}
	if flags&packetSender != 0 {
		v := d.pid()
		p.Sender = &v
	}
	if flags&packetKey != 0 {
		p.Key.Caller = d.text()
		copy(p.Key.Boot[:], d.take(16))
		p.Key.ID = d.u64()
	}
	if flags&packetMessage != 0 {
		p.Message = d.message()
	}
	if flags&packetError != 0 {
		p.Error = &wireActorError{Code: d.text(), Message: d.text()}
	}
	if flags&packetMove != 0 {
		copy(p.Move[:], d.take(16))
	}
	if flags&packetMonitor != 0 {
		p.Monitor = &MonitorRef{Observer: d.pid(), ID: d.u64()}
	}
	if flags&packetFrom != 0 {
		v := d.pid()
		p.From = &v
	}
	if flags&packetLink != 0 {
		copy(p.Link[:], d.take(16))
	}
	if flags&packetScope != 0 {
		copy(p.Scope.Session[:], d.take(16))
		p.Scope.Channel = d.u64()
	}
	if flags&packetSequence != 0 {
		p.Sequence = d.u64()
	}
	if flags&packetBudget != 0 {
		p.BudgetMS = d.u64()
	}
	if d.err != nil || len(d.data) != 0 {
		return actorPacket{}, ErrInvalidMessage
	}
	return p, nil
}
