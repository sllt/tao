package actor

import "encoding/binary"

var handoffKinds = [...]string{"", "begin", "message", "skip", "barrier", "prepared"}

func marshalHandoffFrame(f handoffFrame) ([]byte, error) {
	var kind byte
	for i := 1; i < len(handoffKinds); i++ {
		if f.Kind == handoffKinds[i] {
			kind = byte(i)
			break
		}
	}
	if kind == 0 {
		return nil, ErrInvalidMessage
	}
	size := 256 + len(f.Snapshot)
	if f.Envelope != nil {
		size += len(f.Envelope.Message.Data)
	}
	e := packetEncoder{data: make([]byte, 4, size)}
	e.data = append(e.data, 'H', 'M', 2, kind)
	e.data = append(e.data, f.Move[:]...)
	e.u64(f.Sequence)
	switch f.Kind {
	case "begin":
		e.blob(f.Snapshot)
	case "message":
		if f.Envelope == nil {
			return nil, ErrInvalidMessage
		}
		m := f.Envelope
		e.message(m.Message)
		if m.Sender == nil {
			e.data = append(e.data, 0)
		} else {
			e.data = append(e.data, 1)
			e.pid(*m.Sender)
		}
		e.text(m.Call.Caller)
		e.data = append(e.data, m.Call.Boot[:]...)
		e.u64(m.Call.ID)
		e.u64(m.BudgetMS)
	case "barrier", "prepared":
		e.data = append(e.data, f.Digest[:]...)
	}
	binary.BigEndian.PutUint32(e.data[:4], uint32(len(e.data)-4))
	return e.data, e.err
}

func unmarshalHandoffFrame(data []byte) (handoffFrame, error) {
	var f handoffFrame
	if len(data) < 4 || data[0] != 'H' || data[1] != 'M' || data[2] != 2 || data[3] == 0 || int(data[3]) >= len(handoffKinds) {
		return f, ErrInvalidMessage
	}
	f.Kind = handoffKinds[data[3]]
	d := packetDecoder{data: data[4:]}
	copy(f.Move[:], d.take(16))
	f.Sequence = d.u64()
	switch f.Kind {
	case "begin":
		f.Snapshot = d.take(uint64(d.u32()))
	case "message":
		m := &handoffEnvelope{Message: d.message()}
		sender := d.take(1)
		if len(sender) > 0 {
			switch sender[0] {
			case 0:
			case 1:
				p := d.pid()
				m.Sender = &p
			default:
				d.err = ErrInvalidMessage
			}
		}
		m.Call.Caller = d.text()
		copy(m.Call.Boot[:], d.take(16))
		m.Call.ID, m.BudgetMS = d.u64(), d.u64()
		f.Envelope = m
	case "barrier", "prepared":
		copy(f.Digest[:], d.take(32))
	}
	if d.err != nil || len(d.data) != 0 {
		return handoffFrame{}, ErrInvalidMessage
	}
	return f, nil
}
