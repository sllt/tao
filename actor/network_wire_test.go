package actor

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func fullPacket() actorPacket {
	pid := PID{Node: "node", BootID: [16]byte{1}, ID: 42}
	route := RouteRecord{Path: "app/worker", Owner: pid.Node, BootID: pid.BootID, ProcessID: pid.ID, Epoch: 9, Revision: 12, State: RouteActive, MoveID: MoveID{2}, Kind: "worker", Version: "1"}
	return actorPacket{Kind: "call", Target: wireAddress{Kind: "global", Route: &route}, Sender: &pid, Key: requestKey{"caller", [16]byte{3}, 7}, Message: EncodedMessage{Type: "text", Version: 1, Codec: "json.v1", Data: []byte(`"hello"`)}, Error: &wireActorError{"example", "说明"}, Move: MoveID{4}, Monitor: &MonitorRef{pid, 5}, From: &pid, Link: LinkID{6}, Scope: wireScope{Channel: 10}, Sequence: 11, BudgetMS: 1234}
}

func TestPacketNamesRemainOwnedAfterInputReuse(t *testing.T) {
	registry, err := NewRegistry([]MessageType{{Name: "text", Version: 1, Sample: ""}}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	want := fullPacket()
	want.Message.Codec = registry.encoding
	data, err := marshalPacket(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := unmarshalPacketNames(data, registry.wireNames, "node", "caller")
	if err != nil {
		t.Fatal(err)
	}
	clear(data)
	// Only the business body is borrowed. Both canonical and unknown metadata
	// must remain independent of the input reused by the connection reader.
	got.Message.Data, want.Message.Data = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("packet metadata retained input aliases: %+v", got)
	}
	got.Message.Version++
	if _, err := registry.Decode(got.Message); !errors.Is(err, ErrUnregisteredType) {
		t.Fatalf("canonical name bypassed schema-version validation: %v", err)
	}
}

func TestPacketBinaryRoundTripAndVersionRejection(t *testing.T) {
	minimal := actorPacket{Kind: "cancel"}
	encoded, err := marshalPacket(minimal)
	if err != nil || !bytes.Equal(encoded, []byte{'H', 'A', 2, 4, 0, 0}) {
		t.Fatalf("minimal wire vector: %x %v", encoded, err)
	}
	full := fullPacket()
	packets := []actorPacket{minimal, full}
	for _, target := range []wireAddress{{Kind: "pid", PID: full.From}, {Kind: "ref", Ref: &Ref{Node: "node", Path: "worker"}}} {
		p := full
		p.Target = target
		packets = append(packets, p)
	}
	for _, want := range packets {
		for _, kind := range packetKinds[1:] {
			want.Kind = kind
			data, err := marshalPacket(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := unmarshalPacket(data)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s round trip: %+v %v", kind, got, err)
			}
		}
	}
	old, _ := json.Marshal(full)
	if _, err := unmarshalPacket(old); !errors.Is(err, ErrInvalidMessage) {
		t.Fatal("old JSON protocol accepted")
	}
}

func TestPacketRejectsTruncationFlagsLengthsAndTrailingData(t *testing.T) {
	valid, err := marshalPacket(fullPacket())
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(valid); n++ {
		if _, err := unmarshalPacket(valid[:n]); err == nil {
			t.Fatalf("accepted truncated packet at %d", n)
		}
	}
	for _, offset := range []int{0, 2, 3, 4, 6} {
		bad := bytes.Clone(valid)
		bad[offset] = 0xff
		if _, err := unmarshalPacket(bad); err == nil {
			t.Fatalf("accepted invalid field at %d", offset)
		}
	}
	if _, err := unmarshalPacket(append(valid, 0)); err == nil {
		t.Fatal("accepted trailing data")
	}
	if _, err := unmarshalPacket([]byte{'H', 'A', 2, 1, 0, 1, 1, 0xff, 0xff}); err == nil {
		t.Fatal("accepted oversized string prefix")
	}
}

func FuzzActorPacket(f *testing.F) {
	p, _ := marshalPacket(fullPacket())
	f.Add(p)
	f.Add([]byte{'H', 'A', 2, 4, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		decoded, err := unmarshalPacket(data)
		if err != nil {
			return
		}
		encoded, err := marshalPacket(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := unmarshalPacket(encoded); err != nil {
			t.Fatal(err)
		}
	})
}
