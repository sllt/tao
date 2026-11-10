package protobuf

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sllt/tao/actor"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCodecPreservesNilEmptyAndBinaryPayload(t *testing.T) {
	codec := Codec{}
	for _, input := range []*wrapperspb.BytesValue{nil, {}, {Value: []byte{0, 0xff, 0x80}}} {
		data, err := codec.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		var output *wrapperspb.BytesValue
		if err := codec.Unmarshal(data, &output); err != nil {
			t.Fatal(err)
		}
		if (input == nil) != (output == nil) || string(input.GetValue()) != string(output.GetValue()) {
			t.Fatalf("round trip: %v -> %v", input, output)
		}
		clear(data)
		if string(input.GetValue()) != string(output.GetValue()) {
			t.Fatal("output aliases input")
		}
	}
	var output *wrapperspb.BytesValue
	for _, bad := range [][]byte{nil, {0, 1}, {2}, {1, 0x0a, 0xff}} {
		if err := codec.Unmarshal(bad, &output); err == nil {
			t.Fatalf("accepted %x", bad)
		}
	}
}

func TestRemoteCallWithGeneratedMessages(t *testing.T) {
	newNode := func(name string) *actor.Node {
		n, err := actor.StartNode(name+"@127.0.0.1:0", actor.NodeOptions{Codec: Codec{}, Network: actor.NetworkOptions{AllowInsecureLoopback: true}, Register: []actor.MessageType{{Name: "echo", Version: 1, Sample: (*wrapperspb.BytesValue)(nil)}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := n.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
		return n
	}
	a, b := newNode("a"), newNode("b")
	if err := a.Connect(t.Context(), b.Name(), b.ListenAddr()); err != nil {
		t.Fatal(err)
	}
	pid, err := b.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		if value, ok := message.(*wrapperspb.BytesValue); ok {
			return self.Reply(value)
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []*wrapperspb.BytesValue{nil, {}, {Value: []byte{0, 0xff, 0x80}}} {
		got, err := a.Call[*wrapperspb.BytesValue](t.Context(), pid, value)
		if err != nil || (value == nil) != (got == nil) || string(value.GetValue()) != string(got.GetValue()) {
			t.Fatalf("remote %v -> %v, %v", value, got, err)
		}
	}
	if err := a.Send(pid, "not registered"); !errors.Is(err, actor.ErrUnregisteredType) {
		t.Fatal(err)
	}
}
