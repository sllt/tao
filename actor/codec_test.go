package actor

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

type codecText string

func (v *codecText) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	*v = codecText(strings.ToUpper(text))
	return nil
}

func TestRegistryScalarDecodeValidatesWholeValueAndUsesCustomDecoder(t *testing.T) {
	r, err := newRegistry([]MessageType{{"text", 1, ""}, {"custom", 1, codecText("")}}, 0, JSONCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"text", "custom"} {
		for _, data := range []string{`"one" "two"`, `"unterminated`, `12`, `{"value":"x"}`} {
			if _, err := r.Decode(EncodedMessage{Type: name, Version: 1, Codec: "json.v1", Data: []byte(data)}); !errors.Is(err, ErrInvalidMessage) {
				t.Fatalf("%s accepted %q: %v", name, data, err)
			}
		}
	}
	value, err := r.Decode(EncodedMessage{Type: "custom", Version: 1, Codec: "json.v1", Data: []byte(` "hello" `)})
	if err != nil || value != codecText("HELLO") {
		t.Fatalf("custom decoder: %#v, %v", value, err)
	}
}

type codecMessage struct {
	Count  int64             `json:"count"`
	Labels map[string]string `json:"labels"`
}

func TestRegistryUsesStableNamesAndPreservesFixedWidthValues(t *testing.T) {
	registry, err := NewRegistry([]MessageType{Msg[codecMessage]("counter.state", 1), Msg[*codecMessage]("counter.pointer", 1), Msg[int64]("integer", 1)}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{codecMessage{Count: math.MaxInt64, Labels: map[string]string{"key": "value"}}, &codecMessage{Count: math.MinInt64}, (*codecMessage)(nil), int64(math.MaxInt64), nil} {
		encoded, err := registry.Encode(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := registry.Decode(encoded)
		if err != nil || !reflect.DeepEqual(decoded, value) {
			t.Fatalf("round trip %T: %#v %v", value, decoded, err)
		}
	}
	if _, err = registry.Encode("not registered"); !errors.Is(err, ErrUnregisteredType) {
		t.Fatal(err)
	}
	if _, err = registry.Decode(EncodedMessage{Type: "counter.state", Version: 2, Codec: "cbor.v1", Data: []byte{0xa0}}); !errors.Is(err, ErrUnregisteredType) {
		t.Fatal(err)
	}
}

func TestRegistryRejectsRuntimeTypesAndAmbiguousSchemas(t *testing.T) {
	for _, sample := range []any{Started{}, Stopped{}, Down{}, Exit{}, Actor{}, &Node{}, CallResult{}, RequestID{}, TimerRef{}, func() {}, make(chan int), struct{ Context context.Context }{}, struct{ Value any }{}, int(1)} {
		if _, err := NewRegistry([]MessageType{{"bad", 1, sample}}, 0); err == nil {
			t.Fatalf("registered unsupported type %T", sample)
		}
	}
	if _, err := NewRegistry([]MessageType{{"duplicate", 1, int64(0)}, {"duplicate", 1, ""}}, 0); err == nil {
		t.Fatal("duplicate wire name accepted")
	}
	if _, err := NewRegistry([]MessageType{{"one", 1, int64(0)}, {"two", 2, int64(0)}}, 0); err == nil {
		t.Fatal("ambiguous encoder accepted")
	}
}

func TestRegistryBoundsAndRejectsMalformedJSON(t *testing.T) {
	registry, err := newRegistry([]MessageType{{"state", 1, codecMessage{}}, {"text", 1, ""}}, 64, JSONCodec{})
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{`{"count":9223372036854775808}`, `{"unknown":true}`, `{} {}`, `{"count":`} {
		if _, err = registry.Decode(EncodedMessage{Type: "state", Version: 1, Codec: "json.v1", Data: []byte(data)}); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("accepted %q: %v", data, err)
		}
	}
	if _, err = registry.Encode(string(make([]byte, 65))); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatal(err)
	}
	if _, err = registry.Decode(EncodedMessage{Type: "state", Version: 1, Data: make([]byte, 65)}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatal(err)
	}
}
