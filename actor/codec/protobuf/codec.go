// Package protobuf provides an optional generated-schema business codec.
// It does not change Hive's binary framework envelope or taonet dependencies.
package protobuf

import (
	"fmt"
	"reflect"

	"github.com/sllt/tao/actor"
	"google.golang.org/protobuf/proto"
)

type Codec struct{}

var _ actor.MessageCodec = Codec{}

func (Codec) Name() string { return "protobuf.v1" }
func (Codec) ValidateType(t reflect.Type, maxBytes int) error {
	if t.Kind() != reflect.Pointer || !t.Implements(reflect.TypeFor[proto.Message]()) {
		return fmt.Errorf("%w: protobuf requires a generated message pointer", actor.ErrInvalidMessage)
	}
	if t.Elem().Size() > uintptr(maxBytes) {
		return actor.ErrMessageTooLarge
	}
	return nil
}

func (Codec) Marshal(value any) ([]byte, error) {
	return (Codec{}).MarshalAppend(make([]byte, 0, proto.Size(value.(proto.Message))+1), value)
}

func (Codec) MarshalAppend(dst []byte, value any) ([]byte, error) {
	// Presence distinguishes a typed nil pointer from an empty protobuf value.
	if reflect.ValueOf(value).IsNil() {
		return append(dst, 0), nil
	}
	m := value.(proto.Message)
	return (proto.MarshalOptions{}).MarshalAppend(append(dst, 1), m)
}

func (Codec) Unmarshal(data []byte, target any) error {
	if len(data) == 0 {
		return actor.ErrInvalidMessage
	}
	v := reflect.ValueOf(target).Elem() // Registry supplies **GeneratedMessage.
	if data[0] == 0 && len(data) == 1 {
		v.SetZero()
		return nil
	}
	if data[0] != 1 {
		return actor.ErrInvalidMessage
	}
	m := reflect.New(v.Type().Elem())
	if err := (proto.UnmarshalOptions{RecursionLimit: 32}).Unmarshal(data[1:], m.Interface().(proto.Message)); err != nil {
		return err
	}
	v.Set(m)
	return nil
}
