package actor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
)

var (
	ErrUnregisteredType = errors.New("actor: unregistered wire type")
	ErrInvalidMessage   = errors.New("actor: invalid wire message")
	ErrMessageTooLarge  = errors.New("actor: encoded message too large")
	ErrCodecMismatch    = errors.New("actor: message codec mismatch")
)

// MessageCodec encodes business values, independently of the Actor envelope.
// Methods must support concurrent calls. Marshal returns owned bytes;
// Unmarshal must not retain input aliases after returning. ValidateType runs
// once at registration; target passed to Unmarshal is a pointer to that type.
type MessageCodec interface {
	Name() string
	ValidateType(reflect.Type, int) error
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

// MessageAppender is an optional codec fast path. MarshalAppend appends one
// value to dst, preserving its prefix. It may grow dst, but must not retain it.
// The caller owns the returned bytes, including any partially written result.
type MessageAppender interface {
	MarshalAppend(dst []byte, value any) ([]byte, error)
}

type MessageType struct {
	Name    string
	Version uint32
	Sample  any
}

// Msg registers a concrete message type under a stable wire name and version.
// T may be a typed nil pointer; interfaces are rejected by Registry validation.
func Msg[T any](name string, version uint32) MessageType {
	var sample T
	return MessageType{Name: name, Version: version, Sample: sample}
}

type EncodedMessage struct {
	Type    string `json:"type"`
	Version uint32 `json:"version"`
	Codec   string `json:"codec"`
	Data    []byte `json:"data"`
}
type wireType struct {
	name    string
	version uint32
}
type Registry struct {
	byType    map[reflect.Type]wireType
	byName    map[wireType]reflect.Type
	maxBytes  int
	codec     MessageCodec
	encoding  string
	wireNames map[string]string // immutable canonical names owned by this registry
}

func NewRegistry(types []MessageType, maxBytes int) (*Registry, error) {
	return newRegistry(types, maxBytes, nil)
}

func newRegistry(types []MessageType, maxBytes int, codec MessageCodec) (*Registry, error) {
	if maxBytes == 0 {
		maxBytes = 128 << 10
	}
	if maxBytes < 1 || maxBytes > 4<<20 {
		return nil, fmt.Errorf("actor: invalid codec size limit")
	}
	if codec == nil {
		codec = CBORCodec{}
	}
	encoding := codec.Name()
	if !validSegment(encoding) {
		return nil, fmt.Errorf("actor: invalid codec name")
	}
	r := &Registry{byType: make(map[reflect.Type]wireType), byName: make(map[wireType]reflect.Type), maxBytes: maxBytes, codec: codec, encoding: encoding}
	r.wireNames = make(map[string]string, len(types)+1)
	r.wireNames[encoding] = encoding
	for _, entry := range types {
		t := reflect.TypeOf(entry.Sample)
		key := wireType{entry.Name, entry.Version}
		if !validSegment(entry.Name) || entry.Version == 0 || t == nil {
			return nil, fmt.Errorf("actor: invalid message registration %q", entry.Name)
		}
		if _, ok := r.byType[t]; ok {
			return nil, fmt.Errorf("actor: duplicate Go message type %v", t)
		}
		if _, ok := r.byName[key]; ok {
			return nil, fmt.Errorf("actor: duplicate wire message %s@%d", entry.Name, entry.Version)
		}
		if err := codec.ValidateType(t, maxBytes); err != nil {
			return nil, err
		}
		r.byType[t] = key
		r.byName[key] = t
		r.wireNames[entry.Name] = entry.Name
	}
	return r, nil
}

func validateWireType(t reflect.Type, maxBytes int, tag string, seen map[reflect.Type]bool) error {
	if seen[t] {
		return nil
	}
	seen[t] = true
	if t.Size() > uintptr(maxBytes) {
		return fmt.Errorf("%w: static type %v exceeds codec budget", ErrMessageTooLarge, t)
	}
	if t == reflect.TypeFor[TimerRef]() {
		return fmt.Errorf("%w: runtime type %v", ErrInvalidMessage, t)
	}
	if t == reflect.TypeFor[Actor]() || t == reflect.TypeFor[Node]() || t == reflect.TypeFor[Started]() || t == reflect.TypeFor[Stopped]() || t == reflect.TypeFor[Down]() || t == reflect.TypeFor[Exit]() || t == reflect.TypeFor[DeferredReply]() || t == reflect.TypeFor[Snapshotting]() || t == reflect.TypeFor[Upgraded]() || t == reflect.TypeFor[Activated]() || t == reflect.TypeFor[CallResult]() || t == reflect.TypeFor[RequestID]() {
		return fmt.Errorf("%w: runtime type %v", ErrInvalidMessage, t)
	}
	switch t.Kind() {
	case reflect.Bool, reflect.String, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return nil
	case reflect.Pointer, reflect.Array, reflect.Slice:
		return validateWireType(t.Elem(), maxBytes, tag, seen)
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return fmt.Errorf("%w: map keys must be strings", ErrInvalidMessage)
		}
		return validateWireType(t.Elem(), maxBytes, tag, seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldTag, exists := field.Tag.Lookup(tag)
			if !exists && tag == "cbor" {
				fieldTag = field.Tag.Get("json")
			}
			if !field.IsExported() || strings.Split(fieldTag, ",")[0] == "-" {
				continue
			}
			if err := validateWireType(field.Type, maxBytes, tag, seen); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported type %v; use fixed-width integers and concrete fields", ErrInvalidMessage, t)
	}
}

func (r *Registry) Encode(message any) (encoded EncodedMessage, err error) {
	defer func() {
		if recover() != nil {
			encoded = EncodedMessage{}
			err = fmt.Errorf("%w: codec callback panicked", ErrInvalidMessage)
		}
	}()
	if message == nil {
		return EncodedMessage{Codec: r.encoding}, nil
	}
	key, ok := r.byType[reflect.TypeOf(message)]
	if !ok {
		return EncodedMessage{}, ErrUnregisteredType
	}
	data, err := r.codec.Marshal(message)
	if err != nil {
		return EncodedMessage{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if len(data) > r.maxBytes {
		return EncodedMessage{}, ErrMessageTooLarge
	}
	return EncodedMessage{Type: key.name, Version: key.version, Codec: r.encoding, Data: data}, nil
}

// encodePacket puts the business value directly after its envelope metadata.
// The resulting Message.Data aliases the packet and has the same lifetime.
func (r *Registry) encodePacket(dst []byte, packet actorPacket, value any) (data []byte, encoded EncodedMessage, err error) {
	defer func() {
		if recover() != nil {
			data, encoded = nil, EncodedMessage{}
			err = fmt.Errorf("%w: codec callback panicked", ErrInvalidMessage)
		}
	}()
	encoded.Codec = r.encoding
	if value != nil {
		key, ok := r.byType[reflect.TypeOf(value)]
		if !ok {
			return nil, EncodedMessage{}, ErrUnregisteredType
		}
		encoded.Type, encoded.Version = key.name, key.version
	}
	packet.Message = encoded
	var start, end int
	data, err = appendPacketValue(dst, packet, func(dst []byte) ([]byte, error) {
		start = len(dst)
		var err error
		if value != nil {
			if codec, ok := r.codec.(MessageAppender); ok {
				dst, err = codec.MarshalAppend(dst, value)
			} else {
				var body []byte
				body, err = r.codec.Marshal(value)
				dst = append(dst, body...)
			}
		}
		if err != nil {
			return dst, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
		}
		end = len(dst)
		if end-start > r.maxBytes {
			return dst, ErrMessageTooLarge
		}
		return dst, nil
	})
	if err == nil {
		encoded.Data = data[start:end]
	}
	return data, encoded, err
}

func (r *Registry) Decode(encoded EncodedMessage) (message any, err error) {
	defer func() {
		if recover() != nil {
			message = nil
			err = fmt.Errorf("%w: codec callback panicked", ErrInvalidMessage)
		}
	}()
	if len(encoded.Data) > r.maxBytes {
		return nil, ErrMessageTooLarge
	}
	if encoded.Codec != r.encoding {
		return nil, ErrCodecMismatch
	}
	if encoded.Type == "" && encoded.Version == 0 {
		if len(encoded.Data) != 0 {
			return nil, ErrInvalidMessage
		}
		return nil, nil
	}
	t, ok := r.byName[wireType{encoded.Type, encoded.Version}]
	if !ok {
		return nil, ErrUnregisteredType
	}
	value := reflect.New(t)
	if err := r.codec.Unmarshal(encoded.Data, value.Interface()); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return value.Elem().Interface(), nil
}

// JSONCodec is an explicit interoperability/debugging option, not the default.
type JSONCodec struct{}

func (JSONCodec) Name() string { return "json.v1" }
func (JSONCodec) ValidateType(t reflect.Type, maxBytes int) error {
	return validateWireType(t, maxBytes, "json", make(map[reflect.Type]bool))
}
func (JSONCodec) Marshal(value any) ([]byte, error) { return json.Marshal(value) }
func (JSONCodec) Unmarshal(data []byte, value any) error {
	t := reflect.TypeOf(value).Elem()
	switch t.Kind() {
	case reflect.Bool, reflect.String, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		// Scalars have no unknown fields. Unmarshal validates the complete
		// value without allocating a streaming decoder and its input buffer;
		// named types still run their UnmarshalJSON method.
		return json.Unmarshal(data, value)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON", ErrInvalidMessage)
	}
	return nil
}
