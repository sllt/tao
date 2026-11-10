package actor

import (
	"bytes"
	"reflect"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

// CBORCodec supports concrete Go message types without an IDL. It preserves
// fixed-width integer checks and rejects unknown fields, duplicate map keys,
// trailing input and malformed lengths before allocating decoded collections.
type CBORCodec struct{}

var cborEncoder, cborDecoder = cborModes()

// Retain only Buffer metadata. Every return clears its backing slice so this
// cache neither owns caller data nor retains a large message's allocation.
var cborEncodeBuffers = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func cborModes() (cbor.UserBufferEncMode, cbor.DecMode) {
	enc, err := (cbor.EncOptions{Time: cbor.TimeRFC3339Nano, TimeTag: cbor.EncTagRequired}).UserBufferEncMode()
	if err != nil {
		panic(err) // Constant library configuration, not user/network input.
	}
	dec, err := (cbor.DecOptions{MaxNestedLevels: 32, MaxArrayElements: 65536, MaxMapPairs: 4096, ExtraReturnErrors: cbor.ExtraDecErrorUnknownField, DupMapKey: cbor.DupMapKeyEnforcedAPF}).DecMode()
	if err != nil {
		panic(err)
	}
	return enc, dec
}

func (CBORCodec) Name() string { return "cbor.v1" }
func (CBORCodec) ValidateType(t reflect.Type, maxBytes int) error {
	return validateWireType(t, maxBytes, "cbor", make(map[reflect.Type]bool))
}
func (CBORCodec) Marshal(value any) ([]byte, error) { return cborEncoder.Marshal(value) }
func (CBORCodec) MarshalAppend(dst []byte, value any) ([]byte, error) {
	buffer := cborEncodeBuffers.Get().(*bytes.Buffer)
	*buffer = *bytes.NewBuffer(dst)
	defer func() {
		*buffer = bytes.Buffer{}
		cborEncodeBuffers.Put(buffer)
	}()
	// MarshalToBuffer encodes into AvailableBuffer, then writes that result.
	// Without enough capacity a large scalar grows both temporary and output
	// storage. Reserve its known bound; named/custom types retain codec hooks.
	switch value := value.(type) {
	case string:
		buffer.Grow(len(value) + 9)
	case []byte:
		buffer.Grow(len(value) + 9)
	}
	err := cborEncoder.MarshalToBuffer(value, buffer)
	return buffer.Bytes(), err
}
func (CBORCodec) Unmarshal(data []byte, target any) error {
	return cborDecoder.Unmarshal(data, target)
}
