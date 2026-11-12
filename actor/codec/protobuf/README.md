# Tao Actor Protobuf Codec

English | [简体中文](README_zh.md)

`github.com/sllt/tao/actor/codec/protobuf` is an optional business message codec module for Tao Actor, using package name `protobuf` and requiring Go 1.27+. It replaces only the business payload encoding in `actor` without altering the `HA/2` wire envelope or introducing Protobuf dependencies into `taonet` or the `actor` core.

## Configuration

Pass `actorpb.Codec{}` to `NodeOptions.Codec` and register your generated Protobuf message pointer types with `actor.Msg`:

```go
import (
	"github.com/sllt/tao/actor"
	actorpb "github.com/sllt/tao/actor/codec/protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newNode() (*actor.Node, error) {
	return actor.NewNode(actor.NodeOptions{
		Name:  "protobuf-node",
		Codec: actorpb.Codec{},
		Register: []actor.MessageType{
			actor.Msg[*wrapperspb.BytesValue]("example.payload", 1),
		},
	})
}
```

Check the returned error and call `Node.Stop(ctx)` on shutdown. For networking and peer connection setup, see the [Actor Guide](../../../docs/actor.md). Communicating nodes must use the same codec and register matching `Name`/`Version` entries for all request and reply message types. Only pointer types implementing `proto.Message` may be registered.

## Wire Contract

- Unknown fields follow standard Protobuf compatibility rules; peers sharing a message `Name` and `Version` should use compatible schemas.
- The first payload byte encodes presence: `0` denotes a typed `nil` pointer (with no trailing bytes permitted), and `1` precedes a serialized Protobuf message. Empty non-nil messages are preserved distinctly from typed `nil`.
- Decoding enforces a maximum recursion depth of 32 and never aliases the input buffer.
- `Codec` implements `actor.MessageAppender` to append the presence byte and encoded payload directly into the outbound envelope buffer, while `Marshal` returns an independent byte slice.

## Testing

Run from the workspace root:

```sh
go test ./actor/codec/protobuf/...
```

Or verify the module independently with the race detector from `actor/codec/protobuf/`:

```sh
GOWORK=off go test -race ./...
```

See the [Actor README](../../README.md) and [Contributing Guide](../../../CONTRIBUTING.md) for further details.
