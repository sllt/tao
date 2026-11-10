module github.com/sllt/tao/actor/codec/protobuf

go 1.27.0

toolchain go1.27.3

require (
	github.com/sllt/tao/actor v0.1.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/fxamacker/cbor/v2 v2.10.0 // indirect
	github.com/sllt/tao/taonet v0.1.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
)

replace github.com/sllt/tao/actor => ../..

replace github.com/sllt/tao/taonet => ../../../taonet
