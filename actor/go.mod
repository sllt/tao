module github.com/sllt/tao/actor

go 1.27.0

toolchain go1.27.3

require (
	github.com/fxamacker/cbor/v2 v2.10.0
	github.com/sllt/tao/taonet v0.2.0
)

require github.com/x448/float16 v0.8.4 // indirect

// Use the sibling module while developing this repository.
replace github.com/sllt/tao/taonet => ../taonet
