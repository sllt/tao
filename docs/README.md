# Documentation

English | [简体中文](README_zh.md)

## User Guides

- [Actor Guide](actor.md): Addressing (`PID` vs `Ref`), messaging and requests, supervision trees, remote communication, and state handoff (`Replace` / `Handoff`).
- [taonet Guide](taonet.md): Bidirectional sessions, delivery receipts, buffer ownership, channels, streams, and endpoint management.
- [Erlang/OTP Interoperability Guide](erlang.md): Distribution v6 protocol, ETF data ownership, `gen_server` messaging, monitors/links, and Tao Actor bridging.

## Modules & Extensions

- [Actor Module](../actor/README.md) (`github.com/sllt/tao/actor`)
- [taonet Module](../taonet/README.md) (`github.com/sllt/tao/taonet`)
- [Erlang Distribution Module](../erlang/README.md) (`github.com/sllt/tao/erlang`)
- [Actor Erlang Adapter](../actor/erlang/README.md) (`github.com/sllt/tao/actor/erlang`)
- [Web Observer](../actor/observer/README.md) (`github.com/sllt/tao/actor/observer`)
- [etcd Directory](../actor/etcd/README.md) (`github.com/sllt/tao/actor/etcd`)
- [Protobuf Codec](../actor/codec/protobuf/README.md) (`github.com/sllt/tao/actor/codec/protobuf`)

## Examples

- [HTTP Service + Actor + Observer](../actor/examples/http-api/README.md)
- [Continuous Load & Fault Recovery (Soak)](../actor/examples/soak/README.md)

For development setup, testing, and contribution guidelines, see [CONTRIBUTING.md](../CONTRIBUTING.md).
