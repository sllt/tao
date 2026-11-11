# Tao Erlang Distribution

English | [简体中文](README_zh.md)

`github.com/sllt/tao/erlang` provides standalone **Erlang distribution v6 and `gen_server` messaging interoperability** for Go using only the Go standard library (Go 1.26+).

A Go process can join an Erlang/Elixir cluster directly (as a hidden node by default) without a BEAM sidecar, call or cast native `gen_server` / `GenServer` processes, export virtual Erlang processes under registered atom names or PIDs, and participate in native process monitors and links over TCP or `inet_tls`.

> **Scope**: This module implements **Erlang distribution and `gen_server` messaging interoperability**, not a BEAM virtual machine or full OTP runtime management layer (`sys:*`, `gen_server:stop`, remote `spawn`, `rpc/erpc`, `global`, `pg`, and Mnesia are intentionally out of scope).

## Quick Start

Run the self-contained loopback example from the repository root:

```sh
go run ./erlang/examples/genserver-call
```

Output:

```text
order=1001 stock=42
```

Minimal standalone client and service usage:

```go
node, err := erlang.Start(ctx, erlang.Config{
	Name:   "tao_bridge@127.0.0.1",
	Listen: "127.0.0.1:0",
	Cookie: cookie,
})
if err != nil {
	return err
}
defer node.Shutdown(context.Background())

client, err := node.NewClient()
if err != nil {
	return err
}
defer client.Close()

reply, err := client.Call(ctx,
	erlang.Registered("orders", "beam@127.0.0.1"),
	etf.Tuple{etf.Atom("lookup"), int64(42)},
)
```

## Capabilities & Explicit Boundaries

| Area | Supported Behavior | Boundary |
| :--- | :--- | :--- |
| Discovery & Handshake | External EPMD v6 (`ALIVE2_X_RESP` 32-bit Creation, `PORT_PLEASE2_REQ`) or explicit `StaticRoutes`; v6 `'N'` handshake, MD5 challenge/ack, simultaneous-connect tie-break, `alive` replacement | Static ASCII `name@host` node names; does not auto-spawn an EPMD daemon or support legacy `'n'` handshakes |
| Transport | Native TCP (packet-2 handshake -> packet-4 connected) and `inet_tls` (packet-4 throughout) | TLS never falls back to plaintext; independent of `taonet` |
| Built-in `net_kernel` | Responds `yes` to authenticated `{is_auth, Node}` calls (`net_adm:ping/1` returns `pong`) | Hidden node by default (`Published: false`); rejects user exports of reserved system names |
| Messaging & Calls | `!`, `gen_server:cast`, `gen_server:call`, `gen_server:send_request`, plain `Ref`, `[alias \| Ref]`, nested alias tags, cross-process `gen_server:reply` | `Cast` / `Send` success means bounded local admission, not remote mailbox receipt |
| Monitors & Links | `MONITOR_P` / `DEMONITOR_P` / `MONITOR_P_EXIT` (21/28) preserving raw watched `Atom` or `PID`; `LINK` / `UNLINK_ID` / `UNLINK_ID_ACK` (35/36) including dead-local-PID ACKs | Bound to a specific virtual process instance; does not transfer across same-name re-exports |
| Wire Framing | 2048-slot per-direction atom cache (`131,68`), distribution fragments (`131,69`/`131,70`), `EXIT_PAYLOAD` (24–28), `SEND_SENDER` (22/23) | `ALTACT_SIG` (opcode 37), priority messages, and OTP 29 native records are not advertised in v1 |
| Non-Goals | Inbound `{'system', From, Req}` replies `{error, {tao, unsupported}}` immediately instead of hanging | No `sys:get_state`, `gen_server:stop`, `start_link`, remote `spawn`, or `rpc`/`erpc` execution |

## ETF Data Model (`github.com/sllt/tao/erlang/etf`)

Decoded terms own independent memory and never retain network receive buffers.

- **Explicit types**: `etf.Atom` (UTF-8, max 255 Unicode characters; standalone decoder also reads legacy Latin-1 atoms), `int64` / `*big.Int`, `float64`, `etf.Binary` (`etf.UTF8Binary`), `etf.BitBinary`, `etf.Tuple`, `etf.List` (`ProperList` / `ImproperList` / `Charlist`), `etf.Nil`, `etf.Map` (ordered key/value pairs supporting arbitrary Erlang keys with exact `=:=` duplicate checking), `etf.PID`, `etf.Port`, `etf.Ref`, `etf.Export`, and `etf.Fun` (decoded/re-encoded structurally across connections, never executed).
- **Bounds**: Frame size, decoded allocation budget, container depth, binary/bignum size, and zlib decompression (`80`) are checked before allocation.

## TLS (`-proto_dist inet_tls`) Configuration

Erlang TLS distribution authenticates both transport certificates and the distribution cookie (neither replaces the other). Certificate SANs match hostnames/IPs, whereas `AllowedPeers` restricts authenticated Erlang `name@host` identities.

Matching OTP `ssl_dist.conf` (`erl -proto_dist inet_tls -ssl_dist_optfile ssl_dist.conf`):

```erlang
[{server, [
    {certfile, "/etc/tls/node.pem"},
    {keyfile, "/etc/tls/node-key.pem"},
    {cacertfile, "/etc/tls/ca.pem"},
    {verify, verify_peer},
    {fail_if_no_peer_cert, true}
 ]},
 {client, [
    {certfile, "/etc/tls/node.pem"},
    {keyfile, "/etc/tls/node-key.pem"},
    {cacertfile, "/etc/tls/ca.pem"},
    {verify, verify_peer}
 ]}].
```

Matching Go `erlang.Config`:

```go
tlsCfg := &tls.Config{
	MinVersion:   tls.VersionTLS12,
	Certificates: []tls.Certificate{cert}, // or GetCertificate / GetClientCertificate for zero-downtime rotation
	RootCAs:      caPool,
	ClientCAs:    caPool,
	ClientAuth:   tls.RequireAndVerifyClientCert,
	ServerName:   "beam-host.example.internal",
}
```

- **Certificate rotation**: Populate `GetCertificate` and `GetClientCertificate` on `tls.Config` so new connections pick up rotated certificates automatically; OTP reloads PEM files referenced in `ssl_dist.conf` on new handshakes.
- **Verification failures**: An untrusted CA, mismatched `ServerName` SAN, missing client certificate, or mismatched cookie aborts the handshake immediately (`OutcomeNotSent`) and never falls back to plaintext.

## Backpressure & Failure Outcomes

Native Erlang distribution has no per-message remote mailbox ACK. `*erlang.Error` reports explicit `Outcome` values:

- `OutcomeNotSent`: Failed validation, local quota reservation, or aborted before the peer writer committed the frame.
- `OutcomeUnknown`: Timeout, cancellation, or disconnect after any bytes may have been committed to the wire; never replayed automatically.
- `OutcomeUnsupported`: Unsupported local feature or term rejected prior to transmission.
- Remote `DOWN` errors preserve the raw Erlang `Reason` `etf.Term` on `erlang.Error.Reason`. Normal `{error, Reason}` tuples returned by `gen_server` are delivered as ordinary `etf.Term` replies.

## Testing & License

Run from `erlang/`:

```sh
GOWORK=off go test -race ./...
```

When `erl`, `erlc`, and `epmd` are available in `PATH`, the test suite compiles `testdata/beam/tao_test_server.erl` and runs real BEAM interoperability tests over both TCP and `inet_tls`.

Licensed under the [MIT License](../LICENSE).
