# HTTP + Actor Example

English | [简体中文](README_zh.md)

This example maps HTTP API requests onto per-account Actors (`accounts/<id>`), demonstrating lazy actor spawning under a supervisor, business rejection via `ReplyError`, request deadlines, asynchronous audit notifications, and optional read-only inspection via the [Web Observer](../../observer/README.md).
Account balances, transaction counts, and audit counters live in memory without persistent storage or idempotency keys.

## Running

Run the built-in self-test from `actor/`:

```sh
go run ./examples/http-api
```

By default, the program starts an HTTP server on a random loopback port, executes 20 concurrent deposits of 10 into account `taozi` followed by a withdrawal of 50, verifies the final state (`balance=150 tx_count=21 audit_events=21`), and shuts down cleanly.

To keep the HTTP server running alongside the Web Observer:

```sh
go run ./examples/http-api -serve 127.0.0.1:8080 -observer 127.0.0.1:9912 -call-timeout 5s
```

Then invoke the API from another terminal:

```sh
curl -X POST http://127.0.0.1:8080/accounts/alice/deposit \
  -H 'Content-Type: application/json' -d '{"amount":100}'
curl http://127.0.0.1:8080/accounts/alice
curl -X POST http://127.0.0.1:8080/accounts/alice/withdraw \
  -H 'Content-Type: application/json' -d '{"amount":30}'
```

`-observer` is disabled by default and accepts only explicit loopback IP:port addresses (such as `127.0.0.1:9912` or `[::1]:9912`). Open `http://127.0.0.1:9912` to inspect the `accounts` supervision tree, mailboxes, and per-account `Inspect` snapshots.

## Endpoints & Responses

| Method & Path | Behavior |
| :--- | :--- |
| `GET /accounts/{id}` | Query account snapshot (lazily spawns `accounts/<id>` on first access) |
| `POST /accounts/{id}/deposit` | Deposit positive integer `amount` |
| `POST /accounts/{id}/withdraw` | Withdraw positive integer `amount` |

Account IDs must be 1–64 ASCII alphanumeric, `_`, or `-` characters. Request bodies must be a single JSON object `{"amount": <positive int64>}` within 1 KiB; unknown fields, trailing tokens, non-integers, and integer overflows are rejected without mutating state.

Successful responses return `id`, `balance`, `tx_count`, and `pid`. Error responses return `error` and delivery `outcome`:

| Condition | HTTP Status | `outcome` |
| :--- | :--- | :--- |
| Invalid account ID, malformed JSON, or body too large | 400 / 413 | `not_sent` |
| Insufficient funds or balance overflow | 409 | `responded` |
| Mailbox full | 503 | `rejected` |
| Pending call capacity exhausted | 503 | `not_sent` |
| Target process missing or stale `PID` | 404 | `not_sent` |
| Process starting or restarting | 503 | `rejected` |
| Operation timeout / caller canceled | 504 / 408 | `unknown` |
| Peer explicitly reported `Unknown` | 502 (504 on timeout) | `unknown` |
| Stopped, unresponsive, or unexpected error | 503 / 500 | `unknown` |

## Failure & Retry Boundaries

The gateway never automatically retries business operations. Lazy creation only triggers on explicit `ErrNoProcess` / `ErrStarting` errors—never on `Unknown`. Because a timeout or lost reply does not prove the actor did not execute the mutation, retrying a deposit or withdrawal without an application-level idempotency protocol would apply the mutation twice.

Actors return business rejections via `ReplyError` and ignore `ErrReplyExpired` when a caller stops waiting before the reply is sent.

## Testing

Run from `actor/`:

```sh
go test ./examples/http-api
go test -race ./examples/http-api
```

See the [Actor README](../../README.md), [Web Observer](../../observer/README.md), and [Actor Guide](../../../docs/actor.md) for more details.
