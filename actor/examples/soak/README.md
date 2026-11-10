# Continuous Load & Fault Recovery (Soak)

English | [简体中文](README_zh.md)

This example verifies actor lifecycles, synchronous/asynchronous request outcomes, fault recovery, and resource reclamation under sustained load. Two nodes run in the same Go process communicating over loopback TCP (`127.0.0.1`), executing four workload windows followed by graceful shutdown and post-GC memory/goroutine sampling.

## Running

Run from `actor/`:

```sh
mkdir -p results
go run ./examples/soak -duration=4s -workers=4 -actors=4 \
  > results/soak-smoke.jsonl 2> results/soak-smoke.stderr.jsonl
go run ./examples/soak -duration=2m -workers=8 -actors=8 \
  > results/soak-2m.jsonl 2> results/soak-2m.stderr.jsonl
```

Structured JSONL records are written to `stdout`; runtime logs and fatal errors are written to `stderr`.

| Flag | Default | Description |
| :--- | :--- | :--- |
| `-duration` | `2m` | Total budget across the four workload windows (minimum `4s`) |
| `-workers` | `8` | Number of concurrent callers (`1..256`) |
| `-actors` | `8` | Number of worker actors per node (`1..256`) |

## Workload Windows & Fault Injection

The run progresses through four sequential windows:

1. `mixed_before_faults`: ~50% local and ~50% remote calls, with ~70% targeting a hot actor (`worker-0`) and the remainder distributed across the pool.
2. `mixed_local_restart`: Continues mixed traffic while triggering a panic in an isolated local actor, verifying crash reporting, old `PID` invalidation, and `Ref` resolution to the restarted `PID`.
3. `local_remote_stopped`: Continues local traffic while issuing an in-flight remote `Call` to a dedicated remote actor, confirming handler entry via a synchronization channel, and stopping the remote node mid-call. Verifies the terminal outcome (`Responded/Stopped` or `Unknown`), single execution, and `NotSent` rejection after shutdown.
4. `mixed_remote_recovered`: Starts a replacement remote node on the same TCP address, reconnects explicitly, verifies `BootID` rotation (`NotSent/StalePID` on old `PID`, recovery on logical `Ref`), and resumes mixed traffic.

Every fourth request from each caller routes through a local `async-bridge` actor that calls `Defer()`, forwards the request via `Actor.Request(a.Context(), target, work)`, and completes the `DeferredReply` upon receiving `CallResult` in `Receive`.

## Startup Probes & Output Schema

Two deterministic startup probes verify:
- Canceling an in-flight local `Call` after handler entry and confirming that a late `Reply` returns `ErrReplyExpired`.
- Filling a capacity-1 mailbox while its handler is blocked and confirming that excess sends fail with `ErrMailboxFull` without executing.

Each `stdout` line is a JSON object with `type` (`configuration`, `resources`, `phase`, `probe`, or `summary`), UTC `time`, `elapsed_ms`, and `data`. After both nodes stop (`nodes_closed`), the runner records `after_gc_0`, `after_gc_1`, and `after_gc_2` snapshots and verifies that zero active actors, pending calls, async requests, or queued messages remain.

## Testing

Run from `actor/`:

```sh
go test ./examples/soak
go test -race ./examples/soak
```

See the [Actor Guide](../../../docs/actor.md), [Actor README](../../README.md), and [Web Observer](../../observer/README.md) for more details.
