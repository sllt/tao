# 持续运行与恢复验证（Soak）

[English](README.md) | 简体中文

此示例验证生命周期、请求结果和资源回收。默认执行两分钟负载，随后关闭节点并采样。两个节点在同一 Go 进程通过 `127.0.0.1` 的真实 TCP 通信，依次执行四个负载窗口并验证停机后的资源回收。

## 运行

从 `actor/` 目录运行：

```sh
mkdir -p results
go run ./examples/soak -duration=4s -workers=4 -actors=4 \
  > results/soak-smoke.jsonl 2> results/soak-smoke.stderr.jsonl
go run ./examples/soak -duration=2m -workers=8 -actors=8 \
  > results/soak-2m.jsonl 2> results/soak-2m.stderr.jsonl
```

程序将结构化 JSONL 结果写到 `stdout`，日志和错误写到 `stderr`。

| 参数 | 默认值 | 含义 |
| :--- | :--- | :--- |
| `-duration` | `2m` | 四个负载窗口的总预算，最小 `4s` |
| `-workers` | `8` | 并发 caller 数量，范围 `1..256` |
| `-actors` | `8` | 每个节点的业务 Actor 数量，范围 `1..256` |

## 负载与故障窗口

四个窗口依次为：

1. `mixed_before_faults`：本地/远程约各半，约 70% 调用集中于 `worker-0`，其余分散到业务池。
2. `mixed_local_restart`：继续混合负载，同时使独立本地 Actor panic，检查崩溃错误、旧 `PID` 撤销和同一 `Ref` 到达新 `PID`。
3. `local_remote_stopped`：继续本地负载；向远端专用 Actor 发起一次 `Call`，确认已进入 handler 且尚未回复后停止远端节点，记录该在途 `Call` 的真实终态并确认停机后的新调用以 `NotSent` 拒绝。
4. `mixed_remote_recovered`：在原 TCP 地址启动同名新节点，显式重连；检查 `BootID` 改变、旧 `PID` 以 `NotSent/StalePID` 失败、同一 `Ref` 恢复，然后继续混合负载。

每个 caller 每四次调用中有一次经过本地 `async-bridge`：在 `Receive` 内用 `Defer` 保存原调用，通过 `Request(a.Context(), target, work)` 转发，再在后续 `CallResult` 的 `Receive` 内完成对应 `DeferredReply`。

## 探针与结果格式

启动阶段包含两个确定性探针：取消已进入 handler 的本地 `Call` 并检查迟到 `Reply` 返回 `ErrReplyExpired`；阻塞一个容量为 1 的邮箱并确认额外消息返回 `ErrMailboxFull`。

`stdout` 每行是一个 JSON 对象，包含 `type`（`configuration`、`resources`、`phase`、`probe`、`summary`）、`time`、`elapsed_ms` 和 `data`。停机后依次输出 `nodes_closed`、`after_gc_0`、`after_gc_1`、`after_gc_2`，并检查所有 Actor、pending `Call`、`AsyncRequests` 和排队消息均已清零。

## 验证

从 `actor/` 目录运行示例测试：

```sh
go test ./examples/soak
go test -race ./examples/soak
```

更多请求与生命周期契约见 [Actor 使用指南](../../../docs/actor_zh.md)、[Actor README](../../README_zh.md) 和 [Web Observer](../../observer/README_zh.md)。
