# Tao Actor Erlang 适配器

[English](README.md) | 简体中文

`github.com/sllt/tao/actor/erlang`（包名 `erlangactor`）将本地 Tao Actor（`github.com/sllt/tao/actor`）接入原生 Erlang/OTP distribution v6（`github.com/sllt/tao/erlang`）。
最低要求 Go 1.27。`actor` 核心与 `taonet` 均不依赖 `erlang` 或 `erlangactor`。

## 快速开始

在仓库根目录运行：

```sh
go run ./actor/erlang/examples/bridge
```

输出：

```text
bridged_counter=42
```

## 导出本地 Actor 与调用 BEAM

```go
bridge, err := erlangactor.Attach(taoNode, erlangNode)
if err != nil {
	return err
}
defer bridge.Close(ctx)

// 将具体本地 Tao PID 显式导出为 Erlang 注册名，并绑定值转换 Schema
binding, err := bridge.Export("counter", counterPID, counterSchema)

// 在业务 Actor.Receive 中异步调用 BEAM，不阻塞当前 Actor 邮箱：
reqID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
	Target:  erlang.Registered("orders", "beam@127.0.0.1"),
	Request: etf.Tuple{etf.Atom("lookup"), int64(42)},
})
// 在后续 Receive(actor.CallResult) 中串行处理返回结果。
```

- **显式 `Schema`**：通过 `DecodeCall`、`DecodeCast`、`DecodeInfo` 和 `EncodeReply` 执行无阻塞值转换（提供 `erlangactor.Funcs` 与 `erlangactor.PassthroughSchema()`）。不伪造 `Actor.Sender()`；如需读取来源 Erlang 节点和 PID，可启用 `WithSourceEnvelope(true)` 接收 `erlangactor.Inbound`。
- **有序入站准入**：单条 peer 连接按线序同步执行 `Node.Send`（cast/info）与 `Node.BeginCall`（call），保证同一 Erlang 进程 `cast(add)` 先于后续 `call(get)` 进入本地邮箱，且不在网络线程等待业务完成。
- **入站调用预算**：原生 `$gen_call` 不携带调用方超时值，适配器不会套用 `actor.NodeOptions.CallTimeout` 截断入站调用；如需服务端执行时限，可显式配置 `WithInboundCallTimeout(d)`。
- **Reply-before-DOWN 顺序**：同一轮 `Receive` 先提交 `Reply` / `DeferredReply.Reply` 后立即 `Stop` 时，`Binding` 会先将已提交回复排入发送队列，再发出 `DOWN`；尚未完成的 `Defer` 请求以 `TargetExit` 结束，不会编码第二条业务回复。业务主动调用 `ReplyError(actor.ErrStopped)` 仍按正常 `Reply` 编码，不误判为 Actor 退出。
- **非阻塞出站连接器**：`bridge.Outbound()` 在 `Receive` 内 `Defer()` 并按目标节点 FIFO 提交 `Client.CallAsync`，配合 `Actor.Request` 可安全支持 `Tao Actor A -> BEAM -> Tao Actor A` 回调而不死锁。

## 生命周期、过载与迁移边界

- **借用节点生命周期**：`Attach` 借用 `taoNode` 与 `erlangNode`。推荐关闭顺序为 `bridge.Close(ctx)` -> `erlangNode.Shutdown(ctx)` -> `taoNode.Stop(ctx)`；任一底层节点先停或 EPMD 注册丢失时，`Bridge` 会自动关闭全部绑定并回收自适应 guardian `Monitor` 组。
- **`TrapExit` 与 `exit(Pid, kill)`**：`WithTrapExit(true)` 将可捕获的 Erlang 退出信号作为 `erlangactor.Exit` DTO 投递（不混同于 `actor.Exit`）。显式 `exit(BindingPid, kill)` 立即以 `killed` 终止 Erlang 代理并取消关联桥接请求，默认保留本地 Tao Actor；需要共同生命周期时可配置 `WithStopActor(true)` 发起协作式 `StopActor`。
- **有界过载策略**：目标邮箱已满时，入站 `$gen_call` 通过 `Schema.EncodeReply` 返回 `{error, {tao, overloaded}}`；由于 `$gen_cast` / `!` 无回复通道，邮箱满会以 `{tao, overloaded, DetailBinary}` 终止该 `Binding` 并通知监控方，不静默丢弃消息冒充存活。
- **`Replace` 与 `Handoff`**：guardian 入站 `Monitor` 不会阻止 `Node.Handoff`。由于 `Binding` 绑定具体本地 `PID`，推荐受控迁移顺序为：停止新流量 -> 业务确认存量处理完成 -> `binding.Close(ctx)` -> `Handoff` -> 目标侧重新 `Export`。若直接执行 `Handoff`，迁移仍可成功，源侧 `Binding` 以 `{tao, handed_off, DetailBinary}` 结束。

## 测试与许可

在 `actor/erlang/` 目录执行：

```sh
GOWORK=off go test -race ./...
```

本模块采用 [MIT License](../../LICENSE)。
