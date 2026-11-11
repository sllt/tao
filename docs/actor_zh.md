# Actor 使用指南

[English](actor.md) | 简体中文

本指南说明 `github.com/sllt/tao/actor` 的运行语义。创建与调用的完整示例见 [Actor README](../actor/README_zh.md)。

## 实例与逻辑地址

`Node.Spawn` 接收 `Factory`，创建实例并等待 `Started` 成功后发布身份。行为类型嵌入 `actor.Actor` 并实现 `Receive(any) error`。每次 `Spawn` 和监督重启都调用 `Factory`；不要在多个实例之间共享可变的闭包状态。

| 地址 | 语义 |
| :--- | :--- |
| `PID` | 包含节点名、`BootID` 和实例 ID，绑定具体实例；重启、替换后失效 |
| `Ref{Node: "east", Path: "app/counter"}` | 绑定节点上的逻辑路径，可指向重启后的新实例 |
| `Ref{Path: "app/counter"}` | 未配置 `Directory` 时在本地查找；配置 `Directory` 后作为全局逻辑路径 |

`actor.ParseRef` 接受裸路径或 `node://node/path`，返回 `Ref` 或 `ErrInvalidRef`。它不裁剪空白、解码 URL 转义或清理路径，拒绝空段、点段、凭据、端口、query 和 fragment。`Node.Ref` 是便利构造入口，不是外部输入验证器。

## 消息处理与所有权

同一实例的 `Receive` 串行执行。空邮箱时实例释放执行权，后续消息唤醒它；阻塞的 Actor 不会占满一个固定大小的执行池。邮箱有硬容量上限，默认 1024 条。`ErrMailboxFull` 表示本次消息没有进入该邮箱。

普通消息与系统通知有不同的调度优先级；不要假定二者合成一个全局 FIFO。`Stop`、`Replace`、`Handoff` 和状态检查在执行边界处理。

`Sender`、`Context`、`Send`、`Call`、`Reply` 等 Actor 能力在 `Receive` 的同步调用栈内使用。外部并发发送使用 `Node`；外部回复使用 `DeferredReply`。本地消息允许共享内存，发出后不要继续修改共享 `map`、`slice` 或其底层数据。

## 同步请求与错误

`Node.Call[T]` 和包级 `actor.Call[T]` 返回类型化结果。Actor 内部使用 `CallAs[T]`，或使用返回 `any` 的 `Call`。类型不匹配返回 `ErrReplyType`；`nil` 对可为 `nil` 的类型有效，对 `int` 等标量无效。

未指定 deadline 时，`Call` 使用节点的 `CallTimeout`，默认 5 秒。调用方取消、实例停止或崩溃会使旧回复凭据失效。同步调用自己返回 `ErrSelfCall`。`A->B->A` 的同步调用可能互相等待，需要改用异步请求或调整业务依赖。

| 情况 | 处理方式 |
| :--- | :--- |
| 业务成功 | `Reply(value)` |
| 业务拒绝 | `ReplyError(businessError)`，随后正常返回 |
| 需要延后回复 | 先 `Defer()`，保存一次性凭据 |
| `Receive` 无回复且未 `Defer` | 调用者收到 `ErrNoReply` |
| 调用者已经不再等待 | 回复可能返回 `ErrReplyExpired` |
| 实例自身故障 | `Receive` 返回错误或 panic，交给监督处理 |

`Reply` 返回的是回复状态，不自动等于实例故障。允许丢弃迟到回复的服务可以只过滤 `ErrReplyExpired`：

```go
err := a.Reply(value)
if errors.Is(err, actor.ErrReplyExpired) {
	return nil
}
return err
```

对 `DeferredReply.Reply` / `ReplyError` 也采用相同的业务判断。重复回复或错误使用凭据仍应显露；不要把所有错误都吞掉。

## 异步请求

`Actor.Request(ctx, target, message)` 返回 `RequestID` 或准入错误。接受后，路由、调用额度、超时和业务回复的结果通过 `CallResult{ID, Value, Err}` 进入请求者的系统邮箱，由 `Receive` 串行处理。

- 用 `RequestID` 关联业务状态。结果按完成顺序到达，不保证发起顺序；`Sender` 为 `nil`。
- deadline 从 `Request` 入口计算，未指定时使用 `CallTimeout`。传入 `a.Context()` 会继承当前入站 `Call` 的取消及完成；独立工作使用独立 context。
- `Defer` 延后的是入站回复，`Request` 发起的是出站调用。需要转发结果时，保存 `DeferredReply`，在 `CallResult` 中完成它。
- `Request` 可以请求自身；当前 `Receive` 返回后处理请求，再处理结果。同步 `Call` / `CallAs` 自己仍被拒绝。
- `CancelRequest(id)` 请求取消本实例尚未提交结果的操作。返回 `true` 不表示回复一定没有先赢；最终结果仍可观察，已经发生的业务效果不会撤销。

`NodeOptions.MaxAsyncRequests` 限制运行中的 `Request` 和尚未进入 `Receive` 的完成结果，默认取 `MaxPendingCalls`。单 Actor 同时不超过其 `MailboxSize`。异步额度到结果开始被 `Receive` 处理时才归还。

`Stop` 取消运行项并丢弃未消费结果。`Replace` / `Handoff` 遇到运行中或未消费的结果返回 `ErrAsyncPending`。可执行的 `A->B->A` 示例见 [example_test.go](../actor/example_test.go)，[持续负载示例](../actor/examples/soak/main.go) 的 bridge 演示了 `Defer`、`Request` 和 `CallResult` 的组合。

### 有序本地调用准入（`BeginCall`）

`Node.BeginCall(ctx, targetPID, message, complete)` 面向具体本地 `PID` 同步完成配额检查、请求登记与邮箱入队后立即返回，并在锁外通过 `complete(CallCompletion)` 一次性发布完成结果（`CompletionReply`、`CompletionNoReply`、`CompletionCanceled` 或 `CompletionTargetExit`）。`BeginCall` 原样遵循传入 `ctx` 的期限（无 deadline 时不自动附加 `CallTimeout`）并持续监听父 context 取消，供 [`actor/erlang`](../actor/erlang/README_zh.md) 等外部协议适配层严格保持同一连接上的 `cast`/`call` 入队顺序。

## 监督与生命周期观察

默认策略为 `OneForOne` / `Permanent`，也支持 `OneForAll`、`RestForOne` 和 `Transient` / `Temporary`。显式 `StopActor`、父级关闭和监督策略停止不会触发额外的 `Permanent` 重启。

默认丢弃旧邮箱；`PreserveRef` 只保留尚未执行、发给逻辑 `Ref` 的消息，不转交旧 `PID` 消息或正在执行的消息。停止超时不能强杀 Go goroutine；运行时保留 `Unresponsive` 名称，避免新实例误用仍被旧回调占用的资源。

`Monitor` 绑定具体 `PID`，并通过 `Down` 报告终结或远端不可达。`ErrNodeUnreachable` 不证明远端已经退出。`Demonitor` 可撤销尚未消费的 `Down`。`Link` 是双向关系；异常退出可以传播，启用 `TrapExit` 后以 `Exit` 消息交给业务处理。

## 网络与编码

`NewNode` 不启动网络监听。网络节点使用 `StartNode`，可以将地址写为 `node1@127.0.0.1:0`，也可以在 `NodeOptions.Name` 中指定节点名。空地址创建仅出站节点。`Connect` 或 `Network.Peers` 提供静态对端地址。

默认使用 `Isolated` 连接配置，分别处理 `inbox`、`reply` 和 `control`。远程发送按 peer/class 保持 FIFO；业务回复绕过请求者的业务邮箱。业务消息默认使用 CBOR，框架信封与业务 codec 分离；可选 `JSONCodec` 或独立 [Protobuf Codec](../actor/codec/protobuf/README_zh.md)。

网络消息必须显式注册稳定的 `Name`/`Version`，包括使用的基础类型。

| 操作 | 成功含义 |
| :--- | :--- |
| 远程 `Send` | 本地编码/发送队列接受 |
| `SendConfirmed` | 对端邮箱接受 |
| `Call` | 对应业务回复完成 |

`RemoteError` 包含 `Code` 和 `Outcome`（`NotSent`、`Rejected`、`Responded`、`Unknown`），支持 `errors.Is`。断连或取消后的 `Unknown` 不自动重放。

## Directory 与状态交接

`MemoryDirectory` 用于单进程和测试；跨进程共享所有权使用 [etcd Directory](../actor/etcd/README_zh.md)。目录持有 route、实例和 epoch。网络分区不会自动夺取 owner；过期路由仅在明确未入邮箱时重新解析，`Unknown` 和业务回复不重放。

### Replace

`Node.Replace(ctx, target, kind)` 替换本节点叶子 Actor。它在 `Receive` 边界冻结，等待 `DeferredReply`，要求旧实例在 `Snapshotting` 中 `Reply` 独立状态；新实例处理 `Started{Restoring:true}`、`Upgraded.Restore`，提交后处理 `Activated`，再消费转交的 `Ref` 邮箱。

### Handoff

`Node.Handoff(ctx, target, options)` 将支持 `Handoff` 的叶子 Actor 迁移到另一节点。运行时冻结消费，复制快照、全局 `Ref` 邮箱与增量尾部，再通过 `Directory` CAS 提交所有权。`DrainAndHandoff` 报告各实例的提交、回滚、未知和失败结果。

## 诊断与测试

`Node.Stats` 提供实例、队列、请求、异步额度和生命周期计数。启用诊断后，`Snapshot` / `ActorSnapshot` 提供处理指标和调用关系；`Inspector` 在 Actor 执行边界串行检查业务状态。[Web Observer](../actor/observer/README_zh.md) 是这些接口的可选消费者。

修改运行时请遵循 [贡献指南](../CONTRIBUTING_zh.md)。
