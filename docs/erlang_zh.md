# Erlang/OTP 互通使用指南

[English](erlang.md) | 简体中文

Tao 通过两个职责清晰的独立模块提供原生 **Erlang distribution v6 与 `gen_server` 消息互通**：

- [`github.com/sllt/tao/erlang`](../erlang/README_zh.md)（Go 1.26+，零第三方依赖）：独立 Erlang 节点、外部 EPMD v6 发现、TCP 与 `inet_tls` v6 握手、独立所有权 ETF（`erlang/etf`）、双向 2048 槽 atom cache、distribution 分片重组、虚拟 `Client`、导出 `Endpoint` 及原生 `monitor`/`link`。
- [`github.com/sllt/tao/actor/erlang`](../actor/erlang/README_zh.md)（包名 `erlangactor`，Go 1.27+）：薄适配层，通过显式 `Schema`、有序本地准入（`Node.BeginCall`）与无阻塞出站 `Actor.Request` 将本地 Tao Actor 接入 `erlang.Node`。

`actor` 与 `taonet` 核心均不依赖 `erlang` 或 `erlangactor`。

## 架构定位与支持边界

Go 进程无需 BEAM sidecar 即可直接加入 Erlang/Elixir 集群（默认作为 hidden node）。同一进程内的 Tao 节点之间继续使用 `taonet`（`HA/2` + CBOR），显式导出的本地具体 `actor.PID` 则在独立端口上通过原生 Erlang distribution 对外提供服务。

| 能力领域 | 支持行为 | 明确边界 |
| :--- | :--- | :--- |
| 发现与握手 | 外部 EPMD v6（`ALIVE2_X_RESP`、`PORT_PLEASE2_REQ`）或显式 `StaticRoutes`；v6 `'N'` 握手、MD5 摘要认证、同时拨号裁决、认证后 `alive` 连接接替 | 静态 ASCII `name@host` 节点名；不自动拉起 EPMD，不支持旧版 `'n'` 握手 |
| 传输层 | 原生 TCP（packet-2 -> packet-4）与 `-proto_dist inet_tls`（全程 packet-4） | TLS 失败直接报错，绝不回退明文；与 `taonet` ALPN/证书相互独立 |
| `net_adm:ping/1` | 内置 `net_kernel` 对已认证连接的 `{is_auth, Node}` 返回 `yes` | 默认 hidden node（`Published: false`）；保留名禁止用户导出 |
| `gen_server` 消息 | `!`、`gen_server:cast`、`gen_server:call`、`gen_server:send_request`、普通 `Ref`、`[alias \| Ref]`、嵌套 alias tag、跨进程 `gen_server:reply` | `Cast`/`Send` 成功仅代表有界本地准入，不代表远端邮箱已入队 |
| Monitor 与 Link | 保留原始被观察 `Atom`/`PID` 的 `MONITOR_P` / `DEMONITOR_P` / `MONITOR_P_EXIT`（21/28）；支持 `LINK` / `UNLINK_ID` / `UNLINK_ID_ACK`（35/36，含当前 Creation 已退出本地 PID 的 ACK） | 绑定具体代理实例，不随同名重导出自动转移 |
| 非目标能力 | 入站 `{'system', From, Req}` 立即返回 `{error, {tao, unsupported}}`，不挂起超时 | 不实现 `sys:*`、`gen_server:stop`、`start_link`、远程 `spawn`、`rpc/erpc`、`global`、`pg` 或 Mnesia；OTP 30 的 `ALTACT_SIG_SEND`（opcode 37）留待后续专门支持 |

已在真实 **OTP 27、OTP 28、OTP 29** 官方环境上双向验证 TCP 与 `inet_tls` 互通。

## 身份与 ETF 数据所有权（`erlang/etf`）

Erlang `PID`、`Port`、`Ref` 完整保留节点名、32 位 `Creation` 与全部有效字，不截断塞入 `actor.PID`。单次节点 incarnation 内的 ID 单调递增且不复用。

`etf.Term` 使用受限的显式 Go 类型（`etf.Atom`、`int64` / `*big.Int`、`float64`、`etf.Binary`、`etf.BitBinary`、`etf.Tuple`、`etf.List`、`etf.Nil`、`etf.Map`、`etf.PID`、`etf.Port`、`etf.Ref`、`etf.Export`、`etf.Fun`）：

- 普通 Go `string` 不会自动猜测为 atom；请显式使用 `etf.Atom`、`etf.UTF8Binary` 或 `etf.Charlist`。
- 解码后的 term 拥有独立内存，网络接收缓冲可在进入 Actor 邮箱前立即归还。
- `etf.Map` 以键值序列支持 tuple/list/binary 等任意 Erlang 键，并按 Erlang `=:=` 精确等价（`etf.TermEqual`）拒绝重复键。
- `etf.Fun` 解析为独立结构并按目标连接的 atom cache 重新编码，不直接跨连接重放原始字节。

## Tao Actor 桥接（`erlangactor`）

### 入站导出与有序准入

`bridge.Export(name, localPID, schema, opts...)` 为具体本地 `localPID` 创建虚拟 Erlang 代理，并由自适应 guardian 组通过 `Monitor` 跟踪生命周期：

1. 每条 peer 连接在顺序更新 2048 槽接收 atom cache 与重组 `131,69`/`131,70` 分片后，由单一有序分派链处理消息。
2. `gen_server:cast` 与 `!` 走 `Node.Send`，`gen_server:call` 同步调用 `Node.BeginCall` 完成邮箱准入并立即返回，保证同一调用方连续发出的 `cast(add)` 与 `call(get)` 严格按线序入队。
3. 原生 `$gen_call` 线上不携带调用方 timeout，入站调用不会被截断为 `actor.NodeOptions.CallTimeout`；如需服务端执行时限，可显式配置 `erlangactor.WithInboundCallTimeout(d)`。
4. 目标 Actor 在同一轮提交 `Reply` / `DeferredReply.Reply` 后立即停止时，Binding 会等待已提交回复排入 peer writer 后再发出 `DOWN`（`Reply-before-DOWN`）；尚未完成的 `Defer` 凭据按 `CompletionTargetExit` 结束，不会编码重复业务回复。

### Actor 内异步访问 BEAM

`bridge.Outbound()` 是本地连接 Actor 的 `actor.PID`。在业务 `Receive` 内通过 `Actor.Request` 发起异步调用不会阻塞当前邮箱，可安全处理 `Tao Actor A -> BEAM -> Tao Actor A` 回调环：

```go
reqID, err := a.Request(a.Context(), bridge.Outbound(), erlangactor.Call{
	Target:  erlang.Registered("orders", "beam@host.example"),
	Request: etf.Tuple{etf.Atom("lookup"), int64(42)},
})
```

### 过载、结果分类与状态迁移

- **有界过载**：目标邮箱已满时，`$gen_call` 经 `Schema.EncodeReply` 返回 `{error, {tao, overloaded}}`；`$gen_cast` 与 `!` 无回复通道，邮箱满时以 `{tao, overloaded, DetailBinary}` 终止该代理并通知观察者，不静默丢包。
- **投递分类**：`*erlang.Error` 区分 `OutcomeNotSent`（写出前已拒绝/撤回）、`OutcomeUnknown`（可能有字节已写出后超时或断线，不自动重放）与 `OutcomeUnsupported`，并在 `Reason` 字段保留原始 `DOWN` 原因 term。
- **`Replace` 与 `Handoff`**：guardian 的入站 `Monitor` 不会阻止 `Node.Handoff`。因桥接绑定具体本地 `PID`，推荐迁移顺序为：停止新流量 -> 应用确认存量完成 -> `binding.Close(ctx)` -> `Handoff` -> 目标节点重新 `Export`。若直接执行 `Handoff`，源端代理会收到 `ErrHandedOff` 并以 `{tao, handed_off, DetailBinary}` 正常通知观察者。
