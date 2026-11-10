# taonet 使用指南

[English](taonet.md) | 简体中文

`taonet` 提供基于有序连接的双向请求（`Request`）、通知（`Notify`）、消息通道（`Channel`）和字节流（`Stream`）。模块路径为 `github.com/sllt/tao/taonet`，最低要求 Go 1.26，仅依赖标准库。快速开始命令见 [taonet README](../taonet/README_zh.md)。

## 模块与连接模型

一个 `Session` 对应一条物理连接及其多路复用状态。两端都能注册路由、发起 `Request`、发送 `Notify`，以及打开 `Channel` 或 `Stream`。`Initiator` / `Acceptor` 区分握手角色，不限制业务调用方向。

传输使用仓库自有的 `HN-MUX/1` 多路复用（[handshake.go](../taonet/handshake.go)、[wire.go](../taonet/wire.go) 和 [internal/mux](../taonet/internal/mux)）。连接上有独立控制通道，业务操作使用各自的底层流。`Peer` 身份、功能和额度协商完成后，连接才对应用可用。

## Peer、路由与建立连接

`SessionConfig.Local` 包含 `Peer.ID`、`Peer.Instance` 和 `Peer.Namespace`：

- `ID` 标识逻辑对端；`Instance` 区分运行实例（未指定时使用本进程生成的 ID）。
- `Namespace` 默认是 `"default"`，两端必须相同。
- `Peer.ID` 与 `Namespace` 必须是 1～255 字节的有效 UTF-8 字符串，不能含控制字符。

先调用 `NewMux`，再用 `HandleRequest`、`HandleNotify`、`HandleChannel`、`HandleStream` 登记处理函数。同一种模式下不能重复注册路由；不同模式有独立路由表。`Mux` 用于创建 `Endpoint` 或 `Session` 后会冻结。

| 入口 | 连接所有权与 context |
| :--- | :--- |
| `Dial(ctx, address, cfg)` | 拨号并握手，成功后由返回的 `Session` 管理连接 |
| `Accept(ctx, conn, cfg)` | 对已接受连接执行接收方握手 |
| `NewSession(ctx, conn, role, cfg)` | 接管已有 `net.Conn`，失败也会关闭它 |
| `Endpoint.Serve(ctx, listener)` | 接管监听器，持续接受并登记 `Session` |

`Dial`、`Accept` 和 `NewSession` 的 context 仅控制建立连接，返回后取消它不会结束 `Session`。应用应对持有的 `Session` 或 `Endpoint` 调用 `Shutdown` / `Close`。

## Request：等待业务回复

`Session.Request(ctx, route, msg)` 等待对应 `RequestHandler` 返回完整 `Message` 或错误。处理函数可用 `request.Session()` 在同一连接上调用另一端的路由（见 [reverse-rpc](../taonet/examples/reverse-rpc/main.go)）。

调用方没有 deadline 时，`Request` 使用 `DefaultRequestTimeout`（默认 5 秒）；已有 deadline 时剩余预算随请求传递。业务可返回 `*StatusError` 提供稳定错误码（自定义 `Code` 以 `app.` 开头）。普通业务错误映射为 `CodeApplication`，panic 映射为 `CodeInternal`。

## Notify：单向通知

- `Notify` 将 `Body` 和 `Metadata` 复制到有界本地发送操作，成功返回只表示本地接受。
- `NotifyConfirmed` 进一步等待远端通知队列接受，不等待 `NotifyHandler` 执行完毕。

成功进入本地发送操作后，调用方即可复用原输入；之后取消确认等待不会撤回通知。

## Channel：双向有序消息

`OpenChannel(ctx, route, metadata)` 打开双向消息通道，传入的 context 同时控制返回 `Channel` 的生命周期。每个方向保持消息 FIFO，发送同时受本地队列（最多 4096 项）、`Session` 字节预算及对端消息/字节窗口限制。

### 发送与接受回执

| 方法 | 成功返回或回执完成表示 | 原输入何时可复用 |
| :--- | :--- | :--- |
| `Send` | 有界本地副本已入队 | 方法返回后 |
| `SendConfirmed` | 对端 `Delivery.Accept` | 方法返回后（入队时已复制） |
| `SendWithReceipt` | 返回入队成功的 `SendReceipt` | 方法返回后 |
| `SendOwnedWithReceipt` | 返回入队成功的 `SendReceipt` | `receipt.Released()` 关闭后 |
| `SendOwnedUnconfirmed` | 返回 `WriteReceipt`，报告本地物理写入结果 | `receipt.Released()` 关闭后 |

```go
func sendAccepted(ctx context.Context, channel *taonet.Channel, message taonet.Message) error {
	receipt, err := channel.SendWithReceipt(ctx, message)
	if err != nil {
		return err
	}
	return receipt.Wait(ctx)
}
```

### 接收、决策与 Release

`Receive(ctx)` 返回 `*Delivery`，正常消息方向结束时返回 `io.EOF`。接收方按收到顺序调用 `Accept()` 或 `Reject(code)`，消费完成后**必须调用 `Release()`**。`Delivery.Message().Body` 是借用的，只在 `Release` 前有效；决策与释放分别推进接受确认和窗口归还。

### 借用输入与无接受 ACK 发送

`SendOwnedWithReceipt` 和 `SendOwnedUnconfirmed` 成功入队后，`Body` 底层数组和 `Metadata` 暂由 `Channel` 使用，调用方在 `Released()` 关闭前不能访问或修改。`SendOwnedUnconfirmed` 用于上层协议自带回复的场景（要求协商 `channel-unconfirmed-v1`）。协商 `channel-inline-v1` 后，无 `Metadata` 且不超过 16 KiB 的消息使用紧凑二进制记录和内联 ACK。

### 正常结束消息方向

`Channel.CloseWrite()` 停止本端继续提交消息，已排队消息仍会发送。完整示例见 [duplex-chat](../taonet/examples/duplex-chat/main.go)。

## Stream：双向字节流与半关闭

`OpenStream(ctx, route, metadata)` 返回实现 `io.Reader` / `io.Writer` 的 `Stream`，支持并发读写、`SetReadDeadline` / `SetWriteDeadline`、`CloseWrite` 半关闭，以及多缓冲借用写入 `WritevBorrowed(net.Buffers) (written, released, err)`。参见 [file-copy](../taonet/examples/file-copy/main.go)。

## 错误结果与取消

用 `errors.As(err, &opError)` 读取 `*OpError` 的 `Code`、`Outcome`、`Remote` 和 `Operation`：

| `Outcome` | 含义 |
| :--- | :--- |
| `NotSent` | 操作尚未发出，例如本地校验或准入失败 |
| `Rejected` | 远端明确在业务分发前拒绝 |
| `Unknown` | 已经尝试发送，但没有足够证据判断远端结果 |
| `Responded` | 已收到完整业务错误回复 |

库不自动重试 `Unknown`，新连接也不会重放旧操作。

## Endpoint：管理连接与流量类别

`NewEndpoint(EndpointConfig{Session: cfg})` 创建连接管理器，支持并发拨号合并与两种连接模式：

- **`Compact`（默认）**：为每个 `Peer` 复用单条会话。
- **`Isolated`**：使用主连接（`ClassControl`）及按需建立的分类连接（`ClassInteractive`、`ClassReply`、`ClassBulk`），支持服务端反向请求仅出站客户端建立分类连接（见 [isolated-peer](../taonet/examples/isolated-peer/main.go)）。

## 身份与 TLS

普通连接要求 TLS 1.3、双向已验证证书链及 `taonet/1` ALPN，默认使用对端证书 SAN 校验其声明的 `Peer.ID`。`AllowInsecureLoopback` 只允许实际两端地址都为 loopback 的 TCP 明文连接，供仓库示例和本地测试使用。

## 配额与背压

| 配置项 | 默认值 | 作用 |
| :--- | :--- | :--- |
| `Limits.MaxMessageSize` | 4 MiB | `Request` / `Notify` 的消息上限 |
| `Limits.MaxMetadataBytes` / `MaxMetadataEntries` | 8 KiB / 32 | `Metadata` 编码大小与条目数 |
| `Limits.MaxOperations` / `MaxHandlers` | 256 / `min(64, MaxOperations)` | `Session` 操作数与入站处理额度 |
| `Limits.ReceiveBytes` / `SendBytes` | 16 MiB / 8 MiB | `Session` 消息接收和发送预留 |
| `Limits.ChannelMaxMessage` | 256 KiB | 单条 `Channel` 消息上限 |
| `Limits.ChannelWindowBytes` / `ChannelWindowMessages` | 1 MiB / 256 | 每个 `Channel` 接收方向的窗口 |
| `SessionConfig.StreamReceiveWindow` | 64 KiB | 每条底层普通流接收窗口 |
| `EndpointConfig.MaxSessions` / `ManagedBytes` | 8 / 256 MiB | 含握手中的物理连接数及连接预留 |

## 验证

在 `taonet/` 目录执行：

```sh
go test ./...
go test -race ./...
GOWORK=off go test ./...
go test ./internal/mux -run '^$' -fuzz '^FuzzHeader$' -fuzztime=10s
```
