# Tao Erlang Distribution

[English](README.md) | 简体中文

`github.com/sllt/tao/erlang` 提供仅依赖 Go 标准库的 **Erlang distribution v6 与 `gen_server` 消息互通**实现（最低要求 Go 1.26）。

Go 进程无需部署 BEAM sidecar 即可直接加入 Erlang/Elixir 节点网络（默认作为 hidden node），双向调用或发送 `gen_server` / `GenServer` 消息，导出具名或 PID 虚拟进程，并在原生 TCP 或 `inet_tls` 上支持分布式 `monitor` 与 `link`。

> **兼容定位**：本模块定位于 **Erlang distribution / `gen_server` 消息互通**，不是在 Go 中运行 BEAM 虚拟机或完整 OTP 运行时管理（不包含 `sys:*`、`gen_server:stop`、远程 `spawn`、`rpc/erpc`、`global`、`pg` 或 Mnesia）。

## 快速开始

在仓库根目录运行自包含示例：

```sh
go run ./erlang/examples/genserver-call
```

输出：

```text
order=1001 stock=42
```

最小客户端用法：

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

## 能力范围与明确边界

| 领域 | 支持行为 | 明确边界 |
| :--- | :--- | :--- |
| 发现与握手 | 外部 EPMD v6（`ALIVE2_X_RESP` 32 位 Creation、`PORT_PLEASE2_REQ`）或显式 `StaticRoutes`；v6 `'N'` 握手、MD5 摘要认证、同时拨号字典序裁决、`alive` 连接接替 | 使用静态完整 `name@host`；不自动拉起宿主机 EPMD，不支持旧版 `'n'` 握手 |
| 传输层 | 原生 TCP（握手 packet-2，连接后 packet-4）与 `inet_tls`（全程 packet-4） | TLS 握手失败直接返回错误，不回退明文；与 `taonet` 完全独立 |
| 内置 `net_kernel` | 对已认证连接的 `{is_auth, Node}` 调用返回 `yes`，原生支持 `net_adm:ping/1` | 默认 hidden node（`Published: false`）；禁止用户导出冲突保留名 |
| 消息与调用 | `!`、`gen_server:cast`、`gen_server:call`、`gen_server:send_request`、普通 `Ref`、`[alias \| Ref]`、嵌套 alias tag、跨进程 `gen_server:reply` | `Cast` / `Send` 返回成功仅代表有界本地准入，不代表远端邮箱已接收 |
| Monitor 与 Link | 保留原始 `Atom` 或 `PID` 的 `MONITOR_P` / `DEMONITOR_P` / `MONITOR_P_EXIT`（21/28）；支持 `LINK` / `UNLINK_ID` / `UNLINK_ID_ACK`（35/36，含当前 Creation 已退出本地 PID 的 ACK） | 绑定具体虚拟进程实例，不随同名重注册自动转移 |
| 连接帧与分片 | 收发双向 2048 槽 atom cache（`131,68`）、分片重组（`131,69`/`131,70`）、`EXIT_PAYLOAD`（24–28）、`SEND_SENDER`（22/23） | 首版不公告 `ALTACT_SIG`（opcode 37）、优先级消息与 OTP 29 原生 record |
| 非目标请求 | 入站 `{'system', From, Req}` 立即返回 `{error, {tao, unsupported}}`，不挂起超时 | 不承诺 `sys:get_state`、`gen_server:stop`、`start_link`、远程 `spawn` 或 `rpc/erpc` |

## ETF 数据模型（`github.com/sllt/tao/erlang/etf`）

公开解码结果拥有独立内存拷贝，不引用底层网络接收缓冲。

- **显式类型映射**：`etf.Atom`（UTF-8，最多 255 个 Unicode 字符；独立解码入口只读兼容历史 Latin-1 atom）、`int64` / `*big.Int`、`float64`、`etf.Binary`（`etf.UTF8Binary`）、`etf.BitBinary`、`etf.Tuple`、`etf.List`（`ProperList` / `ImproperList` / `Charlist`）、`etf.Nil`、`etf.Map`（键值序列，支持任意 Erlang term 作 key，按 `=:=` 精确等价拒绝重复键）、`etf.PID`、`etf.Port`、`etf.Ref`、`etf.Export` 与 `etf.Fun`（跨连接结构化重编码，不执行函数）。
- **资源上限**：帧长、解码内存预算、嵌套深度、大整数/二进制长度和压缩包（`80`）输出均先检查再分配。

## TLS（`-proto_dist inet_tls`）配对说明

TLS 证书校验与 Erlang cookie 认证并列执行，互不替代；证书 SAN 校验主机/IP，`AllowedPeers` 限制认证后的完整节点名 `name@host`。

OTP 侧 `ssl_dist.conf`（`erl -proto_dist inet_tls -ssl_dist_optfile ssl_dist.conf`）：

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

对应的 Go `erlang.Config`：

```go
tlsCfg := &tls.Config{
	MinVersion:   tls.VersionTLS12,
	Certificates: []tls.Certificate{cert}, // 热轮换可使用 GetCertificate / GetClientCertificate
	RootCAs:      caPool,
	ClientCAs:    caPool,
	ClientAuth:   tls.RequireAndVerifyClientCert,
	ServerName:   "beam-host.example.internal",
}
```

- **证书轮换**：在 `tls.Config` 中配置 `GetCertificate` 与 `GetClientCertificate` 可在不重启进程的情况下为后续新建连接加载新证书；OTP 侧新建 TLS distribution 连接时也会按 `ssl_dist.conf` 路径读取更新后的 PEM。
- **失败边界**：CA 不匹配、主机 SAN 与 `ServerName` 不符、缺少客户端证书或 cookie 不一致都会在认证阶段直接报错（`OutcomeNotSent`），绝不降级为明文连接。

## 背压与错误分类

原生 Erlang distribution 没有逐条消息的远端邮箱接受确认。`*erlang.Error` 通过 `Outcome` 明确区分：

- `OutcomeNotSent`：参数/编码校验失败、本地额度不足，或在 writer 编码提交前已撤回。
- `OutcomeUnknown`：任何部分可能已写入底层连接后发生超时、取消或断线；库不会自动重放。
- `OutcomeUnsupported`：发出前明确拒绝的不支持能力或类型。
- 远端进程 `DOWN` 在 `erlang.Error.Reason` 保留原始 Erlang reason `etf.Term`；普通 `{error, Reason}` 业务元组按正常 `etf.Term` 回复返回。

## 测试与许可

在 `erlang/` 目录执行：

```sh
GOWORK=off go test -race ./...
```

当 `PATH` 中存在 `erl`、`erlc` 和 `epmd` 时，测试会自动编译 `testdata/beam/tao_test_server.erl`，对真实 BEAM 节点验证 TCP 与 `inet_tls` 双向互通。

本模块采用 [MIT License](../LICENSE)。
