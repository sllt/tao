# Tao

[English](README.md) | 简体中文

**Tao** 是一个 Go Actor 运行时，提供串行消息处理、请求回复、监督树、跨节点通信和在线状态交接。
底层通信库 **[taonet](taonet/README_zh.md)** 可独立使用，支持单连接双向调用、有序消息通道和字节流。

## 核心特性

- **Actor 运行时**：按实例串行处理消息，有界邮箱，支持同步 `Call` 与异步 `Request`、父子监督树（`OneForOne`、`OneForAll`、`RestForOne`）以及生命周期观察（`Monitor` / `Link`）。
- **多节点与状态交接**：基于 `taonet` 的远程调用、稳定逻辑地址（`Ref`）、可选基于 CAS 的 [etcd Directory](actor/etcd/README_zh.md)，以及叶子 Actor 的本地热替换（`Replace`）与跨节点迁移（`Handoff`）。
- **taonet**：仅依赖 Go 标准库，内置自有 `HN-MUX/1` 多路复用、双向 TLS 1.3、流量分类隔离、窗口背压与明确的投递回执（`NotSent`、`Rejected`、`Responded`、`Unknown`）。
- **Web Observer**：可选的嵌入式只读 [Web 界面](actor/observer/README_zh.md)，用于查看监督树、邮箱、在途请求关系和 Actor 业务状态。
- **Erlang/OTP 消息互通**：提供零第三方依赖的 [Erlang distribution v6](erlang/README_zh.md) 与 [Tao Actor 适配器](actor/erlang/README_zh.md)，无需 BEAM sidecar 即可直接双向参与 `gen_server` / `GenServer` 调用、monitor 和 link。

## 模块与环境

仓库使用 Go workspace（`go.work`）管理多个模块；`actor` 与 `taonet` 可以组合或独立使用。

| 模块 | 导入路径 | 最低 Go 版本 | 说明 |
| :--- | :--- | :--- | :--- |
| [actor](actor/README_zh.md) | `github.com/sllt/tao/actor` | 1.27 | Actor 核心运行时、监督与远程通信适配 |
| [taonet](taonet/README_zh.md) | `github.com/sllt/tao/taonet` | 1.26 | 零第三方依赖的多路复用通信库 |
| [erlang](erlang/README_zh.md) | `github.com/sllt/tao/erlang` | 1.26 | 零第三方依赖的 Erlang distribution v6 与 ETF 编解码 |
| [actor/erlang](actor/erlang/README_zh.md) | `github.com/sllt/tao/actor/erlang` | 1.27 | 可选的 Tao Actor 与 Erlang `gen_server` 适配模块 |
| [actor/etcd](actor/etcd/README_zh.md) | `github.com/sllt/tao/actor/etcd` | 1.27 | 可选的 etcd Directory 模块 |
| [actor/codec/protobuf](actor/codec/protobuf/README_zh.md) | `github.com/sllt/tao/actor/codec/protobuf` | 1.27 | 可选的 Protobuf 业务消息编码模块 |

工作区使用 Go 1.27.3 工具链。包名保持为 `actor`（例如 `actor.NewNode(...)`）。
Actor 默认使用 CBOR 编码远程消息；etcd 和 Protobuf 依赖隔离在各自的可选模块中。

## 快速开始

克隆仓库后在根目录直接运行示例：

```sh
git clone https://github.com/sllt/tao.git
cd tao
go run ./actor/examples/counter
```

输出：

```text
counter=42
```

运行单连接双向 RPC 示例：

```sh
go run ./taonet/examples/reverse-rpc
```

输出：

```text
desktop client: hello over one connection
```

完整的 Actor 创建、消息发送与类型化请求回复示例见 [Actor README](actor/README_zh.md)。

## 示例

从仓库根目录运行：

| 场景 | 命令或说明 |
| :--- | :--- |
| 监督重启 | `go run ./actor/examples/recovery` |
| 双进程通信 | `go run ./actor/examples/two-node` |
| 本地行为替换 | `go run ./actor/examples/replace` |
| 跨节点状态交接 | `go run ./actor/examples/handoff` |
| Web Observer | `go run ./actor/examples/observer` |
| Erlang Distribution `gen_server` | `go run ./erlang/examples/genserver-call` |
| Tao Actor <-> Erlang 桥接 | `go run ./actor/erlang/examples/bridge` |
| HTTP 服务与 Observer | [HTTP 示例](actor/examples/http-api/README_zh.md) |
| 持续负载与故障恢复 | [Soak 示例](actor/examples/soak/README_zh.md) |

本地 Actor 只需要 `Node`、`Behavior`、`Spawn` 和 `Send` / `Call`。`Directory`、网络 codec 和迁移能力可按需引入。
`Replace` / `Handoff` 转移内存状态与符合条件的邮箱积压，不提供持久化事件日志或进程崩溃后的无损恢复。

## 文档与开发

- [文档索引](docs/README_zh.md)：使用指南和模块文档入口。
- [Actor 使用指南](docs/actor_zh.md)：身份、消息、监督、远程通信和状态交接。
- [taonet 使用指南](docs/taonet_zh.md)：通信原语、缓冲所有权、背压和连接管理。
- [贡献指南](CONTRIBUTING_zh.md)：仓库结构、测试与代码约定。

独立运行各核心模块测试：

```sh
(cd actor && GOWORK=off go test ./...)
(cd taonet && GOWORK=off go test ./...)
```

## 许可证

本项目采用 [MIT License](LICENSE)。
