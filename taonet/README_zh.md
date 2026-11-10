# taonet

[English](README.md) | 简体中文

`github.com/sllt/tao/taonet` 是一个仅依赖 Go 标准库的双向通信模块，提供 `Request`、`Notify`、`Channel` 和 `Stream`。两端可以在同一 `Session` 上发起请求和开流；`Endpoint` 管理连接复用、流量隔离、配额与优雅关闭。核心不依赖 `actor`、`etcd` 或业务消息编码器。

## 环境与模块

- **最低 Go 版本**：1.26
- **模块与导入路径**：`github.com/sllt/tao/taonet`，包名 `taonet`
- **工作区**：仓库使用 Go 1.27.3 工具链，根目录 `go.work` 关联本模块与 `actor` 模块。

## 快速开始

在本目录运行双向 RPC 示例：

```sh
go run ./examples/reverse-rpc
```

服务端通过同一 `Session` 回调客户端，输出：

```text
desktop client: hello over one connection
```

应用通过 `NewMux` 注册路由，在 `SessionConfig` 中设置本地 `Peer` 和路由表，再用 `Dial`、`Accept` 或 `Endpoint` 建立连接。请求处理函数可以通过 `Request.Session()` 发起反向调用，完整代码见 [reverse-rpc](examples/reverse-rpc/main.go)。

## 通信方式

| API | 用途 | 结果边界 |
| :--- | :--- | :--- |
| `Session.Request` | 有限请求与业务回复 | 已收到处理函数的完整回复 |
| `Session.Notify` / `NotifyConfirmed` | 单向通知 | 本地接受 / 远端通知队列接受 |
| `Session.OpenChannel` | 双向有序消息与逐条确认 | 打开通道后，按发送方法区分入队、接受确认或物理写入 |
| `Session.OpenStream` | 双向字节流、文件传输 | 打开字节流，业务结果由应用协议定义 |

接受确认与业务完成严格区分。借用输入的发送必须等待 `Released()` 后才能复用缓冲；取消和 `Unknown` 不会自动重放业务操作。具体契约见 [taonet 使用指南](../docs/taonet_zh.md)。

普通连接使用 TLS 1.3、双向证书校验和 `taonet/1` ALPN。仓库示例显式使用 loopback 明文便于直接运行；TLS 与身份配置见 [身份与 TLS](../docs/taonet_zh.md#身份与-tls)。

## 示例

以下命令均在 `taonet/` 目录执行：

| 示例 | 命令 | 内容 |
| :--- | :--- | :--- |
| [双向 RPC](examples/reverse-rpc/main.go) | `go run ./examples/reverse-rpc` | 在同一连接中回调请求发起端 |
| [双向消息](examples/duplex-chat/main.go) | `go run ./examples/duplex-chat` | `Channel`、发送回执、接受与释放 |
| [文件传输](examples/file-copy/main.go) | `go run ./examples/file-copy` | 固定缓冲上传、半关闭后读取摘要 |
| [连接隔离](examples/isolated-peer/main.go) | `go run ./examples/isolated-peer` | 四类物理连接与仅出站客户端 |
| [底层多路复用](examples/mux-echo/main.go) | `go run ./examples/mux-echo` | 底层流的双向开流与半关闭 |

## 文档

- [taonet 使用指南](../docs/taonet_zh.md)：生命周期、错误结果、输入所有权、`Endpoint`、TLS 与配额。
- [项目文档](../docs/README_zh.md)：其他模块的使用入口。
- [贡献指南](../CONTRIBUTING_zh.md)：仓库开发与验证方式。

公共 API 位于模块根包，底层多路复用实现在 [internal/mux](internal/mux)。

## 测试

在本目录执行：

```sh
go test ./...
go test -race ./...
GOWORK=off go test ./...
```

`GOWORK=off` 用于独立验证该模块，避免根工作区的其他模块参与解析。测试使用 loopback 和内存连接，无需运行外部服务。

## 许可

本项目使用 [MIT License](../LICENSE)。
