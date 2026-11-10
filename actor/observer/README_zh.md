# Tao Actor Web Observer

[English](README.md) | 简体中文

`github.com/sllt/tao/actor/observer` 提供可选、只读的 Web 观察入口。静态页面内嵌在 Go 二进制中，不需要 Node.js 或前端依赖。运行时快照与 `Inspector` 位于 `actor` 核心；HTTP 服务、宿主进程内存采样和页面位于本包。

## 快速开始

从 `actor/` 运行完整演示：

```sh
go run ./examples/observer -listen 127.0.0.1:9911
```

打开 `http://127.0.0.1:9911`。演示包含两个通过 TCP 通信的节点、监督父子、慢邮箱、周期性故障重启和远程 `Call`。退出时用 `Ctrl-C` 关闭 HTTP 与节点。

## 嵌入应用

已有程序传入本进程中的节点：

```go
import (
	"context"
	"time"

	"github.com/sllt/tao/actor"
	"github.com/sllt/tao/actor/observer"
)

func observe(ctx context.Context, node *actor.Node) error {
	view, err := observer.New(node)
	if err != nil {
		return err
	}
	server, err := view.Listen("") // 默认 127.0.0.1:9911
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	case <-server.Done():
		return server.Err()
	}
}
```

`observer.New` 可传入多个节点，节点名须唯一；它同时启用这些节点的诊断计数。`Observer` 实现 `http.Handler`，也可挂载到已有 HTTP 服务的根路径，由应用提供访问控制。关闭 Observer 的 HTTP 服务后，应用仍负责调用 `Node.Stop` 关闭节点。

## 访问服务器上的 Observer

在服务器启动同样的 loopback 监听后，可在本机建立 SSH 隧道：

```sh
ssh -N -L 19911:127.0.0.1:9911 user@server
```

随后打开 `http://127.0.0.1:19911`，退出隧道使用 `Ctrl-C`。页面只展示传给 `New` 的进程内节点；其他进程各自运行 Observer，通过各自的地址访问。

## 页面与数据

- **概览**：在册 Actor、执行者、业务邮箱、pending `Call`、异步 `Request`、冻结/准备/无响应数量，以及整个宿主进程的 Go 堆和 goroutine 数。
- **Actor**：真实父 `PID` 的监督树、路径/`Kind` 筛选、分页、具体实例详情、`Monitor`/`Link`、`Epoch`/`MoveID`。
- **指标**：`Receive` 次数/失败/耗时、当前消息类型、邮箱高水位/拒绝、当前监督重启链次数。
- **调用与网络**：本节点 pending `Call` 的调用者/接收实例、等待时间、候选等待环、`taonet` 物理连接分类与队列/容量摘要。

`Stats.AsyncRequests` 和 Actor 详情的 `AsyncRequests` 包含运行中的异步 `Request` 及等待 `Receive` 处理的完成结果；已完成结果不再计入 `PendingCalls`。本节点发起的异步调用带 `Asynchronous` 标记，页面将其与同步等待区分，也不将其作为等待环的边。

### 采集开销

`observer.New` 调用 `Node.EnableDiagnostics`，计数从启用或新实例创建时开始。页面约每秒拉取一次，不自动轮询业务 `Inspector`。

## HTTP API

| GET API | 参数与返回 |
| :--- | :--- |
| `/api/nodes` | 可见节点名、版本、统计 |
| `/api/snapshot` | `node`、`after` 数字游标、`limit`（默认 250，最大 1000）、`filter`；返回 `Node` 和进程级 `Host` |
| `/api/actor` | `node`、`pid`（完整 `pid://...`）；返回实例元数据及关系 |
| `/api/inspect` | `node`、`pid`；1 秒等待，返回独立 JSON `Value` |

只有一个节点时可省略 `node`。JSON 字段名称与 Go 公开结构体一致。Snapshot 最多采样 1000 条 `Call`，详情最多展示 1000 条关系。等待环只覆盖当前可见的本地调用边，`DeferredReply` 可能解除等待，不能将其当成已证明的死锁。

## 业务状态检查

业务按需实现独立接口：

```go
func (a *Counter) Inspect(ctx context.Context) (any, error) {
	return struct{ Value int64 }{Value: a.value}, nil
}
```

`Node.Inspect(ctx, pid)` 在 Actor 执行边界串行运行，与 `Receive` 不并发；JSON 编码也在恢复业务执行前完成，不向 HTTP goroutine 暴露可变 `map`/`slice`。

每个实例只允许一个待执行或执行中的检查，重复请求返回 `ErrCapacity`。编码结果受 `NodeOptions.MaxSnapshotBytes` 限制。未实现接口返回 `ErrInspectUnsupported`。

## 验证

从 `actor/` 目录运行：

```sh
go test ./observer
go test -race ./observer
```

更多用法见 [Actor README](../README_zh.md) 和 [Actor 使用指南](../../docs/actor_zh.md)。
