# Tao Actor

[English](README.md) | 简体中文

`github.com/sllt/tao/actor` 提供串行消息处理、有界邮箱、请求回复、监督树和跨节点通信。
Go 包名为 `actor`，最低要求 Go 1.27。

## 快速开始

在仓库根目录运行：

```sh
go run ./actor/examples/counter
```

下面的程序创建一个 Actor，并通过类型化调用获取回复：

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/sllt/tao/actor"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	node, err := actor.NewNode(actor.NodeOptions{Name: "local"})
	if err != nil {
		return err
	}
	defer node.Stop(ctx)

	pid, err := node.Spawn(actor.FromFunc(func(self *actor.Actor, message any) error {
		name, ok := message.(string)
		if !ok {
			return nil
		}
		err := self.Reply("hello " + name)
		if errors.Is(err, actor.ErrReplyExpired) {
			return nil
		}
		return err
	}))
	if err != nil {
		return err
	}

	reply, err := node.Call[string](ctx, pid, "Tao")
	if err != nil {
		return err
	}
	fmt.Println(reply)
	return nil
}
```

输出 `hello Tao`。有实例状态的业务使用结构体嵌入 `actor.Actor`，实现 `Receive(any) error`；每次 `Spawn` 或重启由 `Factory` 创建新实例。`FromFunc` 适合无状态服务和测试（闭包捕获的可变状态不会自动按实例隔离）。

## 主要接口

| 操作 | 入口 | 结果 |
| :--- | :--- | :--- |
| 创建实例 | `Node.Spawn` / `Actor.Spawn` | 等待 `Started` 完成后返回 `PID` |
| 单向消息 | `Node.Send` / `Actor.Send` | 返回准入或投递错误，不等待业务完成 |
| 同步请求 | `Node.Call[T]` / `Actor.CallAs[T]` | 返回类型化结果和错误 |
| 有序调用准入 | `Node.BeginCall` | 同步完成具体本地 `PID` 邮箱准入，通过无阻塞回调发布 `CallCompletion` |
| 异步请求 | `Actor.Request` | 返回 `RequestID`，后续在 `Receive` 中处理 `CallResult` |
| 回复 | `Reply` / `ReplyError` | 向当前调用者返回业务结果或业务拒绝 |
| 延后回复 | `Defer` | 返回可在外部 goroutine 或后续消息中使用的一次性回复凭据 |
| 观察生命周期 | `Monitor` / `Link` | 处理 `Down` / `Exit` 消息 |
| 状态交接 | `Replace` / `Handoff` | 替换本地行为或跨节点迁移叶子 Actor |

`NewNode` 不启动网络监听；远程通信使用 `StartNode` 和 `Connect`。
`PID` 标识具体实例，重启后失效；`Ref` 标识逻辑路径。使用 `ParseRef` 校验配置或外部输入。

## 使用约定

- `Receive` 返回非 nil 错误或发生 panic 会触发监督。业务拒绝通过 `ReplyError` 回复；调用者不再等待导致的 `ErrReplyExpired` 可由业务明确忽略。
- Actor 的状态和能力在 `Receive` 的同步调用栈中使用。异步 `Request` 的完成结果也由 `Receive` 串行处理。
- 本地消息直接传递内存引用，发送后不要修改已交付的 `map`、`slice` 等数据。
- 邮箱与请求数量有上限；显式处理 `ErrMailboxFull`、`ErrCapacity` 和调用超时，不将接受消息当作业务执行完成。
- 远程操作的 `Unknown` 结果不会自动重放。`Replace` / `Handoff` 不提供进程崩溃后的持久化恢复。

完整语义、异步请求容量及迁移条件见 [Actor 使用指南](../docs/actor_zh.md)。

## 示例与扩展

| 示例或模块 | 说明 |
| :--- | :--- |
| [counter](examples/counter) | 本地发送与请求回复 |
| [recovery](examples/recovery) | 监督重启、`PID` 与 `Ref` |
| [two-node](examples/two-node) | 两个 OS 进程之间的远程调用 |
| [replace](examples/replace)、[handoff](examples/handoff) | 行为替换与跨节点状态交接 |
| [HTTP 服务](examples/http-api/README_zh.md) | 请求映射、业务错误和优雅关闭 |
| [Web Observer](observer/README_zh.md) | 监督树、邮箱与状态检查 |
| [etcd Directory](etcd/README_zh.md) | 跨节点逻辑地址与所有权 |
| [Protobuf Codec](codec/protobuf/README_zh.md) | 可选业务消息编码 |
| [Erlang 适配器](erlang/README_zh.md) | 原生 Erlang/OTP distribution v6 与 `gen_server` 消息互通 |
| [持续负载（Soak）](examples/soak/README_zh.md) | 混合调用与故障恢复验证 |

## 测试与许可

在本目录执行：

```sh
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```

开发约定见 [贡献指南](../CONTRIBUTING_zh.md)。本模块采用 [MIT License](../LICENSE)。
