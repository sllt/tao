# Tao Actor Protobuf Codec

[English](README.md) | 简体中文

`github.com/sllt/tao/actor/codec/protobuf` 是可选的业务消息编解码模块，包名为 `protobuf`，最低要求 Go 1.27。它只替换 Actor 的业务编码，不改变 `HA/2` 框架信封，也不进入 `taonet` 的依赖。

## 配置

向 `NodeOptions` 传入 `Codec`，并注册生成消息的指针类型：

```go
import (
	"github.com/sllt/tao/actor"
	actorpb "github.com/sllt/tao/actor/codec/protobuf"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newNode() (*actor.Node, error) {
	return actor.NewNode(actor.NodeOptions{
		Name:  "protobuf-node",
		Codec: actorpb.Codec{},
		Register: []actor.MessageType{
			{Name: "example.payload", Version: 1, Sample: (*wrapperspb.BytesValue)(nil)},
		},
	})
}
```

调用者检查返回错误，并在退出时调用 `Node.Stop(ctx)`。启用网络和建立连接见 [Actor 使用指南](../../../docs/actor_zh.md)；通信双方应使用相同的 codec，并注册所用请求与回复的匹配类型。只有实现 `proto.Message` 的生成消息指针可以注册。

## 编码契约

- 未知字段按 Protobuf 的兼容规则处理；相同消息名和版本的 schema 应在节点间保持一致。
- 正文首字节为 presence：`0` 表示 typed `nil`，后面不能有数据；`1` 后跟 Protobuf 消息。空的非 `nil` 消息与 typed `nil` 保持区分。
- 解码递归上限为 32，解码结果不引用输入缓冲。
- `Codec` 实现可选 `MessageAppender`，可将 presence 和正文直接追加到 Actor 信封缓冲；公开 `Marshal` 返回独立字节切片。

## 验证

从仓库根目录运行：

```sh
go test ./actor/codec/protobuf/...
```

独立验证模块及 race 时，从 `actor/codec/protobuf/` 目录运行：

```sh
GOWORK=off go test -race ./...
```

更多编码与注册约定见 [Actor README](../../README_zh.md)，仓库环境见 [贡献指南](../../../CONTRIBUTING_zh.md)。
