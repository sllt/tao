# 文档索引

[English](README.md) | 简体中文

## 使用指南

- [Actor 使用指南](actor_zh.md)：身份（`PID` 与 `Ref`）、消息与请求、监督树、远程通信和状态交接（`Replace` / `Handoff`）。
- [taonet 使用指南](taonet_zh.md)：双向会话、消息交付回执、缓冲所有权、Channel、Stream 和连接管理。
- [Erlang/OTP 互通使用指南](erlang_zh.md)：原生 distribution v6、ETF 数据所有权、`gen_server` 消息、monitor/link 与 Tao Actor 桥接。

## 模块与扩展

- [Actor 模块](../actor/README_zh.md)（`github.com/sllt/tao/actor`）
- [taonet 模块](../taonet/README_zh.md)（`github.com/sllt/tao/taonet`）
- [Erlang Distribution 模块](../erlang/README_zh.md)（`github.com/sllt/tao/erlang`）
- [Actor Erlang 适配器](../actor/erlang/README_zh.md)（`github.com/sllt/tao/actor/erlang`）
- [Web Observer](../actor/observer/README_zh.md)（`github.com/sllt/tao/actor/observer`）
- [etcd Directory](../actor/etcd/README_zh.md)（`github.com/sllt/tao/actor/etcd`）
- [Protobuf Codec](../actor/codec/protobuf/README_zh.md)（`github.com/sllt/tao/actor/codec/protobuf`）

## 示例

- [HTTP 服务与 Observer](../actor/examples/http-api/README_zh.md)
- [持续负载与故障恢复（Soak）](../actor/examples/soak/README_zh.md)

开发环境、测试与提交约定见 [贡献指南](../CONTRIBUTING_zh.md)。
