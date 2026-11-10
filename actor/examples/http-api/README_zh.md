# HTTP + Actor 示例

[English](README.md) | 简体中文

这个示例把 HTTP 请求交给按账户串行处理的 Actor（`accounts/<id>`），展示懒创建、业务拒绝、超时和只读 Observer。账户、交易计数和审计保存在内存中，实例异常重启或进程退出可能丢失状态；示例未实现持久化、幂等键、事务日志和可靠审计投递。

## 运行

在 `actor/` 目录运行：

```sh
go run ./examples/http-api
```

默认使用随机 loopback 端口执行自测，然后关闭 HTTP 与 Node。自测向 `taozi` 账户并发存入 20 次、每次 10，再取出 50；检查响应后打印 `balance=150 tx_count=21 audit_events=21` 和账户 `PID`。

持续监听并启用独立 Observer：

```sh
go run ./examples/http-api -serve 127.0.0.1:8080 -observer 127.0.0.1:9912 -call-timeout 5s
```

在另一个终端调用账户 API：

```sh
curl -X POST http://127.0.0.1:8080/accounts/alice/deposit \
  -H 'Content-Type: application/json' -d '{"amount":100}'
curl http://127.0.0.1:8080/accounts/alice
curl -X POST http://127.0.0.1:8080/accounts/alice/withdraw \
  -H 'Content-Type: application/json' -d '{"amount":30}'
```

`-observer` 默认关闭，只接受显式 loopback IP 和数字端口，例如 `127.0.0.1:9912` 或 `[::1]:9912`；打开 `http://127.0.0.1:9912` 可查看账户监督树、邮箱、请求和账户 `Inspect` 快照。

## 输入与响应

| 方法与路径 | 行为 |
| :--- | :--- |
| `GET /accounts/{id}` | 查询账户，首次访问时懒创建 |
| `POST /accounts/{id}/deposit` | 存入 `amount` |
| `POST /accounts/{id}/withdraw` | 取出 `amount` |

账户 ID 长度为 1～64 字节，只接受 ASCII 字母、数字、`_`、`-`。首次访问会创建 `accounts/<id>`。存取请求只接受单个 JSON 对象、正 `int64` 整数 `amount`，正文上限 1 KiB；未知字段、额外 JSON 值、非整数和溢出都会拒绝。

成功响应包含 `id`、`balance`、`tx_count`、`pid`。失败响应包含 `error` 和 `outcome`：

| 情况 | HTTP | `outcome` |
| :--- | :--- | :--- |
| 无效地址或输入 / 正文过大 | 400 / 413 | `not_sent` |
| 余额不足或账户数值达到上限 | 409 | `responded` |
| 邮箱已满 | 503 | `rejected` |
| 请求容量不足 | 503 | `not_sent` |
| 实例不存在或 `PID` 已失效 | 404 | `not_sent` |
| 实例正在启动或重启 | 503 | `rejected` |
| 操作超时 / 调用方取消 | 504 / 408 | `unknown` |
| 远端明确返回 `Unknown` | 502，超时仍为 504 | `unknown` |
| 停止、不可响应或意外错误 | 503 / 500 | 保守标记 `unknown` |

## 失败与重试边界

服务端不自动重试业务操作。懒创建只处理明确的 `ErrNoProcess` / `ErrStarting`，`Unknown` 不会进入补建路径。超时、取消或回复丢失不能证明操作未执行；Actor 用 `ReplyError` 返回业务拒绝，只把 `ErrReplyExpired` 视为可丢弃的迟到回复。

## 验证

从 `actor/` 目录运行示例测试：

```sh
go test ./examples/http-api
go test -race ./examples/http-api
```

更多用法见 [Actor README](../../README_zh.md)、[Web Observer](../../observer/README_zh.md) 和 [Actor 使用指南](../../../docs/actor_zh.md)。
