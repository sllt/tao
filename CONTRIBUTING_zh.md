# 贡献指南

[English](CONTRIBUTING.md) | 简体中文

## 开发环境

克隆仓库后使用 Go 1.27.3 工具链。根目录的 `go.work` 包含 `actor`、`taonet`、`actor/etcd` 和 `actor/codec/protobuf` 四个模块，便于统一开发与联调。

```sh
git clone https://github.com/sllt/tao.git
cd tao
go run ./actor/examples/counter
```

各模块的相对 `replace` 指向仓库内依赖，便于直接修改与联调。根目录是工作区，不是包含全部包的单一 module。

## 仓库结构

| 目录 | 内容 |
| :--- | :--- |
| `actor/` | Actor 运行时、默认 CBOR codec、生命周期与网络适配 |
| `actor/observer/` | 可选 Web Observer |
| `actor/etcd/` | 独立 etcd Directory 模块 |
| `actor/codec/protobuf/` | 独立 Protobuf codec 模块 |
| `taonet/` | 仅依赖标准库的通信层与内部 mux |
| `actor/examples/`、`taonet/examples/` | 可运行示例 |
| `docs/` | 使用指南 |

## 修改原则

- 优先具体类型、普通函数和清楚的状态所有权。只有实际需要时再增加抽象或配置。
- 校验放在配置、网络和公共 API 边界；保留协议、内存及并发正确性检查，避免内部重复校验与静默兜底。
- `actor` 与 `taonet` 保持严格依赖分层。`etcd` 和可选 codec 留在独立模块中，不进入 `taonet` 核心。
- 明确区分本地发送、远端接收确认和业务执行完成。`Unknown` 结果不自动重放。
- 公开 API 或协议变更时同步更新示例和相关文档。

## 测试

在仓库根目录测试整个工作区：

```sh
go test ./actor/... ./taonet/... ./actor/etcd/... ./actor/codec/protobuf/...
```

也可以从对应模块独立运行测试，验证不使用工作区时的依赖解析：

```sh
(cd actor && GOWORK=off go test ./...)
(cd taonet && GOWORK=off go test ./...)
(cd actor/etcd && GOWORK=off go test ./...)
(cd actor/codec/protobuf && GOWORK=off go test ./...)
```

并发状态或资源生命周期修改应执行 race 检查：

```sh
(cd actor && GOWORK=off go test -race ./...)
(cd taonet && GOWORK=off go test -race ./...)
```

测试使用临时目录、loopback 随机端口和嵌入式 etcd，无需安装外部常驻服务。
用明确的同步点验证并发行为；超时用于终止卡住的测试，不用极小时间阈值断言机器性能。

## 提交约定

- Go 源码使用 `gofmt` 格式化。
- 提交说明写清问题背景、行为变化与验证方式。
- 公开 API 变化时同步更新示例和文档。
