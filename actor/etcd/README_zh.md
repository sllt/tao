# Tao Actor etcd Directory

[English](README.md) | 简体中文

`github.com/sllt/tao/actor/etcd` 为 Actor 提供持久化、基于 CAS 的 `Directory` 实现。Go 包名为 `etcddirectory`，最低要求 Go 1.27；`actor` 核心和 `taonet` 不依赖 `etcd`。

## 接入

应用提供 etcd v3 客户端，并为各个集群选择独立的绝对 key 前缀。下面的函数创建一个使用该 `Directory` 的节点，等待应用取消 context 后依次关闭节点和客户端：

```go
import (
	"context"
	"errors"
	"time"

	"github.com/sllt/tao/actor"
	etcddirectory "github.com/sllt/tao/actor/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func run(ctx context.Context) (err error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://127.0.0.1:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()

	directory, err := etcddirectory.New(client, "/my-cluster/actors/v1")
	if err != nil {
		return err
	}
	node, err := actor.NewNode(actor.NodeOptions{
		Name:      "worker",
		Directory: directory,
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return node.Stop(shutdown)
}
```

客户端由应用拥有，`Directory` 不负责关闭它。多节点服务可通过 `StartNode` 启用网络；全局 `Ref`、`Replace` 和 `Handoff` 的使用见 [Actor 使用指南](../../docs/actor_zh.md)。

## 所有权与迁移

route 保存 `Owner`、`BootID`、`PID`、`Epoch`、`Kind`/`Version` 和状态；revision 取 etcd `ModRevision`。`Claim`、`Release`、`SwapInstance` 使用 CAS。`Release` 写墓碑，重新登记继续递增 `Epoch`，避免删除后的 ABA。owner 不绑定租约，断网不会自动转移所有权。

`PrepareMove` 将源 route 和 `MoveID` 记录放在同一事务。`CommitMove` 原子切换 route 到目标并记录 `Committed`；`AbortMove` 只能撤销未提交的准备。重复调用同一操作可读回结果。提交返回 `DirectoryError{Unknown: true}` 时，须通过 `ReadMove` 确认结果；查不清时不能恢复源实例。终态 `MoveID` 持久保留，没有自动 GC。

route 的 `MoveID` 保留上一条已提交交接的来源，供待处理 `Call` 的后续迁移验证；`SwapInstance` 和 `AbortMove` 不擦除该来源，`Release`/`Claim` 会重置。调用方不能根据缓存或回复丢失推断提交失败，也不能自动重放结果为 `Unknown` 的业务操作。

## Watch 与缓存

`Watch` 先读取 revision `R` 的完整快照，再从 `R+1` 订阅；compaction、非法删除或消费溢出会终止订阅。`actor.DirectoryCache` 清空失效缓存并重新订阅。

缓存只用于路由提示，不能判断迁移是否提交。etcd `Watch` 不具备线性一致读语义，所有权决定须使用 KV/CAS。

## 验证

从 `actor/etcd/` 目录运行：

```sh
GOWORK=off go test -race ./...
```

测试使用临时目录和随机 loopback 端口启动嵌入式 etcd，无需安装本机 etcd 服务。覆盖 CAS、epoch、竞争注册、迁移、`Watch`，以及提交响应丢失和 compaction；运行时集成测试还覆盖全局 `Send`/`Call`、`Replace` 和跨节点 `Handoff`。

更多模块说明见 [Actor README](../README_zh.md)，仓库环境与贡献方式见 [贡献指南](../../CONTRIBUTING_zh.md)。
