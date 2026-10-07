# eventsourcing — 事件溯源与 CQRS

> DDIA §11.4（"Event Sourcing"）。核心是一个反转：**不存当前状态，存事实序列**。这一个小反转带来四个大好处，也带来一个新问题（事件版本永远不会变，改 schema 要极其小心）。

## 机制

```
命令 → 加载聚合（重放事件）→ 校验不变量 → 产出新事件
     → Append(stream, expectedVersion, events...)   ← 版本号 CAS
     → 投影（投影自己 tail 全局日志）→ 读模型
```

## 关键设计点

### 1. 版本 CAS 就是并发控制

```go
store.Append("acct", expectedVersion, events...)
// 流的当前版本 != expectedVersion → ErrVersionConflict
```

这是**整个聚合上的一次 compare-and-swap**。它同时干掉了两类问题：

| 问题 | 传统做法 | 事件溯源做法 |
|---|---|---|
| 丢失更新 | 行锁 / `SELECT FOR UPDATE` | 版本不匹配 → 冲突 → 重试 |
| 丢失更新之外的不变量违反 | 快照隔离的写偏斜（见 `mvcc`） | 版本 CAS 直接覆盖整个聚合 |

`TestConcurrentCommandsCannotDoubleSpend`：两个调用者都在版本 2 读到余额 100，各自判断"能取 80"。第一个 append 成功；第二个拿到 `ErrVersionConflict`；重试时读到余额 20，"不能取 80" → 被余额规则拒绝。

**重点**：并发安全不是靠锁，而是靠"读完到写回之间版本不能被别人动"。**决策（校验不变量）和写入（CAS）必须是同一个原子步骤**，这正是 `Execute` 封装的东西。

### 2. 命令是纯函数，重试天生安全

```go
func Execute(store, id, command func(*Account) ([]Event, error)) error {
    for attempt := 0; attempt < 5; attempt++ {
        agg := LoadAccount(store, id)      // 每次都重新加载
        events, err := command(agg)         // 重新决策
        if err != nil { return err }
        if err := store.Append(id, agg.Version, events...); err == nil { return nil }
        // 冲突 → 重来
    }
}
```

因为命令**完全由已加载的状态决定、且没有副作用**，重试永远安全。这是事件溯源在高冲突场景下比锁更优雅的原因。

### 3. 读模型是可丢弃的

```go
p.Rebuild(store)   // 清空 + 从位置 0 重放
```

`TestProjectionCanBeRebuiltFromScratch` 故意把读模型改错（`balances["a"] = 999999`），然后 `Rebuild` 就恢复了。这带来两个运维上的巨大便利：

- 投影逻辑有 bug → 修代码、重建，不用回滚数据库；
- 想加一个新的查询维度 → 新起一个投影，从头追，不影响主流程。

代价是**最终一致**：`Checkpoint()` 之后的事件还没进读模型，读它会看到旧值。

### 4. 快照只是优化，删光不改变任何行为

```go
full := LoadAccount(store, "a")                  // 重放全部事件
fast := LoadAccountFast(store, snaps, "a")       // 快照 + 增量
empty := LoadAccountFast(store, NewSnapshotStore(), "a")  // 没有快照
```

`TestSnapshotPlusReplayEqualsFullReplay` 断言三者的**余额和版本完全相同**——包括"快照之后又发生了新事件"的情况（`LoadAccountFast` 里那个 `e.Version > a.Version` 的过滤就是干这个的）。

**如果快照是真相，这个测试就会失败**。这条测试就是这个包最重要的一条断言：**事件是真相，快照是缓存。**

### 5. 全局位置（GlobalPos）是投影的游标

事件有两个序号：`Version`（流内，做 CAS 用）和 `GlobalPos`（全局，做投影游标用）。混用会出 bug：用 `Version` 当游标，多个流的版本号会互相干扰。

## 代码地图

| 位置 | 内容 |
|---|---|
| `EventStore.Append` | 版本 CAS + 分配 `Version` / `GlobalPos` |
| `EventStore.LoadFrom` | 全局日志 tail（投影/CDC 消费者的读法） |
| `Account` + `apply` | 聚合与折叠（**没有任何 setter**） |
| `Execute` | 加载 → 决策 → CAS → 重试 的完整闭环 |
| `Projection` | 读模型、`CatchUp`、`Rebuild`、`Checkpoint` |
| `LoadAccountFast` | 快照 + 增量（与全量重放等价） |

## 实验

```bash
go test ./07-integration/eventsourcing/ -v -count=1     # 7 个测试
go test ./07-integration/eventsourcing/ -run 'Concurrent|Snapshot' -v
```

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 存事件而非状态 | 完整审计日志、可时间旅行、可重建 | 存储无限增长（靠快照 + 归档）；**事件不可变，改 schema 极难** |
| 版本 CAS | 无锁并发安全，冲突可重试 | 高冲突下重试率上升 |
| 投影（CQRS） | 读模型各取所需，可重建 | 最终一致；多份读模型要各自维护 checkpoint |
| 快照 | 恢复快 | 额外的存储与一致性逻辑（必须能和事件重放对得上） |
| 纯函数命令 | 重试安全 | 命令不能有外部副作用（发邮件、调支付要挪到事件订阅侧） |

## 自测题

1. 为什么"版本 CAS"能解决 `mvcc` 里的写偏斜，而行级冲突检测不能？（提示：CAS 的粒度是整个聚合）
2. 命令为什么要写成纯函数？如果不纯（比如中间发了一条短信），重试会发生什么？
3. 快照和事件重放会不会出现不一致？什么情况下会？（提示：快照的版本号必须和事件的版本号对齐）
4. 为什么投影要用 `GlobalPos` 而不是 `Version` 当游标？
5. 事件 schema 要改一个字段名，你会怎么做？（提示：加新字段 + 双写过渡；永远不要原地改历史事件）
6. 如果一个投影挂了三天，恢复时会发生什么？为什么不需要"重跑上游业务"？

## 已知边界

- 全内存事件存储，没有持久化、没有分段、没有归档。
- 没有事件 schema 版本化与 upcasting（把老事件"升级"成新形状的适配层）——这是生产事件溯源最重的一块。
- 投影没有独立进程/独立 checkpoint 存储，"重建"是同步的全量重放。
- 没有订阅推送（`CatchUp` 是拉模式轮询）。
- 没有跨聚合的 saga / 流程管理器（跨聚合一致性要另做编排）。
