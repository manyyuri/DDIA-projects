# cdc — 变更数据捕获与数据集成

> DDIA §12。核心难题一句话：**上游在你拷贝的同时还在变**。这个包把"快照 + 增量"的切割点、schema 演化、以及重放安全性做成可验证的。

## 机制

```
源库（含变更日志）
  ├─ 快照：某一时刻的全量拷贝         ← 返回当时的 LSN
  └─ 变更流：LSN > 该点的所有 change   ← 从同一个 LSN 开始 tail

派生视图：按主键幂等 upsert（不是 append）
```

## 关键设计点

### 1. 一致切割点："快照的 LSN"必须被返回

```go
snap, lsn := src.Snapshot("people")   // 拷贝和取 LSN 是一个原子动作
Tail(src, view, "people", lsn)         // 从那个 LSN 接着流
```

如果先拷贝、后取 LSN（或者反过来），中间落地的写要么**丢**（拷贝完、取 LSN 之前写的没进快照也没进流），要么**重**（取 LSN 之后、拷贝完成之前写的两边都有）。因为应用是幂等的，"重"是安全的，"丢"不是——所以取 LSN 必须和快照在同一临界区里。

`TestBootstrapSnapshotPlusTailIsConsistent`：快照 50 行 → 期间再写 10 行 → tail 之后视图正好 60 行，`Diff` 为空。

### 2. 变更必须带 before 和 after 两个镜像

```go
type Change struct {
    Op     Op     // insert | update | delete
    Before Row    // insert 时为 nil
    After  Row    // delete 时为 nil
}
```

为什么 delete 也要带 before？因为下游可能从这些字段**派生过索引**（比如全文索引、二级索引、缓存 key）。只知道"主键 X 没了"是不够的，你得知道要从索引里删掉哪些词。

`TestChangesCarryBothImages` 断言了 insert 没有 before、update 两个镜像都对。

### 3. 幂等是订阅端唯一的硬要求

```go
if v.appliedLSN[c.LSN] { return nil }   // 至少一次投递，这里去重
v.rows[c.PK] = c.After.Clone()          // upsert，不是 append
```

投递保证是**至少一次**，所以同一条 change 可能来两次。`TestReplayingAChangeIsIdempotent` 把同一批投递三遍，`OpsApplied` 仍然是 1。

> **经典 CDC bug**：视图做成"事件计数"，重复投递就会把它算成 3 倍。所以派生视图必须按主键 upsert，或者用变更里的 LSN 去重。这是"至少一次"最容易被忽略的落地点。

### 4. Schema 演化：兼容与不兼容的分界线

| 变更 | 兼容性 | 为什么 |
|---|---|---|
| 加一列（带默认值） | **向后兼容** | 老行补默认值；老消费者忽略不认识的字段 |
| 加一列（NOT NULL 无默认） | 危险 | 老行没法回填 |
| 删一列 | **向后不兼容** | 还在 `SELECT` 它的消费者直接坏 |
| 改类型（int → string） | 视情况 | 需要 schema registry 给出兼容视图 |

`TestAddColumnIsBackwardCompatible` 演示了正确的做法：每个 change 打上**写入时的 schema 版本**（`SchemaVersion`），所以一个 v1 时期产生的变更，在 schema 已经升到 v2 之后**依然能被正确解码**（`SchemaAt(table, 1)` 还能取到 v1 定义）。

**没有这个版本号，老变更在新 schema 下解码就会错位**——这是 CDC 管道里最隐蔽的一类 bug。

`TestDropColumnBreaksNaiveConsumers` 展示了不兼容变更的样子：删列之后所有行都按新 shape 迁移了，还在读那一列的消费者会失败。

### 5. 重放是运维手段，不是恢复手段

```go
Replay(src, view, "people", 0)   // 从头重放，重建整个视图
```

用途：投影有 bug 修好之后重建、新上线一个搜索集群做 reindex。安全性仍然来自幂等 upsert——`TestReplayRebuildsTheView` 断言第二次重放**不产生任何新的应用**。

## 代码地图

| 位置 | 内容 |
|---|---|
| `Source` | 表 + 显式变更日志；`Insert/Update/Delete` 都产出带镜像的 change |
| `AddColumn` / `DropColumn` | schema 演化 + 现有行回填/迁移 |
| `Snapshot` | 返回 `(副本, LSN)` —— 一致切割点 |
| `View.Apply` | 幂等 upsert + LSN 去重 |
| `Bootstrap` / `Tail` / `Replay` | 快照+增量 / 增量 / 全量重放 |
| `Diff` | 源与视图的逐主键对账（持续跑的收敛性检查） |

## 实验

```bash
go test ./07-integration/cdc/ -v -count=1     # 7 个测试
```

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 快照 + tail | 不必停机 | 需要保留足够久的变更日志（超过快照耗时） |
| 变更带双镜像 | 下游可以维护任意派生索引 | 变更体积翻倍 |
| 幂等 upsert | 重放/重复投递安全 | 下游必须按主键写入，不能是 append-only |
| LSN 打在每个变更上 | 可去重、可断点续传、可重放 | 需要一个全序的位置（单主库容易，多主库难） |
| schema registry | 老变更可解码 | 额外的服务和版本管理 |
| 持续跑 `Diff` | 能发现静默分歧 | 额外扫描成本 |

## 自测题

1. 为什么快照和取 LSN 必须在同一个临界区？先取 LSN 再快照会丢什么？
2. delete 的变更为什么要带 before 镜像？只给主键行不行？
3. 视图做成"每来一条 insert 就 count++"会怎样？为什么"按主键 upsert"能避免？
4. 加列为什么兼容、删列为什么不兼容？schema registry 在里面扮演什么角色？
5. `Diff` 返回非空时你会怎么排查？（提示：先看 LSN 有没有断层，再看是不是有变更被死信/跳过了）
6. **反过来**：如果让你把这份设计压缩成一句话讲给同事，你会说什么？（提示：快照与流必须共享一个位置；订阅端必须幂等）

## 已知边界

- 变更日志是**显式实现**的，不是真的解析 Postgres 逻辑解码 / MySQL binlog。
- 只有一个源表、一个视图，没有多表 join 的派生、没有跨表事务边界的处理。
- schema registry 是一张内存表，没有兼容性检查（不会拒绝不兼容的变更，只会记录）。
- 没有回填进度跟踪、没有断路/背压、没有变更日志的保留期设置。
- `Diff` 是全量对账，没有增量校验（真实系统用 checksum + 抽样）。
