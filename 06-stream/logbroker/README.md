# logbroker — 分区日志与消费组

> DDIA §11.1–11.2。消息队列和日志的区别只有一句话：**offset 是消费者的位置，不是消息的属性**。这一句话带来重放、多订阅者、以及"再均衡会重复"的全部后果。

## 机制

```
Topic
 ├─ Partition 0: [seg][seg][seg]  →  offset 0..N（只追加，永不修改）
 ├─ Partition 1: [seg][seg]
 └─ Partition 2: [seg]

生产者：key -> 分区（同 key 同分区 ⟹ 该 key 内有序；空 key 轮询 ⟹ 无序）
消费者组：分区被分配给组内成员，成员各自提交 offset
retention：整段删除旧 segment，log start offset 前进
```

## 关键设计点

### 1. 顺序只在分区内存在

```go
topic.PartitionForKey("customer-42")   // 永远同一个分区
```

这是**唯一**能拿到 per-key 有序的方法。`TestKeyedOrderingAndPartitionAffinity` 验证 100 条同 key 记录落在同一分区且 offset 严格递增、顺序等于追加顺序。

而空 key（为了吞吐轮询分散）就没有顺序保证——测试断言它至少落在 2 个分区上。**要吞吐还是要顺序，是一个显式的选择。**

### 2. offset 是逻辑位置，所以重放是免费的

```go
recs, _ := p.Read(0, 1000)   // 从 0 重读，拿到完全一样的序列
```

Broker 不因为"你消费过了"就删数据。这带来：

- 同一个 topic 可以被**多个消费者组**独立消费，互不影响；
- 出 bug 了可以**从头重放**；
- 新增一个下游（搜索引擎、数仓）不需要上游改任何东西。

### 3. Retention 会让老 offset 失效——但 offset 永不重用

```go
p.TruncateBefore(6)
p.Read(0, 10)   // → ErrOffsetOutOfRange（明确报错，不是静默返回空）
p.Append(...)   // → 新 offset 接着 12、13…（不会回到 0）
```

**必须报错而不是返回空**：返回空会让消费者以为"这一段没有数据"，而实际上是"数据被删了"。这两者的正确恢复动作完全不同（前者继续，后者得回落到 log start offset）。

消费者内部处理就是 `c.offsets[p] = LogStartOffset()`（丢弃一段数据，通常要告警）。

### 4. 再均衡：offset 跟着分区走，不跟着消费者走

```go
a.Commit(partition, offset)   // 提交的是"分区 p 我处理到哪了"
```

`TestOffsetsFollowThePartitionAcrossRebalance`：消费者 A 提交了 offset，B 加入后接管了那个分区，B 的起始位置就是 A 提交的位置——**不会从头重读**。如果 offset 挂在消费者身上（或没提交），每次扩容都要重放全量。

### 5. At-least-once 是默认保证，重复从哪来

```
处理完了，但还没来得及 commit offset → 再均衡 → 分区给了别人 → 别人从头读
```

`TestRebalanceCanReplayUncommittedWork` 精确复现这个窗口：`RecordOffsets` 只记录了进度，`Join` 触发再均衡后，未提交的工作被重新投递。

**所以"exactly once"不是 broker 能提供的属性。** broker 给的是"至少一次 + 稳定顺序"，幂等要消费者自己做（见 `06-stream/windowing` 的幂等 sink）。Kafka 的事务/幂等生产者是在这个基础上又加了一层，本质仍然是"把去重责任从消费者挪到 broker"。

## 代码地图

| 位置 | 内容 |
|---|---|
| `Segment` / `Partition` | 分段落盘、`Append`/`Read`/`TruncateBefore`、high watermark vs log start offset |
| `Topic` | 分区选择（key 哈希 / 空 key 轮询） |
| `Group` | 成员管理、`rebalanceLocked`（range 策略）、offset 迁移 |
| `Consumer` | `Commit`（处理完之后提交 = at-least-once） |

## 实验

```bash
go test ./06-stream/logbroker/ -v -count=1     # 6 个测试
make exp pkg=./06-stream/logbroker run=TestRetention
```

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 分区（而非单一日志） | 并行吞吐 | 跨分区无序、跨分区无事务 |
| key 哈希分区 | per-key 有序 | 热点 key → 热分区 |
| 空 key 轮询 | 均匀 | 完全无序 |
| offset 存在消费者侧 | broker 简单、支持多组独立消费 | 消费者崩溃会重复（at-least-once） |
| 整段删除（不是逐条） | 快速、顺序写友好 | 删除粒度粗；被删掉的数据读起来是**错误** |
| range 分配策略 | 实现简单、分区连续 | 成员变化时搬动多、倾斜机会大（round-robin/sticky 更好） |

## 自测题

1. 为什么"同 key 同分区"是获得 per-key 有序的**唯一**方法？如果分区数变了呢？
2. retention 删掉老数据后，`Read(0)` 应该返回空还是报错？为什么？
3. 再均衡造成重复的窗口在哪一步？怎么把它缩到最小（提示：commit 的时机）？
4. 消费者组有 3 个成员、topic 只有 2 个分区，会发生什么？为什么？
5. 如果要求"跨分区的全局顺序"，你会怎么设计？（提示：单分区=放弃并行；或者引入全局序号 + 消费者侧重排）

## 已知边界

- 没有真正的组协调协议（leader 选举、会合、代际 fencing），`rebalanceLocked` 是同步模拟的。
- 只有 range 一种分配策略，没有 round-robin / sticky / cooperative rebalance。
- retention 只能按"offset 之前"手动触发，没有按时间/大小自动触发。
- 没有副本与 ISR（分区本身没有容错）。
- 没有生产者幂等、没有事务、没有压缩（compaction）型 topic。
