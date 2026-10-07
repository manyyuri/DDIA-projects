# clocks — 逻辑时钟、混合逻辑时钟、分布式 ID

> DDIA §8.3–8.4。"时间"在分布式系统里是一个**陷阱**：你无法相信任何一台机器的墙钟，但你有四种替代品，能力依次递增、代价也依次递增。

## 能力阶梯

| 时钟 | 能回答 | 不能回答 | 代价 |
|---|---|---|---|
| **Lamport** | 因果顺序（`a → b` ⟹ `L(a) < L(b)`） | **并发还是有序？**（反向不成立） | O(1) |
| **向量时钟** | 因果顺序 **+ 并发检测** | 规模（大小随节点数增长） | O(N) |
| **HLC** | 近似物理时间 + 因果 | 仍受时钟偏移上界约束 | O(1)，一个 `{物理, 逻辑}` |
| **Snowflake** | 可排序的唯一 ID | 不表达因果，只表达"谁什么时候生成的" | 依赖大致正确的墙钟 |

## 关键设计点

### 1. Lamport 的致命局限：反推不成立

```go
l.Observe(remote)   // l = max(l, remote) + 1
```

这保证了 `a → b ⟹ L(a) < L(b)`。但 `L(a) < L(b)` **不推出** `a → b`——它可能只是两台机器各自 `Tick()` 得出来的任意顺序。

`TestLamportRespectsCausality` 里，两个独立节点的并发事件拿到了不同的值、被排了序，但这个顺序是**任意**的。这就是为什么 Lamport 时钟**不能用来检测写冲突**。

### 2. 向量时钟才能回答"是否并发"

```go
CompareVectors(a, b) → Equal | Before | After | Concurrent
```

规则：逐节点取 max 比较，两边各有更大的分量 = **并发**。

`TestVectorClockDetectsConcurrency` 构造了三种关系：独立事件（Concurrent）、观察之后的事件（After）、互相没看见的两次编辑（Concurrent）。`02-replication/leaderless` 就是靠它把"更新"和"冲突"分开的。

### 3. HLC：墙钟可以错，但时间戳不能倒退

```go
wall >  last.Physical            → 跟墙钟走
wall == last.Physical            → Logical++
wall <  last.Physical            → Logical++   ← 墙钟回拨了，用逻辑位兜住
```

`TestHLCSurvivesBackwardsClock`：墙钟被 NTP 往回拨 **1 小时**，HLC 连续产出的三个时间戳**严格递增**；等真实时间追回来之后，HLC 重新跟随墙钟。

这是 CockroachDB 用它做事务时间戳的原因：**既能当排序键用（近似物理时间，可区间查询），又不会因为时钟回拨破坏单调性。**

### 4. 时钟偏移必须设上限，而不是"吸收"它

```go
if remote.Physical - wall > h.maxSkew { return err }   // 拒绝，不是接受
```

一个时钟快了 5 秒的节点，如果它的时间戳被"吸收"进来，会把整个集群的时间推到未来。`TestHLCRejectsExcessiveSkew` 验证：5 秒偏移被拒，100ms 偏移被接受并把本地时钟往前拽。

生产系统通常还有一层：**测量并主动等待**偏移（Spanner 的 `Commit-Wait` 就是让事务提交后多等一个不确定区间，保证外部观察者也看不到时间倒流）。

### 5. Snowflake：唯一性 vs 可用性的显式选择

41 位毫秒 + 10 位机器号 + 12 位序列号。

**关键决策**：墙钟回拨时**返回错误，拒绝发号**，而不是阻塞或发可能重复的号。

```go
if now < s.lastMillis { return 0, fmt.Errorf("clock moved backwards...") }
```

这是"宁可不可用，也不出错"的选择。另一个合法选择是**阻塞等到 lastMillis**（Twitter 官方实现就是这么做的，因为"短暂不可用"比"客户端处理错误"更容易接受）。**两个选择都对，但你必须选一个并说清理由。**

`TestSnowflakeUniquenessAndRollback` 验证：5000 个号无重复且严格递增；不同机器号同毫秒不碰撞；回拨后报错。

## 代码地图

| 位置 | 内容 |
|---|---|
| `Lamport` | `Tick` / `Send` / `Observe` / `Now` |
| `Vector` + `CompareVectors` | 向量时钟与并发判定 |
| `HLC` + `Timestamp.Compare` | 混合逻辑时钟、偏移上界校验 |
| `Snowflake` + `DecodeSnowflake` | ID 生成与解析 |

## 实验

```bash
go test ./04-consensus/clocks/ -v -count=1     # 6 个测试
go test ./04-consensus/clocks/ -run 'Backwards|Rollback' -v
```

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| Lamport | 极省空间 | 检测不了并发（不能用做冲突判定） |
| 向量时钟 | 精确的因果 + 并发判定 | 大小 O(节点数)（Dynamo 因此改成了"最后写入者胜"，代价是丢并发写） |
| HLC | 近似物理时间 + 因果 + O(1) | 需要一个偏移上界（Spanner 用 `Commit-Wait` 消耗掉它） |
| Snowflake | 可排序、无协调、8 字节 | 依赖大致正确的墙钟；机器号要预分配 |
| 回拨时拒绝发号 | 绝不产生重复 | 短暂不可用（要监控告警） |

## 自测题

1. 举一个 `L(a) < L(b)` 但 a、b 并发的例子。为什么这决定了 Lamport 时钟不能用来做 MVCC 的时间戳？
2. 向量时钟在 100 节点集群里会变成什么问题？Dynamo 用了什么替代方案，代价是什么？
3. HLC 的时间戳能不能用来做"查询 3 天前的数据"？为什么能，误差有多大？
4. Snowflake 回拨时"报错"和"阻塞等待"各自适合什么场景？
5. 为什么 Spanner 需要一个原子钟/GPS 加"不确定区间"，而不是直接用 HLC？

## 已知边界

- 向量时钟没有"点版本"和差值压缩（Dynamo 的 dot / Cassandran 的 midi 版本）来限制大小。
- HLC 没有实现 Spanner 式的 `Commit-Wait`。
- Snowflake 没有机器号分配服务（生产上要靠 ZooKeeper/etcd 或固定配置）。
- 没有 NTP 偏移的**主动测量**（只有拒绝）。
