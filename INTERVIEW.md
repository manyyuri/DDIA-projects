# INTERVIEW — 这个仓库怎么在面试里讲

> 每个项目目录下有一份 `INTERVIEW.md`：30 秒定位、2 分钟讲解稿、追问链、真实系统对照、诚实边界。这份总纲讲**整个仓库的讲法**。

---

## 一、整个仓库的定位（对面试官的第一句话）

> "我读 DDIA 的时候发现，书里讲的每个 trade-off 光看是记不住的。所以我把每一章的核心机制都用 Go 写成了**可跑的实现**，而且每个实现都配了**故意让它失败的测试**——丢包、分区、宕机、时钟回拨，全部在一个确定性的虚拟时间网络里注入，测试无 flaky、完全可复现。最后我把存储、复制、分区、共识拼成了一个分片的线性一致 KV。"

这段话在传递三件事：

1. **不是抄概念，是做过选择**——每个模块都有取舍表，都测出了数字；
2. **工程品味**——没有外部依赖、确定性测试、把故障变成可复现的实验；
3. **体系化**——15 个模块不是散的，最后拼成了 minidb。

## 二、讲解路由：面试官问 X，用哪个项目接

| 面试官的问题 | 主打项目 | 辅助 |
|---|---|---|
| "讲讲你最熟的存储引擎 / LSM" | `lsm` + `bitcask`（对照着讲） | 3 种放大率 |
| "Raft 了解吗？" / "一致性怎么保证" | `raft` → `minidb` | 选举限制、no-op 屏障 |
| "分布式事务怎么做" | `twopc`（2PC 为什么卡死 + Saga） | `mvcc`（写偏斜）、`eventsourcing`（CAS） |
| "消息队列 / Kafka 原理" | `logbroker` → `windowing` | offset 归属、幂等 sink |
| "CAP 怎么理解" | `singleleader`（三种 ack 模式）+ `leaderless`（AP 侧）+ `minidb`（少数派拒写） | 用测试说话 |
| "分库分表 / 数据怎么扩容" | `partitioning` | 75% vs 23.2% 这组数字 |
| "时钟不可信怎么办" | `clocks` | HLC、Snowflake 回拨 |
| "MySQL/PG 事务隔离级别" | `mvcc` | 写偏斜是杀手锏 |
| "数据同步 / 缓存一致性 / CDC" | `cdc` + `eventsourcing` | 快照切割点、幂等 upsert |
| "系统设计题：设计一个分布式 KV" | 直接用 `minidb` 的分层当答案骨架 | 每层讲取舍 |
| "大数据 / 离线计算" | `mapreduce` | shuffle 成本、倾斜 |

**策略**：任何一个话题都不要只讲一个模块——讲一个模块 + 用另一个模块做对照，立刻显得有体系。例：讲 Raft 提交规则时带一句"这在 singleleader 包里有个更裸的版本，Async 模式下 ack 之后 leader 崩，写就没了，有专门测试"。

## 三、三条贯穿的杀手锏

### 1. 数字（体现"我真的跑过"）

随手可用、每一条都有测试背书：

- bitcask 覆盖写 **1.06x 写放大**；一次 fsync ≈ **4ms**，比不 fsync 慢**三个数量级**；
- LSM：**10 bits/key → bloom 误判 0.77%、零漏判**；20000 次写换来 473 次 flush + 258 次 compaction；
- 取模哈希加一个节点搬 **75%**，一致性哈希 **23.2%**；vnode 把倾斜从 **1.49x 压到 1.09x**；
- raft：被隔离节点 term 涨到 **9** 也选不上，10 条已提交数据一条不少；
- HLC：墙钟回拨 **1 小时**，时间戳依然严格递增；
- windowing：物理写 4 次，可见状态与写 2 次**完全相同**（幂等 sink）。

### 2. 踩坑故事（体现"这是我写的"）

每个模块的 INTERVIEW.md 里都有 1-2 个真实的踩坑（测试名 + 现象 + 根因 + 修法）。讲项目时**主动**讲一个，比被问"遇到过什么问题"再想强十倍。最有力的三个：

1. raft 提交规则少写了"当前 term 限制" → `k9 vanished after failover` → 新 leader 不敢提交旧 term 条目 → no-op 屏障；
2. windowing 迟到判定第一版用事件时间戳而不是水位线 → 窗口状态已清理却还在更新 → 判定与清理必须用同一个时钟；
3. partitioning 的 FNV-1a 低位聚集 → 环点挤在一起，倾斜 1.31x → 加 murmur3 finalizer 才好 → "哈希看起来能用和分布真的均匀是两回事"。

### 3. 主动承认边界（体现"我知道我没做什么"）

被问"这离生产还差什么"是最常见压力测试。**先于面试官说出来**，每份 INTERVIEW.md 末尾都有"主动承认清单"。通用句式：

> "这个实现故意做减法，比如 raft 的 Storage 是全量重写状态文件，真实系统是增量 append + 批量 fsync；compaction 是整层合并，RocksDB 只合并重叠的 key range。我列了一张已知边界表在 README 里——因为我觉得写清楚'没做什么'比假装完备更诚实。"

## 四、怎么应对"这是不是 AI 写的 / 你自己写的吗"

不用回避，三段式回答：

1. "这是个刻意的学习项目，目标就是把 DDIA 的每个 trade-off 变成亲手调过的参数。**过程有 AI 参与，但每个结论我都能现场验证**——"
2. 现场演示：`go test ./04-consensus/raft/ -run TestElectionRestrictionProtectsCommittedEntries -v`，十几秒出结果，日志里直接打出证据（被隔离节点 term 涨到 9、集群停在 term 1、日志卡在 index 1），然后解释这个测试在断言什么、把哪个参数改了会怎样；
3. 深挖兜底：让面试官随便挑一个模块问到底——每个模块的 INTERVIEW.md 追问链都准备到了第四层。

**核心逻辑**：能不能"扛住提问"的判据不是代码是谁敲的，而是"改一个参数你知道后果、问一层你能再答一层"。

## 五、三分钟版本（如果只给你三分钟讲整个仓库）

> "这个仓库是我把 DDIA 做成的一系列可验证实验，15 个模块按书的顺序走：存储（bitcask/LSM）、复制（单主/无主/分区）、事务（MVCC 写偏斜）、共识（Raft/时钟/2PC）、批流（MapReduce/日志 broker/事件时间开窗）、集成（CDC/事件溯源），最后拼成一个分片线性一致 KV。
>
> 它有两个特点。第一，**每个机制都有会失败的测试**：比如'异步复制丢已 ack 的写'、'2PC 协调者崩溃后参与者持锁卡死'、'快照隔离下写偏斜破坏不变量'——这些不是书上的话，是我能现场跑给你看的断言。第二，**故障注入是确定性的**：我写了一个虚拟时间网络，丢包/分区/宕机全部可复现，所以测试没有 flaky。
>
> 举一个我最喜欢的例子：一致性哈希。教科书说'加节点只搬 1/N'，我实测取模哈希加一个节点搬 75% 的 key、一致性哈希搬 23.2%；再给环加虚拟节点，负载倾斜从 1.49x 压到 1.09x。中间还踩了个坑：FNV-1a 的低位有结构，节点环点会聚集，我加了一步 murmur3 finalizer 才把分布拉平——哈希函数'看起来能用'和'分布真的均匀'是两回事，这只有测了才知道。"

---

## 六、索引

| 模块 | 面试讲解 |
|---|---|
| bitcask | [01-storage/bitcask/INTERVIEW.md](01-storage/bitcask/INTERVIEW.md) |
| lsm | [01-storage/lsm/INTERVIEW.md](01-storage/lsm/INTERVIEW.md) |
| singleleader | [02-replication/singleleader/INTERVIEW.md](02-replication/singleleader/INTERVIEW.md) |
| leaderless | [02-replication/leaderless/INTERVIEW.md](02-replication/leaderless/INTERVIEW.md) |
| partitioning | [02-replication/partitioning/INTERVIEW.md](02-replication/partitioning/INTERVIEW.md) |
| mvcc | [03-transactions/mvcc/INTERVIEW.md](03-transactions/mvcc/INTERVIEW.md) |
| raft | [04-consensus/raft/INTERVIEW.md](04-consensus/raft/INTERVIEW.md) |
| clocks | [04-consensus/clocks/INTERVIEW.md](04-consensus/clocks/INTERVIEW.md) |
| twopc | [04-consensus/twopc/INTERVIEW.md](04-consensus/twopc/INTERVIEW.md) |
| mapreduce | [05-batch/mapreduce/INTERVIEW.md](05-batch/mapreduce/INTERVIEW.md) |
| logbroker | [06-stream/logbroker/INTERVIEW.md](06-stream/logbroker/INTERVIEW.md) |
| windowing | [06-stream/windowing/INTERVIEW.md](06-stream/windowing/INTERVIEW.md) |
| cdc | [07-integration/cdc/INTERVIEW.md](07-integration/cdc/INTERVIEW.md) |
| eventsourcing | [07-integration/eventsourcing/INTERVIEW.md](07-integration/eventsourcing/INTERVIEW.md) |
| minidb | [poc/minidb/INTERVIEW.md](poc/minidb/INTERVIEW.md) |
| simnet（底座，被动提到即可） | [internal/simnet/README.md](internal/simnet/README.md) |
