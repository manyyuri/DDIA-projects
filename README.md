# DDIA Projects

把《Designing Data-Intensive Applications》里的 **trade-off 变成亲手做过的选择**。

这本书最难的地方不是记概念，而是"知道每个方案各自的代价"。所以这里的每个模块都是**可跑的实现 + 会失败的测试**：代码写出机制，测试把代价变成数字。

> 全部用 Go 写，零外部依赖（不需要 Docker、不需要起数据库）。分布式故障注入靠 `internal/simnet` 这个确定性虚拟时间网络，所以测试**可复现、无 flaky、跑得飞快**。
>
> **面试准备**：每个模块目录下有一份 `INTERVIEW.md`（30 秒定位、两分钟讲解稿、追问链、真实系统对照、诚实边界），总纲在 [INTERVIEW.md](INTERVIEW.md)。

---

## 怎么跑

```bash
go test ./...                   # 全量测试（约 2 分钟）
make test                       # 同上
make test-race                  # 加竞态检测（约 4 分钟，raft/minidb 值得跑）
make exp-partitioning           # 看"加一个节点会搬多少数据"
make exp-raft                   # 看选举限制如何保护已提交数据
make bench                      # bitcask / LSM 基准
```

环境：Go 1.24+（开发用的是 1.27.1），macOS/Linux。

---

## 学习路线：一章 = 一个包 = 一个可验证的问题

按书里顺序走，**不要一次看完**。每章花 30–60 分钟：先读该包的 README 和代码，再跑实验，最后回答"自测题"。

| 章节 | 包 | 这章真正的问题 | 你会亲手验证的结论 |
|---|---|---|---|
| §3.1 | `01-storage/bitcask` | 哈希索引 vs B 树 vs LSM，怎么选？ | O(1) 点查、无读放大，代价是**全量重写**（实测 1.06x 写放大）而且**不能范围扫描** |
| §3.2 | `01-storage/lsm` | 写优化的存储引擎长什么样？ | 布隆过滤器把误判压到 0.77%，把读放大从"查 N 张表"降到"查 1 张表"；代价是 compaction 的写放大 |
| §5.1–5.2 | `02-replication/singleleader` | 异步复制到底会丢什么？ | 客户端拿到 ack ≠ 数据持久；**丢掉 quorum 就是丢数据**；超时不等于失败 |
| §5.5 | `02-replication/leaderless` | 没有主节点怎么做一致性？ | W+R>N 保证读能看见最新写；并发写产生 siblings，**冲突要应用自己合并** |
| §6 | `02-replication/partitioning` | 重新平衡要搬多少数据？ | 取模哈希加一个节点搬走 **75%**；一致性哈希只搬 **23.2%**；虚拟节点把倾斜从 1.49x 压到 1.09x |
| §7.2 | `03-transactions/mvcc` | 快照隔离为什么还不够？ | 复现**写偏斜**（两个医生同时下线，都提交成功）；加读集校验后第二个被拒 |
| §9 | `04-consensus/raft` | 共识算法凭什么安全？ | 被隔离的节点 term 涨到 9 也**永远选不上**（选举限制）；日志冲突会被截断；随机 churn 下"同 term 从不两个 leader" |
| §8.3–8.4 | `04-consensus/clocks` | 时钟不可信怎么办？ | HLC 在墙钟回拨 1 小时后仍单调递增；Snowflake 回拨后**拒绝发号**而不是发重复号 |
| §9.3 | `04-consensus/twopc` | 2PC 的致命伤是什么？ | 协调者在 prepare 之后崩溃 → 所有参与者**持有锁进入 in-doubt**，等待无法解决；Saga 用补偿换掉这个阻塞 |
| §10 | `05-batch/mapreduce` | shuffle 到底贵在哪？ | 全量左表要跨网络；热点 key 让 99% 数据落在同一个 reducer（代码会把倾斜报出来） |
| §11.1–11.2 | `06-stream/logbroker` | 消息队列 vs 日志？ | 分区内有序、跨分区无序；offset 属于消费者所以**重放免费**；再均衡会重放未提交的工作（at-least-once） |
| §11.3–11.5 | `06-stream/windowing` | 什么时候能说"这个窗口完了"？ | 水位线只是启发式；迟到数据要么更新结果（revision 2）、要么进侧输出；**幂等 sink 让重放产生相同结果** |
| §12 | `07-integration/cdc` | 派生数据怎么跟上游保持一致？ | 快照必须和变更流在**同一个 LSN** 上切割；加列是兼容变更、删列不是；重放靠幂等 upsert |
| §11.4 | `07-integration/eventsourcing` | 存状态 vs 存事件？ | 版本号 CAS 就是乐观并发控制；投影可以随时删掉重建；快照只是优化，删光也不改变任何行为 |
| 综合 | `poc/minidb` | 这些零件怎么拼起来？ | 分区 + Raft + LSM = 分片线性一致 KV：leader 宕机不丢数据，全量重启后 15 个 key 一个不少 |

---

## 贯穿全书的五个 trade-off

每章其实都在重新回答这五个问题。读完你会发现它们反复出现：

1. **写放大 / 读放大 / 空间放大，三者只能选两个**
   `bitcask` 三者都是 1x，但要求 keydir 放进内存、且放弃范围扫描；
   `lsm` 写放大高、读放大靠 bloom 压住；B+ 树读放大低、写放大高。

2. **一致性 / 可用性 / 延迟**
   `singleleader` 的三种模式就是这个三维的切面：`Async` 快但会丢已 ack 的写，
   `WaitForAll` 强但任何副本宕机就写不进。

3. **顺序 vs 并行**
   分区买来吞吐，代价是跨分区没有顺序、没有事务。
   `logbroker` 里"同 key 同分区"是唯一拿到 per-key 有序的方法。

4. **事件时间 vs 处理时间**
   只有按事件时间开窗，重放结果才可复现（测试里乱序输入和顺序输入产出了完全相同的窗口结果）。
   水位线是"我赌再没有更老的事件"，代价是延迟。

5. **状态 vs 事件**
   存状态要解决并发覆盖；存事件只需要一个版本 CAS。
   `eventsourcing` 里银行账户的余额是推导出来的，没有任何 setter。

---

## 常见误解 → 这里是验证它的测试

| 误解 | 反例（点进去看） |
|---|---|
| "leader 收到写就安全了" | `TestAsyncAckIsLostOnFailover` — ack 之后 leader 崩，写没了 |
| "commit 了就一定在" | `TestLosingTheQuorumLosesCommittedWrites` — 把持有数据的 quorum 全杀掉，数据一起没 |
| "写超时就是失败了" | `TestTimedOutWriteMayStillHaveCommitted` — 超时的写其实已经提交，重试会产生重复 |
| "Quorum 就是共识" | `02-replication/leaderless` — quorum 只保证集合重叠，不保证顺序、不做 leader 选举 |
| "快照隔离就够了" | `TestWriteSkewIsAllowedUnderSnapshotIsolation` — 两个事务都提交，不变量被破坏 |
| "2PC 只是慢一点" | `TestCoordinatorCrashLeavesParticipantsInDoubt` — 等待 5 秒也解决不了，参与者卡死 |
| "时钟不对就同步一下" | `TestHLCSurvivesBackwardsClock`、`TestSnowflakeUniquenessAndRollback` |
| "投递一次就等于处理一次" | `TestReplayIsIdempotentAtTheSink` — 物理写了 4 次，可见状态和写 2 次完全一样 |

---

## 代码结构

```
01-storage/          bitcask（哈希索引）  lsm（SSTable + compaction）
02-replication/      singleleader  leaderless  partitioning
03-transactions/     mvcc（快照隔离 + 写偏斜）
04-consensus/        raft  clocks（Lamport/向量/HLC/Snowflake）  twopc（+ Saga）
05-batch/            mapreduce（shuffle / 倾斜 / join / PageRank）
06-stream/           logbroker（分区日志 + 消费组）  windowing（事件时间 + 水位线）
07-integration/      cdc（快照+tail+schema 演化）  eventsourcing（CAS + 投影）
poc/minidb/          分区 + Raft + LSM 拼成的分片线性一致 KV
internal/simnet/     确定性虚拟时间网络（丢包 / 延迟 / 分区 / 宕机）
```

每个包里的文件分工基本是：

```
<机制>.go        实现
<机制>_test.go   验证机制 + 制造故障
README.md        讲解：机制 → 代码地图 → 实验 → 自测题 → 已知边界
```

---

## 已知边界：这份实现**故意**没做的事

写清楚边界比假装完备有用。每处都是"为了把机制讲清楚"而做的简化：

- **bitcask**：merge 期间阻塞写入（真实 Bitcask 在线合并 + hint 文件）；keydir 全驻内存。
- **lsm**：按整层合并（真实引擎只合并重叠的 key range 以限制写放大）；compaction 同步阻塞读；无并发 flush。
- **raft**：`Storage` 每次变更重写整个状态文件（真实系统增量 append + 批量 fsync）；快照是整体替换，没有分块传输；成员变更是单节点串行变更。
- **leaderless**：读修复是同步全量推送（Cassandra 走异步 digest 请求）；没有 Merkle tree 反熵。
- **mvcc**：serializable 用的是 OCC 式读集校验，比 PostgreSQL 的 SSI 更保守（更容易误杀）。
- **logbroker**：再均衡是 range 策略的简化版，没有真正的组协调协议（leader 选举 + 会合）。
- **cdc**：变更日志是显式实现的，不是真的解析 WAL/binlog。
- **minidb**：**没有多分片事务**（这是分片换来的代价，不是漏做）；没有再平衡；没有快照传输。
- **simnet**：单线程执行、无真实并发，所以它能复现故障但测不出数据竞争（竞态靠 `-race` 单独跑）。

**这些代码是测试驱动出来的，没有经过人工 review。** 测试覆盖的是"我在测试里想到的不变量"，不等于生产级正确性。当成一个能动手的教具，不要当成可直接上线的库。

---

## 推荐配合

- 课程：MIT 6.824/6.5840（Raft lab）、CMU 15-445（存储引擎）、15-721
- 书：《Database Internals》
- 论文：GFS、Bigtable、Dynamo、Spanner、Raft、Kafka、Dataflow（批流统一的源头）
- 实战：Jepsen 报告（看真实系统怎么在角落里翻车）
- 源码对照：etcd（Raft）、TiKV（Raft + 存储）、RocksDB（LSM）、Kafka、Flink
