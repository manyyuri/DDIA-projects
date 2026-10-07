# singleleader — 单主复制

> DDIA §5.1–5.2。这一章真正的问题不是"怎么复制"，而是**"什么算写成功"**和**"副本读为什么是错的"**。

## 机制

```
客户端 → leader：写进自己的日志 → 立即 ack？（取决于 SyncMode）
       ↓ 复制流（带 prevIndex/prevTerm 的日志匹配）
     follower n1, n2 …：追加，按 leader 告知的 commit 水位推进"可读水位"
```

**刻意不含选举协议。** 提升新 leader 是一个**外部决定**（运维脚本、独立的协调服务）。很多真实部署就是这样，而这正是它们丢数据的原因——所以这个包里可以亲手选一种提升策略，然后观察后果。

## 三种同步模式 = 一致性与可用性的三个切面

| 模式 | "写成功"的含义 | 故障时的后果 |
|---|---|---|
| `Async` | leader 的日志有了就算成功 | 快，但 **failover 会丢掉已经 ack 给客户端的写** |
| `WaitForQuorum` | 多数派活着的副本都有了 | 容忍 1 个副本故障；延迟 = 中位数副本 |
| `WaitForAll` | **所有配置中的**副本都有了 | 任何副本宕机都写不进去（可用性代价） |

注意 `WaitForAll` 用的是**配置里的全部副本**，不是"活着的全部"——这才叫"到处都持久"，代价就是不可用。

## 三种读模式：副本是缓存，它可以骗你

| 模式 | 行为 |
|---|---|
| `ReadLeader` | 强制走 leader，新鲜但不能扩展 |
| `ReadFollower` | 便宜的、**可能是陈旧的**回答 |
| `ReadYourWrites` | 带会话水位：副本落后于"我上次写到的位置"就拒绝回答 |

关键认识：**"读己之写"是客户端会话的属性，不是服务器属性**。服务器没法知道这个客户端上次写了什么。

## 关键设计点

### 1. ack ≠ commit

`Write()` 返回的 index **不是持久化承诺**。它必须在"多数派持有 **且** 这一条属于当前 term"之后才提交（Raft 的提交规则，这里也照做）。`Async` 模式绕过了它，代价见 `TestAsyncAckIsLostOnFailover`。

### 2. "commit 了"只意味着"有一个多数派持有它"

把持有数据的那个多数派全杀掉，数据就跟着没了。`TestLosingTheQuorumLosesCommittedWrites` 用 5 节点做这件事：n4/n5 被网络隔离（所以写从没到过它们），然后 n1/n2/n3 全部宕机 → 新 leader 上台，3 条已提交的写全丢。

**持久性是"副本集合"的属性，不是某个节点上的一个布尔值。**

### 3. 超时 ≠ 失败

`TestTimedOutWriteMayStillHaveCommitted`：`WaitForAll` 遇到副本宕机会超时返回失败，但这条写**其实已经在 quorum 上提交了**。客户端如果重试，会产生重复——这就是幂等键存在的原因。

### 4. 新 leader 不能直接宣布旧条目已提交

`Promote` 之后新 leader 先追加一条**自己 term 的 no-op**，等它提交时，它前面的所有条目随之提交（Raft §5.4.2）。少了这一步，被提升的节点会发现"日志里有数据但读不到"——`TestSyncFailoverKeepsCommittedWrites` 一开始就是这么失败的。

## 代码地图

| 位置 | 内容 |
|---|---|
| `cluster.go` | 节点状态、`appendEntries`/`appendResp`/投票消息 |
| `handleAppend` | 日志匹配、冲突后缀截断、commit 水位推进 |
| `handleAppendResp` | matchIndex/nextIndex 维护、快进回退（带冲突提示） |
| `advanceCommitLocked` | quorum 提交 + 当前 term 限制 |
| `Promote` | 运维式 failover + 丢失量报告 |

## 实验

```bash
go test ./02-replication/singleleader/ -v -count=1     # 9 个测试
make exp pkg=./02-replication/singleleader run=TestAsyncAckIsLostOnFailover
```

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| Async | 低延迟、高可用 | 丢已 ack 的写；RPO > 0 |
| Quorum | 能容忍少数派故障 | 延迟由中位数副本决定 |
| WaitForAll | 强持久性 | 任一副本故障即不可写 |
| 副本读 | 可扩展 | 陈旧读，需要会话一致性兜住 |
| 无选举、外部提升 | 简单 | 提升策略错了就丢数据（`FailoverReport` 会告诉你丢了多少） |

## 自测题

1. `AckCount(idx)` 数的是"日志里有这一条"，为什么它不等于"已提交"？
2. `TestLeaderInMinorityCannotCommit` 里，被隔离的 leader 还在接受写。客户端怎么发现自己的写永远提交不了？（提示：想想超时之后客户端该做什么）
3. 为什么 `advanceCommitLocked` 要求 `termAt(candidate) == currentTerm`？去掉它会破坏哪个测试？（提示：想一想上任 leader 遗留的条目）
4. `ReadYourWrites` 解决了"读自己的写"，但没解决"单调读"。后者需要记录什么？

## 已知边界

- 没有选举：`Promote` 是手工/外部触发的。
- 提升时**不比较日志的新旧**，只按"活着的里面日志最长"——这是无共识系统里能做的最好猜测，也是它不安全的原因。
- 没有快照，日志无限增长。
- `handleAppendResp` 里每条回复都会顺手广播/追赶，不是批量流水线。
