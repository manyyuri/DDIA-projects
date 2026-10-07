# raft — 共识

> DDIA §9。共识算法不是"选举一个 leader"，而是**在部分失效下保证任意两个节点看到的日志前缀一致**。选举只是达成这个目标的手段。

## 它防的是什么（§8）

| 故障 | 会不会破坏安全 | 这个实现怎么应对 |
|---|---|---|
| 进程崩溃 | 否 | term + votedFor **先落盘再回复** |
| 网络分区 | 否 | 只有多数派能提交 |
| 消息丢失/延迟/乱序 | 否 | 幂等的日志匹配 + 重试 |
| 时钟漂移 | 否 | 完全不依赖墙钟（只用逻辑 term） |
| 拜占庭故障（说谎节点） | **是** | **不防**。Raft 假设节点要么正确、要么停止 |

## 五条安全性质

| 性质 | 靠什么保证 | 对应测试 |
|---|---|---|
| **选举安全**：一个 term 最多一个 leader | 每 term 一票 + 多数派相交 | `TestRandomisedChurnPreservesSafety`（持续观测 term→leader 映射） |
| **Leader 只追加**：leader 从不覆盖自己的日志 | 只在末尾 append | `becomeLeaderLocked` 只 append no-op |
| **日志匹配**：两条日志若 index/term 相同，则前缀完全相同 | `prevLogIndex/prevLogTerm` 检查 | `TestDivergentLogIsTruncated` |
| **Leader 完整性**：已提交的条目一定出现在未来任意 leader 的日志里 | **选举限制**（§5.4.1） | `TestElectionRestrictionProtectsCommittedEntries` |
| **状态机安全**：节点不会在同一个 index 应用不同的命令 | 上面四条合起来 | 全量测试 + 重启测试 |

## 最容易写错的三处（我都踩过）

### 1. 提交规则不能只看"多数派持有"

```go
// 错的
if candidate > n.commitIndex { n.commitIndex = candidate }

// 对的（Raft §5.4.2）
if candidate > n.commitIndex && n.termAt(candidate) == n.term { ... }
```

不能直接提交**旧 term** 的条目。新人 leader 上台后必须先提交一条**自己 term 的 no-op**，前面的条目才随之提交。

**踩坑现场**：`TestSyncFailoverKeepsCommittedWrites` 一开始报 `k9 vanished after failover`——新 leader 的日志里有 10 条，但它不敢提交，因为那些条目属于旧 term。加上 no-op 屏障就好了。真实系统的表现是"切换后读不到刚写的数据"，非常容易被误判成丢数据。

### 2. `nextIndex` 回退时不能钳到 `snapIndex + 1`

```go
// 错：把 next 钳到 snapIndex+1，然后发 prevIndex = snapIndex
// follower: "我没有 index 61" → 拒绝 → conflictIndex = 2 → 又被钳到 62 → 死循环
if next <= n.snapIndex { next = n.snapIndex + 1 }

// 对：不钳。让 replicateTo 判断"这个位置我已经压缩掉了，只能发快照"
n.nextIndex[from] = next
```

**踩坑现场**：`TestSnapshotCatchesUpFarBehindFollower` 里落后节点永远追不上，leader 和 follower 无限 ping-pong 一条无用的 append。教训：**"回退"逻辑里的每一个 clamp 都要问一句"如果它需要的东西我已经删了怎么办"**。

### 3. 回复不能触发无条件广播

```go
// 错：每一次 appendResp 都 broadcastAppend → follower 再回复 → 无限风暴
c.advanceCommitLocked(); c.broadcastAppendLocked()

// 对：只有 commit 水位推进了才广播；否则只追这一个 follower
if c.advanceCommitLocked() { c.broadcastAppendLocked(); return }
if c.nextIndex[from] <= lead.lastIndex() { c.sendAppendLocked(from) }
```

**踩坑现场**：测试直接挂死（虚拟时间跑不完）。这类 bug 在真集群里的表现是 CPU 打满 + 网络打满，很难定位。

## 另外两处设计选择

### 持久化顺序：先落盘，再回复

```go
n.votedFor = msg.CandidateID
n.persistLocked()          // 必须在这之前
n.sendLocked(from, resp)
```

如果先回复再落盘，节点崩溃重启后可能忘记自己投过票 → **给同一个 term 投两票** → 选出两个 leader。

`HardState{Term, VotedFor, SnapIndex, SnapTerm, Snapshot}` 就是必须持久化的最小集合。

### 成员变更：一次只加/删一个

```go
n.AddPeer(id)     // 单节点变更：旧 quorum 和新 quorum 必然相交
```

一次变更多个节点可能产生两个**不相交**的多数派，集群立刻裂开。`TestMembershipChangeAddsAPeer` 在把集群从 3 扩到 4 之后，杀掉 2 个节点，断言剩下的 2 个**拒绝选出 leader**（因为 4 节点的多数派是 3）。

## 代码地图

| 文件 | 内容 |
|---|---|
| `raft.go` | 状态机、选举、日志复制、提交规则、快照、成员变更、apply loop |
| `storage.go` | `HardState` / `PersistedState`；`MemStorage` 与 `FileStorage`（写临时文件 + fsync + rename） |
| `kv.go` | 线性一致 KV：**读也走日志**，所以被废黜的 leader 无法提供陈旧读 |

几个值得单独看的函数：

- `handleAppendEntries` — 日志匹配检查（带 conflict hint 的快进回退）
- `advanceCommitLocked` — 提交规则
- `applyLoop` — 用局部 channel 变量避免重启时的竞态（这是 `-race` 抓出来的）
- `handleInstallSnapshot` — 快照整体替换状态机，用 `ApplyMsg{Type: "snapshot"}` 送出

## 实验

```bash
make exp-raft
go test ./04-consensus/raft/ -v -count=1 -timeout 300s     # 11 个测试
go test ./04-consensus/raft/ -race -count=1                # 值得跑
```

**实测数据：**

| 测试 | 观察到的现象 |
|---|---|
| `TestElectionRestrictionProtectsCommittedEntries` | 被隔离的节点 term 涨到 **9**，集群还停在 **term 1**；网络恢复后这个日志落后的节点**永远选不上**，10 条已提交数据一条不少 |
| `TestSnapshotCatchesUpFarBehindFollower` | leader 压缩到 `snap@61`，落后节点通过 `InstallSnapshot` 追上 |
| `TestLeaderInMinorityCannotCommit` | 少数派 leader 能接受 `Propose`（本地日志），但 commit 停在原处；恢复后未提交的"孤儿写"被回滚 |
| `TestRandomisedChurnPreservesSafety` | 12 轮随机分区/宕机/重启，"同 term 两个 leader"断言从未被触发，所有被 ack 的写全部存活 |

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 随机化选举超时 | 打破对称，避免活锁式分票 | 最坏情况下选举延迟不确定 |
| 只有多数派可提交 | 安全性 | 少数派分区时**不可写**（这是设计，不是 bug） |
| 读走日志 | 线性一致读 | 每次读一个 RTT + 一次日志写入 |
| 单节点成员变更 | 安全性 | 大规模扩容要一轮一轮来 |
| 快照整体传输 | 实现简单 | 快照很大时网络打满（真实系统分块） |
| 日志先落盘再回复 | 崩溃安全 | 每次写一次 fsync；批量提交是必做的优化 |

## 自测题

1. 为什么"选举限制"能保证 leader 完整性？如果去掉它，`TestElectionRestriction…` 里那 10 条数据会怎样？
2. 一个新 leader 为什么必须先提交自己 term 的 no-op？能不能直接提交旧 term 的条目？
3. follower 回复 `conflictTerm` 有什么用？（提示：一次回退一个 entry 还是一个 term）
4. `ApplyMsg` 里为什么要区分 `Type: "snapshot"`？如果只发普通 command 会怎样？
5. 如果一个 follower 收到了 term 更高的 `RequestVote`，但它刚收到现任 leader 的心跳，应该怎么做？
6. 把 `ElectionTimeout` 调到比网络延迟还小会发生什么？系统能活下来吗？（提示：term 疯涨、活跃度下降）

## 已知边界

- `Storage` 每次变更**重写整个状态文件**；真实系统增量 append + 批量 fsync。
- 快照是整体替换，没有分块（chunked）传输和断点续传。
- 成员变更没有"联合共识"（joint consensus）阶段，只有单节点近似。
- 没有 `PreVote` / `CheckQuorum`：被隔离的节点会不停涨 term，恢复后强迫集群重新选举（`TestElectionRestriction` 里那个 term 9 就是它）。
- 没有流水线（pipelining）和批量提交，吞吐比生产实现低一个数量级。
- `InstallSnapshot` 期间没有对日志做并发保护——真实实现要在快照传输期间缓冲新日志。
