# minidb — 分区 + Raft + LSM

> 综合项目。前面每个章节都是一个零件；这里把它们拼成 DDIA 一直在描述、但很少一次性讲完的东西：**一个分片的、线性一致的分布式 KV**。

## 分层

```
客户端
  │  key → 一致性哈希环 → 分片
  │  分片 → 当前 leader（找不到就等、就重试）
  ▼
分片 shard-0            shard-1            shard-2
 ├─ n0 (Raft 副本)      ├─ n0 (Raft)      …
 ├─ n1 (Raft 副本)      ├─ n1 (Raft)
 └─ n2 (Raft 副本)      └─ n2 (Raft)
     每层：Raft 日志是真相 → LSM 是派生状态
```

三层各司其职，**每一层都是一个明确的取舍点**：

| 层 | 解决 | 代价 |
|---|---|---|
| 一致性哈希（`02-replication/partitioning`） | 水平扩展、加节点只搬 ~1/N | **跨分片没有事务、没有顺序** |
| Raft（`04-consensus/raft`） | 分片内的多数派复制 + 自动故障切换 | 少数派分区时**不可写**；每次写一个 RTT |
| LSM（`01-storage/lsm`） | 单机状态机高效持久化 | compaction 写放大 |

**注意这些代价是叠加的，不是替代关系**：一次 `Put` 同时承担了"跨分片的不可原子"、"多数派才能提交"、"状态机要写 WAL + 可能触发 compaction"。

## 关键设计点

### 1. 日志是真相，LSM 是派生数据

```go
// consume：apply 出来的日志条目 → LSM 写
for msg := range rep.node.ApplyCh() {
    rep.store.Put([]byte(cmd.Key), []byte(cmd.Value))
    rep.applied.Store(msg.Index)
}
```

这意味着副本可以**从日志重建**自己的本地存储。`TestDurabilityAcrossFullShardRestart` 把整个集群全杀掉再启回来，15 个 key 一个不少——因为它同时依赖两个持久层：

- Raft 的 `HardState` + 日志（谁投过票、日志到哪了）；
- LSM 的 SSTable（已经应用的状态）。

少任何一个，重启后要么丢失数据，要么重新选举出错。

### 2. 读也走日志

```go
func (c *Cluster) Get(key string) (string, bool, error) {
    payload, _ := json.Marshal(Command{Op: "get", Key: key})
    idx, _ := rep.node.Propose(payload)          // 读命令也进日志
    c.Until(..., rep.node.Status().CommitIndex >= idx && rep.applied.Load() >= idx)
    val, _ := rep.store.Get([]byte(key))          // 然后才读本地状态机
}
```

**为什么不能直接读 leader 的本地状态机？** 因为一个被网络隔离的旧 leader **不知道自己已经被废黜**——它会继续用本地状态回答，给出陈旧读（DDIA §8.3.1）。

代价是每次读一个 RTT + 一次日志写入。这就是"线性一致读"的价格。优化手段（ReadIndex / LeaseRead）都是在**牺牲某一部分保证**换回延迟：

- **ReadIndex**：先确认自己仍是 leader（一轮心跳），再等本地状态机追到该位置 → 省掉一条日志，但仍有 RTT；
- **LeaseRead**：靠租约在本地直接读 → 省掉 RTT，但要**依赖时钟**（回到 `04-consensus/clocks` 那个问题）。

### 3. 客户端必须重试，而且重试必须安全

```go
for c.net.Now() < deadline {
    if rep := c.leaderOf(sh); rep != nil {
        if idx, err := rep.node.Propose(payload); err == nil { ... }
    }
    c.net.Run(2 * time.Millisecond)
}
```

`leaderOf` 是**尽力而为**的：它读的是各副本的本地状态，可能刚好在选举中间返回 nil 或者一个正在被取代的旧 leader。所以客户端循环是协议的一部分：

- `Propose` 返回 `not the leader` → 下一轮重新找；
- 提交超时 → **不代表写失败**（见 `singleleader` 的 `TestTimedOutWriteMayStillHaveCommitted`），所以这里用"等待提交"而不是"失败就报错"。

### 4. 少数派必须拒绝写，而不是丢写

`TestMinorityPartitionRefusesWrites`：把 leader 和它拉到一个只剩 2/3 的分区里，写请求**阻塞直到超时**，而不是"本地接受、以后同步"。

这是共识换来的核心保证：**客户端感受到的是不可用（超时），而不是静默丢数据**。想接受这个代价的话，另一条路是 AP 系统（`leaderless`）——用"写成功后可能读不到"换可用性。

## 代码地图

| 位置 | 内容 |
|---|---|
| `New` | 建分片、建 Raft 组、建 LSM 目录 |
| `consume` | 状态机：日志 → LSM |
| `Put` / `Get` | 路由 + 找 leader + 等待提交 |
| `leaderOf` / `WaitForLeaders` | 尽力而为的 leader 发现 |
| `Crash` / `Restart` / `CrashLeader` | 故障注入 |
| `Info` / `Describe` | 观测分片拓扑与提交位点 |

## 实验

```bash
go test ./poc/minidb/ -v -count=1          # 6 个测试（约 10s）
go test ./poc/minidb/ -race -count=1
```

**实测数据：**

| 测试 | 结果 |
|---|---|
| 300 个 key 的分片分布 | `shard-0: 107, shard-1: 106, shard-2: 87` |
| `TestPutGetAcrossShardsIsLinearizable` | 请求分布 `{shard-0: 50, shard-1: 32, shard-2: 39}`，60 个 key 全部读回一致 |
| `TestLeaderFailoverKeepsDataAndAvailability` | 杀掉 leader 后选出新 leader；10 个 key 全在；写入恢复；旧 leader 回来后**没有重新夺回 leadership** |
| `TestDurabilityAcrossFullShardRestart` | 全部副本杀掉再启，15 个 key 一个不少 |
| `TestMinorityPartitionRefusesWrites` | 少数派写超时，多数派仍能读已提交数据 |

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 按 key 分片 | 水平扩展 | **没有多分片事务**（这不是漏做，是分片换来的代价） |
| 每分片 3 副本 Raft | 容 1 个节点故障 + 自动切换 | 少数派不可用；写延迟 = 多数派中最快的一个 |
| 读走日志 | 线性一致读 | 每次读一个 RTT |
| LSM 作为状态机 | 单机持久化高效 | compaction 写放大、重启要重放日志 |
| 3 个分片固定 | 实现简单 | 没有动态分裂、没有 rebalance |
| 客户端重试 | 不需要服务端做重定向 | 客户端必须知道"超时 ≠ 失败" |

## 自测题

1. 一次 `Put` 依次经过了哪几层？每一层可能失败在哪里？分别返回什么错误？
2. 为什么读也走日志？直接读 leader 的 LSM 会在什么场景下给出错误答案？
3. `ReadIndex` 和 `LeaseRead` 各省掉了什么、又引入了什么假设？
4. 如果分片 1 的 3 个副本全部宕机，整个集群还能服务其他 key 吗？为什么？这个性质叫什么？
5. 要给这个系统加"跨分片转账"，你需要什么？（提示：2PC/Saga，见 `04-consensus/twopc`）
6. `TestLeaderFailoverKeepsDataAndAvailability` 断言"旧 leader 回来不会重新夺回 leadership"。如果不加这条断言，什么样的 bug 会溜过去？

## 已知边界

- **没有多分片事务**；没有分布式查询；没有跨分片二级索引。
- 没有动态 rebalance（分片数是启动时固定的）；没有一致性哈希的迁移协议。
- 没有快照传输（Raft 的 `InstallSnapshot` 路径在这个组合里没有被压测过）。
- 状态机没有实现快照（`Raft.Snapshot` 没有被调用），所以 Raft 日志会一直增长。
- 客户端没有去重（幂等键），所以"超时重试"可能产生重复写——真实系统要靠请求 ID 解决。
- 没有认证、没有多租户、没有真正的网络传输层（用的是 `internal/simnet`）。
- **这份实现是教学用的**：它验证的是机制的正确性，不是生产级的性能与鲁棒性。
