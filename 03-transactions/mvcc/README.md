# mvcc — 多版本并发控制与快照隔离

> DDIA §7.2 与 §7.3。关键是理解**快照隔离为什么不是可串行化**，以及那个缺口叫什么名字：**写偏斜（write skew）**。

## 机制：一次写不覆盖任何东西

```
版本链：每个 key 一串版本，每个版本有 [startTS, endTS) 的生命周期

   k 的版本链（新→旧）:
     v2 [ts=7, endTS=0)   ← 当前
     v1 [ts=3, endTS=7)
     v0 [ts=1, endTS=3)

读：版本对快照 ts 可见  ⟺  startTS <= ts 且 (endTS == 0 或 endTS > ts)
写：追加一个新版本，把前一个的 endTS 设成本次提交时间
```

得到的两条性质：

- **读不阻塞写、写不阻塞读**（读者只看历史版本，写者只追加）；
- **可重复读**：事务的 `readTS` 在开始时固定，之后别人怎么提交都看不见。

`TestSnapshotIsRepeatable` 验证了这一条：读者两次读 `k` 之间有人提交了 `v2`，读者依然看到 `v1`。

## 关键设计点

### 1. 首提交者胜（first-committer-wins）

快照隔离下两个事务同时读 `counter=1`，各自算出 `2`，各自写回。如果没有冲突检测，后写的会**静默覆盖**先写的 = **丢失更新**。

所以提交时要检查：**我快照之后，这个 key 是否被别人改过？** 是的话第二个事务必须失败。

```go
for k := range t.writes {
    if db.changedAfterLocked(k, t.readTS) { return ErrConflict }
}
```

`TestLostUpdateIsPrevented` 里第二个提交者拿到 `ErrConflict`，最终计数器是 `1+1` 而不是 `1+1+1`。

### 2. 为什么这还不够：写偏斜

经典场景（DDIA §7.2.3）：值班制度要求**至少一个医生在岗**。

| 步骤 | Alice 的事务 | Bob 的事务 |
|---|---|---|
| 1 | 读到"2 人在岗" | 读到"2 人在岗" |
| 2 | 判断：我下线后还有 1 人 ✓ | 判断：我下线后还有 1 人 ✓ |
| 3 | 写 `alice:on_call = false` | 写 `bob:on_call = false` |
| 4 | 提交 ✓ | 提交 ✓ |

两个事务**读的是同一批数据、写的是不同的行**。首提交者胜只检查写集冲突，所以两个都通过 → **在岗 0 人，不变量被破坏，没有任何错误。**

`TestWriteSkewIsAllowedUnderSnapshotIsolation` 就是这个实验，它**故意断言两个事务都提交成功**，让你亲眼看到这个 bug。

### 3. 修法：校验读集（OCC 式）

`Serializable` 模式下，提交时额外检查**读集里的 key 有没有在我快照之后变过**：

```go
if t.iso == Serializable {
    for k := range t.reads {
        if _, wrote := t.writes[k]; wrote { continue }
        if db.changedAfterLocked(k, t.readTS) { return ErrConflict }
    }
}
```

Bob 的提交被拒 → 应用重试 → 重试时读到"Alice 已经下线，只剩 1 人在岗" → 自己拒绝下线。**不变量保住了。**

> 注意：这是乐观并发控制（OCC）式的读校验，**比 PostgreSQL 的 SSI 保守**——SSI 有"读写依赖"的判定规则，能放过更多安全的事务。这里的实现是"教学上最容易解释对的版本"，不是性能最优的版本。

### 4. GC：历史只能删到"最老的活跃快照"为止

MVCC 的根本成本是版本堆积。回收规则很硬：

```
minTS = min(所有活跃事务的 readTS, 当前时间)
可以删除的版本 = endTS != 0 且 endTS <= minTS      ← 没有任何活着的快照还能看见它
```

`TestGarbageCollectionRespectsOpenSnapshots`：一个长事务占着 v0 的快照，中间插入 5 个新版本，此时 `GC()` 必须返回 **0**（一个都不能删）。等它结束，才能收回 5 个。

**长事务是 MVCC 的天敌**——它会钉住整段历史，让空间无限膨胀。这就是为什么生产系统会强杀长事务（PostgreSQL 的 `idle_in_transaction_session_timeout`）。

### 5. 删除也是一条版本

`Delete` 写一个 `deleted=true` 的版本，不是从链上摘掉。否则老快照会读到"这个 key 从来不存在"，而不是"它当时存在的值是 v1"。

## 代码地图

| 位置 | 内容 |
|---|---|
| `version` + `visibleAt` | 可见性判定（整个 MVCC 的核心一行） |
| `Txn.Get/Put/Delete` | 记录读集与写集，未提交的写只在自己的写集里可见 |
| `Txn.Commit` | 读集校验（可选）→ 写集校验 → 追加版本 |
| `DB.GC` | 按最老活快照回收 |
| `DB.History` | 打印某个 key 的完整版本链，调 bug 神器 |

## 实验

```bash
make exp-tx
go test ./03-transactions/mvcc/ -v -count=1        # 8 个测试
```

两个对照实验最值得跑：

```bash
go test ./03-transactions/mvcc/ -run TestWriteSkew -v
```

它会先在一个函数里断言"两个事务都提交成功（这是 bug）"，然后在另一个函数里断言"第二个被拒（这是修复）"。

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 追加版本而非原地更新 | 读不阻塞写、写不阻塞读、天然可重复读 | 空间膨胀，需要 GC；长事务钉住历史 |
| 首提交者胜 | 消除丢失更新，无需锁 | 写偏斜依然存在；高冲突下重试率上升 |
| 读集校验（Serializable） | 真的可串行化 | 更保守，误杀更多；读集本身也是内存开销 |
| 快照隔离 | 大部分场景够用且快 | 需要开发者知道哪些不变量会被写偏斜破坏 |

## 自测题

1. 为什么"读不阻塞写"是免费得到的？（提示：读的是哪个版本）
2. 用一句话说清"丢失更新"和"写偏斜"的区别。（提示：冲突发生在写集还是读集上）
3. 如果把 `Serializable` 的读集校验去掉，`TestLostUpdateIsPrevented` 还会通过吗？为什么？
4. GC 的 `minTS` 为什么要取"所有活跃事务"的最小值，而不是当前时间？
5. 一个跑了 2 小时的长事务，在一个每秒写 1 万次的系统里会造成什么？你会怎么处理？

## 已知边界

- 全内存实现，没有磁盘持久化，没有 WAL。
- 没有行锁/表锁，也没有 `SELECT FOR UPDATE` 那种显式加锁。
- 读集校验是 OCC 式的，比 SSI 保守（误杀更多），也没有 SSI 的"危险结构"检测。
- 没有死锁检测（因为没有锁）；冲突是**立即失败**，重试由调用方负责。
- `Scan` 会全表遍历，没有索引。
