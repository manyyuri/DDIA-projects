# lsm — SSTable + 内存表 + 分层合并

> DDIA §3.2 "SSTables and LSM-Trees"。写优化的存储引擎，也是"三种放大只能选两个"最典型的例子。

## 机制

```
写：  WAL（崩溃安全）→ memtable（跳表，有序）→ 满了刷成 L0 SSTable
      → compaction 把 L0 合进 L1，L1 合进 L2 …（层容量比 10x）
读：  memtable → 不可变 memtable → L0（新的先查）→ L1..Ln（二分定位唯一候选表）
范围扫描：所有有序来源做 k-way 归并
```

和 `bitcask` 是同一道题的两个答案：

| | bitcask | LSM |
|---|---|---|
| 点查 | O(1)，无读放大 | 要查多层（读放大），靠 bloom 压住 |
| 范围扫描 | **做不到** | 天然支持 |
| 索引 | 全内存哈希表 | 磁盘上的稀疏索引 + bloom |
| 写 | 每次全量追加 | 顺序写 + compaction 的额外重写 |
| 空间 | 更新越多越鼓 | compaction 之后有界 |

## 关键设计点

### 1. 跳表而不是哈希表

memtable 必须是**有序**的，否则刷盘时没法输出有序的 SSTable，也就没有范围扫描。跳表是"带索引的链表"，插入 O(log n)，天然有序。

### 2. Tombstone 是一条记录，不是一个删除操作

覆盖写和删除都只是写一条**更新的格式版本**：

```
逻辑上：  "k = v2"、"k 已删除"
物理上：  memtable 里是新值 / 墓碑；老值可能还稳稳地躺在 L2 的某个 SSTable 里
```

所以读路径必须**从新到旧分层查找，第一个命中的版本就是答案**。`mergeIter` 用堆做 k-way 归并，同 key 时按"数据源的新旧排名"取第一个，其余全丢——这就是 shadowing。

墓碑只有在**最底层**才允许被丢弃。中间层丢掉墓碑会让下层的老值复活。

### 3. 布隆过滤器决定了读放大的常数

没有 bloom，一次点查要碰 L0 的每一张表加每一层一张表。有了 bloom，绝大多数表在**不打开文件**的情况下就能排除。

实测（`TestBloomFilterEffectiveness`）：**10 bits/key → 误判率 0.77%**，且**零漏判**（布隆过滤器不会有 false negative，这是它安全的原因）。

### 4. 层容量与 compaction

- L0 是**重叠**的（每次 flush 一张新表），所以 L0 要线性扫描、按 id 从新到旧查；
- L1 以上**不重叠且按 minKey 排序**，所以二分能找到唯一候选；
- 触发条件：L0 表数 ≥ 4，或第 i 层字节数超过 `base × ratio^(i-1)`。

`spillWriter` 按目标大小（默认 2MB）切分合并输出，这正是保持 L1+ 不重叠的原因。

### 5. MANIFEST 是"哪些表现在属于这个库"的唯一真相

flush / compaction 会先写新表 → `fsync` → 原子替换 MANIFEST → 才删旧表。启动时**不在 MANIFEST 里的 `.sst` 一律当垃圾删掉**（那是没提交成功的 flush 留下的）。

### 6. WAL 必须先落盘，且刷盘顺序不能反

```
1. 追加 WAL 记录（必要时 fsync）
2. 改 memtable
3. 若 memtable 满：先把不可变 memtable 写成 L0 表并提交 MANIFEST，再删 WAL
```

第 3 步顺序反了就会丢已确认的写。`TestDurabilityViaWALAndReopen` 里"崩溃"（不调用 Close）后重开，WAL 重放的数据必须先落成 L0 表才能删 WAL。

## 代码地图

| 文件 | 内容 |
|---|---|
| `memtable.go` | 跳表 + WAL 编解码与重放 |
| `sstable.go` | 布隆过滤器、SSTable 读写、稀疏索引、**顺序读缓冲** |
| `iterator.go` | 统一迭代器接口、`mergeIter`（含 shadowing）、对外的 `Iterator` |
| `db.go` | Open/恢复、读写路径、flush/compaction、MANIFEST、Stats |

## 实验

```bash
go test ./01-storage/lsm/ -v -count=1
go test ./01-storage/lsm/ -run TestCompactionBoundsReadAmplification -v   # 约 20s
```

**实测数据（`TestCompactionBoundsReadAmplification`：20000 次写、5000 个不同 key、mem 4KB）：**

```
层分布   L0=1  L1=0  L2=8  L3=12  L4=54
层字节   3234 / 0 / 54670 / 71610 / 379456   （约 500KB）
flush    473 次
compaction 258 次
```

看这几个数就能理解 LSM 的成本结构：**473 次 flush 和 258 次 compaction 换来"写永远顺序、数据永远有序"**，而落盘 500KB 里绝大部分是 compaction 重写的历史版本，不是当前数据。

| 实验 | 验证了什么 |
|---|---|
| `TestShadowingAcrossLevels` | 老值进了 L2 之后，新值和墓碑依然能盖住它 |
| `TestTornWALTailIsDropped` | WAL 尾巴的撕裂写被精确丢弃，前面的记录完好 |
| `TestCompactionPreservesFullDataset` | 3000 个写 + 混入删除，重启后逐 key 校验一致 |
| `TestIterateSortedAndSeek` | 有序遍历 + range seek（bitcask 做不到的事） |

## 取舍

| 得到 | 代价 |
|---|---|
| 顺序写，吞吐高 | **写放大**：一条记录会被 compaction 重写很多次 |
| 天然有序，范围扫描免费 | **读放大**：要查多层（bloom 把常数压到很低，但不为零） |
| 磁盘上的索引，不占内存 | 空间放大有峰值（合并前新旧数据共存） |
| 布隆过滤器 | 每个 key 额外 10 bit |

## 自测题

1. 为什么 L0 可以重叠、L1 以上必须不重叠？（提示：查找时能不能二分）
2. 墓碑为什么只能在最底层丢弃？如果 L2 的墓碑被丢了、L3 有同 key 的老值，会发生什么？
3. 布隆过滤器有误判但没有漏判。这个不对称性为什么恰好是它安全的前提？
4. 把 `L0CompactionTrigger` 从 4 调到 8，读放大和写放大各怎么变？
5. WAL 先写、memtable 后改。反过来会怎样？（提示：改完 memtable 就返回给客户端了）

## 已知边界

- Compaction 按**整层**合并，真实引擎只合并重叠的 key range，写放大低得多。
- flush 和 compaction 都在 `db.mu` 下**同步**执行（不阻塞写、但阻塞读），没有后台线程。
- 不支持反向迭代（`Iterator` 只有 `Next`）。
- 没有 `Iterator` 的 resume/checkpoint，长扫描会一直持读锁、挡住 compaction。
- MANIFEST 是全量重写，不是增量 version edit。
