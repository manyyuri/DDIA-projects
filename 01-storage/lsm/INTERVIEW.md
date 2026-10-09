# INTERVIEW — lsm 讲解

## 30 秒定位

> "LSM 是写优化存储引擎的标准答案：所有写先进内存跳表 memtable，满了刷成有序的 SSTable，后台 compaction 分层合并。我实现了一整套——WAL、memtable、SSTable、稀疏索引、布隆过滤器、分层 compaction、MANIFEST——并把读放大和写放大都测出了数字：10 bits/key 的 bloom 把误判率压到 0.77%，20000 次写换来 473 次 flush 加 258 次 compaction。"

## 两分钟讲解稿

> "写路径：先追加 WAL 保证崩溃安全，再写进跳表的 memtable；memtable 满了变成不可变的，刷成一张 L0 SSTable，后台 compaction 把 L0 往 L1、L2 合并，每层容量 10 倍递增。读路径：memtable → 不可变 memtable → L0 从新到旧 → 每层二分定位唯一候选表，第一个命中的版本就是答案——这就是 shadowing：**新值和墓碑盖住老值，而不是删除老值**。
>
> 几个我特意做对的关键点。**第一，墓碑只能在最底层丢**：如果 L2 的墓碑被 compaction 丢了、L3 还有同 key 老值，老值就复活了。**第二，MANIFEST 是唯一真相**：flush 先写新表、fsync、原子替换 MANIFEST、才删旧表；启动时不在 MANIFEST 里的 .sst 一律当垃圾删掉。**第三，WAL 和 memtable 的顺序不能反，删 WAL 必须在 flush 提交之后**——反了就丢已确认的写，我有专门的测试模拟'不调 Close 直接崩'然后重开。
>
> 数字上，最有意思的是 **bloom 过滤器决定了读放大的常数**：10 bits/key、7 个哈希函数，理论误判率约 0.8%，我实测 0.77%，和公式吻合，而且零漏判——bloom 没有 false negative，这个不对称性恰好是它能安全用于读路径的原因：误判最多多查一张表，漏判就会返回错误答案。
>
> 另一个数字是写放大的结构：20000 次写、5000 个不同 key，落盘 500KB，其中绝大部分是 compaction 重写的历史版本。**LSM 的本质就是用后台重写换前台顺序写**——RocksDB、LevelDB、TiKV、Cassandra 全是这个骨架。"

## 可主动抛出的数字

- 20000 写 / 5000 key：**473 次 flush、258 次 compaction**，落盘 ~500KB，层分布 L0=1 L2=8 L3=12 L4=54
- bloom：**10 bits/key → 误判 0.77%、零漏判**（理论 (1−e^(−7/10))^7 ≈ 0.8%）
- 对比 bitcask：点查从 O(1) 变成多层查找，但换来**范围扫描免费**（k-way 归并迭代器）

## 追问链

**Q1：memtable 为什么用跳表，不用红黑树或 B+ 树？**
> "memtable 的硬需求是'有序 + 能范围遍历 + 高频插入'。跳表三层全满足：插入 O(log n) 无旋转、中序遍历天然有序、实现远比平衡树简单。工程加分项是并发友好——Java 的 ConcurrentSkipListMap 就是靠跳表做的无锁读。红黑树能做但范围遍历要中序栈、代码复杂；B+ 树的批量刷盘优势在磁盘上，memtable 是内存结构，用不上。LevelDB 和 RocksDB 的 memtable 也都是跳表。"

**Q2（追）：你说 L1 以上不重叠所以能二分，为什么 L0 必须重叠？**
> "因为 L0 的表来自不同次 flush，每次 flush 的是整个 memtable 的快照，key 范围天然重叠。compaction 把多张 L0 合成一张有序的 L1，从此每层内部不重叠，所以每层最多一张候选表、可以二分 minKey。L0 只能按 id 从新到旧线性查——这就是为什么 L0 表数是读放大的第一项，我的 compaction 触发条件就是 L0 ≥ 4 张。"

**Q3：读一个不存在的 key 要查多少次磁盘？**
> "最坏情况：memtable、不可变 memtable、L0 全部 4 张、每层一张，全都不命中才算不存在——这就是读放大。bloom 的作用是让绝大多数层在**不读数据块**的情况下就排除掉：每张 SSTable 的 footer 里有布隆过滤器，先查 bloom 再碰索引和数据。10 bits/key 意味着 100 万 key 只要 1.2MB bloom，非常便宜。"

**Q4（追）：bloom 误判率 0.77% 怎么算出来的？**
> "公式 p = (1 − e^(−kn/m))^k，我用了 m/n=10 bits/key、k=7 个哈希（对 10 bits/key 的最优 k 是 ln2 × m/n ≈ 7）。代进去 (1−e^(−0.7))^7 ≈ 0.008。测试是 5000 个 key 建 bloom、查 5000 个不存在 key，命中 0.77%。要降到 0.1% 只要把预算加到 ~15 bits/key——这是一个纯内存换 IO 的线性调节旋钮。"

**Q5：compaction 为什么会带来写放大？一条数据被写几次？**
> "leveled compaction 下一条记录从 L0 到 Ln，每次层合并都被重写一次，10 倍容量比下一条记录平均被重写约 10 次——空间放大换来的。我的测试里 473 次 flush + 258 次 compaction 落盘 500KB，而当前数据只有几十 KB，重写的绝大多数是历史版本。这也是为什么真实引擎用**增量 compaction**（只合并 key range 有重叠的表），我这个教学版是整层合并，写放大比 RocksDB 高。"

**Q6：如果是删除一个 key，磁盘上发生了什么？**
> "写一条墓碑进 memtable，之后一路 flush、compaction 往下传。读路径照常从新到旧，先撞见墓碑就返回不存在——所以墓碑其实让'删除'变得更快（不用等老值真消失）。只有当墓碑随 compaction 到达最底层、确认下面没有老值了，才能物理丢弃。如果提前丢，下层老值复活。"

**Q7：和 RocksDB 比，你这个还差什么？**
> 主动列："① 增量 compaction——RocksDB 按 key range 只合并重叠部分，限制单次重写量；② 后台线程池 + 优先级——我的 compaction 在锁下同步做；③ MANIFEST 增量 version edit——我是全量重写；④ partitioned index filter、前缀 bloom、rate limiter、压缩。但骨架是一致的：WAL→memtable→L0→分层 compaction，读路径的 shadowing 语义完全相同。"

**Q8：什么场景 LSM 反而不如 B+ 树？**
> "读多写少 + 对点查延迟敏感：LSM 点查要过多层、有 bloom 常数；B+ 树一次树下降就到位。还有空间放大敏感的场景——LSM 合并前新旧共存有峰值。另外 compaction 的后台 IO 会造成延迟毛刺（p99 杀手），B+ 树原地更新没有这个问题。InnoDB 选 B+ 树、Cassandra 选 LSM，就是这两种负载画像的分歧。"

## 踩坑故事（可主动讲）

**WAL 删除时机**：flush 的正确顺序是"写新表 → fsync → 提交 MANIFEST → 才能截断 WAL"。如果 flush 完就删 WAL，MANIFEST 提交前崩溃，这段数据既不在 SSTable 也不在 WAL——丢已确认的写。`TestDurabilityViaWALAndReopen` 模拟不 Close 直接崩、重开后靠 WAL 重放补齐，这条顺序是被测试逼着写对的。

## 主动承认的边界

- 整层合并（真实引擎按重叠 key range 增量合并，写放大低得多）；
- flush/compaction 持锁同步执行（真实是后台线程 + 读写不互斥）；
- 无反向迭代、无迭代器 checkpoint（长扫描会挡住 compaction）；
- MANIFEST 全量重写，非增量 version edit。

一句话收尾："这个模块加 bitcask 是同一道题的两个答案——点查无读放大 vs 范围扫描 + 有界空间，三种放大率只能挑两个，我两个都亲手量过。"
