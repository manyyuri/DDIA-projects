# mapreduce — 批处理

> DDIA §10。批处理的编程模型本身很简单（map → shuffle → reduce），**成本全在 shuffle 上**。这个包把 shuffle 的形状和代价做成可测量的。

## 机制

```
map 阶段      无共享，可任意并行（输入切块，每块一个 goroutine）
shuffle       按 key 哈希分区 → 全量数据跨网络移动    ← 真正贵的地方
reduce 阶段   每个分区内按 key 分组，独立计算
```

复杂计算就是**多次 map/reduce**（PageRank 的每一次迭代都是一个 job）或者**多路输入**（join 靠给两边打 tag）。

## 关键设计点

### 1. Shuffle 是全量搬运

`Join` 的实现是**reduce-side join**：给左表和右表的每一行都打上 `L:` / `R:` 前缀，键变成 join key，然后整个左表 + 整个右表都参与 shuffle。

```
中间结果 pairs 数 + 字节数会被统计出来 —— 这就是"这次 join 要搬多少数据"
```

对照方案是 **map-side（广播）join**：小表加载进内存，大表直接流式过滤。**当一侧足够小、能塞进内存时，它便宜一个数量级。**这是批处理里最常见的优化。

### 2. 数据倾斜：一个热 key 毁掉整个 job

`TestSkewIsDetected` 构造了 1000 行都带 `hotkey` 的输入：

```
skewed keys: [hotkey(1000)]
MaxPartitionShare = 1.00     ← 99% 的数据落在一个 reducer 上
```

**即使集群有 200 台机器，这个 job 也只能用到 1 台。** 这是批处理最大的实际瓶颈。三种标准打法：

1. **combiner**：map 侧先本地聚合（word count 的 `sum(1)` 可以本地相加）；
2. **给热 key 加盐**：`hotkey` → `hotkey#0..9`，两阶段聚合；
3. **map-side join**：把小表广播下去，根本不 shuffle。

框架把倾斜**报出来**而不是默默变慢——生产系统里这就是一个需要告警的指标。

### 3. 分区策略决定并行度

`partitionOf` 用 FNV 哈希，所以同一 key 必然进同一个 reducer（join 和聚合的前提）。`TestShuffleSpreadsKeysAcrossReducers` 断言了三点：每个 reducer 都拿到活、没有 reducer 拿 0、最大份额不超过 60%。

> 哈希分区**均匀但不保序**。要输出有序结果就得分区有序（range partitioner）或者最后再做一次全局排序——这是 shuffle 的经典取舍。

### 4. 确定性是重试安全的前提

map / reduce 必须是**纯函数**。`groupByKey` 里对 key 排序就是为了让输出可复现：**同一个 job 重跑两次，输出必须逐字节相同**，否则"失败任务重试"就会污染结果。

（真实系统里反例很多：随机 ID、当前时间、依赖遍历顺序的 map —— 这些都会让重试产生不同结果。）

## 代码地图

| 位置 | 内容 |
|---|---|
| `Run` | map 阶段（goroutine 池）→ shuffle → reduce 阶段 |
| `partitionOf` / `groupByKey` | 分区与分组（含确定性排序） |
| `Stats` | **中间结果对数、shuffle 字节数、各分区大小、最大份额、倾斜 key 列表** |
| `WordCount` / `Join` / `PageRank` | 三个例子 job |

## 实验

```bash
go test ./05-batch/mapreduce/ -v -count=1     # 4 个测试
```

**实测数据：**

| job | 统计 |
|---|---|
| `WordCount`（3 个文档） | `mapTasks=3 reduceTasks=3 pairs=13 bytes=63 partitions=[4 5 4] skew=0.38` |
| 倾斜测试（1001 行） | `skewed keys: [hotkey(1000)]`，最大份额 ≈ 1.0 |
| `PageRank`（a,b,c → hub） | `hub=0.4780 a=0.4470 b=0.0375 c=0.0375`，rank 之和 = 1 |

把 `NumReducers` 从 3 改成 10，观察 `PartitionSizes` 怎么变——分区越多，单分区越小，但 shuffle 的固定开销和任务调度成本上升。

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 哈希分区 | 均匀、同 key 必同 reducer | 输出无序；热点 key 依然倾斜 |
| reduce-side join | 通用，两边多大都行 | 全量 shuffle |
| map-side join | 无 shuffle，极快 | 一侧必须能装进内存 |
| 更多 reducer | 并行度、单分区更小 | 调度开销、小文件问题 |
| 确定性 map/reduce | 重试安全、结果可复现 | 不能用随机、时间、遍历顺序 |

## 自测题

1. 为什么"整个左表跨网络"是 reduce-side join 的固有成本？什么条件下能避免？
2. `hotkey` 出现 1000 次时，加盐 `hotkey#0..9` 之后聚合怎么保证结果正确？（提示：两阶段，先局部再全局）
3. 为什么 map/reduce 必须是纯函数？说一个不纯就会出错的真实例子。
4. PageRank 为什么要迭代多次 job？每次迭代里 shuffle 的是什么？（提示：每个节点把自己的 rank 除以出度发给邻居）
5. 如果 reducer 数大于不同 key 数，会发生什么？值得吗？

## 已知边界

- 单进程、内存执行，没有真集群、没有磁盘溢写（spill）。
- 没有 combiner 阶段的显式支持（只能靠 map 自己先聚合）。
- `PageRank` 是硬编码的迭代循环，不是通用的迭代 job 框架。
- 没有容错/重试/推测执行（speculative execution）。
- 没有排序保证的 reduce 输出（只有一个最终的按 key 排序）。
