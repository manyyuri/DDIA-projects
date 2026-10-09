# INTERVIEW — mapreduce 讲解

## 30 秒定位

> "MapReduce 的教学实现，重点不在 map/reduce 本身——那个很简单——而在**把 shuffle 的代价做成可测量的**：中间结果对数、字节数、各分区大小、倾斜 key 列表全在 Stats 里。核心测试：1000 行同一个 hot key，最大分区份额 1.0——两百台机器的集群这个 job 也只能用一台。"

## 两分钟讲解稿

> "map 阶段无共享、任意并行；shuffle 按 key 哈希分区、全量数据跨网络移动；reduce 每个分区内按 key 分组独立计算。**成本几乎全在 shuffle**：我的 Join 是 reduce-side join，左右表每行都打上 tag 进 shuffle——Stats 会把要搬的对数和字节数直接报出来，'这个 join 贵不贵'第一次变成了数字而不是感觉。
>
> 第二个重点是**数据倾斜**。TestSkewIsDetected 构造 1000 行 hotkey：MaxPartitionShare = 1.0，99% 的数据落在一个 reducer——集群规模再大，job 时长被一个 reducer 绑架。三种标准打法我都对应到了实现：combiner 让 map 侧先本地聚合（word count 的 sum 可以本地相加）；给热 key 加盐 hotkey#0..9 做两阶段聚合；以及 map-side（广播）join——小表进内存、大表流式过，**当一侧装得进内存，它比 reduce-side 便宜一个数量级**，这是批处理最常见的优化。
>
> 第三个重点是**确定性**：map/reduce 必须是纯函数，groupByKey 里对 key 排序让输出可复现——同一个 job 重跑两次输出逐字节相同。这不是洁癖：批处理靠'失败任务重跑'容错，如果 map 里有随机数、当前时间、或者依赖哈希遍历顺序，重试就会污染结果。真实系统里'重跑和第一次结果不一样'的 bug 很多都是这么来的。"

## 可主动抛出的数字

- 倾斜测试：`skewed keys: [hotkey(1000)]`，**MaxPartitionShare ≈ 1.0**（200 台机器只用 1 台）
- WordCount（3 文档）：pairs=13、bytes=63、partitions=[4 5 4]、skew=0.38
- PageRank：每轮迭代一个 job，shuffle 的是 (邻居, rank/出度) 对
- 分区测试断言：无空分区、最大份额 ≤ 60%

## 追问链

**Q1：shuffle 到底贵在哪？拆开说。**
> "三笔账：① **磁盘**——map 输出先写本地盘（spill），不是直接发网络；② **网络**——全量中间数据跨节点搬，数据集多大搬多大；③ **排序**——reduce 侧按 key 分组前要归并排序。reduce-side join 是极端情况：**两张表全部内容**都要过这三笔账，而 join 实际只需要匹配的部分。我的 Stats 把 pairs 和 bytes 直接统计出来，就是为了让人看见这笔账。"

**Q2：加盐怎么保证聚合结果是对的？**
> "两阶段：第一阶段 map 把 hotkey 映射到 hotkey#0..9（随机或轮询），reducer 各自聚合出局部和——因为同一个逻辑 key 被打散到 10 个 reducer，谁也不倾斜。第二阶段再来一个 job，去掉盐后缀，把 10 个部分和加总。正确性来自聚合函数的结合律：sum(sum(x_i 分组)) == sum(all)。注意这只对可结合的聚合成立——avg、distinct 要换成 sum/count、bitmap 这类可合并的中间表示。"

**Q3（追）：什么聚合没法加盐救？**
> "不可分解的：median（要全量数据才能算，只能采样或近似算法如 t-digest）、全局 distinct count（要用 HLL 这类可合并 sketch）、top-k 带复杂比较逻辑的要用两阶段 heap。共同思路都是**把'必须看到全量数据'的运算改写成'局部可算 + 可合并'**，这是分布式聚合的一般方法论。"

**Q4：为什么 map/reduce 必须是纯函数？给个真实反例。**
> "反例：map 里用当前时间打标，或者用 UUID。map task 跑了 70% 失败，重试后那 30% 的时间戳变了——同一个输入文件，两次执行产出不同的中间结果，reduce 端无法区分新旧，结果集被污染且不可察觉。更隐蔽的：依赖下游遍历顺序的 map（比如从 HashMap 迭代），JDK 版本一变结果就变，'重跑历史 job 对不上账'。我在 groupByKey 里显式排序，就是把这个不变量写进代码。"

**Q5：PageRank 每轮 shuffle 的是什么？为什么不能一个 job 算完？**
> "每轮：每个节点把自己的 rank 除以出度，向每个邻居发 (邻居ID, 贡献值)，shuffle 按邻居 ID 聚合，reducer 把收到的贡献求和、加阻尼系数得到新 rank。迭代收敛（rank 变化 < 阈值）才停，所以是**多个 job 串起来**，每轮都是一次全量 shuffle——这就是图计算在 MapReduce 上低效的原因（迭代间数据落盘再读回），Spark 把中间结果放内存、Pregel 用 BSP 模型常驻，都是对这个成本的直接回应。"

**Q6：map-side join 什么时候可用？什么情况下必须 reduce-side？**
> "map-side 的前提：一侧**完整装进每个 mapper 的内存**（或本地盘有副本）。大表流式读，每行查内存小表，零 shuffle。代价是每台机器都要有完整小表（广播成本）。两表都大就必须 reduce-side：按 join key shuffle 后匹配，双方同 key 必然落同一 reducer——这是分区函数保证的。中间状态是'半连接'类优化：先统计一侧 key 分布、只广播命中的部分。我的实现两个都写了，Stats 能直接对比两种方案的 shuffle 字节数。"

**Q7（追）：那 skew join 有没有不加盐的工业方案？**
> "有。Spark 的 AQE（Adaptive Query Execution）：运行时发现分区倾斜，自动拆分热分区重分布；Hive 的 skewjoin hint 走专门的 skew 表副本。本质都是'把倾斜的发现和拆分从程序员的先验知识挪到运行时统计'——和我的'把倾斜报出来而不是默默变慢'是同一个哲学：**倾斜要可见，不可见就没法治**。"

**Q8：reducer 数量怎么选？**
> "经验起点：约等于总输入/目标单 reducer 处理量（常见 1–10GB 每 reducer，视聚合密度）。太多：小任务、调度开销、输出大量小文件（下游 NameNode 压力、下次读取寻址成本）；太少：并行度低、倾斜风险集中、单点内存压力。我的测试里 NumReducers=3 时最大份额 0.38，倾斜的动态性意味着这个数和 key 分布耦合——所以真实的答案是**先看分区统计再定**，又是 Stats 的价值。"

## 对照真实系统

- Hadoop MR：spill 到盘、combiner 显式阶段、speculative execution（慢节点复制一份任务赛跑）；
- Spark：内存迭代（解决 PageRank 类的 shuffle 累积）、AQE 运行时反倾斜；
- 我实现里没有的：溢写、推测执行、真集群调度——单进程内存版。

## 主动承认的边界

- 单进程内存执行，无磁盘溢写、无容错/重试/推测执行；
- combiner 不是显式阶段（map 里自己聚合）；
- PageRank 是硬编码循环，非通用迭代框架；
- 输出无全局排序保证（哈希分区天然无序）。

一句话收尾："这个模块给我的是**给批处理算账的能力**——任何一个 join/聚合，我能当场估出它 shuffle 多少数据、倾斜在哪，这个习惯从 Stats 来。"
