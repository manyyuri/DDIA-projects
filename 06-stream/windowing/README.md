# windowing — 事件时间处理

> DDIA §11.3–11.5。这一章的三句"反常识"：**处理时间不等于事件时间**；**你永远无法知道一个窗口的数据收完了没有**；**迟到是常态，不是异常**。

## 机制

```
事件 → 分配窗口（tumbling：1 个；sliding：多个）
     → 累加进 pane（窗口 × key 的状态）
水位线推进 → 窗口到期 → 发射结果 → 落进 sink（幂等 upsert）

水位线 = 见过的最大事件时间 - 允许乱序的界限
"我赌不会再收到比这更老的事件了"
```

## 关键设计点

### 1. 只有按事件时间开窗，重放才可复现

`TestOutOfOrderWithinBoundIsEquivalentToInOrder`：同一批事件，一次按 `1,2,3,4,5` 到达，一次按 `5,1,4,2,3` 到达，**产出的结果完全相同**：

```
both produced [{[12:00:00,12:00:10) a 15 5 1}]
```

如果按**处理时间**开窗，两次到达顺序会落进不同的窗口，结果天差地别，而且没法重放来复现。这就是为什么流处理必须显式支持事件时间。

### 2. 水位线是一个赌注，不是保证

```go
Watermark = maxSeenEventTime - MaxOutOfOrderness
```

`MaxOutOfOrderness` 调大 → 结果更完整，但**每个结果都要多等那么久**。这是一个纯粹的延迟/完整性取舍，没有正确答案。

`TestWatermarkDrivesEmission` 精确演示了触发时机：事件时间到 14s、乱序界限 2s 时水位线 = 12s，才关掉 `[0,10)` 这个窗口。

### 3. 迟到数据的三种处理，必须显式选一个

| 情况 | 判定条件 | 处理 |
|---|---|---|
| 还没发射 | 水位线未过窗口结束 | 正常累加 |
| **迟到但可救** | 水位线过了结束，但没超过"结束 + 允许迟到" | **更新已发射的结果**（revision 2） |
| **无救** | 水位线超过"结束 + 允许迟到" | **侧输出（侧流/dead letter）** |

`TestLateEventWithinAllowedLatenessUpdatesResult`：

```
result corrected from 1 to 11 after a late event (revision 2)
```

**关键实现细节**：判定标准是**水位线**，不是事件自身的时间戳。我第一版写成"事件时间是否超过窗口结束 + 允许迟到"，结果窗口状态已经被丢掉了却还在尝试更新它——测试直接暴露了这个 bug。

`TestTooLateEventGoesToSideOutput` 验证第三种：`dead letter: a@12:00:02=99.0`，并且断言这条 99 **没有**被折进任何已发射的结果里。

> 为什么是"侧输出"而不是"丢弃"？因为静默丢数据是最糟的失败模式。侧输出至少让运维能看见"有多少事件根本没进结果"，而这个数字通常就是"允许迟到设小了"的信号。

### 4. revision 是撤回 + 重发

真实系统里，更新一个已发布的结果需要**两件事**：把旧值撤回、把新值发出（DDIA §11.3 的 retraction）。下游消费者要能处理"减掉旧值、加上新值"。

测试里用 `Revision` 字段标记第几次发布，模拟了这个协议的存在。

### 5. Exactly-once 是在 sink 侧实现的

```
物理写入 2 -> 4 次，但可见状态完全相同（2 行）
```

`TestReplayIsIdempotentAtTheSink` 用一个**共享的 sink** 跑两遍完整流程（模拟"写成功了但 offset 没提交，重启后重放"）：物理写次数翻倍，但**可见状态完全一致**。

实现方式很土但很有效：`Upsert(fmt.Sprintf("%s|%s", window, key), sum)`——按 `(窗口, key)` 幂等覆盖，而不是 append。

**结论：exactly-once 语义 = 至少一次投递 + 幂等写入。** 它不是靠"投递恰好一次"实现的（那在分布式里做不到），而是靠"重复投递不产生副作用"。

## 代码地图

| 位置 | 内容 |
|---|---|
| `Tumbling` / `Sliding` | 窗口分配（sliding 一个事件进多个窗口，成本更高） |
| `WatermarkGenerator` | 水位线推算 |
| `Pipeline.Process` | 迟到判定 → 累加 → 触发 |
| `fireDueLocked` | 发射 / 更新 / 丢弃状态 的判定 |
| `Sink` | 幂等 upsert + `Attempts` vs `Logical` 两个计数 |
| `Pipeline.Checkpoint` | 算子状态快照（水位线 + 活跃 pane） |

## 实验

```bash
make exp-stream
go test ./06-stream/windowing/ -v -count=1     # 8 个测试
```

试试把 `MaxOutOfOrderness` 从 2s 改成 0：`TestOutOfOrderWithinBoundIsEquivalentToInOrder` 会失败（乱序事件落进已关闭的窗口，进侧输出）。这就是那个取舍的代价。

## 取舍

| 选择 | 得到 | 代价 |
|---|---|---|
| 事件时间 | 可重放、结果确定 | 必须维护窗口状态、必须处理迟到 |
| 大 `MaxOutOfOrderness` | 结果完整 | 结果延迟高 |
| 小 `MaxOutOfOrderness` | 结果快 | 更多事件变侧输出 |
| 允许迟到 + revision | 结果被修正 | 下游要处理撤回/更新；状态保留更久 |
| 幂等 sink | 重放安全 | 需要给每条结果一个稳定主键；下游必须支持 upsert |
| Sliding 窗口 | 结果更平滑 | 一个事件进多个窗口，状态和计算量成倍 |

## 自测题

1. 为什么水位线不能精确判断"窗口收完了"？它到底是什么？
2. 迟到判定为什么要用**水位线**而不是事件自身的时间戳？用后者会出什么错？
3. `MaxOutOfOrderness = 0` 时，一个乱序事件会去哪？这个选择适合什么场景？
4. 撤回（retraction）为什么是必要的？下游如果只支持 append 会怎样？
5. 幂等 sink 的 key 应该由什么组成？只用 `key`（不带窗口）会出什么问题？
6. Checkpoint 里为什么要存水位线？只存 pane 状态会怎样？

## 已知边界

- 单机内存算子，没有分布式状态、没有 checkpoint 的持久化与恢复。
- 没有空闲分区处理（真实系统里一个没数据的分区会把全局水位线卡住）。
- 没有窗口合并（session window）和自定义 trigger（early/late firing）。
- 撤回只是一个 `Revision` 标记，没有真正把"减掉旧值"的语义传给下游。
- 没有背压（backpressure）。
