# internal/simnet — 确定性虚拟时间网络

> 所有分布式项目的底座。DDIA §8–9 讲的其实都是**部分失效**，而部分失效用真 socket 是没法可复现地研究的。

## 为什么需要它

真实网络里你没法复现"leader 在收到 quorum ack 之后、把 commit 广播出去之前崩溃"。但这句话描述的正是 Raft 安全性的核心场景。所以这里的做法是：

- **虚拟时间**：没有 `time.Sleep`，`net.Run(d)` 把时钟推进 d，按 `(时刻, 序号)` 顺序派发事件；
- **确定性**：`simnet.New(seed)` 固定随机源，丢包与抖动全部可复现；
- **单线程**：所有 handler 在调用 `Run` 的那个 goroutine 里同步执行，测试不会 flaky。

代价：它测不出真正的数据竞争（这要靠 `go test -race`），而且 handler 里不能再次调用 `Run`（要延后动作请用 `After`）。

## 故障注入 API

| 方法 | 模拟什么 | 典型用法 |
|---|---|---|
| `SetLatency(d, jitter)` | 网络延迟与抖动 | 让"复制落后"变成可观测的 |
| `SetDropRate(p)` | 随机丢包 | Raft 在丢包下能否收敛 |
| `Partition(groups...)` | 网络分区（跨组消息丢弃） | 少数派 leader、脑裂 |
| `Isolate(id, others)` | 单节点隔离 | 选举限制、日志冲突 |
| `Unregister(id)` | 进程崩溃（节点不再收消息） | 需要"磁盘还在"的崩溃 |
| `DropLink(a, b)` | 单向黑洞 | 慢节点 / 半连通 |
| `After(d, fn)` | 定时器 | 选举超时、心跳、重试 |

`Alive(id)` 是判断节点是否还活着的唯一正确方式——**崩溃的节点仍然认为自己是 leader**，`raft` 的 `anyLeader()` 就是靠这个避免误判。

## 事件顺序为什么可复现

队列按 `(at, seq)` 排序，`seq` 是入队序号。同一时刻的事件严格 FIFO，所以"同一毫秒内先发的消息先到"是确定的。

## 一个使用示例

```go
net := simnet.New(42)
net.SetLatency(3*time.Millisecond, 2*time.Millisecond)
net.Register("n1", func(m simnet.Message) { /* 处理 m.Body */ })

net.Partition([]string{"n1"}, []string{"n2", "n3"}) // n1 被隔离
net.Send("n1", "n2", appendEntries{...})            // 会被丢弃
net.Run(100 * time.Millisecond)                     // 推进虚拟时间
net.Heal()
```

## 自测题

1. 为什么 `RunUntilIdle` 对带周期性定时器（如 Raft 的 tick）的系统会一直不 idle？
2. 如果 handler 里直接调用 `net.Run`，会发生什么？为什么？
3. `Dropped` 计数在哪些情况下会增加？崩溃节点的丢弃和分区的丢弃语义上有什么不同？
