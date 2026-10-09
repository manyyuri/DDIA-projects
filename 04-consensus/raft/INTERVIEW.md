# INTERVIEW — raft 讲解（重点模块，建议放简历主位）

## 30 秒定位

> "我实现了完整的 Raft：选举、日志复制、提交规则、快照、成员变更，外加一个跑在顶上的线性一致 KV。所有故障场景在确定性虚拟时间网络里注入——丢包、分区、随机 churn。最有分量的测试是：被隔离的节点 term 涨到 9 也永远选不上 leader，10 条已提交数据一条不少——这就是选举限制在保护数据。我自己踩过 Raft 论文 Figure 8 那个坑，用 no-op 屏障修好的。"

## 两分钟讲解稿

> "Raft 要解决的不是'选 leader'，是**部分失效下任意两个节点的日志前缀一致**——选举只是手段。五条安全性质环环相扣：每 term 一票 + 多数派相交保证一个 term 至多一个 leader；prevLogIndex/prevLogTerm 检查保证日志匹配；**选举限制**保证已提交的条目一定出现在未来所有 leader 上；合起来就是状态机安全。
>
> 实现里三处最容易写错，我全踩过。
>
> **第一处，提交规则**。不能数到多数派持有就直接 commit——Raft §5.4.2 要求那条目**属于当前 term**。我的第一版没写这个条件，failover 测试报 `k9 vanished after failover`：新 leader 日志里有 10 条旧 term 的数据，但它不敢提交。修法就是论文的做法——新 leader 先提交一条**自己 term 的 no-op**当屏障，它提交时前面的条目随之提交。这个 bug 在真实系统里的表现是'切换后读不到刚写的数据'，特别容易被误诊为丢数据。
>
> **第二处，nextIndex 回退不能钳到快照边界**。我一开始把回退钳到 snapIndex+1，结果 follower 说'我没有这个 index'、leader 又钳回去，无限 ping-pong——落后节点永远追不上。教训是：回退逻辑里每个 clamp 都要问'如果它要的东西已经被我压缩掉了怎么办'，答案必须是'发快照'而不是'再试一次'。
>
> **第三处，回复不能触发无条件广播**。每条 appendResp 都广播心跳，follower 再回、无限风暴，测试直接挂死——真集群里就是 CPU 和网卡打满。修法：只有 commit 水位推进了才广播，否则只追赶那一个 follower。
>
> 安全性验证上，`TestRandomisedChurnPreservesSafety` 跑 12 轮随机分区/宕机/重启，持续断言'term→leader 映射无冲突'（同 term 从不两个 leader）、所有被 ack 的写全部存活。随机 seed 固定，完全可复现。"

## 可主动抛出的数字 / 断言

- `TestElectionRestrictionProtectsCommittedEntries`：隔离节点 term **9** vs 集群 term **1**，10 条已提交数据零丢失
- `TestRandomisedChurnPreservesSafety`：12 轮随机故障，"同 term 双 leader"断言从未触发
- `TestSyncFailoverKeepsCommittedWrites`：no-op 屏障的回归测试（踩坑现场）
- `TestLeaderInMinorityCannotCommit`：少数派 leader 能收 Propose 但 commit 停住；恢复后孤儿写被回滚

## 追问链

**Q1：选举限制为什么能保护已提交数据？推理链给我走一遍。**
> "四步。① 已提交的条目存在于**某个多数派 M** 的日志里。② 赢得选举需要**某个多数派**投票，任何两个多数派必相交，所以至少一个 M 的成员投了票。③ 投票规则：只投给'日志至少和我一样新'（candidate 的最后 term 更大，或 term 相同 index ≥ 我的）的候选人——所以这个 M 成员投票时，自己的日志 ≤ 新 leader 的日志。④ 日志匹配性质归纳：日志前缀一致，所以 M 成员有的，新 leader 全有。已提交条目在新 leader 上，之后永远在。我隔离那个 term 涨到 9 的节点时，集群数据 10 条不少，跑的就是这条推理。"

**Q2（追）：如果去掉选举限制会发生什么？具体点。**
> "可以选出一个日志更短的 leader。具体反例：5 节点，S1..S3 有 index=5 的已提交条目（多数派）。S4/S5 只有 index=3。若 S5 靠 S1 崩溃、S2/S3 网络抖动时凑齐 S4+它自己……只要凑出一个不含 S1..S3 日志的新多数派（在有成员变更或旧节点重启时序混乱下更危险），新 leader 缺 index=5，它用自己的空缺日志覆盖别人——已提交数据被抹掉。我的测试把这条不变量当成断言跑在随机 churn 里。"

**Q3：为什么不能直接提交旧 term 的条目？Figure 8 到底在说什么？**
> "Figure 8 的时序：S1 在 term2 把 index=2 复制到自己（仅自己），term3 时 S5 靠 S2/S3/S4 当选、也在 index=2 写了自己的条目，然后 S1 在 term4 重新当选、把**自己 term2 的** index=2 复制到 S3——此刻 index=2 在 S1/S3 多数派上，但它是旧 term 的。如果此时 S1 崩溃、S5 靠 S2/S3/S4（S2/S4 有 S5 的 term3 同 index 条目…）再度当选，S5 的 term3 条目会覆盖 S1 的 term2 条目——**多数派持有过的条目被覆盖了**。所以'多数派持有'对旧 term 条目不构成提交承诺；必须等新 leader 用自己 term 的条目（no-op）把它'背书'进提交序列。我的 k9 崩溃就是这个的反向版本：不是覆盖，是'不敢提交'。"

**Q4：term 和 votedFor 为什么要先落盘再回消息？**
> "否则崩溃重启后节点忘了投过票。同一个 term 里它先投 A、崩溃、重启、再投 B——A 和 B 都凑齐多数派，同 term 双 leader，安全性破了。所以 HardState{Term, VotedFor} 必须在发送任何投票响应之前 persist。我的代码里 `n.votedFor = msg.CandidateID; n.persistLocked(); n.sendLocked(...)` 顺序是测试逼出来的。反过来说，Raft 只需要持久化这一小撮状态，成本可控。"

**Q5：被网络隔离的旧 leader 还在提供读服务，会出什么问题？怎么解？**
> "它不知道自己已被废黜，会拿本地状态机回答**陈旧读**，线性一致性被打破——DDIA §8 那个 fencing token 讲的就是它。解法三档：① **读也走日志**——读命令当一条日志条目提交后才读本地状态机，绝对正确但每次读一个 RTT 加一条日志；② **ReadIndex**——一轮心跳确认自己仍是 leader，等状态机 apply 到 readIndex 再回，省日志条目，仍有一个 RTT；③ **LeaseRead**——心跳维系租约，租约内直接本地读，零 RTT 但**引入时钟假设**，时钟错就出错读。我的实现选了①，因为教学上它是唯一无条件的正确答案；etcd 默认 ReadIndex、可选 LeaseRead。"

**Q6（追）：LeaseRead 的时钟假设具体错在哪？**
> "租约有效期 T 内不再确认 leadership。但如果 GC 停顿/时钟跳变，旧 leader 以为租约还在，实际早已被废黜——它在停止的瞬间错过了一切，醒来时还是旧世界观。Jepsen 抓过 etcd/CockroachDB 的这类问题。所以才有了 fencing token：即使读本地，每个响应带递增的 token，下游校验 token 新旧。共识系统绕不开的哲学：**不依赖时钟的正确性，要用别的机制买回来**。"

**Q7：conflictTerm 回退是怎么优化的？**
> "follower 拒绝 AppendEntries 时返回 conflictIndex + conflictTerm。leader 若自己日志里有 conflictTerm，就把 nextIndex 直接跳到'该 term 在我日志里的最后一条+1'——一次跨过一个 term，而不是一次退一条。没有 conflictTerm 才退到 conflictIndex。最坏情况退化成逐条回退，常见情况一次到位。这是 etcd/raft 的标准做法，我照做并在注释里推导了为什么它不会跳过头。"

**Q8：和 etcd 的 raft 库比，你的实现缺什么？**
> 主动列清单："① **PreVote**——隔离节点不涨 term 就先预投票，避免恢复后强迫全集群重新选举（我那个 term 9 就是没 PreVote 的现象）；② **CheckQuorum**——leader 主动发现失联主动下台；③ pipeline/批量/流水线复制，吞吐差一个数量级；④ joint consensus 成员变更（我是单节点变更近似）；⑤ leader transfer、flow control、分级心跳。etcd 的库还把 IO 全部交给应用层驱动（Ready 结构），纯状态机——我的实现 IO 和逻辑耦合在一起，这是架构上最大的差距。"

**Q9：为什么选举超时是随机的？随机范围怎么定？**
> "打破对称：固定超时下所有 follower 同时竞选、互相分票、谁都凑不齐多数、全部超时再来——活锁。随机化让某个节点先迈出一步。范围要满足：broadcastTime ≪ electionTimeout ≪ MTBF。我默认 150–300ms（模拟网络里等效换算），太窄反复分票、太宽故障切换慢。真实 etcd 是 1s 默认 tick×10，还要配 PreVote 才稳。"

**Q10：你的快照怎么做的？有什么坑？**
> "触发 Snapshot 时把状态机整体序列化进 HardState，apply 到 snapIndex 的日志可以截掉。落后太多（nextIndex < snapIndex）的 follower 走 InstallSnapshot 整体替换状态机，用 ApplyMsg{Type:\"snapshot\"} 送进 apply 循环——必须区分类型，否则会被当普通命令重放到状态机上。坑就是前面说的 nextIndex 钳制死循环。边界：真实系统快照要分块传输+断点续传+传输期间缓冲新日志，我是整体替换。"

## 踩坑故事（三个都在两分钟稿里，追问时展开）

1. **提交规则**（Figure 8 / no-op 屏障）：`k9 vanished after failover` → 加当前 term 检查 + no-op；
2. **nextIndex 钳制**：`TestSnapshotCatchesUpFarBehindFollower` 无限 ping-pong → 允许回退越过快照边界、由 replicateTo 决定发快照；
3. **广播风暴**：测试挂死 → 只在 commit 推进时广播；
4. **apply loop 竞态**：`-race` 抓到重启时 apply channel 的复用竞态，用局部 channel 变量修好——这是我为什么坚持 `make test-race` 值得单独跑。

## 主动承认的边界

- Storage 每次全量重写状态文件（真实：增量 append + 批量 fsync）；
- 无 PreVote/CheckQuorum、无 pipeline/批量提交；
- 单节点成员变更，无 joint consensus；
- 快照整体传输无分块；
- 吞吐离生产差一个数量级（每写一次 fsync 的路径没优化）。

一句话收尾："Raft 我不是背下来的，是**被测试打出来的**——论文的每个 'must' 背后都有一个我能复现的失败测试。随便挑一条性质问我推导链。"
