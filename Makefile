GO      ?= go
TIMEOUT ?= 900s
PKGS    ?= ./...

.PHONY: help test test-race vet fmt bench exp exp-storage exp-partitioning exp-raft exp-stream exp-tx clean

help:
	@echo "DDIA-projects — 常用目标"
	@echo ""
	@echo "  make test                  全量测试（约 2 分钟）"
	@echo "  make test-race             全量测试 + 竞态检测（约 4 分钟）"
	@echo "  make vet / fmt             静态检查 / 格式化"
	@echo "  make bench                 bitcask 与 LSM 的基准测试"
	@echo ""
	@echo "  make exp-storage           存储引擎：写放大、bloom 误判率、compaction"
	@echo "  make exp-partitioning      分区：加节点搬多少数据，虚拟节点如何削倾斜"
	@echo "  make exp-raft              共识：选举限制、快照追赶、随机故障下的安全性"
	@echo "  make exp-tx                事务：写偏斜的复现与修复"
	@echo "  make exp-stream            流处理：水位线、迟到数据、幂等重放"
	@echo ""
	@echo "  make exp pkg=<包> run=<正则>   跑任意单个实验，例如："
	@echo "    make exp pkg=./06-stream/windowing run=TestLateEvent"
	@echo ""
	@echo "  make clean                 清理测试缓存"

# ------------------------------------------------------------------ 基础

test:
	$(GO) test $(PKGS) -count=1 -timeout $(TIMEOUT)

test-race:
	$(GO) test $(PKGS) -count=1 -race -timeout $(TIMEOUT)

vet:
	$(GO) vet $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

bench:
	@echo "=== bitcask：顺序写 + 随机读（16B key / 256B value） ==="
	$(GO) test ./01-storage/bitcask/ -run '^$$' -bench 'BenchmarkPutSequential$$|BenchmarkGetRandom' -benchtime 5000x
	@echo ""
	@echo "=== 每次写都 fsync：持久性的价格 ==="
	$(GO) test ./01-storage/bitcask/ -run '^$$' -bench BenchmarkPutSequentialSync -benchtime 500x
	@echo ""
	@echo "=== lsm：写放大与读放大的静态统计 ==="
	$(GO) test ./01-storage/lsm/ -run 'TestCompactionBoundsReadAmplification' -v -count=1

clean:
	$(GO) clean -testcache

# --------------------------------------------------------------- 实验

exp:
	@test -n "$(pkg)" || (echo "用法: make exp pkg=<包> run=<正则>"; exit 1)
	$(GO) test $(pkg) -run '$(run)' -v -count=1

exp-storage:
	@echo "### bitcask：1000 次覆盖写一个 key，看到全量重写的代价"
	$(GO) test ./01-storage/bitcask/ -run 'TestWriteAmplification|TestTornWriteRecovery|TestCorruptedRecordIsDetected' -v -count=1
	@echo ""
	@echo "### lsm：bloom 误判率 + compaction 后的层分布"
	$(GO) test ./01-storage/lsm/ -run 'TestBloomFilterEffectiveness|TestShadowingAcrossLevels' -v -count=1

exp-partitioning:
	$(GO) test ./02-replication/partitioning/ -v -count=1 -run 'TestKeyMovement|TestVirtualNodes|TestRebalance'

exp-raft:
	$(GO) test ./04-consensus/raft/ -v -count=1 -timeout 300s \
		-run 'TestClusterElectsExactlyOneLeader|TestElectionRestriction|TestSnapshotCatches|TestLeaderInMinority'

exp-tx:
	$(GO) test ./03-transactions/mvcc/ -v -count=1 \
		-run 'TestWriteSkew|TestLostUpdate|TestGarbageCollection'
	@echo ""
	@echo "### 对比：2PC 的阻塞 vs Saga 的补偿"
	$(GO) test ./04-consensus/twopc/ -v -count=1 -run 'TestCoordinatorCrash|TestSaga'

exp-stream:
	$(GO) test ./06-stream/windowing/ -v -count=1 \
		-run 'TestWatermark|TestLateEvent|TestTooLate|TestReplay|TestOutOfOrder'
	@echo ""
	$(GO) test ./06-stream/logbroker/ -v -count=1 -run 'TestRetention|TestConsumerGroup|TestRebalanceCanReplay'
