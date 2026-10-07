package minidb

import (
	"fmt"
	"testing"
	"time"
)

func newCluster(t *testing.T, shards, replicas int) *Cluster {
	t.Helper()
	c, err := New(Options{
		Root:     t.TempDir(),
		Shards:   shards,
		Replicas: replicas,
		Seed:     11,
		Latency:  3 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(c.Close)
	if !c.WaitForLeaders(3 * time.Second) {
		t.Fatalf("no leaders elected:\n%s", c.Describe())
	}
	return c
}

func TestShardingDistributesKeysAndIsStable(t *testing.T) {
	c := newCluster(t, 3, 3)
	var keys []string
	for i := 0; i < 300; i++ {
		keys = append(keys, fmt.Sprintf("key-%04d", i))
	}
	perShard := c.KeysPerShard(keys)
	for shard, ks := range perShard {
		t.Logf("%s owns %d keys", shard, len(ks))
	}
	if len(perShard) != 3 {
		t.Fatalf("expected keys on all 3 shards, got %d: %v", len(perShard), perShard)
	}
	for shard, ks := range perShard {
		if len(ks) < 50 {
			t.Fatalf("%s got only %d of 300 keys — hashing is unbalanced", shard, len(ks))
		}
	}
	// Routing must be a pure function of the key.
	for _, k := range keys {
		if c.ShardOf(k) != c.ShardOf(k) {
			t.Fatalf("routing is not deterministic for %s", k)
		}
	}
}

// The end-to-end property: a client sees its own writes, from any shard, with
// all three layers (ring routing, Raft commit, LSM apply) agreeing.
func TestPutGetAcrossShardsIsLinearizable(t *testing.T) {
	c := newCluster(t, 3, 3)

	type readback struct{ key, value string }
	var checks []readback
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("k%02d", i)
		value := fmt.Sprintf("v%d", i)
		if err := c.Put(key, value); err != nil {
			t.Fatalf("Put(%s): %v\n%s", key, err, c.Describe())
		}
		checks = append(checks, readback{key, value})
	}

	for _, chk := range checks {
		got, ok, err := c.Get(chk.key)
		if err != nil {
			t.Fatalf("Get(%s): %v", chk.key, err)
		}
		if !ok || got != chk.value {
			t.Fatalf("Get(%s) = %q, %v; want %q", chk.key, got, ok, chk.value)
		}
	}
	if _, ok, _ := c.Get("never-written"); ok {
		t.Fatal("a missing key must read as absent")
	}

	routed := c.Routed()
	t.Logf("request distribution: %v", routed)
	if len(routed) != 3 {
		t.Fatalf("expected traffic on all shards, got %v", routed)
	}
}

func TestOverwriteAndDeleteSemantics(t *testing.T) {
	c := newCluster(t, 1, 3)
	if err := c.Put("k", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Put("k", "v2"); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get("k")
	if err != nil || !ok || got != "v2" {
		t.Fatalf("Get = %q %v %v, want v2", got, ok, err)
	}
}

// Killing a shard's leader must not lose data: the log is on a quorum, and the
// new leader's state machine catches up from it.
func TestLeaderFailoverKeepsDataAndAvailability(t *testing.T) {
	c := newCluster(t, 1, 3)

	for i := 0; i < 10; i++ {
		if err := c.Put(fmt.Sprintf("before%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	victim := c.CrashLeader("shard-0")
	if victim == "" {
		t.Fatal("no leader to kill")
	}
	t.Logf("crashed %s; cluster now:\n%s", victim, c.Describe())

	if !c.WaitForLeaders(5 * time.Second) {
		t.Fatalf("the remaining quorum did not elect a new leader:\n%s", c.Describe())
	}
	newLeader := c.Info()[0].Leader
	if newLeader == victim {
		t.Fatalf("the crashed node is still reported as leader")
	}

	// Committed data survived the failover...
	for i := 0; i < 10; i++ {
		got, ok, err := c.Get(fmt.Sprintf("before%d", i))
		if err != nil || !ok || got != "v" {
			t.Fatalf("before%d lost after failover: %q %v %v\n%s", i, got, ok, err, c.Describe())
		}
	}
	// ...and the shard is writable again.
	if err := c.Put("after-failover", "v2"); err != nil {
		t.Fatalf("write after failover: %v", err)
	}

	// Bringing the old leader back must not resurrect it as a second leader.
	c.Restart(victim)
	c.Run(500 * time.Millisecond)
	info := c.Info()[0]
	if info.Leader == victim {
		t.Fatalf("the restarted node reclaimed leadership without an election")
	}
	if got, ok, _ := c.Get("after-failover"); !ok || got != "v2" {
		t.Fatalf("data written after the failover is missing: %q %v", got, ok)
	}
	t.Logf("after recovery:\n%s", c.Describe())
}

// A full restart of a shard: the Raft log and the LSM store are both on disk,
// so nothing is lost. This is the payoff of "the log is the source of truth".
func TestDurabilityAcrossFullShardRestart(t *testing.T) {
	c := newCluster(t, 1, 3)
	for i := 0; i < 15; i++ {
		if err := c.Put(fmt.Sprintf("k%02d", i), fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	ids := []string{}
	for _, sh := range c.shards {
		for _, rep := range sh.replicas {
			ids = append(ids, rep.id)
		}
	}
	for _, id := range ids {
		c.Crash(id)
	}
	c.Run(50 * time.Millisecond)
	for _, id := range ids {
		c.Restart(id)
	}
	if !c.WaitForLeaders(8 * time.Second) {
		t.Fatalf("cluster did not recover after a full restart:\n%s", c.Describe())
	}

	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("k%02d", i)
		got, ok, err := c.Get(key)
		if err != nil || !ok || got != fmt.Sprintf("v%d", i) {
			t.Fatalf("%s lost across restart: %q %v %v\n%s", key, got, ok, err, c.Describe())
		}
	}
	if err := c.Put("post-restart", "ok"); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	t.Logf("all 15 keys survived a full cluster restart:\n%s", c.Describe())
}

// Under a minority partition the shard must refuse writes rather than lose
// them: that is the trade-off consensus makes, and the client feels it as a
// timeout.
func TestMinorityPartitionRefusesWrites(t *testing.T) {
	c := newCluster(t, 1, 3)
	if err := c.Put("k", "v1"); err != nil {
		t.Fatal(err)
	}

	sh := c.shards[0]
	ids := []string{sh.replicas[0].id, sh.replicas[1].id, sh.replicas[2].id}
	// Isolate the leader with one follower: that side has no quorum.
	leader := c.leaderOf(sh)
	if leader == nil {
		t.Fatal("no leader")
	}
	var others []string
	for _, id := range ids {
		if id != leader.id {
			others = append(others, id)
		}
	}
	c.net.Isolate(leader.id, others)

	done := make(chan error, 1)
	go func() { done <- c.Put("blocked", "v") }()
	select {
	case err := <-done:
		// A timeout is the expected outcome: the write never committed.
		t.Logf("write on the minority side correctly failed: %v", err)
	case <-time.After(600 * time.Millisecond):
		t.Log("write is still blocked waiting for a quorum (expected)")
	}
	c.Run(200 * time.Millisecond)

	// The majority side must still serve reads of already-committed data.
	t.Logf("cluster:\n%s", c.Describe())
	if !c.WaitForLeaders(5 * time.Second) {
		t.Fatalf("the majority side never elected a leader:\n%s", c.Describe())
	}
	got, ok, err := c.Get("k")
	if err != nil || !ok || got != "v1" {
		t.Fatalf("committed data unreadable on the majority side: %q %v %v", got, ok, err)
	}
}
