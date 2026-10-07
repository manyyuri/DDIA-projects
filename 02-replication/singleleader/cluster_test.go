package singleleader

import (
	"fmt"
	"testing"
	"time"
)

func newTestCluster(t *testing.T, n int, latency time.Duration) *Cluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i+1)
	}
	return NewCluster(ids, 7, latency)
}

func TestAsyncReplicationReachesAllReplicas(t *testing.T) {
	c := newTestCluster(t, 3, 5*time.Millisecond)
	for i := 0; i < 5; i++ {
		c.Append(Op{Key: fmt.Sprintf("k%d", i), Value: fmt.Sprintf("v%d", i)})
	}
	c.Net().Drain()

	if got := c.AckCount(5); got != 3 {
		t.Fatalf("AckCount = %d, want 3 (%s)", got, c.Dump())
	}
	if got := c.CommittedIndex(); got != 5 {
		t.Fatalf("committed = %d, want 5", got)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		if got := c.AppliedIndex(id); got != 5 {
			t.Fatalf("%s applied=%d, want 5 (%s)", id, got, c.Dump())
		}
	}
}

// Quorum replication tolerates one failure: the write still commits.
func TestQuorumSurvivesOneReplicaDown(t *testing.T) {
	c := newTestCluster(t, 3, 5*time.Millisecond)
	c.Fail("n3")

	idx, ok := c.Write(Op{Key: "a", Value: "1"}, WaitForQuorum, 200*time.Millisecond)
	if !ok {
		t.Fatalf("quorum write failed (%s)", c.Dump())
	}
	if got := c.CommittedIndex(); got < idx {
		t.Fatalf("committed=%d < idx=%d", got, idx)
	}
	if got := c.AckCount(idx); got != 2 {
		t.Fatalf("AckCount=%d, want 2", got)
	}
}

// Timeouts are not failures. With one replica down, WaitForAll never returns,
// yet the write still commits on a quorum. A client that retries the timed-out
// write must therefore be idempotent — this is where duplicate writes come from.
func TestTimedOutWriteMayStillHaveCommitted(t *testing.T) {
	c := newTestCluster(t, 3, 5*time.Millisecond)
	c.Fail("n2")

	idx, acked := c.Write(Op{Key: "a", Value: "1"}, WaitForAll, 100*time.Millisecond)
	if acked {
		t.Fatalf("WaitForAll must not ack with a replica down (%s)", c.Dump())
	}
	c.Net().Drain()
	if got := c.CommittedIndex(); got < idx {
		t.Fatalf("the write did commit on a quorum: committed=%d idx=%d", got, idx)
	}
	if v, ok, _ := c.Get(ReadLeader, c.Leader(), "a", 0); !ok || v != "1" {
		t.Fatalf("expected the timed-out write to be visible, got %q ok=%v", v, ok)
	}
}

// The core lesson of §5.1: with async replication the client's ack means
// nothing durable. The leader says "ok", the leader dies, the write is gone.
func TestAsyncAckIsLostOnFailover(t *testing.T) {
	c := newTestCluster(t, 3, 100*time.Millisecond)
	c.Fail("n3") // only n1 (leader) and n2 are live

	idx, acked := c.Write(Op{Key: "b", Value: "2"}, Async, 0)
	if !acked {
		t.Fatal("Async write should ack immediately")
	}
	if c.CommittedIndex() >= idx {
		t.Fatal("precondition: the entry should not have reached quorum yet")
	}

	c.Log("leader dies before the write reaches anyone else")
	c.Fail("n1")
	rep := c.Promote("")
	if rep.NewLeader != "n2" {
		t.Fatalf("expected n2 to take over, got %s", rep.NewLeader)
	}
	c.Net().Drain()
	if _, ok, _ := c.Get(ReadLeader, rep.NewLeader, "b", 0); ok {
		t.Fatal("an asynchronously acked write must not survive failover")
	}
}

// "Committed" only means "a quorum had it". Destroy that quorum and the data
// is gone — durability is a property of the *set of replicas*, not of a
// boolean on one node.
func TestLosingTheQuorumLosesCommittedWrites(t *testing.T) {
	c := newTestCluster(t, 5, 5*time.Millisecond)
	// n4/n5 are network-isolated, so the writes only ever reach n1/n2/n3.
	c.Net().Partition([]string{"n1", "n2", "n3"}, []string{"n4", "n5"})
	for i := 0; i < 3; i++ {
		if _, ok := c.Write(Op{Key: fmt.Sprintf("k%d", i), Value: "v"}, WaitForQuorum, 500*time.Millisecond); !ok {
			t.Fatalf("write %d did not commit (%s)", i, c.Dump())
		}
	}
	if got := c.CommittedIndex(); got != 3 {
		t.Fatalf("committed=%d, want 3", got)
	}
	if got := c.LogLength("n4"); got != 0 {
		t.Fatalf("n4 should be empty while partitioned, has %d", got)
	}
	// Kill exactly the quorum that holds the data.
	c.Fail("n1")
	c.Fail("n2")
	c.Fail("n3")
	c.Net().Heal()
	rep := c.Promote("")
	c.Net().Drain()
	if rep.LostEntries != 3 {
		t.Fatalf("expected all 3 committed entries to be lost, got %+v", rep)
	}
	c.Net().Drain()
	for i := 0; i < 3; i++ {
		if _, ok, _ := c.Get(ReadLeader, rep.NewLeader, fmt.Sprintf("k%d", i), 0); ok {
			t.Fatalf("k%d should be gone", i)
		}
	}
}

func TestSyncFailoverKeepsCommittedWrites(t *testing.T) {
	c := newTestCluster(t, 3, 20*time.Millisecond)
	for i := 0; i < 10; i++ {
		idx, ok := c.Write(Op{Key: fmt.Sprintf("k%d", i), Value: "v"}, WaitForQuorum, 500*time.Millisecond)
		if !ok {
			t.Fatalf("write %d did not reach quorum (%s)", i, c.Dump())
		}
		if c.CommittedIndex() < idx {
			t.Fatalf("write %d not committed", i)
		}
	}
	c.Fail("n1")
	rep := c.Promote("")
	if rep.LostEntries != 0 {
		t.Fatalf("quorum-committed writes must survive failover, lost %d: %+v", rep.LostEntries, rep)
	}
	c.Net().Drain()
	for i := 0; i < 10; i++ {
		if _, ok, _ := c.Get(ReadLeader, rep.NewLeader, fmt.Sprintf("k%d", i), 0); !ok {
			t.Fatalf("k%d vanished after failover", i)
		}
	}
}

// Replica reads are cheap and stale. This is not a bug, it is the trade-off;
// the fix is a session watermark.
func TestFollowerReadsAreStaleAndReadYourWritesFixesIt(t *testing.T) {
	c := newTestCluster(t, 3, 50*time.Millisecond)
	idx, _ := c.Write(Op{Key: "x", Value: "new"}, WaitForQuorum, 500*time.Millisecond)

	// A follower read before replication lands sees the old (absent) value.
	if _, ok, err := c.Get(ReadFollower, "n3", "x", 0); err != nil || ok {
		t.Fatalf("expected a stale follower read to miss, got ok=%v err=%v", ok, err)
	}
	// Read-your-writes refuses to answer from that replica.
	if _, _, err := c.Get(ReadYourWrites, "n3", "x", idx); err == nil {
		t.Fatal("read-your-writes should have rejected the stale replica")
	}
	// After replication catches up, both modes are fine.
	c.Net().Drain()
	for _, mode := range []ReadMode{ReadFollower, ReadYourWrites} {
		v, ok, err := c.Get(mode, "n3", "x", idx)
		if err != nil || !ok || v != "new" {
			t.Fatalf("mode=%v: v=%q ok=%v err=%v", mode, v, ok, err)
		}
	}
}

func TestLaggingReplicaCatchesUpAfterPartitionHeals(t *testing.T) {
	c := newTestCluster(t, 3, 10*time.Millisecond)
	c.Net().Isolate("n3", []string{"n1", "n2"})

	for i := 0; i < 5; i++ {
		c.Append(Op{Key: fmt.Sprintf("k%d", i), Value: "v"})
	}
	c.Net().Drain()
	if lag := c.ReplicationLag("n3"); lag == 0 {
		t.Fatalf("n3 should be behind while partitioned (%s)", c.Dump())
	}

	c.Net().Heal()
	for i := 5; i < 10; i++ {
		c.Append(Op{Key: fmt.Sprintf("k%d", i), Value: "v"})
	}
	c.Net().Drain()

	if lag := c.ReplicationLag("n3"); lag != 0 {
		t.Fatalf("n3 should have caught up, lag=%d (%s)", lag, c.Dump())
	}
	if got := c.AppliedIndex("n3"); got != 10 {
		t.Fatalf("n3 applied=%d, want 10", got)
	}
}
