package logbroker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func ts(i int) time.Time { return time.Unix(1700000000+int64(i), 0) }

// consumers exposes the group members for tests.
func (g *Group) consumers() []*Consumer {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*Consumer(nil), g.members...)
}

func TestAppendAndReadByOffset(t *testing.T) {
	p := NewPartition(0, 4) // tiny segments so retention has something to drop
	for i := 0; i < 10; i++ {
		off := p.Append(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), ts(i))
		if off != int64(i) {
			t.Fatalf("offset %d, want %d", off, i)
		}
	}
	if p.HighWatermark() != 10 {
		t.Fatalf("high watermark = %d", p.HighWatermark())
	}
	recs, err := p.Read(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 4 || recs[0].Offset != 3 || recs[3].Offset != 6 {
		t.Fatalf("read from 3 gave %v", recs)
	}
	// Reading at the head returns nothing; it is not an error.
	if recs, err := p.Read(10, 4); err != nil || len(recs) != 0 {
		t.Fatalf("read at head: %v %v", recs, err)
	}
	// Replaying from 0 gives the identical sequence: replay is free.
	replay, _ := p.Read(0, 100)
	if len(replay) != 10 || replay[9].Value != "v9" {
		t.Fatalf("replay mismatch: %v", replay)
	}
}

func TestRetentionTruncatesButOffsetsKeepClimbing(t *testing.T) {
	p := NewPartition(0, 3)
	for i := 0; i < 12; i++ {
		p.Append("k", fmt.Sprintf("v%d", i), ts(i))
	}
	before := p.LogStartOffset()
	removed := p.TruncateBefore(6)
	if removed == 0 {
		t.Fatal("retention removed nothing")
	}
	if p.LogStartOffset() <= before {
		t.Fatalf("log start offset should advance: %d -> %d", before, p.LogStartOffset())
	}
	// An old offset is now genuinely gone.
	if _, err := p.Read(0, 10); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("reading a truncated offset must fail loudly, got %v", err)
	}
	// ...but offsets are never reused, so the head keeps climbing.
	off := p.Append("k", "new", ts(100))
	if off <= 11 {
		t.Fatalf("offset was reused after truncation: %d", off)
	}
	t.Logf("retention kept offsets %d..%d, next offset %d", p.LogStartOffset(), p.HighWatermark(), off)
}

func TestKeyedOrderingAndPartitionAffinity(t *testing.T) {
	topic := NewTopic("orders", 3, 100)
	// The same key must always land in the same partition, and within a
	// partition the log order is the append order.
	for i := 0; i < 100; i++ {
		topic.Produce("customer-42", fmt.Sprintf("order-%d", i), ts(i))
	}
	p, _ := topic.PartitionForKey("customer-42"), 0
	_ = p
	part := topic.PartitionForKey("customer-42")
	for i := 0; i < 20; i++ {
		if got := topic.PartitionForKey("customer-42"); got != part {
			t.Fatalf("key affinity broke: %d != %d", got, part)
		}
	}
	recs, err := topic.Partitions[part].Read(0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 100 {
		t.Fatalf("expected 100 records in the key's partition, got %d", len(recs))
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("partition log is not in append order at %d: %v", i, r)
		}
		if r.Value != fmt.Sprintf("order-%d", i) {
			t.Fatalf("per-key ordering broken: %v at %d", r, i)
		}
	}

	// A null key is spread for throughput and therefore has no ordering.
	seen := map[int]bool{}
	for i := 0; i < 30; i++ {
		p, _ := topic.Produce("", "x", ts(i))
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Fatalf("null-key writes should spread across partitions, got %v", seen)
	}
}

func TestConsumerGroupRebalances(t *testing.T) {
	topic := NewTopic("events", 6, 1000)
	g := NewGroup(topic)

	a := g.Join("consumer-A")
	st := g.Stats()
	if len(st.Assignment["consumer-A"]) != 6 {
		t.Fatalf("a lone consumer should own every partition: %v", st.Assignment)
	}

	g.Join("consumer-B")
	st = g.Stats()
	if st.Rebalances != 2 {
		t.Fatalf("expected 2 rebalances, got %d", st.Rebalances)
	}
	if len(st.Assignment["consumer-A"]) != 3 || len(st.Assignment["consumer-B"]) != 3 {
		t.Fatalf("6 partitions should split 3/3: %v", st.Assignment)
	}

	g.Join("consumer-C")
	st = g.Stats()
	if len(st.Assignment["consumer-A"]) != 2 || len(st.Assignment["consumer-C"]) != 2 {
		t.Fatalf("6 partitions should split 2/2/2: %v", st.Assignment)
	}
	t.Logf("assignments after 3 joins: %v", st.Assignment)

	g.Leave("consumer-B")
	st = g.Stats()
	if len(st.Assignment) != 2 || len(st.Assignment["consumer-A"])+len(st.Assignment["consumer-C"]) != 6 {
		t.Fatalf("partitions were lost on leave: %v", st.Assignment)
	}
	_ = a
	t.Logf("partitions conserved after B left: %v", st.Assignment)
}

// Offsets belong to the *group*, not the consumer: a partition moving to a new
// owner must not restart from zero, or you would reprocess everything.
func TestOffsetsFollowThePartitionAcrossRebalance(t *testing.T) {
	topic := NewTopic("t", 2, 1000)
	for i := 0; i < 20; i++ {
		topic.Produce(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), ts(i))
	}
	g := NewGroup(topic)
	a := g.Join("A")

	// Consume and commit everything currently visible.
	recs := g.Poll(a, 1000)
	perPartition := map[int]int64{}
	for _, r := range recs {
		perPartition[topic.PartitionForKey(r.Key)] = r.Offset + 1
	}
	for p, off := range perPartition {
		a.Commit(p, off)
	}
	t.Logf("A committed %v", perPartition)

	// A second consumer joins: it inherits one partition and its offset.
	b2 := g.Join("B")
	st := g.Stats()
	t.Logf("after rebalance: %v offsets=%v", st.Assignment, st.Offsets)

	got := g.Poll(b2, 1000)
	if len(got) != 0 {
		t.Fatalf("the new owner re-read already-committed records: %v", got)
	}

	// New writes are picked up by whoever owns the partition now.
	topic.Produce("k1", "late", ts(50))
	found := false
	for _, consumer := range []*Consumer{a, b2} {
		for _, r := range g.Poll(consumer, 1000) {
			if r.Value == "late" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("the new record was not delivered")
	}
}

// At-least-once: a crash between "processed" and "committed" replays. This is
// why consumers must be idempotent (upsert by key, not append).
func TestRebalanceCanReplayUncommittedWork(t *testing.T) {
	topic := NewTopic("t", 1, 1000)
	for i := 0; i < 5; i++ {
		topic.Produce("k", fmt.Sprintf("v%d", i), ts(i))
	}
	g := NewGroup(topic)
	a := g.Join("A")

	recs := g.Poll(a, 1000)
	if len(recs) != 5 {
		t.Fatalf("expected 5 records, got %d", len(recs))
	}
	// Simulate "processed all 5 but crashed before committing".
	a.RecordOffsets(0, 0)

	g.Join("B") // rebalance while A's work was not committed
	b := g.Join("C")
	_ = b

	// Poll from B and C: someone will see the records again.
	total := 0
	for _, c := range []*Consumer{a, b} {
		total += len(g.Poll(c, 1000))
	}
	if total == 0 {
		t.Fatal("expected the uncommitted work to be redelivered")
	}
	t.Logf("at-least-once: %d records were replayed after the rebalance", total)
}
