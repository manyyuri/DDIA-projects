// Package logbroker implements DDIA §11.1-11.2: a partitioned, replicated log
// as the transport for stream processing (the Kafka model).
//
// The properties that make a log-based broker different from a message queue:
//
//	order            guaranteed within a partition, never across partitions
//	offsets          a consumer position, not a message property: replay is free
//	consumers        a group pulls; the broker does not push or delete on ack
//	retention        the log is truncated by time/size, so old offsets expire
//
// The consequence people trip over: "exactly once" is not a broker property.
// The broker gives you at-least-once delivery plus a stable order; idempotence
// has to come from the consumer (see ../windowing's idempotent sink).
package logbroker

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Record is one log entry.
type Record struct {
	Offset    int64
	Key       string
	Value     string
	Timestamp time.Time
}

func (r Record) String() string {
	return fmt.Sprintf("%d:%s=%s", r.Offset, r.Key, r.Value)
}

// Segment is a chunk of the log. Retention deletes whole segments, which is why
// a log's offsets are logical: the file position of offset N changes over time.
type Segment struct {
	BaseOffset int64
	Records    []Record
	MaxTime    time.Time
}

// Partition is an append-only log with a truncatable head.
type Partition struct {
	mu       sync.Mutex
	id       int
	segments []*Segment
	// baseOffset is the offset of the oldest retained record. It is what makes
	// "the log starts at 4,000,000" a normal state rather than an error.
	baseOffset    int64
	highWatermark int64
	nextOffset    int64
	segmentLimit  int
}

// NewPartition creates a partition whose segments hold up to segmentLimit records.
func NewPartition(id, segmentLimit int) *Partition {
	if segmentLimit <= 0 {
		segmentLimit = 100
	}
	p := &Partition{id: id, segmentLimit: segmentLimit}
	p.segments = []*Segment{{BaseOffset: 0}}
	return p
}

// Append writes a record and returns its offset. The offset is assigned by the
// broker, monotonically, and never reused — even after retention deletes the
// record.
func (p *Partition) Append(key, value string, ts time.Time) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	off := p.nextOffset
	p.nextOffset++
	last := p.segments[len(p.segments)-1]
	last.Records = append(last.Records, Record{Offset: off, Key: key, Value: value, Timestamp: ts})
	last.MaxTime = ts
	if len(last.Records) >= p.segmentLimit {
		p.segments = append(p.segments, &Segment{BaseOffset: p.nextOffset})
	}
	p.highWatermark = p.nextOffset
	return off
}

// ErrOffsetOutOfRange means the requested offset was truncated away.
var ErrOffsetOutOfRange = fmt.Errorf("logbroker: offset out of range")

// Read returns up to max records starting at from. Reading at the high
// watermark returns an empty slice, not an error: a consumer simply waits.
func (p *Partition) Read(from int64, max int) ([]Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if from < p.baseOffset {
		return nil, fmt.Errorf("%w: %d was truncated (log starts at %d)", ErrOffsetOutOfRange, from, p.baseOffset)
	}
	if from > p.nextOffset {
		return nil, fmt.Errorf("%w: %d is beyond the head %d", ErrOffsetOutOfRange, from, p.nextOffset)
	}
	if max <= 0 {
		max = 1000
	}
	var out []Record
	for _, seg := range p.segments {
		for _, r := range seg.Records {
			if r.Offset < from {
				continue
			}
			out = append(out, r)
			if len(out) >= max {
				return out, nil
			}
		}
	}
	return out, nil
}

// HighWatermark is the offset the next record will get.
func (p *Partition) HighWatermark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.highWatermark
}

// LogStartOffset is the oldest offset still readable.
func (p *Partition) LogStartOffset() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.baseOffset
}

// TruncateBefore drops every record older than offset (time/size-based
// retention collapses to this).
func (p *Partition) TruncateBefore(offset int64) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	removed := 0
	var kept []*Segment
	for _, seg := range p.segments {
		// Keep the segment if it contains anything at or after offset.
		if len(seg.Records) > 0 && seg.Records[len(seg.Records)-1].Offset < offset {
			removed += len(seg.Records)
			if seg.BaseOffset >= p.baseOffset {
				p.baseOffset = seg.Records[len(seg.Records)-1].Offset + 1
			}
			continue
		}
		kept = append(kept, seg)
	}
	if len(kept) == 0 {
		kept = []*Segment{{BaseOffset: p.nextOffset}}
	}
	p.segments = kept
	return removed
}

// ---------------------------------------------------------------- topic --

// Topic is a set of partitions.
type Topic struct {
	Name       string
	Partitions []*Partition
	mu         sync.Mutex
	rr         int
}

// NewTopic creates a topic with n partitions.
func NewTopic(name string, n, segmentLimit int) *Topic {
	t := &Topic{Name: name}
	for i := 0; i < n; i++ {
		t.Partitions = append(t.Partitions, NewPartition(i, segmentLimit))
	}
	return t
}

// PartitionForKey maps a key to a partition. Same key -> same partition, which
// is the only way to get per-key ordering on a partitioned log. A null key is
// round-robined for load spread (and therefore unordered).
func (t *Topic) PartitionForKey(key string) int {
	if key == "" {
		t.mu.Lock()
		defer t.mu.Unlock()
		p := t.rr % len(t.Partitions)
		t.rr++
		return p
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(len(t.Partitions)))
}

// Produce appends a record to the partition chosen by key.
func (t *Topic) Produce(key, value string, ts time.Time) (int, int64) {
	p := t.PartitionForKey(key)
	return p, t.Partitions[p].Append(key, value, ts)
}

// NumPartitions reports the partition count.
func (t *Topic) NumPartitions() int { return len(t.Partitions) }

// ------------------------------------------------------------ consumer group --

// Consumer is one member of a group. It owns a set of partitions and a
// committed offset per partition.
type Consumer struct {
	ID         string
	assignment []int
	offsets    map[int]int64
	// at-least-once: after a rebalance a consumer may re-read records it had
	// already processed but not yet committed.
	processed  int
	duplicates int
}

// Group is a consumer group coordinator.
type Group struct {
	mu         sync.Mutex
	topic      *Topic
	members    []*Consumer
	strategies []string
	generation int
	rebalances int
}

// NewGroup creates a consumer group over a topic.
func NewGroup(topic *Topic) *Group {
	return &Group{topic: topic}
}

// Join adds a consumer and triggers a rebalance.
func (g *Group) Join(id string) *Consumer {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := &Consumer{ID: id, offsets: map[int]int64{}}
	g.members = append(g.members, c)
	g.rebalanceLocked()
	return c
}

// Leave removes a consumer and rebalances the rest.
func (g *Group) Leave(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	kept := g.members[:0:0]
	for _, m := range g.members {
		if m.ID != id {
			kept = append(kept, m)
		}
	}
	g.members = kept
	g.rebalanceLocked()
}

// rebalanceLocked implements the range assignment strategy: partition i goes to
// member (i mod len(members)). A real coordinator does this with a leader
// election and a group protocol; the *effect* is what matters here.
func (g *Group) rebalanceLocked() {
	g.generation++
	g.rebalances++
	if len(g.members) == 0 {
		return
	}
	for _, m := range g.members {
		m.assignment = nil
	}
	for i := 0; i < g.topic.NumPartitions(); i++ {
		m := g.members[i%len(g.members)]
		m.assignment = append(m.assignment, i)
	}
	// Offsets follow the partition, not the consumer: a partition handed to a
	// new owner must resume where the group left off.
	committed := map[int]int64{}
	for _, m := range g.members {
		for p, off := range m.offsets {
			if _, ok := committed[p]; !ok {
				committed[p] = off
			}
		}
	}
	for _, m := range g.members {
		m.offsets = map[int]int64{}
		for _, p := range m.assignment {
			if off, ok := committed[p]; ok {
				m.offsets[p] = off
			}
		}
	}
}

// Assignment returns the partitions owned by each member.
func (g *Group) Assignment() map[string][]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := map[string][]int{}
	for _, m := range g.members {
		out[m.ID] = append([]int(nil), m.assignment...)
	}
	return out
}

// Poll reads the next batch for a consumer from one of its partitions.
func (g *Group) Poll(c *Consumer, maxPerPartition int) []Record {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []Record
	for _, p := range c.assignment {
		from := c.offsets[p]
		recs, err := g.topic.Partitions[p].Read(from, maxPerPartition)
		if err != nil {
			// The offset was truncated away: the only safe restart is the log start.
			c.offsets[p] = g.topic.Partitions[p].LogStartOffset()
			continue
		}
		out = append(out, recs...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
	return out
}

// Commit records the consumer's progress. Committing after processing (not
// before) is what makes the delivery guarantee at-least-once.
func (c *Consumer) Commit(partition int, nextOffset int64) {
	c.offsets[partition] = nextOffset
	c.processed++
}

// RecordOffsets lets a test model "processed but not committed" work, which a
// rebalance will hand to someone else.
func (c *Consumer) RecordOffsets(partition int, off int64) {
	c.offsets[partition] = off
}

// Stats summarises the group.
type Stats struct {
	Members    int
	Generation int
	Rebalances int
	Assignment map[string][]int
	Offsets    map[string]map[int]int64
}

// Stats returns a snapshot of group state.
func (g *Group) Stats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()
	st := Stats{Members: len(g.members), Generation: g.generation, Rebalances: g.rebalances,
		Assignment: map[string][]int{}, Offsets: map[string]map[int]int64{}}
	for _, m := range g.members {
		st.Assignment[m.ID] = append([]int(nil), m.assignment...)
		st.Offsets[m.ID] = map[int]int64{}
		for p, o := range m.offsets {
			st.Offsets[m.ID][p] = o
		}
	}
	return st
}
