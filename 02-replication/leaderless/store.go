// Package leaderless implements DDIA §5.5: quorum replication without a
// leader (Dynamo style).
//
// The three ideas this package exists to make hands-on:
//
//  1. W + R > N gives you overlap, and overlap is what makes a read see the
//     latest write — without any node tracking "the" newest value globally.
//     W + R <= N and you can silently read stale data.
//  2. Without a leader, two writers can produce two *concurrent* versions.
//     Vector clocks are how a replica can tell "newer" from "conflicting",
//     and siblings are what the application then has to merge.
//  3. Read repair + hinted handoff + anti-entropy are the three mechanisms
//     that pull replicas back together, at three different time scales.
package leaderless

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// VectorClock counts writes per coordinating node. Comparing two clocks is
// how you distinguish "newer" from "concurrent" (DDIA §5.4 "Detecting
// Concurrent Writes").
type VectorClock map[string]uint64

func (vc VectorClock) clone() VectorClock {
	out := make(VectorClock, len(vc))
	for k, v := range vc {
		out[k] = v
	}
	return out
}

// bump increments the coordinating node's own counter.
func (vc VectorClock) bump(node string) {
	if vc == nil {
		return
	}
	vc[node]++
}

// merge takes the per-node maximum of two clocks.
func (vc VectorClock) merge(other VectorClock) VectorClock {
	out := vc.clone()
	if out == nil {
		out = VectorClock{}
	}
	for k, v := range other {
		if v > out[k] {
			out[k] = v
		}
	}
	return out
}

// Relation is the causal relation between two versions.
type Relation int

const (
	Equal Relation = iota
	Before
	After
	Concurrent
)

func (r Relation) String() string {
	switch r {
	case Equal:
		return "equal"
	case Before:
		return "before"
	case After:
		return "after"
	case Concurrent:
		return "concurrent"
	}
	return "?"
}

// Compare reports how a relates to b.
func Compare(a, b VectorClock) Relation {
	less, greater := false, false
	for k, v := range a {
		if b[k] < v {
			greater = true
		}
	}
	for k, v := range b {
		if a[k] < v {
			less = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case greater:
		return After
	case less:
		return Before
	default:
		return Equal
	}
}

// VersionedValue is one replica's copy of a key: the payload plus the causal
// history that lets replicas decide whether it is superseded.
type VersionedValue struct {
	Value     string
	Deleted   bool
	Clock     VectorClock
	Timestamp int64 // only a tiebreaker for display; never used for ordering
}

func (v VersionedValue) String() string {
	tag := v.Value
	if v.Deleted {
		tag = "<tombstone>"
	}
	return fmt.Sprintf("%s%v", tag, v.Clock)
}

// ---------------------------------------------------------------- the store --

type replica struct {
	id     string
	alive  bool
	data   map[string][]VersionedValue // key -> siblings (usually exactly 1)
	repair int                         // counts read-repair writes it absorbed
}

// Cluster is N replicas plus the client-side quorum logic. Real Dynamo systems
// put the coordinator role in a client library; so do we.
type Cluster struct {
	nodes   []string
	n, w, r int

	replicas map[string]*replica

	// hinted handoff: writes parked on a healthy replica, addressed to a
	// replica that was down (sloppy quorum).
	hints map[string][]hintedWrite

	stats struct {
		reads, writes, repairs, hinted, antiEntropy int
	}
}

type hintedWrite struct {
	dest  string
	key   string
	value VersionedValue
}

// New builds a cluster of n replicas with the given W and R.
func New(nodes []string, w, r int) *Cluster {
	c := &Cluster{
		nodes:    append([]string(nil), nodes...),
		n:        len(nodes),
		w:        w,
		r:        r,
		replicas: map[string]*replica{},
		hints:    map[string][]hintedWrite{},
	}
	for _, id := range nodes {
		c.replicas[id] = &replica{id: id, alive: true, data: map[string][]VersionedValue{}}
	}
	return c
}

// N, W, R expose the configured quorum numbers.
func (c *Cluster) N() int { return c.n }
func (c *Cluster) W() int { return c.w }
func (c *Cluster) R() int { return c.r }

// QuorumOverlap reports whether W+R > N, i.e. whether read and write quorums
// must intersect.
func (c *Cluster) QuorumOverlap() bool { return c.w+c.r > c.n }

// Fail marks a replica unavailable.
func (c *Cluster) Fail(id string) {
	if rp := c.replicas[id]; rp != nil {
		rp.alive = false
	}
}

// Recover brings a replica back and delivers any hinted writes parked for it.
func (c *Cluster) Recover(id string) int {
	rp := c.replicas[id]
	if rp == nil {
		return 0
	}
	rp.alive = true
	delivered := 0
	for holder, list := range c.hints {
		var keep []hintedWrite
		for _, h := range list {
			if h.dest == id {
				rp.data[h.key] = mergeSiblings(rp.data[h.key], []VersionedValue{h.value})
				delivered++
				continue
			}
			keep = append(keep, h)
		}
		if len(keep) == 0 {
			delete(c.hints, holder)
		} else {
			c.hints[holder] = keep
		}
	}
	return delivered
}

// preferenceList returns the N replicas responsible for a key, using a hash
// ring so successive keys spread across different starting points.
func (c *Cluster) preferenceList(key string) []string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	start := int(h.Sum32()) % len(c.nodes)
	out := make([]string, 0, len(c.nodes))
	for i := 0; i < len(c.nodes); i++ {
		out = append(out, c.nodes[(start+i)%len(c.nodes)])
	}
	return out
}

// livePrefs returns the first count live replicas in preference order,
// continuing past failed ones (sloppy quorum).
func (c *Cluster) livePrefs(key string, count int) (live []string, down []string) {
	for _, id := range c.preferenceList(key) {
		if c.replicas[id].alive {
			if len(live) < count {
				live = append(live, id)
				continue
			}
		} else {
			down = append(down, id)
		}
	}
	return live, down
}

// Put writes value through coordinator ("" = pick the first live preference).
//
// Note the two-phase shape: read the current siblings from a write quorum,
// merge their clocks, then write one strictly-newer version. This is what a
// real Dynamo client library does, and it is why concurrent writers can still
// produce siblings.
func (c *Cluster) Put(key, value string) error { return c.PutVia("", key, value) }

// PutVia writes value through an explicit coordinator.
func (c *Cluster) PutVia(coordinator, key, value string) error {
	return c.writeVersion(coordinator, key, VersionedValue{Value: value})
}

// Del writes a tombstone: a delete is just another version that happens to say
// "gone". It cannot be a removal, because a replica that never saw the delete
// would resurrect the old value on the next anti-entropy pass.
func (c *Cluster) Del(key string) error {
	return c.writeVersion("", key, VersionedValue{Deleted: true})
}

// DelVia writes a tombstone through an explicit coordinator.
func (c *Cluster) DelVia(coordinator, key string) error {
	return c.writeVersion(coordinator, key, VersionedValue{Deleted: true})
}

// writeVersion is the whole write path: read a quorum, merge the causal
// history, increment our own counter, then write a strictly newer version to
// W replicas (parking hints for any that are down).
func (c *Cluster) writeVersion(coordinator, key string, nv VersionedValue) error {
	if coordinator == "" {
		for _, id := range c.preferenceList(key) {
			if c.replicas[id].alive {
				coordinator = id
				break
			}
		}
	}
	if coordinator == "" {
		return fmt.Errorf("no live replica to coordinate the write")
	}

	// 1+2. Observe then increment: this is what makes a write causally after
	// everything the coordinator could see.
	existing := c.gather(key, c.preferenceList(key), c.r)
	clock := VectorClock{}
	for _, v := range existing {
		clock = clock.merge(v.Clock)
	}
	clock.bump(coordinator)
	nv.Clock = clock

	// 3. Write to W live replicas; park hints for the rest.
	live, down := c.livePrefs(key, c.w)
	if len(live) < c.w {
		return fmt.Errorf("write quorum unavailable: only %d of %d replicas live", len(live), c.w)
	}
	for _, id := range live {
		rp := c.replicas[id]
		rp.data[key] = mergeSiblings(rp.data[key], []VersionedValue{nv})
	}
	for _, dest := range down {
		holder := live[0] // sloppy quorum: park the write on a healthy replica
		c.hints[holder] = append(c.hints[holder], hintedWrite{dest: dest, key: key, value: nv})
		c.stats.hinted++
	}
	c.stats.writes++
	return nil
}

// Inject plants a version directly on one replica. Tests use it to build the
// divergent state that a network partition would have produced.
func (c *Cluster) Inject(node, key, value string, clock VectorClock) {
	c.replicas[node].data[key] = append(c.replicas[node].data[key], VersionedValue{Value: value, Clock: clock})
}

// Get reads from R replicas, merges what it finds, repairs the stragglers, and
// returns one value or a set of conflicting siblings.
func (c *Cluster) Get(key string) ([]string, bool, error) {
	prefs := c.preferenceList(key)
	var targets []string
	for _, id := range prefs {
		if c.replicas[id].alive {
			targets = append(targets, id)
		}
	}
	if len(targets) < c.r {
		return nil, false, fmt.Errorf("read quorum unavailable: only %d of %d replicas live", len(targets), c.r)
	}
	targets = targets[:c.r]

	found := c.gather(key, prefs, c.r)
	c.stats.reads++

	// Merge everything seen into the freshest consistent set.
	merged := mergeSiblings(nil, found)
	if len(merged) == 0 {
		return nil, false, nil
	}

	// Read repair: push the merged result back to the replicas we read from
	// (Dynamo repairs synchronously on the read path; Cassandra instead sends
	// an async digest request to the replicas that did not answer with the
	// winning value).
	_ = targets
	for _, id := range prefs {
		rp := c.replicas[id]
		if !rp.alive {
			continue
		}
		before := fmt.Sprint(dropSuperseded(rp.data[key]))
		rp.data[key] = dropSuperseded(mergeSiblings(rp.data[key], merged))
		if after := fmt.Sprint(rp.data[key]); after != before {
			rp.repair++
			c.stats.repairs++
		}
	}

	var values []string
	conflict := len(merged) > 1
	for _, v := range merged {
		if v.Deleted {
			continue
		}
		values = append(values, v.Value)
	}
	sort.Strings(values)
	return values, conflict, nil
}

// GetVia reads from one specific replica, bypassing the quorum logic. It is
// how tests expose staleness that a quorum read would hide.
func (c *Cluster) GetVia(node, key string) ([]string, bool, error) {
	rp := c.replicas[node]
	if rp == nil || !rp.alive {
		return nil, false, nil
	}
	merged := dropSuperseded(rp.data[key])
	var values []string
	for _, v := range merged {
		if !v.Deleted {
			values = append(values, v.Value)
		}
	}
	return values, len(merged) > 1, nil
}

func (c *Cluster) gather(key string, prefs []string, count int) []VersionedValue {
	var out []VersionedValue
	seen := 0
	for _, id := range prefs {
		rp := c.replicas[id]
		if !rp.alive {
			continue
		}
		if seen >= count {
			break
		}
		seen++
		out = append(out, rp.data[key]...)
	}
	return out
}

// AntiEntropy compares two replicas key by key and copies missing versions —
// the slow, background mechanism that fixes what read repair missed.
func (c *Cluster) AntiEntropy() (transferred int) {
	live := make([]string, 0, len(c.nodes))
	for _, id := range c.nodes {
		if c.replicas[id].alive {
			live = append(live, id)
		}
	}
	for i := 0; i < len(live); i++ {
		for j := i + 1; j < len(live); j++ {
			a, b := c.replicas[live[i]], c.replicas[live[j]]
			keys := map[string]bool{}
			for k := range a.data {
				keys[k] = true
			}
			for k := range b.data {
				keys[k] = true
			}
			for k := range keys {
				beforeA, beforeB := len(a.data[k]), len(b.data[k])
				a.data[k] = dropSuperseded(mergeSiblings(a.data[k], b.data[k]))
				b.data[k] = dropSuperseded(mergeSiblings(b.data[k], a.data[k]))
				if len(a.data[k]) != beforeA || len(b.data[k]) != beforeB {
					transferred++
				}
			}
		}
	}
	c.stats.antiEntropy++
	return transferred
}

// Inspect returns a replica's raw siblings, for assertions in tests.
func (c *Cluster) Inspect(node, key string) []VersionedValue {
	rp := c.replicas[node]
	if rp == nil {
		return nil
	}
	return append([]VersionedValue(nil), rp.data[key]...)
}

// Stats is a snapshot of the cluster counters.
type Stats struct {
	Reads           int
	Writes          int
	ReadRepairs     int
	HintedWrites    int
	AntiEntropyRuns int
	PendingHints    int
}

// Stats returns the counters.
func (c *Cluster) Stats() Stats {
	pending := 0
	for _, list := range c.hints {
		pending += len(list)
	}
	return Stats{
		Reads:           c.stats.reads,
		Writes:          c.stats.writes,
		ReadRepairs:     c.stats.repairs,
		HintedWrites:    c.stats.hinted,
		AntiEntropyRuns: c.stats.antiEntropy,
		PendingHints:    pending,
	}
}

// ------------------------------------------------------------------- merging --

// mergeSiblings unions version sets, keeping only mutually-concurrent ones.
func mergeSiblings(dst, src []VersionedValue) []VersionedValue {
	out := append([]VersionedValue(nil), dst...)
	for _, s := range src {
		out = addVersion(out, s)
	}
	return out
}

// addVersion inserts v, dropping any element v supersedes, and dropping v if
// something already present supersedes it. Equal clocks with different
// payloads are treated as the same version (they came from the same write).
func addVersion(set []VersionedValue, v VersionedValue) []VersionedValue {
	out := set[:0:0]
	for _, e := range set {
		switch Compare(e.Clock, v.Clock) {
		case Before, Equal:
			continue // e is older: drop it
		case After:
			return set // e is newer: keep the set as-is
		}
		out = append(out, e)
	}
	return append(out, v)
}

func dropSuperseded(set []VersionedValue) []VersionedValue {
	var out []VersionedValue
	for _, v := range set {
		out = addVersion(out, v)
	}
	return out
}

// FormatClock renders a clock for readable test output.
func FormatClock(vc VectorClock) string {
	keys := make([]string, 0, len(vc))
	for k := range vc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, vc[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}
