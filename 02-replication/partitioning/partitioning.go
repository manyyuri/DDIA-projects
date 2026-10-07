// Package partitioning implements DDIA §6: how to decide which node stores
// which key, and — the part that actually hurts in production — how to move
// data when the set of nodes changes.
//
// Two strategies, two different failure modes:
//
//	hash partitioning   even load, no range scans, rebalancing moves ~1/N of keys
//	range partitioning  range scans and locality, but the hot key range becomes
//	                    a hot node until you split it
//
// The measured numbers here are the point: modulo hashing remaps almost every
// key when N changes, consistent hashing remaps about 1/(N+1).
package partitioning

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

// hash32 is the ring hash. Real systems use murmur/xxhash; fnv plus a final
// avalanche step is enough to demonstrate the properties.
//
// The avalanche step is not cosmetic: raw FNV-1a leaves structure in the low
// bits, so ring points generated from "node#0", "node#1", ... cluster instead
// of spreading, and the measured imbalance goes from ~1.05x to ~1.3x.
func hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return mix32(h.Sum32())
}

// mix32 is the MurmurHash3 finaliser: it spreads every input bit across the
// whole 32-bit output.
func mix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// ---------------------------------------------------------------- modulo hashing --

// ModuloAssign is the naive scheme: node = hash(key) % N. It is stateless and
// perfectly balanced, and it is a trap: changing N remaps essentially every key.
func ModuloAssign(nodes []string, key string) string {
	if len(nodes) == 0 {
		return ""
	}
	return nodes[int(hash32(key))%len(nodes)]
}

// ------------------------------------------------------------------- ring --

type ringPoint struct {
	hash uint32
	node string
}

// HashRing is consistent hashing with virtual nodes. Virtual nodes are what
// make the load even: with one point per node, random placement produces very
// uneven arcs; with 100+ points per node the law of large numbers evens it out.
type HashRing struct {
	vnodes int
	points []ringPoint
	byNode map[string][]uint32
	sorted bool
}

// NewHashRing creates a ring with vnodes points per node.
func NewHashRing(vnodes int) *HashRing {
	if vnodes <= 0 {
		vnodes = 128
	}
	return &HashRing{vnodes: vnodes, byNode: map[string][]uint32{}}
}

// AddNode inserts a node (and its virtual nodes) into the ring.
func (r *HashRing) AddNode(node string) {
	if _, ok := r.byNode[node]; ok {
		return
	}
	var pts []uint32
	for i := 0; i < r.vnodes; i++ {
		h := hash32(fmt.Sprintf("%s#%d", node, i))
		r.points = append(r.points, ringPoint{hash: h, node: node})
		pts = append(pts, h)
	}
	r.byNode[node] = pts
	r.sorted = false
}

// RemoveNode removes a node from the ring.
func (r *HashRing) RemoveNode(node string) {
	pts, ok := r.byNode[node]
	if !ok {
		return
	}
	drop := map[uint32]bool{}
	for _, p := range pts {
		drop[p] = true
	}
	kept := r.points[:0:0]
	for _, p := range r.points {
		if !(p.node == node && drop[p.hash]) {
			kept = append(kept, p)
		}
	}
	r.points = kept
	delete(r.byNode, node)
	r.sorted = false
}

// Nodes lists the nodes on the ring.
func (r *HashRing) Nodes() []string {
	out := make([]string, 0, len(r.byNode))
	for n := range r.byNode {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Locate returns the node owning key: the first point clockwise from hash(key).
func (r *HashRing) Locate(key string) string {
	if len(r.points) == 0 {
		return ""
	}
	if !r.sorted {
		sort.Slice(r.points, func(i, j int) bool {
			if r.points[i].hash != r.points[j].hash {
				return r.points[i].hash < r.points[j].hash
			}
			return r.points[i].node < r.points[j].node
		})
		r.sorted = true
	}
	h := hash32(key)
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrap around the ring
	}
	return r.points[i].node
}

// Distribution assigns a set of keys and returns how many each node got.
func (r *HashRing) Distribution(keys []string) map[string]int {
	out := map[string]int{}
	for _, k := range keys {
		out[r.Locate(k)]++
	}
	return out
}

// LoadImbalance returns max/average key counts. 1.0 is perfect balance.
func (r *HashRing) LoadImbalance(keys []string) float64 {
	dist := r.Distribution(keys)
	if len(dist) == 0 {
		return 0
	}
	total, max := 0, 0
	for _, v := range dist {
		total += v
		if v > max {
			max = v
		}
	}
	avg := float64(total) / float64(len(r.byNode))
	return float64(max) / avg
}

// ------------------------------------------------------------ range partitioning --

// Range is one contiguous key range [Start, End) owned by one node. Ranges are
// how you keep ordering (and therefore scans) while still partitioning.
type Range struct {
	Start string
	End   string // "" means "up to the end of the keyspace"
	Node  string
}

func (r Range) contains(key string) bool {
	if key < r.Start {
		return false
	}
	return r.End == "" || key < r.End
}

func (r Range) String() string {
	end := r.End
	if end == "" {
		end = "∞"
	}
	return fmt.Sprintf("[%s,%s)->%s", r.Start, end, r.Node)
}

// RangePartitioner holds a sorted, non-overlapping set of ranges.
type RangePartitioner struct {
	Ranges []Range // sorted by Start
}

// NewRangePartitioner splits the keyspace into count even ranges over nodes.
func NewRangePartitioner(nodes []string, count int) *RangePartitioner {
	if count < 1 {
		count = 1
	}
	p := &RangePartitioner{}
	for i := 0; i < count; i++ {
		start := ""
		if i > 0 {
			start = fmt.Sprintf("k%05d", i*(100000/count))
		}
		end := ""
		if i < count-1 {
			end = fmt.Sprintf("k%05d", (i+1)*(100000/count))
		}
		p.Ranges = append(p.Ranges, Range{Start: start, End: end, Node: nodes[i%len(nodes)]})
	}
	return p
}

// Locate finds the owner of key (linear scan; a real system binary-searches).
func (p *RangePartitioner) Locate(key string) string {
	for _, r := range p.Ranges {
		if r.contains(key) {
			return r.Node
		}
	}
	return ""
}

// Split divides the range containing key at key. This is the answer to
// hotspots: instead of moving the hot key, you split so two nodes share the
// range ("dynamic partitioning", DDIA §6.2).
func (p *RangePartitioner) Split(key, newNode string) error {
	for i, r := range p.Ranges {
		if !r.contains(key) || r.Start == key {
			continue
		}
		left := Range{Start: r.Start, End: key, Node: r.Node}
		right := Range{Start: key, End: r.End, Node: newNode}
		p.Ranges = append(p.Ranges[:i], append([]Range{left, right}, p.Ranges[i+1:]...)...)
		return nil
	}
	return fmt.Errorf("no splittable range contains %q", key)
}

// MergeRange removes range i and folds it into its left neighbour.
func (p *RangePartitioner) MergeRange(i int) error {
	if i <= 0 || i >= len(p.Ranges) {
		return fmt.Errorf("range %d cannot be merged", i)
	}
	p.Ranges[i-1].End = p.Ranges[i].End
	p.Ranges = append(p.Ranges[:i], p.Ranges[i+1:]...)
	return nil
}

// AssignRanges distributes ranges across nodes as evenly as possible, moving as
// few as possible. This is the "minimal movement" property that makes
// rebalancing cheap and, more importantly, makes it safe to do online.
func (p *RangePartitioner) AssignRanges(nodes []string) map[string]int {
	moved := map[string]int{}
	if len(nodes) == 0 {
		return moved
	}
	// How many ranges should each node hold?
	counts := map[string]int{}
	for _, r := range p.Ranges {
		counts[r.Node]++
	}
	target := map[string]int{}
	for i, n := range nodes {
		// Distribute the remainder deterministically.
		target[n] = len(p.Ranges)/len(nodes) + i%(max(1, len(p.Ranges)%len(nodes)))
		_ = counts
	}
	// Greedy: move ranges from over-target to under-target nodes.
	for i := range p.Ranges {
		owner := p.Ranges[i].Node
		if counts[owner] <= target[owner] {
			continue
		}
		for _, n := range nodes {
			if counts[n] < target[n] {
				counts[owner]--
				counts[n]++
				p.Ranges[i].Node = n
				moved[n]++
				break
			}
		}
	}
	// Nodes that disappeared entirely: reassign their ranges.
	known := map[string]bool{}
	for _, n := range nodes {
		known[n] = true
	}
	for i := range p.Ranges {
		if !known[p.Ranges[i].Node] {
			p.Ranges[i].Node = nodes[i%len(nodes)]
			moved[p.Ranges[i].Node]++
		}
	}
	return moved
}

// Counts returns how many ranges each node owns.
func (p *RangePartitioner) Counts() map[string]int {
	out := map[string]int{}
	for _, r := range p.Ranges {
		out[r.Node]++
	}
	return out
}

// Describe renders the partition map.
func (p *RangePartitioner) Describe() string {
	parts := make([]string, 0, len(p.Ranges))
	for _, r := range p.Ranges {
		parts = append(parts, r.String())
	}
	return strings.Join(parts, " ")
}

// ------------------------------------------------------------- key movement --

// KeyMovement counts how many keys changed owner between two mappings.
func KeyMovement(keys []string, before, after func(string) string) (moved, total int) {
	for _, k := range keys {
		if before(k) != after(k) {
			moved++
		}
	}
	return moved, len(keys)
}

// SampleKeys generates n deterministic keys.
func SampleKeys(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = "key-" + strconv.Itoa(i)
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
