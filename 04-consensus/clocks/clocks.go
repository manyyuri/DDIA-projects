// Package clocks implements DDIA §8.3-8.4: ordering events without (and with)
// physical time.
//
// The ladder of guarantees, and what each one costs:
//
//	Lamport clock    total order consistent with causality   cannot detect concurrency
//	vector clock     detects concurrency, causal order        O(N) per timestamp
//	HLC              near-physical time + causality          bounded by clock skew
//	Snowflake ID     sortable unique ids                     needs a trustworthy wall clock
//
// The tests deliberately move a node's wall clock backwards, because that is
// what actually happens in production (NTP steps, VM migration, leap seconds)
// and it is where naive implementations lose uniqueness.
package clocks

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// ------------------------------------------------------------ Lamport clocks --

// Lamport is a scalar logical clock. It gives you a total order that respects
// happens-before, but the converse does not hold: a < b in Lamport time does
// not mean a caused b. That is exactly why it cannot detect concurrent writes.
type Lamport struct {
	mu sync.Mutex
	t  uint64
}

// NewLamport creates a clock starting at zero.
func NewLamport() *Lamport { return &Lamport{} }

// Tick labels a local event.
func (l *Lamport) Tick() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t++
	return l.t
}

// Send labels a message send (same as Tick, named for readability).
func (l *Lamport) Send() uint64 { return l.Tick() }

// Observe merges a received timestamp: max(local, remote) + 1. This is the rule
// that makes the clock causal.
func (l *Lamport) Observe(remote uint64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if remote > l.t {
		l.t = remote
	}
	l.t++
	return l.t
}

// Now returns the current value without advancing.
func (l *Lamport) Now() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.t
}

// Cast returns the timestamp for a local event without advancing.
func (l *Lamport) Cast() uint64 { return l.Now() }

// ------------------------------------------------------------- vector clocks --

// Relation between two events.
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

// Vector is a vector clock: one counter per node.
type Vector struct {
	mu   sync.Mutex
	self string
	m    map[string]uint64
}

// NewVector creates a clock for node self.
func NewVector(self string) *Vector {
	return &Vector{self: self, m: map[string]uint64{}}
}

// Tick labels a local event.
func (v *Vector) Tick() map[string]uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.m[v.self]++
	return v.snapshot()
}

// Observe merges a received vector clock (per-node maximum).
func (v *Vector) Observe(other map[string]uint64) map[string]uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	for k, val := range other {
		if val > v.m[k] {
			v.m[k] = val
		}
	}
	v.m[v.self]++
	return v.snapshot()
}

// Snapshot returns a copy of the current vector.
func (v *Vector) Snapshot() map[string]uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snapshot()
}

func (v *Vector) snapshot() map[string]uint64 {
	out := make(map[string]uint64, len(v.m))
	for k, val := range v.m {
		out[k] = val
	}
	return out
}

// CompareVectors orders two vector clocks. Concurrency is the answer a Lamport
// clock can never give you.
func CompareVectors(a, b map[string]uint64) Relation {
	less, greater := false, false
	for k, av := range a {
		if b[k] < av {
			greater = true
		}
	}
	for k, bv := range b {
		if a[k] < bv {
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

// Format renders a vector clock deterministically.
func Format(m map[string]uint64) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// ------------------------------------------------- hybrid logical clocks (HLC) --

// Timestamp is an HLC value: physical milliseconds plus a logical counter that
// breaks ties when the wall clock does not advance.
type Timestamp struct {
	Physical int64
	Logical  uint32
}

// Compare orders timestamps.
func (t Timestamp) Compare(o Timestamp) int {
	switch {
	case t.Physical != o.Physical:
		if t.Physical < o.Physical {
			return -1
		}
		return 1
	case t.Logical != o.Logical:
		if t.Logical < o.Logical {
			return -1
		}
		return 1
	}
	return 0
}

func (t Timestamp) String() string { return fmt.Sprintf("%d.%d", t.Physical, t.Logical) }

// HLC is a hybrid logical clock (Kulkarni et al.): it stays close to wall time
// but never goes backwards, and it respects causality when messages carry
// timestamps. CockroachDB uses this to give transactions a timestamp that is
// both sortable and causal.
type HLC struct {
	mu      sync.Mutex
	last    Timestamp
	maxSkew time.Duration
	nowFn   func() time.Time
	skew    time.Duration // simulated clock offset for experiments
}

// NewHLC creates a clock with a maximum tolerated skew (a message from further
// in the future than this is rejected as a broken clock).
func NewHLC(maxSkew time.Duration) *HLC {
	return &HLC{maxSkew: maxSkew, nowFn: time.Now}
}

// SetClockSkew simulates a node whose wall clock is wrong.
func (h *HLC) SetClockSkew(d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.skew = d
}

// SetNow overrides the wall clock (for deterministic tests).
func (h *HLC) SetNow(f func() time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nowFn = f
}

// Now issues a timestamp for a local event. It never returns a value <= the
// previous one, even if the wall clock jumped backwards.
func (h *HLC) Now() Timestamp {
	h.mu.Lock()
	defer h.mu.Unlock()
	wall := h.wallMillis()
	switch {
	case wall > h.last.Physical:
		h.last = Timestamp{Physical: wall}
	case wall == h.last.Physical:
		h.last.Logical++
	default:
		// Clock went backwards (NTP step). Keep monotonicity by advancing the
		// logical part; the physical part stays behind, which is exactly the
		// bound on how far HLC can drift from real time.
		h.last.Logical++
	}
	return h.last
}

// Update merges a remote timestamp (called on message receive).
func (h *HLC) Update(remote Timestamp, remoteWallMillis int64) (Timestamp, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	wall := h.wallMillis()
	if remote.Physical-wall > h.maxSkew.Milliseconds() {
		return h.last, fmt.Errorf("clocks: remote timestamp %s is %dms ahead, exceeds max skew %v",
			remote, remote.Physical-wall, h.maxSkew)
	}
	prev := h.last
	switch {
	case wall > prev.Physical && wall > remote.Physical:
		h.last = Timestamp{Physical: wall}
	case remote.Physical > wall && remote.Physical > prev.Physical:
		h.last = Timestamp{Physical: remote.Physical, Logical: remote.Logical + 1}
	case prev.Physical == remote.Physical:
		if prev.Logical > remote.Logical {
			h.last = Timestamp{Physical: prev.Physical, Logical: prev.Logical + 1}
		} else {
			h.last = Timestamp{Physical: remote.Physical, Logical: remote.Logical + 1}
		}
	default:
		h.last = Timestamp{Physical: prev.Physical, Logical: prev.Logical + 1}
	}
	return h.last, nil
}

func (h *HLC) wallMillis() int64 {
	return h.nowFn().Add(h.skew).UnixMilli()
}

// PhysicalTime reconstructs a wall-clock estimate from a timestamp (CockroachDB
// uses this for "as of system time" queries).
func (h *HLC) PhysicalTime() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.UnixMilli(h.last.Physical)
}

// ------------------------------------------------------------- Snowflake ids --

// Snowflake generates sortable 64-bit ids: 41 bits of milliseconds, 10 bits of
// node id, 12 bits of sequence. It is the simplest possible answer to
// "distributed unique ids" and it fails in exactly one way: a wall clock that
// jumps backwards.
type Snowflake struct {
	mu         sync.Mutex
	node       uint64
	lastMillis int64
	seq        uint64
	nowFn      func() time.Time
	seqMax     uint64
}

const (
	snowflakeEpoch   = int64(1700000000000) // arbitrary fixed epoch
	nodeBits         = 10
	sequenceBits     = 12
	snowflakeNodeMax = int64(-1 ^ (-1 << nodeBits))
)

// NewSnowflake creates a generator for node id.
func NewSnowflake(node uint64) (*Snowflake, error) {
	if int64(node) > snowflakeNodeMax || int64(node) < 0 {
		return nil, fmt.Errorf("clocks: node id %d out of range 0..%d", node, snowflakeNodeMax)
	}
	return &Snowflake{node: node, nowFn: time.Now, seqMax: (1 << sequenceBits) - 1}, nil
}

// SetNow overrides the wall clock so tests can jump it backwards.
func (s *Snowflake) SetNow(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nowFn = f
}

// Next returns the next unique id, or an error if the clock went backwards.
// Returning an error instead of blocking is a deliberate choice: refusing to
// mint ids is safer than minting duplicates.
func (s *Snowflake) Next() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFn().UnixMilli()
	if now < s.lastMillis {
		return 0, fmt.Errorf("clocks: clock moved backwards by %dms, refusing to generate ids",
			s.lastMillis-now)
	}
	if now == s.lastMillis {
		s.seq++
		if s.seq > s.seqMax {
			// Sequence exhausted inside this millisecond: busy-wait for the next
			// one. Real implementations block here.
			for now <= s.lastMillis {
				now = s.nowFn().UnixMilli()
			}
			s.lastMillis, s.seq = now, 0
		}
	} else {
		s.lastMillis, s.seq = now, 0
	}
	id := (uint64(s.lastMillis-snowflakeEpoch) << (nodeBits + sequenceBits)) |
		(s.node << sequenceBits) | s.seq
	return id, nil
}

// DecodeSnowflake splits an id back into its parts.
func DecodeSnowflake(id uint64) (ms int64, node uint64, seq uint64) {
	seq = id & ((1 << sequenceBits) - 1)
	node = (id >> sequenceBits) & ((1 << nodeBits) - 1)
	ms = int64(id>>(nodeBits+sequenceBits)) + snowflakeEpoch
	return
}
