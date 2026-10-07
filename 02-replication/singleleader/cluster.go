// Package singleleader implements DDIA §5.1-5.2: leader-based replication,
// with the trade-offs that chapter is really about.
//
// Deliberately NOT consensus: there is no election protocol here. Promotion is
// an *external* decision (the failover script, an operator, a separate
// consensus service), which is exactly how many real deployments work and is
// exactly why they lose data. Pick a promotion policy and watch the
// consequences:
//
//	SyncMode=Async        leader acks immediately      -> fast, loses commits on failover
//	SyncMode=WaitForQuorum replication quorum acks     -> survives one failure, latency = median replica
//	SyncMode=WaitForAll   every replica acks           -> any replica down blocks writes
//
// Read modes are the second half of the lesson: a replica is a *cache* that
// answers stale, and the fix (read-your-writes / monotonic reads) is a client
// session property, not a server property.
package singleleader

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

// SyncMode decides what "the write succeeded" means.
type SyncMode int

const (
	// Async: ack as soon as the leader has it in its log.
	Async SyncMode = iota
	// WaitForQuorum: ack once a majority of live replicas has it.
	WaitForQuorum
	// WaitForAll: ack once every live replica has it.
	WaitForAll
)

func (m SyncMode) String() string {
	switch m {
	case Async:
		return "async"
	case WaitForQuorum:
		return "quorum"
	case WaitForAll:
		return "all"
	}
	return "?"
}

// ReadMode decides which replica may answer a read.
type ReadMode int

const (
	// ReadLeader forces the read to the leader (fresh but not scalable).
	ReadLeader ReadMode = iota
	// ReadFollower lets any follower answer: cheap, and possibly stale.
	ReadFollower
	// ReadYourWrites routes to a replica that has caught up to the caller's
	// session watermark.
	ReadYourWrites
)

func (m ReadMode) String() string {
	switch m {
	case ReadLeader:
		return "leader"
	case ReadFollower:
		return "follower"
	case ReadYourWrites:
		return "your-writes"
	}
	return "?"
}

// Op is a replicated write.
type Op struct {
	Key   string
	Value string
	Del   bool
	// Noop is a leader's "commit barrier": a new leader appends one empty entry
	// in its own term so that everything before it can be committed.
	Noop bool
}

// Entry is one slot of the replication log.
type Entry struct {
	Index uint64
	Term  uint64
	Op    Op
}

// ------------------------------------------------------------------ messages --

type appendEntries struct {
	term         uint64
	prevIndex    uint64
	prevTerm     uint64
	entries      []Entry
	leaderCommit uint64
}

type appendResp struct {
	term          uint64
	node          string
	success       bool
	matchIndex    uint64
	conflictIndex uint64 // hint: where the follower's log diverges
}

type node struct {
	id     string
	log    []Entry // log[0] is a placeholder so indexes are 1-based
	commit uint64
	// applied is the "readable" watermark: a node must never expose entries it
	// does not know to be committed.
	applied uint64
	store   map[string]string
}

func (n *node) lastIndex() uint64 { return uint64(len(n.log) - 1) }

func (n *node) termAt(idx uint64) uint64 {
	if idx == 0 || idx >= uint64(len(n.log)) {
		return 0
	}
	return n.log[idx].Term
}

func (n *node) applyUpTo(upto uint64) {
	if upto > n.lastIndex() {
		upto = n.lastIndex()
	}
	for i := n.applied + 1; i <= upto; i++ {
		e := n.log[i]
		if e.Op.Noop {
			continue
		}
		if e.Op.Del {
			delete(n.store, e.Op.Key)
		} else {
			n.store[e.Op.Key] = e.Op.Value
		}
	}
	if upto > n.applied {
		n.applied = upto
	}
}

// ------------------------------------------------------------------- cluster --

// Cluster is a leader plus N followers talking over a simulated network.
type Cluster struct {
	mu  sync.Mutex
	net *simnet.Network

	ids    []string
	leader string
	term   uint64

	nodes map[string]*node

	// leader-side replication bookkeeping
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	dead       map[string]bool

	events []string // human-readable timeline, for test reports
}

// NewCluster builds a cluster whose leader is ids[0].
func NewCluster(ids []string, seed int64, latency time.Duration) *Cluster {
	c := &Cluster{
		net:        simnet.New(seed),
		ids:        append([]string(nil), ids...),
		nodes:      map[string]*node{},
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		dead:       map[string]bool{},
	}
	c.leader = ids[0]
	c.term = 1
	c.net.SetLatency(latency, latency/2)
	for _, id := range ids {
		n := &node{
			id:    id,
			log:   []Entry{{}}, // placeholder at index 0
			store: map[string]string{},
		}
		c.nodes[id] = n
		c.bind(id)
	}
	c.resetLeaderStateLocked()
	return c
}

func (c *Cluster) bind(id string) {
	c.net.Register(id, func(m simnet.Message) {
		switch body := m.Body.(type) {
		case appendEntries:
			c.handleAppend(id, m.From, body)
		case appendResp:
			c.handleAppendResp(id, body)
		}
	})
}

func (c *Cluster) resetLeaderStateLocked() {
	lead := c.nodes[c.leader]
	for _, id := range c.ids {
		if id == c.leader {
			continue
		}
		c.nextIndex[id] = lead.lastIndex() + 1
		c.matchIndex[id] = 0
	}
	c.matchIndex[c.leader] = lead.lastIndex()
}

// CommitNow forces the leader to re-evaluate its commit index (used after a
// promotion so the no-op barrier can commit in simulated time).
func (c *Cluster) CommitNow() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.advanceCommitLocked() {
		c.broadcastAppendLocked()
	}
}

// Net exposes the simulator so tests can partition, drop and crash nodes.
func (c *Cluster) Net() *simnet.Network { return c.net }

// Leader returns the current leader id.
func (c *Cluster) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// Log records a line in the timeline (useful in failure reports).
func (c *Cluster) Log(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logLocked(format, args...)
}

// logLocked is Log for callers that already hold c.mu.
func (c *Cluster) logLocked(format string, args ...any) {
	c.events = append(c.events, fmt.Sprintf("@%v "+format, append([]any{c.net.Now()}, args...)...))
}

// Timeline returns the recorded events.
func (c *Cluster) Timeline() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.events...)
}

// ---------------------------------------------------------------- replication --

// Append puts op into the leader's log and starts replication, without waiting.
// The returned index is *not* a durable promise: it commits only once a quorum
// has it (see Write).
func (c *Cluster) Append(op Op) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	lead := c.nodes[c.leader]
	idx := lead.lastIndex() + 1
	lead.log = append(lead.log, Entry{Index: idx, Term: c.term, Op: op})
	c.matchIndex[c.leader] = idx
	c.nextIndex[c.leader] = idx + 1
	c.broadcastAppendLocked()
	return idx
}

// Write appends an op and replicates synchronously *in simulated time*.
//
// mode=Async returns the index immediately (ack = leader's WAL).
// mode=WaitForQuorum / WaitForAll return the highest index that reached the
// required number of replicas within timeout, and whether it made it.
func (c *Cluster) Write(op Op, mode SyncMode, timeout time.Duration) (uint64, bool) {
	idx := c.Append(op)
	if mode == Async {
		return idx, true
	}
	want := c.requiredAcks(mode)
	deadline := c.net.Now() + timeout
	for {
		c.mu.Lock()
		committed := c.nodes[c.leader].commit
		c.mu.Unlock()
		if committed >= idx {
			// Quorum commit is enough for WaitForQuorum; WaitForAll additionally
			// waits for every *live* replica to hold a copy.
			if mode == WaitForQuorum || c.AckCount(idx) >= want {
				return idx, true
			}
		}
		if c.net.Now() >= deadline {
			return idx, false
		}
		c.net.Run(time.Millisecond)
	}
}

func (c *Cluster) requiredAcks(mode SyncMode) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	live := c.liveCountLocked()
	switch mode {
	case WaitForAll:
		// Every *configured* replica, not just the live ones: that is the
		// availability cost of "durable everywhere".
		return len(c.ids)
	default:
		return live/2 + 1
	}
}

func (c *Cluster) liveCountLocked() int {
	n := 0
	for _, id := range c.ids {
		if !c.dead[id] {
			n++
		}
	}
	return n
}

func (c *Cluster) broadcastAppendLocked() {
	for _, id := range c.ids {
		if id == c.leader || c.dead[id] {
			continue
		}
		c.sendAppendLocked(id)
	}
}

// sendAppendLocked pushes the entries a single follower is missing. Keeping it
// per-follower (instead of always broadcasting to everyone) matters: a naive
// "reply -> broadcast" loop never quiesces.
func (c *Cluster) sendAppendLocked(id string) {
	lead := c.nodes[c.leader]
	next := c.nextIndex[id]
	if next < 1 {
		next = 1
	}
	last := lead.lastIndex()
	var entries []Entry
	if next <= last {
		entries = append([]Entry(nil), lead.log[next:last+1]...)
	}
	c.net.Send(c.leader, id, appendEntries{
		term:         c.term,
		prevIndex:    next - 1,
		prevTerm:     lead.termAt(next - 1),
		entries:      entries,
		leaderCommit: lead.commit,
	})
}

// handleAppend runs on the replica: self is the receiver, leader is the sender.
func (c *Cluster) handleAppend(self, leader string, msg appendEntries) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[self]
	if n == nil || c.dead[self] {
		return
	}
	resp := appendResp{term: c.term, node: self}
	if msg.term < c.term {
		c.sendRespLocked(self, leader, resp)
		return
	}
	// Log matching property: the entry before the new ones must agree.
	if msg.prevIndex > n.lastIndex() {
		resp.conflictIndex = n.lastIndex() + 1
		c.sendRespLocked(self, leader, resp)
		return
	}
	if msg.prevIndex > 0 && n.termAt(msg.prevIndex) != msg.prevTerm {
		// Divergent history: report where to resume so the leader can back up.
		resp.conflictIndex = msg.prevIndex
		c.logLocked("%s rejected append (term mismatch at %d)", self, msg.prevIndex)
		c.sendRespLocked(self, leader, resp)
		return
	}
	// Accept: truncate any conflicting suffix, then append.
	for i, e := range msg.entries {
		idx := msg.prevIndex + uint64(i) + 1
		if idx <= n.lastIndex() {
			if n.log[idx].Term != e.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, e)
			}
			continue
		}
		n.log = append(n.log, e)
	}
	// Commit watermark comes from the leader, so followers never expose
	// uncommitted data.
	if msg.leaderCommit > n.commit {
		n.commit = min(msg.leaderCommit, n.lastIndex())
	}
	n.applyUpTo(n.commit)
	resp.success = true
	resp.matchIndex = n.lastIndex()
	c.sendRespLocked(self, leader, resp)
}

func (c *Cluster) sendRespLocked(self, leader string, resp appendResp) {
	if c.dead[self] {
		return
	}
	c.net.Send(self, leader, resp)
}

// handleAppendResp runs on the leader and advances matchIndex / commitIndex.
func (c *Cluster) handleAppendResp(self string, msg appendResp) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if self != c.leader {
		return
	}
	from := msg.node
	if _, ok := c.nodes[from]; !ok || c.dead[from] {
		return
	}
	if msg.success {
		if msg.matchIndex > c.matchIndex[from] {
			c.matchIndex[from] = msg.matchIndex
		}
		c.nextIndex[from] = c.matchIndex[from] + 1
	} else {
		if msg.conflictIndex > 0 {
			c.nextIndex[from] = msg.conflictIndex
		} else if c.nextIndex[from] > 1 {
			c.nextIndex[from]--
		}
	}
	if c.advanceCommitLocked() {
		// New commit watermark: everyone needs to hear about it.
		c.broadcastAppendLocked()
		return
	}
	// Otherwise just keep pushing to a follower that is still behind.
	lead := c.nodes[c.leader]
	if c.nextIndex[from] <= lead.lastIndex() {
		c.sendAppendLocked(from)
	}
}

// advanceCommitLocked implements the DDIA/Raft commit rule: an index is
// committed once a quorum of live replicas holds it *and* it belongs to the
// current term.
func (c *Cluster) advanceCommitLocked() bool {
	lead := c.nodes[c.leader]
	matches := make([]uint64, 0, len(c.ids))
	for _, id := range c.ids {
		if c.dead[id] {
			continue
		}
		matches = append(matches, c.matchIndex[id])
	}
	if len(matches) == 0 {
		return false
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	quorum := len(matches)/2 + 1
	candidate := matches[quorum-1]
	if candidate > lead.commit && lead.termAt(candidate) == c.term {
		lead.commit = candidate
		lead.applyUpTo(lead.commit)
		return true
	}
	return false
}

// -------------------------------------------------------------- observations --

// AckCount reports how many live replicas actually hold entry idx in their log.
func (c *Cluster) AckCount(idx uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, id := range c.ids {
		if c.dead[id] {
			continue
		}
		n := c.nodes[id]
		if idx <= n.lastIndex() {
			count++
		}
	}
	return count
}

// CommittedIndex is the highest index the leader knows to be committed.
func (c *Cluster) CommittedIndex() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[c.leader].commit
}

// AppliedIndex reports a node's readable watermark.
func (c *Cluster) AppliedIndex(id string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.nodes[id]; n != nil {
		return n.applied
	}
	return 0
}

// ReplicationLag reports how far behind the given replica is, in entries.
func (c *Cluster) ReplicationLag(id string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	lead, n := c.nodes[c.leader], c.nodes[id]
	if lead == nil || n == nil {
		return 0
	}
	if lead.lastIndex() < n.lastIndex() {
		return 0
	}
	return lead.lastIndex() - n.lastIndex()
}

// Get reads through the requested mode. sessionIndex is the caller's
// read-your-writes watermark (0 disables the check).
func (c *Cluster) Get(mode ReadMode, replica, key string, sessionIndex uint64) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := replica
	if mode == ReadLeader {
		target = c.leader
	}
	n := c.nodes[target]
	if n == nil || c.dead[target] {
		return "", false, fmt.Errorf("replica %q is not available", target)
	}
	if mode == ReadYourWrites && n.applied < sessionIndex {
		return "", false, fmt.Errorf("replica %q is stale: applied=%d < session=%d", target, n.applied, sessionIndex)
	}
	v, ok := n.store[key]
	return v, ok, nil
}

// ----------------------------------------------------------------- failover --

// FailoverReport explains what a promotion cost.
type FailoverReport struct {
	OldLeader    string
	NewLeader    string
	OldCommit    uint64
	LostEntries  uint64 // committed entries the new leader never received
	LostOps      []Op
	ChosenReason string
}

// Fail crashes a node.
func (c *Cluster) Fail(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dead[id] = true
	c.net.Unregister(id)
	c.logLocked("node %s FAILED", id)
}

// Recover brings a node back (it keeps its log: this models a restart with
// durable local state).
func (c *Cluster) Recover(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dead[id] = false
	c.bind(id)
	c.logLocked("node %s RECOVERED", id)
}

// Promote performs an *operator-driven* failover. It picks the live replica
// with the longest log (the only safe-ish heuristic without consensus), makes
// it leader, and reports what was lost.
func (c *Cluster) Promote(prefer string) FailoverReport {
	c.mu.Lock()
	defer c.mu.Unlock()

	rep := FailoverReport{OldLeader: c.leader}
	old := c.nodes[c.leader]
	if old != nil {
		rep.OldCommit = old.commit
	}

	var best *node
	for _, id := range c.ids {
		if c.dead[id] {
			continue
		}
		n := c.nodes[id]
		if best == nil {
			best = n
			continue
		}
		if prefer == id {
			best = n
			continue
		}
		if n.lastIndex() > best.lastIndex() {
			best = n
		}
	}
	if best == nil {
		rep.ChosenReason = "no live replica"
		return rep
	}
	if prefer != "" && !c.dead[prefer] {
		best = c.nodes[prefer]
		rep.ChosenReason = "operator preference"
	} else {
		rep.ChosenReason = "longest log"
	}

	c.leader = best.id
	c.term++
	rep.NewLeader = best.id
	_ = old

	// Anything committed under the old leader that the new leader does not have
	// is gone. This is the classic "async replication loses commits" outcome.
	for i := uint64(1); i <= rep.OldCommit; i++ {
		if i > best.lastIndex() {
			rep.LostEntries++
			if old != nil && i < uint64(len(old.log)) {
				rep.LostOps = append(rep.LostOps, old.log[i].Op)
			}
		}
	}
	// A new leader cannot simply declare the old entries committed: it must
	// first commit an entry of its *own* term. Everything before it then
	// commits with it (Raft's commit rule), which is how a promoted replica
	// catches its readable state up to its own log.
	lead := c.nodes[best.id]
	noopIdx := lead.lastIndex() + 1
	lead.log = append(lead.log, Entry{Index: noopIdx, Term: c.term, Op: Op{Noop: true}})

	c.resetLeaderStateLocked()
	c.broadcastAppendLocked()
	c.logLocked("FAILOVER %s -> %s (lost %d entries, reason: %s)", rep.OldLeader, rep.NewLeader, rep.LostEntries, rep.ChosenReason)
	return rep
}

// LogLength reports the number of entries a node holds (excluding the
// placeholder).
func (c *Cluster) LogLength(id string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n := c.nodes[id]; n != nil {
		return n.lastIndex()
	}
	return 0
}

// Dump returns a compact view of the cluster for test logs.
func (c *Cluster) Dump() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := fmt.Sprintf("term=%d leader=%s commit=%d | ", c.term, c.leader, c.nodes[c.leader].commit)
	for _, id := range c.ids {
		n := c.nodes[id]
		state := ""
		if c.dead[id] {
			state = "(dead)"
		}
		out += fmt.Sprintf("%s%s:log=%d/applied=%d ", id, state, n.lastIndex(), n.applied)
	}
	return out
}
