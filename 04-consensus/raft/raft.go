// Package raft is a from-scratch implementation of the consensus algorithm from
// DDIA §9 (and the Raft paper). Everything here exists to be *observed*:
//
//	Figure 2 safety      elections, terms, log matching, commit rules
//	§5.4.1               the election restriction (a candidate can only win with
//	                     an up-to-date log — this is what makes committed data safe)
//	§5.4.2               a leader may not commit entries from previous terms
//	                     directly; it must first commit one of its own
//	§7                   log compaction + InstallSnapshot
//	§6                   single-server membership changes
//
// The failure model it defends against is DDIA §8: crashes, network partitions,
// packet loss and unbounded delay. It explicitly does NOT defend against
// Byzantine faults (§8.3.3).
package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

// State is the role a node plays at a given moment.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "?"
}

// Entry is one slot of the replicated log.
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data"`
	// Type distinguishes client commands from cluster-configuration changes.
	Type string `json:"type,omitempty"` // "" = command, "config" = membership change
}

// ApplyMsg is a committed entry handed to the state machine, in log order.
// Type "snapshot" carries a whole state-machine image instead of one command:
// the node was too far behind to catch up entry by entry.
type ApplyMsg struct {
	Index    uint64
	Term     uint64
	Command  []byte
	Type     string
	Snapshot []byte
}

// Config configures a node.
type Config struct {
	ID                string
	Peers             []string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	Storage           Storage
	Seed              int64
}

func (c Config) withDefaults() Config {
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 150 * time.Millisecond
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 30 * time.Millisecond
	}
	if c.Storage == nil {
		c.Storage = NewMemStorage()
	}
	return c
}

// ------------------------------------------------------------------- iface --

type requestVote struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type requestVoteResp struct {
	Term        uint64
	VoteGranted bool
}

type appendEntries struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type appendEntriesResp struct {
	Term          uint64
	Success       bool
	MatchIndex    uint64
	ConflictIndex uint64
	ConflictTerm  uint64
}

type installSnapshot struct {
	Term              uint64
	LeaderID          string
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

type installSnapshotResp struct {
	Term        uint64
	MatchIndex  uint64
	CaughtUpNow bool
}

// Stats exposes counters that make the algorithm's behaviour visible in tests.
type Stats struct {
	ElectionsWon     int
	ElectionsStarted int
	VotesGranted     int
	AppendsSent      int
	AppendsRecv      int
	EntriesRejected  int
	SnapshotsSent    int
	SnapshotsRecv    int
	Truncations      int
}

// Node is one Raft peer.
type Node struct {
	cfg     Config
	net     *simnet.Network
	storage Storage

	mu       sync.Mutex
	state    State
	term     uint64
	votedFor string
	leaderID string

	snapIndex uint64
	snapTerm  uint64
	entries   []Entry // entries[0] has index snapIndex+1

	commitIndex uint64
	lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	electionDeadline time.Duration
	nextHeartbeat    time.Duration
	rng              *rand.Rand
	stopped          bool

	applyCh     chan ApplyMsg
	applySignal chan struct{}
	stopCh      chan struct{}
	closed      bool

	votesForTerm map[string]bool
	snapshotData []byte

	stats Stats
}

const tickInterval = 2 * time.Millisecond

// NewNode builds a node bound to a simulated network. Call Start to run it.
func NewNode(net *simnet.Network, cfg Config) *Node {
	cfg = cfg.withDefaults()
	n := &Node{
		cfg:          cfg,
		net:          net,
		storage:      cfg.Storage,
		state:        Follower,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		nextIndex:    map[string]uint64{},
		matchIndex:   map[string]uint64{},
		votesForTerm: map[string]bool{},
		applyCh:      make(chan ApplyMsg, 4096),
	}
	n.loadState()
	return n
}

// Start registers the message handler and the ticker, and starts the applier.
func (n *Node) Start() {
	// The applier gets its own copies of the channels: a restarted node must
	// never make the previous applier goroutine touch the new ones.
	stop := make(chan struct{})
	signal := make(chan struct{}, 1)

	n.mu.Lock()
	n.stopped = false
	n.closed = false
	n.stopCh = stop
	n.applySignal = signal
	n.resetElectionTimerLocked()
	n.mu.Unlock()

	n.net.Register(n.cfg.ID, n.dispatch)
	go n.applyLoop(stop, signal)
	n.scheduleTick()
}

// Stop halts the node without losing its durable state (a restart).
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	if !n.closed {
		n.closed = true
		close(n.stopCh)
	}
	n.mu.Unlock()
	n.net.Unregister(n.cfg.ID)
}

// Crash stops the node and unregisters it from the network.
func (n *Node) Crash() { n.Stop() }

// Restart models a process restart: durable state is reloaded from storage and
// the applied index is reset to the snapshot, so the in-memory state machine is
// rebuilt by replaying the log. Term and vote MUST come back, or the node could
// vote twice in the same term.
func (n *Node) Restart() {
	n.mu.Lock()
	n.loadState()
	n.mu.Unlock()
	n.Start()
}

// ApplyCh is the ordered stream of committed entries.
func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }

// Self reports the node id.
func (n *Node) ID() string { return n.cfg.ID }

// Status is a snapshot of the node's role, for assertions and diagnostics.
type Status struct {
	ID          string
	State       State
	Term        uint64
	LeaderID    string
	LastIndex   uint64
	LastTerm    uint64
	CommitIndex uint64
	LastApplied uint64
	SnapIndex   uint64
	LogLen      int
	Peers       []string
	Stats       Stats
}

// Status returns the current view of the node.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.cfg.ID,
		State:       n.state,
		Term:        n.term,
		LeaderID:    n.leaderID,
		LastIndex:   n.lastIndexLocked(),
		LastTerm:    n.lastTermLocked(),
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		SnapIndex:   n.snapIndex,
		LogLen:      len(n.entries),
		Peers:       append([]string(nil), n.cfg.Peers...),
		Stats:       n.stats,
	}
}

// ---------------------------------------------------------------- dispatch --

func (n *Node) dispatch(m simnet.Message) {
	switch b := m.Body.(type) {
	case requestVote:
		n.handleRequestVote(m.From, b)
	case requestVoteResp:
		n.handleRequestVoteResp(m.From, b)
	case appendEntries:
		n.handleAppendEntries(m.From, b)
	case appendEntriesResp:
		n.handleAppendEntriesResp(m.From, b)
	case installSnapshot:
		n.handleInstallSnapshot(m.From, b)
	case installSnapshotResp:
		n.handleInstallSnapshotResp(m.From, b)
	}
}

func (n *Node) scheduleTick() {
	var tick func()
	tick = func() {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return
		}
		now := n.net.Now()
		if n.state == Leader {
			if now >= n.nextHeartbeat {
				n.nextHeartbeat = now + n.cfg.HeartbeatInterval
				n.broadcastAppendLocked()
			}
		} else if now >= n.electionDeadline {
			n.becomeCandidateLocked()
		}
		n.mu.Unlock()
		n.scheduleTick()
	}
	n.net.After(tickInterval, tick)
}

// ---------------------------------------------------------------- elections --

func (n *Node) resetElectionTimerLocked() {
	// Randomised timeouts are what break symmetry: without jitter, every
	// follower times out together and split votes forever.
	jitter := time.Duration(n.rng.Int63n(int64(n.cfg.ElectionTimeout)))
	n.electionDeadline = n.net.Now() + n.cfg.ElectionTimeout + jitter
}

func (n *Node) becomeCandidateLocked() {
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.persistLocked()
	n.resetElectionTimerLocked()
	n.stats.ElectionsStarted++

	n.votesForTerm = map[string]bool{n.cfg.ID: true}

	lastIdx, lastTerm := n.lastIndexLocked(), n.lastTermLocked()
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.net.Send(n.cfg.ID, p, requestVote{
			Term:         n.term,
			CandidateID:  n.cfg.ID,
			LastLogIndex: lastIdx,
			LastLogTerm:  lastTerm,
		})
	}
	// A single-node cluster wins immediately.
	if len(n.votesForTerm) >= n.quorumLocked() {
		n.becomeLeaderLocked()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state = Leader
	n.leaderID = n.cfg.ID
	n.stats.ElectionsWon++
	last := n.lastIndexLocked()
	for _, p := range n.cfg.Peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	// A new leader appends a no-op in its own term. Without it, entries carried
	// over from previous terms could never be committed (rule §5.4.2).
	n.appendLocked(Entry{Index: last + 1, Term: n.term, Data: nil, Type: "noop"})
	n.matchIndex[n.cfg.ID] = n.lastIndexLocked()
	n.persistLocked()
	// A single-node cluster is its own quorum and commits right here.
	n.advanceCommitLocked()
	n.broadcastAppendLocked()
}

func (n *Node) handleRequestVote(from string, msg requestVote) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, "")
	}
	resp := requestVoteResp{Term: n.term}
	if msg.Term < n.term {
		n.sendLocked(from, resp)
		return
	}
	// One vote per term.
	if n.votedFor != "" && n.votedFor != msg.CandidateID {
		n.sendLocked(from, resp)
		return
	}
	// Election restriction (§5.4.1): a candidate whose log is behind ours cannot
	// win. This is what guarantees a leader always holds every committed entry.
	lastIdx, lastTerm := n.lastIndexLocked(), n.lastTermLocked()
	upToDate := msg.LastLogTerm > lastTerm ||
		(msg.LastLogTerm == lastTerm && msg.LastLogIndex >= lastIdx)
	if !upToDate {
		n.sendLocked(from, resp)
		return
	}
	n.votedFor = msg.CandidateID
	n.persistLocked()
	n.resetElectionTimerLocked() // granting a vote means we heard from a candidate
	n.stats.VotesGranted++
	resp.VoteGranted = true
	n.sendLocked(from, resp)
}

func (n *Node) handleRequestVoteResp(from string, msg requestVoteResp) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, "")
		return
	}
	if n.state != Candidate || msg.Term != n.term || !msg.VoteGranted {
		return
	}
	n.votesForTerm[from] = true

	votes := 0
	for p := range n.votesForTerm {
		if contains(n.cfg.Peers, p) {
			votes++
		}
	}
	if votes >= n.quorumLocked() {
		n.becomeLeaderLocked()
	}
}

// -------------------------------------------------------------- replication --

// Propose appends a client command (leader only).
func (n *Node) Propose(data []byte) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, fmt.Errorf("not the leader (leader is %q)", n.leaderID)
	}
	idx := n.lastIndexLocked() + 1
	n.appendLocked(Entry{Index: idx, Term: n.term, Data: data})
	n.matchIndex[n.cfg.ID] = idx
	n.persistLocked()
	n.advanceCommitLocked()
	n.broadcastAppendLocked()
	return idx, nil
}

func (n *Node) broadcastAppendLocked() {
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.replicateToLocked(p)
	}
}

func (n *Node) replicateToLocked(peer string) {
	next := n.nextIndex[peer]
	if next == 0 {
		next = n.lastIndexLocked() + 1
	}
	// Too far behind to catch up with entries alone: ship a snapshot.
	if next <= n.snapIndex {
		n.sendSnapshotLocked(peer)
		return
	}
	prevIndex := next - 1
	prevTerm, _ := n.termAtLocked(prevIndex)
	entries := n.entriesFromLocked(next, 128)
	n.stats.AppendsSent++
	n.sendLocked(peer, appendEntries{
		Term:         n.term,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) sendSnapshotLocked(peer string) {
	n.stats.SnapshotsSent++
	n.sendLocked(peer, installSnapshot{
		Term:              n.term,
		LeaderID:          n.cfg.ID,
		LastIncludedIndex: n.snapIndex,
		LastIncludedTerm:  n.snapTerm,
		Data:              n.snapshotData,
	})
}

func (n *Node) handleAppendEntries(from string, msg appendEntries) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats.AppendsRecv++

	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, msg.LeaderID)
	}
	resp := appendEntriesResp{Term: n.term}
	if msg.Term < n.term {
		n.sendLocked(from, resp)
		return
	}
	// Any message from a current leader resets the election timer: this is how
	// a healthy leader keeps followers from starting needless elections.
	n.state = Follower
	n.leaderID = msg.LeaderID
	n.resetElectionTimerLocked()

	// Log matching property: we may only append if the entry before the new
	// ones agrees on term.
	if msg.PrevLogIndex > 0 {
		t, ok := n.termAtLocked(msg.PrevLogIndex)
		if !ok || t != msg.PrevLogTerm {
			n.stats.EntriesRejected++
			resp.Success = false
			resp.ConflictIndex = n.lastIndexLocked() + 1
			if ok && t != msg.PrevLogTerm {
				resp.ConflictTerm = t
				// Back up to the first index of the conflicting term so the
				// leader can skip the whole term in one round trip.
				i := msg.PrevLogIndex
				for i > n.snapIndex+1 {
					pt, ok := n.termAtLocked(i - 1)
					if !ok || pt != t {
						break
					}
					i--
				}
				resp.ConflictIndex = i
			}
			n.sendLocked(from, resp)
			return
		}
	}

	for i, e := range msg.Entries {
		idx := msg.PrevLogIndex + uint64(i) + 1
		existing, ok := n.entryAtLocked(idx)
		if ok {
			if existing.Term != e.Term {
				// Conflict: M and N differ at this point, so delete everything
				// from here on and take the leader's version.
				n.truncateFromLocked(idx)
				n.stats.Truncations++
				n.appendLocked(msg.Entries[i:]...)
				break
			}
			continue // identical, already have it
		}
		n.appendLocked(msg.Entries[i:]...)
		break
	}
	n.persistLocked()

	resp.Success = true
	resp.MatchIndex = msg.PrevLogIndex + uint64(len(msg.Entries))

	// Commit only what the leader says is committed, and never below what we
	// already applied.
	if msg.LeaderCommit > n.commitIndex {
		n.commitIndex = min64(msg.LeaderCommit, n.lastIndexLocked())
		n.signalApplyLocked()
	}
	n.sendLocked(from, resp)
}

func (n *Node) handleAppendEntriesResp(from string, msg appendEntriesResp) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, "")
		return
	}
	if n.state != Leader || msg.Term != n.term {
		return
	}
	if msg.Success {
		if msg.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = msg.MatchIndex
		}
		n.nextIndex[from] = n.matchIndex[from] + 1
		n.advanceCommitLocked()
		// Push more if this follower is still behind.
		if n.nextIndex[from] <= n.lastIndexLocked() {
			n.replicateToLocked(from)
		}
		return
	}
	// Fast backup using the follower's conflict hint.
	next := msg.ConflictIndex
	if msg.ConflictTerm > 0 {
		// If we have entries of that term, resume just after the last one.
		for i := n.lastIndexLocked(); i > n.snapIndex; i-- {
			t, _ := n.termAtLocked(i)
			if t == msg.ConflictTerm {
				next = i + 1
				break
			}
			if t < msg.ConflictTerm {
				break
			}
		}
	}
	if next < 1 {
		next = 1
	}
	// Deliberately do NOT clamp `next` up to snapIndex+1: if the follower needs
	// an index we already compacted, the only way to help it is a snapshot, and
	// clamping would make the leader resend an unusable append forever.
	n.nextIndex[from] = next
	n.replicateToLocked(from)
}

func (n *Node) advanceCommitLocked() {
	matches := make([]uint64, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			matches = append(matches, n.lastIndexLocked())
			continue
		}
		matches = append(matches, n.matchIndex[p])
	}
	sortDesc(matches)
	if len(matches) == 0 {
		return
	}
	quorum := len(matches)/2 + 1
	candidate := matches[quorum-1]
	if candidate <= n.commitIndex {
		return
	}
	// §5.4.2: count only entries from the current term. Committing an older
	// term's entry by counting replicas is unsafe.
	if t, ok := n.termAtLocked(candidate); !ok || t != n.term {
		return
	}
	n.commitIndex = candidate
	n.signalApplyLocked()
}

func (n *Node) persistLocked() {
	_ = n.storage.Save(PersistedState{
		Hard: HardState{
			Term:      n.term,
			VotedFor:  n.votedFor,
			SnapIndex: n.snapIndex,
			SnapTerm:  n.snapTerm,
			Snapshot:  n.snapshotData,
		},
		Entries: n.entries,
	})
}

func (n *Node) loadState() {
	st, err := n.storage.Load()
	if err != nil {
		return
	}
	n.term = st.Hard.Term
	n.votedFor = st.Hard.VotedFor
	n.snapIndex = st.Hard.SnapIndex
	n.snapTerm = st.Hard.SnapTerm
	n.snapshotData = st.Hard.Snapshot
	n.entries = st.Entries
	n.commitIndex = n.snapIndex
	n.lastApplied = n.snapIndex
}

func (n *Node) stepDownLocked(newTerm uint64, leaderID string) {
	n.state = Follower
	n.term = newTerm
	n.votedFor = ""
	n.leaderID = leaderID
	n.votesForTerm = map[string]bool{}
	n.persistLocked()
	n.resetElectionTimerLocked()
}

func (n *Node) sendLocked(to string, body any) {
	if n.stopped {
		return
	}
	n.net.Send(n.cfg.ID, to, body)
}

// ------------------------------------------------------------------ snapshot --

// SnapshotData returns the state-machine image the log was compacted from, so
// a restarted node can rebuild its state machine before replaying new entries.
func (n *Node) SnapshotData() []byte {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]byte(nil), n.snapshotData...)
}

// Snapshot compacts the log up to the last applied index and hands the state
// machine's snapshot to the log. Without this, the log grows without bound and
// a restarted node replays history forever.
func (n *Node) Snapshot(data []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	idx := n.lastApplied
	if idx <= n.snapIndex {
		return fmt.Errorf("nothing to compact (applied=%d snap=%d)", idx, n.snapIndex)
	}
	term, ok := n.termAtLocked(idx)
	if !ok {
		return fmt.Errorf("cannot snapshot index %d: not in log", idx)
	}
	n.snapIndex, n.snapTerm, n.snapshotData = idx, term, data
	if idx > n.snapIndex && idx <= n.lastIndexLocked() {
		n.entries = n.entries[idx-n.snapIndex:]
	} else {
		n.entries = nil
	}
	n.persistLocked()
	return nil
}

func (n *Node) handleInstallSnapshot(from string, msg installSnapshot) {
	var deliver *ApplyMsg

	n.mu.Lock()
	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, msg.LeaderID)
	}
	if msg.Term < n.term {
		term := n.term
		n.mu.Unlock()
		n.net.Send(n.cfg.ID, from, installSnapshotResp{Term: term})
		return
	}
	n.state = Follower
	n.leaderID = msg.LeaderID
	n.resetElectionTimerLocked()
	n.stats.SnapshotsRecv++

	if msg.LastIncludedIndex > n.lastIndexLocked() {
		n.snapIndex, n.snapTerm, n.snapshotData = msg.LastIncludedIndex, msg.LastIncludedTerm, msg.Data
		n.entries = nil
		if n.commitIndex < msg.LastIncludedIndex {
			n.commitIndex = msg.LastIncludedIndex
		}
		if n.lastApplied < msg.LastIncludedIndex {
			n.lastApplied = msg.LastIncludedIndex
			// The state machine must be replaced wholesale, not fast-forwarded.
			m := ApplyMsg{Index: msg.LastIncludedIndex, Term: msg.LastIncludedTerm, Type: "snapshot", Snapshot: msg.Data}
			deliver = &m
		}
		n.persistLocked()
	}
	resp := installSnapshotResp{Term: n.term, MatchIndex: n.snapIndex}
	n.mu.Unlock()

	if deliver != nil {
		n.applyCh <- *deliver
	}
	n.net.Send(n.cfg.ID, from, resp)
}

func (n *Node) handleInstallSnapshotResp(from string, msg installSnapshotResp) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if msg.Term > n.term {
		n.stepDownLocked(msg.Term, "")
		return
	}
	if n.state != Leader || msg.Term != n.term {
		return
	}
	n.matchIndex[from] = msg.MatchIndex
	n.nextIndex[from] = msg.MatchIndex + 1
	n.replicateToLocked(from)
}

// ------------------------------------------------------------------ members --

// AddPeer performs a single-server membership change: one node at a time, so
// the old and new quorums always overlap. Joining several at once can produce
// two disjoint majorities and split the cluster.
func (n *Node) AddPeer(id string) (uint64, error) {
	return n.changeConfig(func(peers []string) []string {
		return append(peers, id)
	})
}

// RemovePeer removes a node from the configuration.
func (n *Node) RemovePeer(id string) (uint64, error) {
	return n.changeConfig(func(peers []string) []string {
		out := peers[:0:0]
		for _, p := range peers {
			if p != id {
				out = append(out, p)
			}
		}
		return out
	})
}

func (n *Node) changeConfig(f func([]string) []string) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return 0, fmt.Errorf("not the leader")
	}
	newPeers := f(append([]string(nil), n.cfg.Peers...))
	enc, err := encodeConfig(newPeers)
	if err != nil {
		return 0, err
	}
	idx := n.lastIndexLocked() + 1
	n.appendLocked(Entry{Index: idx, Term: n.term, Data: enc, Type: "config"})
	n.matchIndex[n.cfg.ID] = idx
	n.persistLocked()
	n.advanceCommitLocked()
	n.broadcastAppendLocked()
	return idx, nil
}

// applyConfigLocked installs a committed configuration change.
func (n *Node) applyConfigLocked(peers []string) {
	n.cfg.Peers = peers
	known := map[string]bool{}
	for _, p := range peers {
		known[p] = true
		if _, ok := n.nextIndex[p]; !ok {
			n.nextIndex[p] = n.lastIndexLocked() + 1
		}
		if _, ok := n.matchIndex[p]; !ok {
			n.matchIndex[p] = 0
		}
	}
	for p := range n.nextIndex {
		if !known[p] {
			delete(n.nextIndex, p)
			delete(n.matchIndex, p)
		}
	}
}

// --------------------------------------------------------------- apply loop --

func (n *Node) signalApplyLocked() {
	select {
	case n.applySignal <- struct{}{}:
	default:
	}
}

// applyLoop hands committed entries to the state machine in log order. It is
// the only goroutine besides the simulator, which is why the entry-to-command
// translation happens under the node lock.
func (n *Node) applyLoop(stop <-chan struct{}, signal <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-signal:
		}
		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			var msgs []ApplyMsg
			for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
				e, ok := n.entryAtLocked(i)
				if !ok {
					break
				}
				if e.Type == "config" {
					if peers, err := decodeConfig(e.Data); err == nil {
						n.applyConfigLocked(peers)
					}
				}
				msgs = append(msgs, ApplyMsg{Index: i, Term: e.Term, Command: e.Data, Type: e.Type})
			}
			if len(msgs) > 0 {
				n.lastApplied = msgs[len(msgs)-1].Index
			}
			n.mu.Unlock()

			for _, m := range msgs {
				n.applyCh <- m
			}
		}
	}
}

// -------------------------------------------------------------------- state --

// WaitApplied reports whether the node has applied everything up to index.
func (n *Node) WaitApplied(index uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied >= index
}

// ------------------------------------------------------------- log helpers --

func (n *Node) lastIndexLocked() uint64 { return n.snapIndex + uint64(len(n.entries)) }

func (n *Node) lastTermLocked() uint64 {
	if len(n.entries) == 0 {
		return n.snapTerm
	}
	return n.entries[len(n.entries)-1].Term
}

func (n *Node) entryAtLocked(i uint64) (Entry, bool) {
	if i <= n.snapIndex || i > n.lastIndexLocked() {
		return Entry{}, false
	}
	return n.entries[i-n.snapIndex-1], true
}

func (n *Node) termAtLocked(i uint64) (uint64, bool) {
	if i == 0 {
		return 0, true
	}
	if i == n.snapIndex {
		return n.snapTerm, true
	}
	if i < n.snapIndex {
		return 0, false // compacted away: we cannot answer for it
	}
	e, ok := n.entryAtLocked(i)
	if !ok {
		return 0, false
	}
	return e.Term, true
}

func (n *Node) entriesFromLocked(i uint64, maxCount int) []Entry {
	if i <= n.snapIndex {
		i = n.snapIndex + 1
	}
	if i > n.lastIndexLocked() {
		return nil
	}
	start := i - n.snapIndex - 1
	end := start + uint64(maxCount)
	if end > uint64(len(n.entries)) {
		end = uint64(len(n.entries))
	}
	return append([]Entry(nil), n.entries[start:end]...)
}

func (n *Node) truncateFromLocked(i uint64) {
	if i <= n.snapIndex {
		return
	}
	n.entries = n.entries[:i-n.snapIndex-1]
}

func (n *Node) appendLocked(es ...Entry) {
	n.entries = append(n.entries, es...)
}

func (n *Node) quorumLocked() int { return len(n.cfg.Peers)/2 + 1 }

// LogTerms returns the (index, term) pairs of the log, for test assertions.
func (n *Node) LogTerms() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.entries))
	for _, e := range n.entries {
		out = append(out, fmt.Sprintf("%d/%d", e.Index, e.Term))
	}
	if n.snapIndex > 0 {
		out = append([]string{fmt.Sprintf("snap@%d/%d", n.snapIndex, n.snapTerm)}, out...)
	}
	return out
}

// Dump renders the node for failure messages.
func (n *Node) Dump() string {
	s := n.Status()
	return fmt.Sprintf("%s: %v term=%d log=%d commit=%d applied=%d snap=%d peers=%v leader=%q",
		s.ID, s.State, s.Term, s.LastIndex, s.CommitIndex, s.LastApplied, s.SnapIndex, s.Peers, s.LeaderID)
}

// ------------------------------------------------------------------ helpers --

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func sortDesc(xs []uint64) {
	// The slices are tiny (cluster size), so insertion sort is fine and keeps
	// hot paths allocation-free.
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j] > xs[j-1]; j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}
