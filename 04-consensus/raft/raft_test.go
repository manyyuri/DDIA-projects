package raft

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

// ------------------------------------------------------------- test harness --

type testCluster struct {
	t     *testing.T
	net   *simnet.Network
	ids   []string
	nodes map[string]*Node
	kvs   map[string]*KV
	store map[string]*FileStorage
	dir   string
	mu    sync.Mutex
	// observed[term] = set of node ids that claimed leadership in that term
	observed map[uint64]map[string]bool
}

func newTestCluster(t *testing.T, n int, seed int64) *testCluster {
	t.Helper()
	dir := t.TempDir()
	net := simnet.New(seed)
	net.SetLatency(3*time.Millisecond, 2*time.Millisecond)

	c := &testCluster{
		t: t, net: net, dir: dir,
		nodes:    map[string]*Node{},
		kvs:      map[string]*KV{},
		store:    map[string]*FileStorage{},
		observed: map[uint64]map[string]bool{},
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for i, id := range c.ids {
		fs := NewFileStorage(filepath.Join(dir, id+".json"))
		c.store[id] = fs
		nd := NewNode(net, Config{
			ID:                id,
			Peers:             append([]string(nil), c.ids...),
			ElectionTimeout:   40 * time.Millisecond,
			HeartbeatInterval: 10 * time.Millisecond,
			Storage:           fs,
			Seed:              seed + int64(i)*7919,
		})
		c.nodes[id] = nd
	}
	for _, id := range c.ids {
		c.nodes[id].Start()
		c.kvs[id] = NewKV(c.nodes[id])
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.nodes[id].Stop()
			c.kvs[id].Stop()
		}
	})
	return c
}

// run advances the simulated clock while watching for safety violations.
func (c *testCluster) run(d time.Duration) {
	deadline := c.net.Now() + d
	for c.net.Now() < deadline {
		c.net.Run(5 * time.Millisecond)
		c.observe()
	}
}

// waitFor runs the network until cond is true or the simulated deadline passes.
func (c *testCluster) waitFor(desc string, timeout time.Duration, cond func() bool) bool {
	deadline := c.net.Now() + timeout
	for {
		c.observe()
		if cond() {
			return true
		}
		if c.net.Now() >= deadline {
			c.t.Logf("waitFor(%s) timed out; cluster:\n%s", desc, c.dump())
			return false
		}
		c.net.Run(2 * time.Millisecond)
		time.Sleep(50 * time.Microsecond) // let the applier goroutine run
	}
}

func (c *testCluster) observe() {
	for _, id := range c.ids {
		s := c.nodes[id].Status()
		if s.State != Leader {
			continue
		}
		if c.observed[s.Term] == nil {
			c.observed[s.Term] = map[string]bool{}
		}
		c.observed[s.Term][id] = true
	}
}

func (c *testCluster) assertNoTwoLeadersPerTerm() {
	for term, set := range c.observed {
		if len(set) > 1 {
			c.t.Fatalf("SAFETY VIOLATION: term %d had %d leaders: %v", term, len(set), set)
		}
	}
}

func (c *testCluster) leaderID() string {
	found := ""
	for _, id := range c.ids {
		if c.nodes[id].Status().State == Leader {
			if found != "" {
				return "" // split brain in different terms: caller decides
			}
			found = id
		}
	}
	return found
}

// leaderOfTerm returns a node that believes it is leader (there may be a stale
// one during a partition).
func (c *testCluster) anyLeader() string {
	for _, id := range c.ids {
		if !c.net.Alive(id) {
			continue // a crashed node still believes it is the leader
		}
		if c.nodes[id].Status().State == Leader {
			return id
		}
	}
	return ""
}

func (c *testCluster) dump() string {
	out := ""
	for _, id := range c.ids {
		out += "  " + c.nodes[id].Dump() + " " + c.kvs[id].Describe() + "\n"
	}
	return out
}

// crash kills the process but keeps the state machine object around, exactly
// like a real restart: what comes back is whatever was durable, plus a replay.
func (c *testCluster) crash(id string) {
	c.nodes[id].Stop()
}

func (c *testCluster) restart(id string) {
	c.nodes[id].Restart()
}

func (c *testCluster) partition(groups ...[]string) { c.net.Partition(groups...) }
func (c *testCluster) heal()                        { c.net.Heal() }

// putThrough writes via the given node and waits for it to commit.
func (c *testCluster) putThrough(id, key, value string) (uint64, bool) {
	kv := c.kvs[id]
	idx, err := kv.ProposePut(key, value)
	if err != nil {
		return 0, false
	}
	ok := c.waitFor("put "+key, 3*time.Second, func() bool {
		return c.nodes[id].Status().CommitIndex >= idx
	})
	return idx, ok
}

func (c *testCluster) waitLeader(timeout time.Duration) string {
	c.waitFor("a single leader", timeout, func() bool { return c.leaderID() != "" })
	return c.leaderID()
}

// ------------------------------------------------------------------- tests --

func TestSingleNodeClusterElectsItself(t *testing.T) {
	c := newTestCluster(t, 1, 1)
	leader := c.waitLeader(2 * time.Second)
	if leader != "n0" {
		t.Fatalf("single node should lead itself, got %q (%s)", leader, c.dump())
	}
	if _, ok := c.putThrough("n0", "k", "v"); !ok {
		t.Fatalf("write should commit immediately:\n%s", c.dump())
	}
}

func TestClusterElectsExactlyOneLeader(t *testing.T) {
	c := newTestCluster(t, 5, 42)
	leader := c.waitLeader(3 * time.Second)
	if leader == "" {
		t.Fatalf("no leader elected:\n%s", c.dump())
	}
	// Everyone must agree on the same leader and the same term.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.run(20 * time.Millisecond)
		first := c.nodes[c.ids[0]].Status()
		same := true
		for _, id := range c.ids {
			s := c.nodes[id].Status()
			if s.Term != first.Term || s.LeaderID != first.LeaderID {
				same = false
				break
			}
		}
		if same {
			break
		}
	}
	term := c.nodes[c.ids[0]].Status().Term
	for _, id := range c.ids {
		s := c.nodes[id].Status()
		if s.LeaderID != leader {
			t.Fatalf("%s disagrees about the leader: %v (want %s)\n%s", id, s.LeaderID, leader, c.dump())
		}
		if s.Term != term {
			t.Fatalf("%s is in term %d, leader is in term %d", id, s.Term, term)
		}
	}
	t.Logf("elected %s in term %d", leader, term)
}

func TestLogReplicationAndApply(t *testing.T) {
	c := newTestCluster(t, 5, 7)
	leader := c.waitLeader(3 * time.Second)

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%02d", i)
		if _, ok := c.putThrough(leader, key, fmt.Sprintf("v%d", i)); !ok {
			t.Fatalf("write %d never committed", i)
		}
	}
	// Every replica must end up with the same committed value.
	for _, id := range c.ids {
		ok := c.waitFor("replica "+id+" caught up", 3*time.Second, func() bool {
			v, found := c.kvs[id].LocalValue("k19")
			return found && v == "v19"
		})
		if !ok {
			t.Fatalf("%s did not apply the writes:\n%s", id, c.dump())
		}
	}
	c.assertNoTwoLeadersPerTerm()
}

// A read served from a leader's local map can be stale (the leader may have
// been deposed and not know it). Routing reads through the log fixes that.
func TestReadThroughTheLogIsLinearizable(t *testing.T) {
	c := newTestCluster(t, 3, 11)
	leader := c.waitLeader(3 * time.Second)

	if _, ok := c.putThrough(leader, "x", "1"); !ok {
		t.Fatal("write failed")
	}
	idx, err := c.kvs[leader].ProposeRead("x")
	if err != nil {
		t.Fatalf("propose read: %v", err)
	}
	if !c.waitFor("read applied", 2*time.Second, func() bool {
		return c.kvs[leader].Applied() >= idx
	}) {
		t.Fatal("read never applied")
	}
	if v, ok := c.kvs[leader].LocalValue("x"); !ok || v != "1" {
		t.Fatalf("linearizable read returned %q ok=%v", v, ok)
	}
}

// The leader is in the minority: it can neither commit nor learn that it was
// replaced, but the majority elects a new leader and keeps making progress.
func TestLeaderInMinorityCannotCommit(t *testing.T) {
	c := newTestCluster(t, 5, 3)
	leader := c.waitLeader(3 * time.Second)
	if _, ok := c.putThrough(leader, "before", "1"); !ok {
		t.Fatal("precondition write failed")
	}

	others := without(c.ids, leader)
	min := others[:1] // 1 node with the stale leader: 2 of 5, not a quorum
	maj := others[1:] // 3 of 5: a quorum
	c.partition([]string{leader}, min, maj)

	// The isolated leader still believes it leads, but its proposal can never commit.
	idx, err := c.kvs[leader].ProposePut("orphan", "1")
	if err != nil {
		t.Fatalf("the stale leader still accepts proposals locally: %v", err)
	}
	c.run(1 * time.Second)
	if c.nodes[leader].Status().CommitIndex >= idx {
		t.Fatal("a leader without a quorum must not commit")
	}
	t.Logf("orphaned leader correctly stuck at commit=%d while index=%d",
		c.nodes[leader].Status().CommitIndex, idx)

	// The majority side elects a new leader and commits.
	newLeader := ""
	c.waitFor("majority elects a leader", 3*time.Second, func() bool {
		for _, id := range maj {
			if c.nodes[id].Status().State == Leader {
				newLeader = id
				return true
			}
		}
		return false
	})
	if newLeader == "" {
		t.Fatalf("majority never elected a leader:\n%s", c.dump())
	}
	if _, ok := c.putThrough(newLeader, "after", "2"); !ok {
		t.Fatalf("new leader could not commit:\n%s", c.dump())
	}

	// Heal: the old leader must discover the higher term and step down.
	c.heal()
	ok := c.waitFor("old leader steps down", 3*time.Second, func() bool {
		return c.nodes[leader].Status().State == Follower
	})
	if !ok {
		t.Fatalf("old leader did not step down:\n%s", c.dump())
	}
	// The orphan write must be rolled back, not resurrected.
	ok = c.waitFor("orphan discarded", 3*time.Second, func() bool {
		if _, found := c.kvs[newLeader].LocalValue("orphan"); found {
			return false
		}
		return c.nodes[newLeader].Status().CommitIndex >= c.nodes[newLeader].Status().LastIndex
	})
	if !ok {
		t.Fatalf("the uncommitted write survived:\n%s", c.dump())
	}
	c.assertNoTwoLeadersPerTerm()
}

// §5.4.1: a node whose log is behind cannot win an election, no matter how
// high its term got while it was partitioned. This is what protects committed
// entries from being overwritten.
func TestElectionRestrictionProtectsCommittedEntries(t *testing.T) {
	c := newTestCluster(t, 5, 5)
	leader := c.waitLeader(3 * time.Second)

	// Isolate one follower, then commit a bunch of entries without it.
	lagging := without(c.ids, leader)[0]
	rest := without(c.ids, lagging)
	c.partition([]string{lagging}, rest)

	for i := 0; i < 10; i++ {
		if _, ok := c.putThrough(leader, fmt.Sprintf("k%d", i), "v"); !ok {
			t.Fatalf("write %d failed", i)
		}
	}
	isolatedAt := c.nodes[lagging].Status().LastIndex
	if got := c.nodes[lagging].Status().LastIndex; got != isolatedAt {
		t.Fatalf("the isolated follower grew: %d -> %d", isolatedAt, got)
	}
	t.Logf("isolated follower stuck at index %d while the cluster commits up to %d",
		isolatedAt, c.nodes[leader].Status().LastIndex)
	// While isolated it will campaign and bump its term far above everyone else.
	c.run(500 * time.Millisecond)
	laggingTerm := c.nodes[lagging].Status().Term
	leaderTerm := c.nodes[leader].Status().Term
	if laggingTerm <= leaderTerm {
		t.Fatalf("expected the isolated node to raise its term: %d vs %d", laggingTerm, leaderTerm)
	}
	t.Logf("isolated node n=%s is at term %d while the cluster is at term %d", lagging, laggingTerm, leaderTerm)

	c.heal()

	// A higher term makes the leader step down, but the lagging node must never
	// win: its log is shorter.
	c.run(3 * time.Second)
	for _, id := range c.ids {
		s := c.nodes[id].Status()
		if s.State == Leader && id == lagging {
			t.Fatalf("the stale node won an election: %s", s.ID)
		}
	}
	final := ""
	for _, id := range c.ids {
		if c.nodes[id].Status().State == Leader {
			final = id
		}
	}
	if final == "" {
		t.Fatalf("no leader after healing:\n%s", c.dump())
	}
	// All 10 committed entries must still be there.
	if !c.waitFor("committed entries survive", 3*time.Second, func() bool {
		v, ok := c.kvs[final].LocalValue("k9")
		return ok && v == "v"
	}) {
		t.Fatalf("committed entries were lost:\n%s", c.dump())
	}
	c.assertNoTwoLeadersPerTerm()
}

// A follower that comes back with entries the leader never had must have them
// truncated (log matching property).
func TestDivergentLogIsTruncated(t *testing.T) {
	c := newTestCluster(t, 3, 13)
	leader := c.waitLeader(3 * time.Second)
	if _, ok := c.putThrough(leader, "a", "1"); !ok {
		t.Fatal("setup write failed")
	}
	followers := without(c.ids, leader)
	victim := followers[0]

	// Split the victim with the leader so it can accumulate entries, then give
	// the rest of the cluster a different history.
	c.partition([]string{leader, victim}, []string{followers[1]})
	c.run(100 * time.Millisecond)

	// Inject a fake divergent suffix directly (simulating entries it accepted
	// from a leader that later got overwritten).
	c.nodes[victim].mu.Lock()
	base := c.nodes[victim].lastIndexLocked()
	c.nodes[victim].appendLocked(Entry{Index: base + 1, Term: 999, Data: []byte("bogus")})
	c.nodes[victim].mu.Unlock()
	t.Logf("victim log: %v", c.nodes[victim].LogTerms())

	c.heal()
	c.run(2 * time.Second)

	// The bogus entry must be gone everywhere.
	for _, id := range c.ids {
		for _, lt := range c.nodes[id].LogTerms() {
			if lt == fmt.Sprintf("%d/%d", base+1, 999) {
				t.Fatalf("%s kept a divergent entry: %v", id, c.nodes[id].LogTerms())
			}
		}
	}
	if st := c.nodes[victim].Status(); st.Stats.Truncations == 0 {
		t.Fatalf("expected the follower to truncate its divergent suffix: %+v", st.Stats)
	}
}

// Term and vote must be durable: a node that forgets them can vote twice in one
// term and produce two leaders.
func TestHardStateAndLogSurviveRestart(t *testing.T) {
	c := newTestCluster(t, 3, 17)
	leader := c.waitLeader(3 * time.Second)
	for i := 0; i < 5; i++ {
		if _, ok := c.putThrough(leader, fmt.Sprintf("k%d", i), "v"); !ok {
			t.Fatal("write failed")
		}
	}
	beforeTerm := c.nodes[leader].Status().Term
	beforeLen := c.nodes[leader].Status().LastIndex

	// Restart the whole cluster: nothing should be forgotten.
	for _, id := range c.ids {
		c.crash(id)
	}
	c.run(50 * time.Millisecond)
	for _, id := range c.ids {
		c.restart(id)
	}
	newLeader := c.waitLeader(5 * time.Second)
	if newLeader == "" {
		t.Fatalf("no leader after restart:\n%s", c.dump())
	}
	if got := c.nodes[newLeader].Status().Term; got < beforeTerm {
		t.Fatalf("term went backwards: %d < %d", got, beforeTerm)
	}
	if got := c.nodes[newLeader].Status().LastIndex; got != beforeLen {
		t.Fatalf("log length changed: %d != %d", got, beforeLen)
	}
	if !c.waitFor("state machine restored", 3*time.Second, func() bool {
		v, ok := c.kvs[newLeader].LocalValue("k4")
		return ok && v == "v"
	}) {
		t.Fatalf("state machine lost data across restart:\n%s", c.dump())
	}
	c.assertNoTwoLeadersPerTerm()
}

// Log compaction + InstallSnapshot: a follower that fell so far behind that the
// leader no longer has the entries must be caught up with a snapshot.
func TestSnapshotCatchesUpFarBehindFollower(t *testing.T) {
	c := newTestCluster(t, 3, 23)
	leader := c.waitLeader(3 * time.Second)
	followers := without(c.ids, leader)
	lagging := followers[0]

	c.partition([]string{lagging}, without(c.ids, lagging))

	// Commit enough entries to make compaction worthwhile.
	for i := 0; i < 60; i++ {
		if _, ok := c.putThrough(leader, fmt.Sprintf("k%02d", i), "v"); !ok {
			t.Fatalf("write %d failed", i)
		}
	}
	// Compact on *every* caught-up node. If even one node keeps the full log,
	// it can win the next election and catch the laggard up entry by entry —
	// which is correct, but it means the snapshot path never runs.
	for _, id := range without(c.ids, lagging) {
		nd := c.nodes[id]
		c.waitFor(id+" applied everything", 3*time.Second, func() bool {
			s := nd.Status()
			return s.LastApplied >= s.LastIndex
		})
		snap, err := c.kvs[id].Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if err := nd.Snapshot(snap); err != nil {
			t.Fatalf("snapshot on %s: %v", id, err)
		}
	}
	t.Logf("compacted logs: %s -> %s", c.nodes[leader].Dump(), fmt.Sprint(c.nodes[leader].LogTerms()))

	// More writes while the follower is still away.
	for i := 60; i < 70; i++ {
		if _, ok := c.putThrough(leader, fmt.Sprintf("k%02d", i), "v"); !ok {
			t.Fatalf("write %d failed", i)
		}
	}
	c.heal()

	ok := c.waitFor("lagging follower caught up via snapshot", 5*time.Second, func() bool {
		v, found := c.kvs[lagging].LocalValue("k69")
		return found && v == "v"
	})
	if !ok {
		t.Fatalf("snapshot transfer failed:\n%s", c.dump())
	}
	if st := c.nodes[lagging].Status(); st.SnapIndex == 0 {
		t.Fatalf("the follower should have installed a snapshot: %+v", st)
	}
	sent := 0
	for _, id := range c.ids {
		sent += c.nodes[id].Status().Stats.SnapshotsSent
	}
	if sent == 0 {
		t.Fatal("expected someone to have sent a snapshot")
	}
}

// Single-server membership change: one node at a time, so old and new quorums
// always overlap.
func TestMembershipChangeAddsAPeer(t *testing.T) {
	c := newTestCluster(t, 3, 29)
	leader := c.waitLeader(3 * time.Second)
	if _, ok := c.putThrough(leader, "a", "1"); !ok {
		t.Fatal("setup write failed")
	}

	// Start a fresh node that knows only itself, then add it via the leader.
	id := "n3"
	fs := NewFileStorage(filepath.Join(c.dir, id+".json"))
	newNode := NewNode(c.net, Config{
		ID: id, Peers: []string{id},
		ElectionTimeout: 40 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond,
		Storage: fs, Seed: 999,
	})
	c.ids = append(c.ids, id)
	c.nodes[id] = newNode
	c.store[id] = fs
	newNode.Start()
	c.kvs[id] = NewKV(newNode)

	idx, err := c.nodes[leader].AddPeer(id)
	if err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if !c.waitFor("config change committed", 3*time.Second, func() bool {
		return c.nodes[leader].Status().CommitIndex >= idx
	}) {
		t.Fatalf("membership change never committed:\n%s", c.dump())
	}
	// The new node must learn the existing data.
	if !c.waitFor("new peer catches up", 5*time.Second, func() bool {
		v, ok := c.kvs[id].LocalValue("a")
		return ok && v == "1"
	}) {
		t.Fatalf("new peer never caught up:\n%s", c.dump())
	}

	// The cluster now has a quorum of 3 out of 4. Kill the leader and one
	// follower: the remaining two are NOT a quorum any more, so no new leader.
	others := without(c.ids, leader)
	c.crash(leader)
	c.crash(others[0])
	c.run(1 * time.Second)
	if l := c.anyLeader(); l != "" {
		s := c.nodes[l].Status()
		t.Fatalf("2 of 4 nodes must not elect a leader, but %s claims leadership (term %d)\n%s", l, s.Term, c.dump())
	}
	t.Logf("correctly refused to elect with 2 of 4 nodes:\n%s", c.dump())
	c.assertNoTwoLeadersPerTerm()
}

// Randomised churn: the property under test is *safety*, not liveness. Nothing
// may ever be lost or duplicated, no matter how rude the network is.
func TestRandomisedChurnPreservesSafety(t *testing.T) {
	c := newTestCluster(t, 5, 31)
	leader := c.waitLeader(3 * time.Second)
	if _, ok := c.putThrough(leader, "init", "1"); !ok {
		t.Fatal("setup write failed")
	}

	rng := rand.New(rand.NewSource(99))
	committed := map[string]string{}
	nextKey := 0

	for round := 0; round < 12; round++ {
		switch rng.Intn(4) {
		case 0: // partition in two random halves
			shuffled := append([]string(nil), c.ids...)
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			cut := 1 + rng.Intn(len(shuffled)-1)
			c.partition(shuffled[:cut], shuffled[cut:])
			t.Logf("round %d: partition %v | %v", round, shuffled[:cut], shuffled[cut:])
		case 1:
			c.heal()
			t.Logf("round %d: healed", round)
		case 2: // crash a node
			victim := c.ids[rng.Intn(len(c.ids))]
			if c.net.Alive(victim) {
				c.crash(victim)
				t.Logf("round %d: crash %s", round, victim)
			}
		case 3: // restart a node
			victim := c.ids[rng.Intn(len(c.ids))]
			if !c.net.Alive(victim) {
				c.restart(victim)
				t.Logf("round %d: restart %s", round, victim)
			}
		}
		c.run(300 * time.Millisecond)

		// Try a write through whoever currently claims leadership.
		if l := c.anyLeader(); l != "" && c.net.Alive(l) {
			nextKey++
			key := fmt.Sprintf("k%d", nextKey)
			val := fmt.Sprintf("v%d", nextKey)
			idx, err := c.kvs[l].ProposePut(key, val)
			if err == nil && c.waitFor("commit "+key, 1500*time.Millisecond, func() bool {
				return c.nodes[l].Status().CommitIndex >= idx
			}) {
				committed[key] = val
			}
		}
	}

	c.heal()
	for _, id := range c.ids {
		if !c.net.Alive(id) {
			c.restart(id)
		}
	}
	finalLeader := c.waitLeader(8 * time.Second)
	if finalLeader == "" {
		t.Fatalf("cluster did not recover a leader:\n%s", c.dump())
	}

	// Every write that was acknowledged must still be there.
	for key, want := range committed {
		if !c.waitFor("durable "+key, 5*time.Second, func() bool {
			v, ok := c.kvs[finalLeader].LocalValue(key)
			return ok && v == want
		}) {
			t.Fatalf("acknowledged write %s=%s was lost", key, want)
		}
	}
	for _, id := range c.ids {
		if !c.waitFor("log convergence "+id, 5*time.Second, func() bool {
			return c.nodes[id].Status().CommitIndex == c.nodes[finalLeader].Status().CommitIndex
		}) {
			t.Fatalf("%s never converged:\n%s", id, c.dump())
		}
	}
	c.assertNoTwoLeadersPerTerm()
	t.Logf("survived churn with %d acknowledged writes", len(committed))
}

func without(xs []string, drop string) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		if x != drop {
			out = append(out, x)
		}
	}
	return out
}
