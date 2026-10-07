// Package minidb is the capstone: it wires the pieces from the previous
// chapters into the system DDIA keeps describing but never builds in one piece.
//
//	partitioning  consistent hashing decides which shard owns a key
//	raft          each shard is a 3-replica consensus group: one leader, linearizable
//	lsm           each replica's state machine is an LSM engine on local disk
//	client        routes by key, retries on "not the leader", waits for commit
//
// The layering is the lesson. Every component is replaceable and every boundary
// is a place where a trade-off was made:
//
//	log vs state     the Raft log is the source of truth; the LSM store is derived
//	                  data. That is why a replica can rebuild from its log.
//	read vs write    reads go *through* the log so a deposed leader cannot serve
//	                  stale data (DDIA §8.3.1).
//	shard vs cluster sharding buys scale and costs you cross-shard transactions:
//	                  there are none here, by construction.
//
// What it deliberately does not do: multi-shard transactions, snapshots,
// rebalancing, or a real network. Each of those would need its own chapter.
package minidb

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manyyuri/DDIA-projects/01-storage/lsm"
	"github.com/manyyuri/DDIA-projects/02-replication/partitioning"
	"github.com/manyyuri/DDIA-projects/04-consensus/raft"
	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

// Command is the replicated unit of work.
type Command struct {
	Op    string `json:"op"` // put | get
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Options configures a cluster.
type Options struct {
	Root            string
	Shards          int
	Replicas        int
	Seed            int64
	Latency         time.Duration
	ElectionTimeout time.Duration
	Heartbeat       time.Duration
	// Verbose logs every routed request, for teaching runs.
	Verbose bool
}

func (o Options) withDefaults() Options {
	if o.Shards <= 0 {
		o.Shards = 3
	}
	if o.Replicas <= 0 {
		o.Replicas = 3
	}
	if o.Latency <= 0 {
		o.Latency = 4 * time.Millisecond
	}
	if o.ElectionTimeout <= 0 {
		o.ElectionTimeout = 60 * time.Millisecond
	}
	if o.Heartbeat <= 0 {
		o.Heartbeat = 15 * time.Millisecond
	}
	return o
}

// replica is one node of one shard: a Raft peer plus the state machine it feeds.
type replica struct {
	id      string
	node    *raft.Node
	store   *lsm.DB
	dir     string
	applied atomic.Uint64

	// mu serialises writes to the LSM engine, which is not itself concurrent-safe
	// for the whole API surface we use here.
	mu sync.Mutex
}

type shard struct {
	idx      int
	id       string
	replicas []*replica
}

// Cluster is the whole thing.
type Cluster struct {
	opts   Options
	net    *simnet.Network
	ring   *partitioning.HashRing
	shards []*shard

	mu      sync.Mutex
	routed  map[string]int // shard id -> requests routed there
	verbose []string
}

// New builds a cluster: opts.Shards consensus groups, each with opts.Replicas
// members, each member with its own LSM store on disk.
func New(opts Options) (*Cluster, error) {
	opts = opts.withDefaults()
	net := simnet.New(opts.Seed)
	net.SetLatency(opts.Latency, opts.Latency/2)

	c := &Cluster{
		opts:   opts,
		net:    net,
		ring:   partitioning.NewHashRing(64),
		routed: map[string]int{},
	}

	for s := 0; s < opts.Shards; s++ {
		sh := &shard{idx: s, id: fmt.Sprintf("shard-%d", s)}
		peers := make([]string, 0, opts.Replicas)
		for r := 0; r < opts.Replicas; r++ {
			peers = append(peers, fmt.Sprintf("%s/n%d", sh.id, r))
		}
		for r := 0; r < opts.Replicas; r++ {
			id := peers[r]
			dir := filepath.Join(opts.Root, sh.id, fmt.Sprintf("n%d", r))
			store, err := lsm.Open(lsm.Options{Dir: filepath.Join(dir, "kv")})
			if err != nil {
				return nil, err
			}
			rep := &replica{
				id:    id,
				dir:   dir,
				store: store,
			}
			rep.node = raft.NewNode(net, raft.Config{
				ID:                id,
				Peers:             peers,
				ElectionTimeout:   opts.ElectionTimeout,
				HeartbeatInterval: opts.Heartbeat,
				Storage:           raft.NewFileStorage(filepath.Join(dir, "raft.json")),
				Seed:              opts.Seed + int64(s*100+r),
			})
			sh.replicas = append(sh.replicas, rep)
		}
		c.shards = append(c.shards, sh)
		// The ring maps keys to *shards*; replication is Raft's job.
		c.ring.AddNode(sh.id)
	}

	for _, sh := range c.shards {
		for _, rep := range sh.replicas {
			rep.node.Start()
			go c.consume(rep)
		}
	}
	return c, nil
}

// consume is the state machine: applied log entries become LSM writes.
func (c *Cluster) consume(rep *replica) {
	for msg := range rep.node.ApplyCh() {
		if msg.Type != "" {
			// no-op / config entries still advance the applied watermark
			rep.applied.Store(msg.Index)
			continue
		}
		var cmd Command
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			rep.applied.Store(msg.Index)
			continue
		}
		rep.mu.Lock()
		switch cmd.Op {
		case "put":
			_ = rep.store.Put([]byte(cmd.Key), []byte(cmd.Value))
		case "get":
			// A read through the log carries no payload: its only job is to
			// prove that this node is the leader and that time has moved on.
		}
		rep.mu.Unlock()
		rep.applied.Store(msg.Index)
	}
}

// Close releases every LSM store.
func (c *Cluster) Close() {
	for _, sh := range c.shards {
		for _, rep := range sh.replicas {
			rep.node.Stop()
			_ = rep.store.Close()
		}
	}
}

// Run advances simulated time. Every client call drives this internally, so
// tests rarely need it.
func (c *Cluster) Run(d time.Duration) {
	deadline := c.net.Now() + d
	for c.net.Now() < deadline {
		c.net.Run(2 * time.Millisecond)
	}
}

// Until runs the network in small steps until cond is true or the simulated
// deadline passes.
func (c *Cluster) Until(desc string, timeout time.Duration, cond func() bool) bool {
	deadline := c.net.Now() + timeout
	for {
		if cond() {
			return true
		}
		if c.net.Now() >= deadline {
			return false
		}
		c.net.Run(2 * time.Millisecond)
		time.Sleep(30 * time.Microsecond) // let the state-machine goroutines run
	}
}

// ---------------------------------------------------------------- routing --

// ShardOf returns the shard id that owns a key.
func (c *Cluster) ShardOf(key string) string { return c.ring.Locate(key) }

func (c *Cluster) shardByID(id string) *shard {
	for _, sh := range c.shards {
		if sh.id == id {
			return sh
		}
	}
	return nil
}

// leaderOf returns the replica that currently believes it leads the shard, or
// nil. It may be momentarily wrong; the client's retry loop handles that.
func (c *Cluster) leaderOf(sh *shard) *replica {
	for _, rep := range sh.replicas {
		if rep.node.Status().State == raft.Leader && c.net.Alive(rep.id) {
			return rep
		}
	}
	return nil
}

func (c *Cluster) log(format string, args ...any) {
	if !c.opts.Verbose {
		return
	}
	c.mu.Lock()
	c.verbose = append(c.verbose, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

// Trace returns the verbose log.
func (c *Cluster) Trace() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.verbose...)
}

// ---------------------------------------------------------------- client --

// Put writes a key, routing it to its shard and retrying until it commits.
func (c *Cluster) Put(key, value string) error {
	sh := c.shardByID(c.ShardOf(key))
	if sh == nil {
		return fmt.Errorf("minidb: no shard owns %q", key)
	}
	c.mu.Lock()
	c.routed[sh.id]++
	c.mu.Unlock()

	payload, err := json.Marshal(Command{Op: "put", Key: key, Value: value})
	if err != nil {
		return err
	}

	deadline := c.net.Now() + 10*time.Second
	for c.net.Now() < deadline {
		if rep := c.leaderOf(sh); rep != nil {
			idx, err := rep.node.Propose(payload)
			if err == nil {
				// Wait for commit and for the local state machine to apply it.
				if c.Until("commit", 5*time.Second, func() bool {
					return rep.node.Status().CommitIndex >= idx && rep.applied.Load() >= idx
				}) {
					c.log("put %s -> %s (%s via %s @%d)", key, sh.id, sh.id, rep.id, idx)
					return nil
				}
			}
		}
		c.net.Run(2 * time.Millisecond)
	}
	return fmt.Errorf("minidb: put %q timed out (shard %s has no usable leader)", key, sh.id)
}

// Get reads a key. The read is proposed through the log so it cannot be served
// by a deposed leader: consistency costs a round trip, and that is the honest
// price of a linearizable read.
func (c *Cluster) Get(key string) (string, bool, error) {
	sh := c.shardByID(c.ShardOf(key))
	if sh == nil {
		return "", false, fmt.Errorf("minidb: no shard owns %q", key)
	}
	c.mu.Lock()
	c.routed[sh.id]++
	c.mu.Unlock()

	payload, _ := json.Marshal(Command{Op: "get", Key: key})

	deadline := c.net.Now() + 10*time.Second
	for c.net.Now() < deadline {
		rep := c.leaderOf(sh)
		if rep != nil {
			idx, err := rep.node.Propose(payload)
			if err == nil {
				if !c.Until("read index", 5*time.Second, func() bool {
					return rep.node.Status().CommitIndex >= idx && rep.applied.Load() >= idx
				}) {
					break
				}
				rep.mu.Lock()
				val, err := rep.store.Get([]byte(key))
				rep.mu.Unlock()
				if err == lsm.ErrNotFound {
					return "", false, nil
				}
				if err != nil {
					return "", false, err
				}
				return string(val), true, nil
			}
		}
		c.net.Run(2 * time.Millisecond)
	}
	return "", false, fmt.Errorf("minidb: get %q timed out (shard %s)", key, sh.id)
}

// ------------------------------------------------------------ operations --

// WaitForLeaders blocks until every shard has a live leader.
func (c *Cluster) WaitForLeaders(timeout time.Duration) bool {
	return c.Until("leaders", timeout, func() bool {
		for _, sh := range c.shards {
			if c.leaderOf(sh) == nil {
				return false
			}
		}
		return true
	})
}

// Crash kills one replica (by its node id) without losing its disk.
func (c *Cluster) Crash(id string) bool {
	for _, sh := range c.shards {
		for _, rep := range sh.replicas {
			if rep.id == id {
				rep.node.Stop()
				return true
			}
		}
	}
	return false
}

// CrashLeader kills whichever replica currently leads a shard.
func (c *Cluster) CrashLeader(shardID string) string {
	sh := c.shardByID(shardID)
	if sh == nil {
		return ""
	}
	if rep := c.leaderOf(sh); rep != nil {
		rep.node.Stop()
		return rep.id
	}
	return ""
}

// Restart brings a crashed replica back with its durable state.
func (c *Cluster) Restart(id string) bool {
	for _, sh := range c.shards {
		for _, rep := range sh.replicas {
			if rep.id == id {
				rep.node.Restart()
				return true
			}
		}
	}
	return false
}

// ShardInfo describes one shard's current leadership.
type ShardInfo struct {
	ID          string
	Leader      string
	Term        uint64
	CommitIndex uint64
	Followers   []string
}

// Info returns a snapshot of cluster topology and leadership.
//
// Only *live* replicas may be reported as leader. A crashed node keeps its
// last known role in memory, so trusting Status alone reports a dead node as
// the leader — the same trap as raft's own "a crashed node still thinks it
// leads" behaviour.
func (c *Cluster) Info() []ShardInfo {
	var out []ShardInfo
	for _, sh := range c.shards {
		info := ShardInfo{ID: sh.id}
		for _, rep := range sh.replicas {
			st := rep.node.Status()
			if st.State == raft.Leader && c.net.Alive(rep.id) {
				info.Leader = rep.id
				info.Term = st.Term
				info.CommitIndex = st.CommitIndex
				continue
			}
			label := fmt.Sprintf("%s(%v)", rep.id, st.State)
			if !c.net.Alive(rep.id) {
				label += "(dead)"
			}
			info.Followers = append(info.Followers, label)
		}
		out = append(out, info)
	}
	return out
}

// Describe renders the cluster for logs.
func (c *Cluster) Describe() string {
	out := ""
	for _, info := range c.Info() {
		leader := info.Leader
		if leader == "" {
			leader = "(no leader)"
		}
		out += fmt.Sprintf("  %s leader=%s term=%d commit=%d followers=%v\n",
			info.ID, leader, info.Term, info.CommitIndex, info.Followers)
	}
	return out
}

// Routed returns how many requests each shard served.
func (c *Cluster) Routed() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.routed))
	for k, v := range c.routed {
		out[k] = v
	}
	return out
}

// KeysPerShard is a helper for assertions: which shard each key lands on.
func (c *Cluster) KeysPerShard(keys []string) map[string][]string {
	out := map[string][]string{}
	for _, k := range keys {
		out[c.ShardOf(k)] = append(out[c.ShardOf(k)], k)
	}
	for _, v := range out {
		sort.Strings(v)
	}
	return out
}
