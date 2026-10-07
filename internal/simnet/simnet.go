// Package simnet is a deterministic, virtual-time network simulator used by
// every distributed-systems project in this repo.
//
// The point (DDIA chapters 8-9) is that distributed systems are mostly about
// *partial failure*. To study it you need to be able to reproduce failure
// deterministically: packet loss, latency, reordering, network partitions and
// crashed nodes. Real sockets cannot do that reproducibly; a simulated network
// with a seeded RNG and a virtual clock can.
//
// Everything runs on one goroutine: Run() pops events in (time, seq) order and
// calls node handlers. There is no real concurrency inside the simulation, so
// tests are fast, deterministic and free of flaky sleeps.
package simnet

import (
	"container/heap"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Message is one in-flight envelope.
type Message struct {
	From string
	To   string
	Body any
}

// Handler receives a message when it is delivered. Handlers must not call Run
// themselves (that would re-enter the simulator): schedule follow-up work with
// After instead.
type Handler func(Message)

type event struct {
	at  time.Duration
	seq int64
	msg Message
	fn  func()
}

type eventQueue []*event

func (q eventQueue) Len() int      { return len(q) }
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq // tie-break: FIFO, so runs are reproducible
}
func (q *eventQueue) Push(x any) { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return ev
}

// Network is the simulator.
type Network struct {
	mu     sync.Mutex
	now    time.Duration
	seq    int64
	q      eventQueue
	handle map[string]Handler

	latency time.Duration
	jitter  time.Duration
	dropPct float64
	rng     *rand.Rand

	// fault injection
	groups  map[string]int // node -> partition group
	blocked map[string]bool
	drops   map[string]bool // persistent link-level drop ("from->to")

	Delivered int
	Dropped   int
}

// New creates a network with a deterministic RNG.
func New(seed int64) *Network {
	return &Network{
		q:       eventQueue{},
		handle:  map[string]Handler{},
		latency: 5 * time.Millisecond,
		rng:     rand.New(rand.NewSource(seed)),
		groups:  map[string]int{},
		blocked: map[string]bool{},
		drops:   map[string]bool{},
	}
}

// Register attaches a handler to a node id (a node coming back up).
func (n *Network) Register(id string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handle[id] = h
}

// Unregister models a crash: the node stops receiving anything.
func (n *Network) Unregister(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handle, id)
}

// Alive reports whether the node is currently reachable.
func (n *Network) Alive(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.handle[id]
	return ok
}

// SetLatency sets the base one-way delay, plus optional jitter.
func (n *Network) SetLatency(d, jitter time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latency, n.jitter = d, jitter
}

// SetDropRate sets the fraction of messages silently dropped (0..1).
func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropPct = p
}

// DropLink makes every message on this one-way link vanish (slow/blackholed node).
func (n *Network) DropLink(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.drops[from+"->"+to] = true
}

// HealLink restores a link.
func (n *Network) HealLink(from, to string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.drops, from+"->"+to)
}

// Partition splits the nodes into the given groups. Messages crossing groups
// are dropped; messages inside a group flow normally.
func (n *Network) Partition(groups ...[]string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.groups = map[string]int{}
	for gi, ids := range groups {
		for _, id := range ids {
			n.groups[id] = gi + 1
		}
	}
}

// Isolate partitions one node away from everyone else.
func (n *Network) Isolate(id string, others []string) {
	n.Partition([]string{id}, others)
}

// Heal removes all partitions.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.groups = map[string]int{}
}

// Now returns virtual time.
func (n *Network) Now() time.Duration {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.now
}

// Send enqueues a message for delivery after the current latency.
func (n *Network) Send(from, to string, body any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.blocked[from+"->"+to] || n.drops[from+"->"+to] {
		n.Dropped++
		return
	}
	if n.groups[from] != 0 && n.groups[from] != n.groups[to] {
		n.Dropped++ // partitioned
		return
	}
	if n.dropPct > 0 && n.rng.Float64() < n.dropPct {
		n.Dropped++
		return
	}
	d := n.latency
	if n.jitter > 0 {
		d += time.Duration(n.rng.Int63n(int64(n.jitter) + 1))
	}
	n.seq++
	heap.Push(&n.q, &event{at: n.now + d, seq: n.seq, msg: Message{From: from, To: to, Body: body}})
}

// After schedules a bare callback (timers: elections, timeouts, retries).
func (n *Network) After(d time.Duration, fn func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.seq++
	heap.Push(&n.q, &event{at: n.now + d, seq: n.seq, fn: fn})
}

// Run advances virtual time by d, delivering everything scheduled inside it.
func (n *Network) Run(d time.Duration) {
	deadline := n.Now() + d
	for {
		n.mu.Lock()
		if n.q.Len() == 0 || n.q[0].at > deadline {
			if deadline > n.now {
				n.now = deadline
			}
			n.mu.Unlock()
			return
		}
		ev := heap.Pop(&n.q).(*event)
		n.now = ev.at
		var h Handler
		if ev.fn == nil {
			h = n.handle[ev.msg.To]
		}
		n.mu.Unlock()

		switch {
		case ev.fn != nil:
			ev.fn()
		case h != nil:
			n.Delivered++
			h(ev.msg)
		default:
			n.Dropped++ // destination is down
		}
	}
}

// RunUntilIdle advances time until the event queue drains or max elapses.
func (n *Network) RunUntilIdle(max time.Duration) {
	deadline := n.Now() + max
	step := n.latency
	if step <= 0 {
		step = time.Millisecond
	}
	for iter := 0; iter < 100000; iter++ {
		n.mu.Lock()
		empty := n.q.Len() == 0
		n.mu.Unlock()
		if empty {
			return
		}
		n.Run(step)
		if n.Now() >= deadline {
			return
		}
	}
}

// Drain is RunUntilIdle with a generous default timeout.
func (n *Network) Drain() { n.RunUntilIdle(2 * time.Second) }

// Pending reports how many events are queued.
func (n *Network) Pending() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.q.Len()
}

// Describe returns a human-readable state summary for test failure messages.
func (n *Network) Describe() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	alive := make([]string, 0, len(n.handle))
	for id := range n.handle {
		alive = append(alive, id)
	}
	sort.Strings(alive)
	return fmt.Sprintf("t=%v alive=%v pending=%d delivered=%d dropped=%d",
		n.now, alive, n.q.Len(), n.Delivered, n.Dropped)
}
