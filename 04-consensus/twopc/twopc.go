// Package twopc implements DDIA §9.3: atomic commit across partitions.
//
// Two-phase commit is the simplest protocol that gives atomicity across nodes,
// and it is *blocking*: a participant that has voted "yes" must hold its locks
// until it hears the decision, and if the coordinator dies in between, nobody
// can safely decide. That in-doubt state is not a bug in this implementation;
// it is the defining property of 2PC, and the reason Spanner pays for it with
// Paxos groups and others avoid it with sagas.
//
//	phase 1  prepare  -> participants take locks and vote
//	phase 2  commit/abort -> participants apply and release
//
// Compare with the Saga implementation at the bottom of this file: no locks, no
// blocking, and no isolation — you get atomicity (all-or-nothing) by
// *compensating* instead of by waiting.
package twopc

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

// Vote is a participant's answer to a prepare request.
type Vote int

const (
	VoteCommit Vote = iota
	VoteAbort
)

func (v Vote) String() string {
	if v == VoteAbort {
		return "abort"
	}
	return "commit"
}

// ------------------------------------------------------------- participants --

// Participant is a resource manager holding locks for one shard.
type Participant struct {
	id  string
	net *simnet.Network

	mu       sync.Mutex
	prepared map[string]bool   // txn -> holds locks, voted commit
	decision map[string]string // txn -> "commit"/"abort"
	applied  []string          // timeline, for assertions
	failNext bool              // force a "no" vote on the next prepare
}

// NewParticipant creates a participant on the simulated network.
func NewParticipant(net *simnet.Network, id string) *Participant {
	p := &Participant{
		id:       id,
		net:      net,
		prepared: map[string]bool{},
		decision: map[string]string{},
	}
	net.Register(id, func(m simnet.Message) {
		switch body := m.Body.(type) {
		case prepare:
			p.handlePrepare(m.From, body)
		case decide:
			p.handleDecide(m.From, body)
		}
	})
	return p
}

type prepare struct{ txn string }
type voteMsg struct {
	txn  string
	vote Vote
}
type decide struct {
	txn    string
	commit bool
}
type ack struct{ txn string }

func (p *Participant) handlePrepare(from string, msg prepare) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failNext {
		p.applied = append(p.applied, msg.txn+":voted-abort")
		p.mu.Unlock()
		p.net.Send(p.id, from, voteMsg{txn: msg.txn, vote: VoteAbort})
		p.mu.Lock()
		return
	}
	// The vote is a promise: from here on we cannot unilaterally abort.
	p.prepared[msg.txn] = true
	p.applied = append(p.applied, msg.txn+":prepared")
	p.net.Send(p.id, from, voteMsg{txn: msg.txn, vote: VoteCommit})
}

func (p *Participant) handleDecide(from string, msg decide) {
	p.mu.Lock()
	d := "abort"
	if msg.commit {
		d = "commit"
	}
	p.decision[msg.txn] = d
	delete(p.prepared, msg.txn) // release locks
	p.applied = append(p.applied, msg.txn+":"+d)
	p.mu.Unlock()
	p.net.Send(p.id, from, ack{txn: msg.txn})
}

// InDoubt reports whether the participant is holding locks waiting for a
// decision. This is the state 2PC cannot escape from on its own.
func (p *Participant) InDoubt() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.prepared))
	for txn := range p.prepared {
		out = append(out, txn)
	}
	sort.Strings(out)
	return out
}

// Timeline returns what this participant did, in order.
func (p *Participant) Timeline() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.applied...)
}

// FailNextPrepare makes the next prepare vote abort.
func (p *Participant) FailNextPrepare() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failNext = true
}

// --------------------------------------------------------------- coordinator --

// Coordinator drives the two phases.
type Coordinator struct {
	id           string
	net          *simnet.Network
	participants []string

	mu        sync.Mutex
	decisions map[string]string
	prepared  map[string]map[string]bool
	committed map[string]bool
	timeline  []string
	alive     bool
}

// NewCoordinator creates a coordinator that will talk to the given participants.
func NewCoordinator(net *simnet.Network, id string, participants []string) *Coordinator {
	c := &Coordinator{
		id:           id,
		net:          net,
		participants: append([]string(nil), participants...),
		decisions:    map[string]string{},
		prepared:     map[string]map[string]bool{},
		committed:    map[string]bool{},
		alive:        true,
	}
	net.Register(id, func(m simnet.Message) {
		switch body := m.Body.(type) {
		case voteMsg:
			c.handleVote(m.From, body)
		case ack:
			c.handleAck(m.From, body)
		}
	})
	return c
}

// Commit runs the full protocol for txn. It returns whether the transaction
// committed, and whether the outcome is known at all.
func (c *Coordinator) Commit(txn string) (committed bool, decided bool) {
	c.mu.Lock()
	c.prepared[txn] = map[string]bool{}
	c.timeline = append(c.timeline, txn+":start")
	c.mu.Unlock()

	for _, p := range c.participants {
		c.net.Send(c.id, p, prepare{txn: txn})
	}
	c.net.Drain()

	c.mu.Lock()
	for _, p := range c.participants {
		if !c.prepared[txn][p] {
			c.mu.Unlock()
			// Someone said no (or never answered): abort is the only legal move.
			c.broadcast(func(p string) { c.net.Send(c.id, p, decide{txn: txn, commit: false}) })
			c.net.Drain()
			c.mu.Lock()
			c.decisions[txn] = "abort"
			c.timeline = append(c.timeline, txn+":abort")
			c.mu.Unlock()
			return false, true
		}
	}
	c.mu.Unlock()

	// The commit decision is the single point of atomicity: it is made once,
	// here, before anyone is told.
	return c.decide(txn, true)
}

func (c *Coordinator) decide(txn string, commit bool) (bool, bool) {
	c.broadcast(func(p string) { c.net.Send(c.id, p, decide{txn: txn, commit: commit}) })
	c.net.Drain()
	c.mu.Lock()
	defer c.mu.Unlock()
	if commit {
		c.decisions[txn] = "commit"
		c.committed[txn] = true
		c.timeline = append(c.timeline, txn+":commit")
	} else {
		c.decisions[txn] = "abort"
		c.timeline = append(c.timeline, txn+":abort")
	}
	return commit, true
}

func (c *Coordinator) handleVote(from string, msg voteMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msg.vote == VoteCommit {
		if c.prepared[msg.txn] == nil {
			c.prepared[msg.txn] = map[string]bool{}
		}
		c.prepared[msg.txn][from] = true
		c.timeline = append(c.timeline, msg.txn+":vote-yes-from-"+from)
	} else {
		c.timeline = append(c.timeline, msg.txn+":vote-no-from-"+from)
	}
}

func (c *Coordinator) handleAck(from string, msg ack) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.timeline = append(c.timeline, msg.txn+":ack-from-"+from)
}

func (c *Coordinator) broadcast(f func(string)) {
	for _, p := range c.participants {
		f(p)
	}
}

// Crash stops the coordinator. If it crashes after phase 1, every participant
// that voted yes is stuck holding locks.
func (c *Coordinator) Crash() {
	c.mu.Lock()
	c.alive = false
	c.mu.Unlock()
	c.net.Unregister(c.id)
}

// Timeline returns the coordinator's decision log.
func (c *Coordinator) Timeline() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.timeline...)
}

// Decision returns "commit", "abort" or "" if the outcome is unknown.
func (c *Coordinator) Decision(txn string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.decisions[txn]
}

// PrepareAndCrash runs phase 1 and then dies before phase 2. This is the
// scenario that makes 2PC blocking, and it is deliberately reproducible here.
func (c *Coordinator) PrepareAndCrash(txn string) {
	c.mu.Lock()
	c.prepared[txn] = map[string]bool{}
	c.timeline = append(c.timeline, txn+":start")
	c.mu.Unlock()
	for _, p := range c.participants {
		c.net.Send(c.id, p, prepare{txn: txn})
	}
	c.net.Drain()
	c.Crash()
}

// WaitForRecoveryTimeout models what an operator does when the coordinator is
// gone: wait, and hope it comes back. There is no safe local decision.
func WaitForRecoveryTimeout(net *simnet.Network, d time.Duration) {
	net.Run(d)
}

// --------------------------------------------------------------- saga --

// Saga is the lock-free alternative: a sequence of local transactions, each
// with a compensating action. There is no isolation (intermediate states are
// visible), but nothing blocks and nothing is in doubt.
type Saga struct {
	mu    sync.Mutex
	steps []SagaStep
	log   []string
}

// SagaStep is one local transaction plus its compensation.
type SagaStep struct {
	Name       string
	Do         func() error
	Compensate func() error
}

// Add appends a step.
func (s *Saga) Add(step SagaStep) { s.steps = append(s.steps, step) }

// Run executes steps in order, compensating the completed prefix in reverse on
// the first failure.
func (s *Saga) Run() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var done []SagaStep
	for _, step := range s.steps {
		if err := step.Do(); err != nil {
			s.log = append(s.log, fmt.Sprintf("%s FAILED: %v", step.Name, err))
			for i := len(done) - 1; i >= 0; i-- {
				if cerr := done[i].Compensate(); cerr != nil {
					s.log = append(s.log, fmt.Sprintf("%s compensate FAILED: %v", done[i].Name, cerr))
					continue
				}
				s.log = append(s.log, done[i].Name+" compensated")
			}
			return err
		}
		s.log = append(s.log, step.Name+" done")
		done = append(done, step)
	}
	return nil
}

// Log returns the execution trace.
func (s *Saga) Log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}
