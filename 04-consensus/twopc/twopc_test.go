package twopc

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/manyyuri/DDIA-projects/internal/simnet"
)

func newSystem(t *testing.T, participants int) (*simnet.Network, *Coordinator, []*Participant) {
	t.Helper()
	net := simnet.New(1)
	net.SetLatency(2*time.Millisecond, time.Millisecond)
	ids := make([]string, participants)
	ps := make([]*Participant, participants)
	for i := range ids {
		ids[i] = fmt.Sprintf("p%d", i)
		ps[i] = NewParticipant(net, ids[i])
	}
	c := NewCoordinator(net, "coordinator", ids)
	return net, c, ps
}

func TestHappyPathCommitsEverywhere(t *testing.T) {
	_, coord, ps := newSystem(t, 3)
	committed, decided := coord.Commit("t1")
	if !committed || !decided {
		t.Fatalf("committed=%v decided=%v", committed, decided)
	}
	for _, p := range ps {
		if inDoubt := p.InDoubt(); len(inDoubt) != 0 {
			t.Fatalf("%s still in doubt: %v", p.id, inDoubt)
		}
		tl := strings.Join(p.Timeline(), " ")
		if !strings.Contains(tl, "t1:prepared") || !strings.Contains(tl, "t1:commit") {
			t.Fatalf("%s timeline = %q", p.id, tl)
		}
	}
	t.Logf("coordinator timeline: %v", coord.Timeline())
}

// One "no" vote is enough: the decision is atomic, so everyone aborts — even
// the participants that already said yes.
func TestOneAbortVoteAbortsEveryone(t *testing.T) {
	_, coord, ps := newSystem(t, 3)
	ps[1].FailNextPrepare()

	committed, decided := coord.Commit("t2")
	if committed || !decided {
		t.Fatalf("committed=%v decided=%v", committed, decided)
	}
	for _, p := range ps {
		tl := strings.Join(p.Timeline(), " ")
		if strings.Contains(tl, "t2:commit") {
			t.Fatalf("%s committed a transaction that was aborted: %q", p.id, tl)
		}
		if len(p.InDoubt()) != 0 {
			t.Fatalf("%s is still holding locks: %v", p.id, p.InDoubt())
		}
	}
	t.Logf("coordinator timeline: %v", coord.Timeline())
}

// THE problem with 2PC. The coordinator dies after phase 1: every participant
// holds locks, none can decide, and no timeout makes it safe. A participant
// cannot even ask its peers — they are in exactly the same state.
func TestCoordinatorCrashLeavesParticipantsInDoubt(t *testing.T) {
	net, coord, ps := newSystem(t, 3)

	coord.PrepareAndCrash("t3")

	// Waiting does not resolve anything.
	WaitForRecoveryTimeout(net, 5*time.Second)

	for _, p := range ps {
		inDoubt := p.InDoubt()
		if len(inDoubt) != 1 || inDoubt[0] != "t3" {
			t.Fatalf("%s should be blocked on t3, got %v (timeline %v)", p.id, inDoubt, p.Timeline())
		}
		tl := strings.Join(p.Timeline(), " ")
		if strings.Contains(tl, "t3:commit") || strings.Contains(tl, "t3:abort") {
			t.Fatalf("%s decided unilaterally: %q", p.id, tl)
		}
	}
	t.Log("all three participants are in-doubt and holding locks; this is 2PC's fundamental cost")

	// The only resolution is the coordinator coming back with the decision.
	// (A real system persists the decision and replays it from a log/txn table.)
	revived := NewCoordinator(net, "coordinator2", []string{"p0", "p1", "p2"})
	committed, decided := revived.decide("t3", true)
	if !committed || !decided {
		t.Fatal("recovered coordinator failed to decide")
	}
	for _, p := range ps {
		if len(p.InDoubt()) != 0 {
			t.Fatalf("%s is still in doubt after recovery: %v", p.id, p.InDoubt())
		}
		if !strings.Contains(strings.Join(p.Timeline(), " "), "t3:commit") {
			t.Fatalf("%s never applied the decision: %v", p.id, p.Timeline())
		}
	}
}

// The saga alternative: no locks, no in-doubt state, but intermediate states
// are visible and you must write a compensation for every step.
func TestSagaCompensatesOnFailure(t *testing.T) {
	var mu sync.Mutex
	balance := map[string]int{"A": 100, "B": 0}
	calls := []string{}

	s := &Saga{}
	s.Add(SagaStep{
		Name:       "debit A",
		Do:         func() error { mu.Lock(); balance["A"] -= 30; mu.Unlock(); return nil },
		Compensate: func() error { mu.Lock(); balance["A"] += 30; mu.Unlock(); return nil },
	})
	s.Add(SagaStep{
		Name:       "credit B",
		Do:         func() error { mu.Lock(); balance["B"] += 30; mu.Unlock(); return nil },
		Compensate: func() error { mu.Lock(); balance["B"] -= 30; mu.Unlock(); return nil },
	})
	s.Add(SagaStep{
		Name: "notify (always fails)",
		Do: func() error {
			calls = append(calls, "notify")
			return errors.New("email service down")
		},
		Compensate: func() error { return nil },
	})

	err := s.Run()
	if err == nil {
		t.Fatal("expected the saga to fail")
	}
	t.Logf("saga log: %v", s.Log())

	if balance["A"] != 100 || balance["B"] != 0 {
		t.Fatalf("compensation did not restore the balances: %v", balance)
	}
	// Compensation runs in reverse order.
	log := strings.Join(s.Log(), " | ")
	if strings.Index(log, "credit B compensated") > strings.Index(log, "debit A compensated") {
		t.Fatalf("compensations must run in reverse order: %s", log)
	}
}
