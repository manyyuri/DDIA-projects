package eventsourcing

import (
	"errors"
	"testing"
)

func TestAppendIsACasOnTheStreamVersion(t *testing.T) {
	store := NewEventStore()
	if err := store.Append("acct-1", 0, Open("acct-1")); err != nil {
		t.Fatal(err)
	}
	if got := store.StreamVersion("acct-1"); got != 1 {
		t.Fatalf("version = %d, want 1", got)
	}
	// A second writer that still thinks the stream is empty must be refused.
	err := store.Append("acct-1", 0, Event{Type: "Deposited", Data: map[string]any{"amount": 100}})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
	// The correct version succeeds.
	if err := store.Append("acct-1", 1, Event{Type: "Deposited", Data: map[string]any{"amount": 100}}); err != nil {
		t.Fatal(err)
	}
	events := store.Load("acct-1")
	if len(events) != 2 || events[0].Version != 1 || events[1].Version != 2 {
		t.Fatalf("versions are not sequential: %v", events)
	}
	for _, e := range events {
		t.Logf("%s", e)
	}
}

// The business rule is evaluated on state derived from the log, and the CAS
// makes that decision safe: it cannot be invalidated between read and write.
func TestBusinessRulesAreEnforcedByTheCAS(t *testing.T) {
	store := NewEventStore()
	if err := Execute(store, "acct-1", func(a *Account) ([]Event, error) { return []Event{Open(a.ID)}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := Execute(store, "acct-1", func(a *Account) ([]Event, error) { return a.Deposit(100) }); err != nil {
		t.Fatal(err)
	}
	// Overdraft is refused and nothing is written.
	err := Execute(store, "acct-1", func(a *Account) ([]Event, error) { return a.Withdraw(500) })
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected ErrInsufficientFunds, got %v", err)
	}
	if got := store.StreamVersion("acct-1"); got != 2 {
		t.Fatalf("a refused command must not append anything: version=%d", got)
	}
	if err := Execute(store, "acct-1", func(a *Account) ([]Event, error) { return a.Withdraw(40) }); err != nil {
		t.Fatal(err)
	}
	agg := LoadAccount(store, "acct-1")
	if agg.Balance != 60 {
		t.Fatalf("balance = %d, want 60", agg.Balance)
	}
	t.Logf("rebuilt from %d events: balance=%d version=%d", store.StreamVersion("acct-1"), agg.Balance, agg.Version)
}

// Two concurrent commands both read version 2 and both try to append. One wins;
// the loser's rule check is redone against the new state.
func TestConcurrentCommandsCannotDoubleSpend(t *testing.T) {
	store := NewEventStore()
	Execute(store, "acct", func(a *Account) ([]Event, error) { return []Event{Open(a.ID)}, nil })
	Execute(store, "acct", func(a *Account) ([]Event, error) { return a.Deposit(100) })

	// Both callers load the aggregate at version 2 and decide to withdraw 80.
	a1 := LoadAccount(store, "acct")
	a2 := LoadAccount(store, "acct")
	e1, err1 := a1.Withdraw(80)
	e2, err2 := a2.Withdraw(80)
	if err1 != nil || err2 != nil {
		t.Fatalf("both decisions should look legal in isolation: %v %v", err1, err2)
	}
	if err := store.Append("acct", 2, e1...); err != nil {
		t.Fatalf("first writer should win: %v", err)
	}
	err := store.Append("acct", 2, e2...)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("second writer must be refused, got %v", err)
	}

	// The retry path: re-read, re-check, and now the rule says no.
	var retryErr error
	for i := 0; i < 3; i++ {
		agg := LoadAccount(store, "acct")
		events, err := agg.Withdraw(80)
		if err != nil {
			retryErr = err
			break
		}
		if err := store.Append("acct", agg.Version, events...); err == nil {
			break
		}
	}
	if !errors.Is(retryErr, ErrInsufficientFunds) {
		t.Fatalf("the retry should have hit the balance rule, got %v", retryErr)
	}
	if bal := LoadAccount(store, "acct").Balance; bal != 20 {
		t.Fatalf("double spend: balance = %d, want 20", bal)
	}
}

func TestProjectionTailsTheGlobalLog(t *testing.T) {
	store := NewEventStore()
	for _, id := range []string{"a", "b"} {
		Execute(store, id, func(acc *Account) ([]Event, error) { return []Event{Open(acc.ID)}, nil })
		Execute(store, id, func(acc *Account) ([]Event, error) { return acc.Deposit(50) })
	}
	Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Withdraw(20) })

	p := NewProjection()
	p.CatchUp(store)
	bal := p.Balances()
	if bal["a"] != 30 || bal["b"] != 50 {
		t.Fatalf("projection = %v, want a=30 b=50", bal)
	}
	if p.Checkpoint() == 0 {
		t.Fatal("projection did not record a checkpoint")
	}
	t.Logf("projection %v at checkpoint %d after %d events", bal, p.Checkpoint(), p.EventsSeen())

	// Incremental catch-up only touches new events.
	Execute(store, "b", func(acc *Account) ([]Event, error) { return acc.Deposit(10) })
	before := p.EventsSeen()
	p.CatchUp(store)
	if p.EventsSeen() != before+1 {
		t.Fatalf("catch-up applied %d events, want 1", p.EventsSeen()-before)
	}
	if v, _ := p.Balance("b"); v != 60 {
		t.Fatalf("b = %d, want 60", v)
	}
}

// A buggy projection is fixed by deleting it and replaying: that is the whole
// operational advantage of derived read models.
func TestProjectionCanBeRebuiltFromScratch(t *testing.T) {
	store := NewEventStore()
	Execute(store, "a", func(acc *Account) ([]Event, error) { return []Event{Open(acc.ID)}, nil })
	Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Deposit(70) })

	p := NewProjection()
	p.CatchUp(store)
	want := p.Balances()

	// Corrupt the read model on purpose.
	p.mu.Lock()
	p.balances["a"] = 999999
	p.mu.Unlock()

	applied := p.Rebuild(store)
	got := p.Balances()
	if got["a"] != want["a"] {
		t.Fatalf("rebuild did not restore the read model: %v vs %v", got, want)
	}
	t.Logf("rebuilt by replaying %d events: %v", applied, got)
}

func TestSnapshotPlusReplayEqualsFullReplay(t *testing.T) {
	store := NewEventStore()
	Execute(store, "a", func(acc *Account) ([]Event, error) { return []Event{Open(acc.ID)}, nil })
	for i := 0; i < 10; i++ {
		if err := Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Deposit(10) }); err != nil {
			t.Fatal(err)
		}
	}
	snaps := NewSnapshotStore()
	snaps.Save(LoadAccount(store, "a"))
	// More events after the snapshot: the fast path must still see them.
	Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Deposit(5) })

	full := LoadAccount(store, "a")
	fast := LoadAccountFast(store, snaps, "a")
	if full.Balance != fast.Balance || full.Version != fast.Version {
		t.Fatalf("snapshot path diverged: full=%+v fast=%+v", full, fast)
	}
	if full.Balance != 105 {
		t.Fatalf("balance = %d, want 105", full.Balance)
	}

	// Discarding every snapshot must not change any result.
	empty := NewSnapshotStore()
	if got := LoadAccountFast(store, empty, "a"); got.Balance != full.Balance {
		t.Fatalf("without snapshots the result changed: %+v", got)
	}
	t.Logf("full replay=%d, snapshot+tail=%d, no-snapshot=%d", full.Balance, fast.Balance, full.Balance)
}

func TestCloseRequiresZeroBalance(t *testing.T) {
	store := NewEventStore()
	Execute(store, "a", func(acc *Account) ([]Event, error) { return []Event{Open(acc.ID)}, nil })
	Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Deposit(30) })

	if err := Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Close() }); err == nil {
		t.Fatal("closing a funded account should be refused")
	}
	Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Withdraw(30) })
	if err := Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Close() }); err != nil {
		t.Fatalf("closing an empty account should work: %v", err)
	}
	agg := LoadAccount(store, "a")
	if !agg.Closed || agg.Balance != 0 {
		t.Fatalf("aggregate = %+v", agg)
	}
	// Further commands are refused by the newly derived state.
	if err := Execute(store, "a", func(acc *Account) ([]Event, error) { return acc.Deposit(1) }); !errors.Is(err, ErrAccountClosed) {
		t.Fatalf("expected ErrAccountClosed, got %v", err)
	}
}
