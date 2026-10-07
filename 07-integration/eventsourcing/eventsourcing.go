// Package eventsourcing implements DDIA §11.4 ("Event Sourcing") and the CQRS
// split it implies.
//
// The inversion is worth stating plainly: instead of storing the *current
// state* and mutating it, you store the *sequence of facts* and derive state
// from it. Then:
//
//	optimistic concurrency  is "append at exactly version N" — the store rejects
//	                        a second writer, so two clients cannot both decide
//	                        based on the same stale state
//	audit log               is free: the log is the database
//	read models             are projections you can throw away and rebuild
//	snapshots               are an optimisation, never the source of truth
package eventsourcing

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrVersionConflict is returned when the expected stream version does not
// match. It is the event-sourcing equivalent of a serialization failure: the
// caller re-reads and retries.
var ErrVersionConflict = errors.New("eventsourcing: version conflict, someone else appended first")

// Event is an immutable fact.
type Event struct {
	Stream string // aggregate id
	// Version is 1-based within the stream. Gaps or duplicates are impossible.
	Version int
	// GlobalPos is the total order across all streams: what a projection tails.
	GlobalPos int64
	Type      string
	Data      map[string]any
	// Time is recorded for humans and for time-travel queries; ordering never
	// depends on it (clocks are not trustworthy — see DDIA §8.3).
	Time time.Time
}

func (e Event) String() string {
	keys := make([]string, 0, len(e.Data))
	for k := range e.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += fmt.Sprintf(" %s=%v", k, e.Data[k])
	}
	return fmt.Sprintf("%s#%d@%d %s%s", e.Stream, e.Version, e.GlobalPos, e.Type, out)
}

// EventStore is an append-only log of events, partitioned into per-aggregate
// streams with a global order across them.
type EventStore struct {
	mu        sync.Mutex
	streams   map[string][]Event
	all       []Event
	globalPos int64
	nowFn     func() time.Time
}

// NewEventStore creates an empty store.
func NewEventStore() *EventStore {
	return &EventStore{streams: map[string][]Event{}, nowFn: time.Now}
}

// SetNow overrides the clock for deterministic tests.
func (s *EventStore) SetNow(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nowFn = f
}

// StreamVersion returns the current version of a stream (0 if it does not exist).
func (s *EventStore) StreamVersion(stream string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams[stream])
}

// Append writes events to a stream, but only if the stream is still at
// expectedVersion. This single check is what gives event sourcing its
// concurrency control: it is a compare-and-swap on the whole aggregate.
func (s *EventStore) Append(stream string, expectedVersion int, events ...Event) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current := len(s.streams[stream])
	if current != expectedVersion {
		return fmt.Errorf("%w: stream %q is at version %d, caller expected %d",
			ErrVersionConflict, stream, current, expectedVersion)
	}
	for i := range events {
		ev := events[i]
		ev.Stream = stream
		ev.Version = current + i + 1
		s.globalPos++
		ev.GlobalPos = s.globalPos
		if ev.Time.IsZero() {
			ev.Time = s.nowFn()
		}
		s.streams[stream] = append(s.streams[stream], ev)
		s.all = append(s.all, ev)
		events[i] = ev
	}
	return nil
}

// Load returns every event of a stream, oldest first.
func (s *EventStore) Load(stream string) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Event(nil), s.streams[stream]...)
}

// LoadFrom tails the global log: what a projection or a CDC consumer reads.
func (s *EventStore) LoadFrom(globalPos int64, limit int) []Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = len(s.all)
	}
	var out []Event
	for _, e := range s.all {
		if e.GlobalPos > globalPos {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out
}

// GlobalPos returns the head of the global log.
func (s *EventStore) GlobalPos() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.globalPos
}

// TotalEvents counts everything ever appended.
func (s *EventStore) TotalEvents() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.all)
}

// -------------------------------------------------------------- aggregate --

// Account is a bank account aggregate. State is *derived*: apply an event and
// the fields change; there is no setter for Balance.
type Account struct {
	ID      string
	Balance int
	Version int
	Closed  bool
}

type accountEvent struct {
	Type string
	Data map[string]any
}

func (a *Account) apply(e Event) {
	switch e.Type {
	case "Opened":
		a.ID, _ = e.Data["id"].(string)
	case "Deposited":
		a.Balance += toInt(e.Data["amount"])
	case "Withdrawn":
		a.Balance -= toInt(e.Data["amount"])
	case "Closed":
		a.Closed = true
	}
	a.Version = e.Version
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

// Replay folds events into an aggregate without validating anything: events are
// facts, they have already happened.
func Replay(id string, events []Event) *Account {
	a := &Account{ID: id}
	for _, e := range events {
		a.apply(e)
	}
	return a
}

// LoadAccount rebuilds an aggregate from its stream.
func LoadAccount(store *EventStore, id string) *Account {
	return Replay(id, store.Load(id))
}

// ErrInsufficientFunds is a business rule violation, refused *before* any event
// is written. Nothing is persisted, so nothing has to be compensated.
var ErrInsufficientFunds = errors.New("eventsourcing: insufficient funds")

// ErrAccountClosed is a second business rule.
var ErrAccountClosed = errors.New("eventsourcing: account is closed")

// Open produces the creation event.
func Open(id string) Event {
	return Event{Type: "Opened", Data: map[string]any{"id": id}}
}

// Deposit returns the events for a deposit, without writing them.
func (a *Account) Deposit(amount int) ([]Event, error) {
	if a.Closed {
		return nil, ErrAccountClosed
	}
	if amount <= 0 {
		return nil, fmt.Errorf("eventsourcing: amount must be positive")
	}
	return []Event{{Type: "Deposited", Data: map[string]any{"amount": amount}}}, nil
}

// Withdraw returns the events for a withdrawal, but only if the invariant holds
// *within this version of the aggregate*. Because the append is a CAS on the
// version, that decision cannot be invalidated by a concurrent writer.
func (a *Account) Withdraw(amount int) ([]Event, error) {
	if a.Closed {
		return nil, ErrAccountClosed
	}
	if amount <= 0 {
		return nil, fmt.Errorf("eventsourcing: amount must be positive")
	}
	if a.Balance-amount < 0 {
		return nil, ErrInsufficientFunds
	}
	return []Event{{Type: "Withdrawn", Data: map[string]any{"amount": amount}}}, nil
}

// Close returns the events for closing the account.
func (a *Account) Close() ([]Event, error) {
	if a.Closed {
		return nil, ErrAccountClosed
	}
	if a.Balance != 0 {
		return nil, fmt.Errorf("eventsourcing: cannot close an account with a non-zero balance")
	}
	return []Event{{Type: "Closed", Data: map[string]any{}}}, nil
}

// Execute runs a command against a freshly loaded aggregate and appends the
// resulting events with a CAS on the version. On conflict the caller retries:
// the whole command is pure, so a retry is always safe.
func Execute(store *EventStore, id string, command func(*Account) ([]Event, error)) error {
	for attempt := 0; attempt < 5; attempt++ {
		agg := LoadAccount(store, id)
		events, err := command(agg)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		if err := store.Append(id, agg.Version, events...); err != nil {
			if errors.Is(err, ErrVersionConflict) {
				continue // someone else got there first: re-read and re-decide
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("%w: gave up after 5 attempts", ErrVersionConflict)
}

// ------------------------------------------------------------- projections --

// Projection is a read model maintained by tailing the global log. It is
// disposable: delete it and rebuild from position 0.
type Projection struct {
	mu         sync.Mutex
	balances   map[string]int
	eventsSeen int
	checkpoint int64
	failures   int
}

// NewProjection creates an empty read model.
func NewProjection() *Projection {
	return &Projection{balances: map[string]int{}}
}

// CatchUp applies every event after the projection's checkpoint.
func (p *Projection) CatchUp(store *EventStore) int {
	applied := 0
	for {
		batch := store.LoadFrom(p.checkpoint, 256)
		if len(batch) == 0 {
			return applied
		}
		for _, e := range batch {
			p.apply(e)
			applied++
		}
	}
}

func (p *Projection) apply(e Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch e.Type {
	case "Opened":
		if id, ok := e.Data["id"].(string); ok {
			p.balances[id] = 0
		}
	case "Deposited":
		p.balances[e.Stream] += toInt(e.Data["amount"])
	case "Withdrawn":
		p.balances[e.Stream] -= toInt(e.Data["amount"])
	case "Closed":
		delete(p.balances, e.Stream)
	}
	p.eventsSeen++
	if e.GlobalPos > p.checkpoint {
		p.checkpoint = e.GlobalPos
	}
}

// Balance reads the projected balance.
func (p *Projection) Balance(id string) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	v, ok := p.balances[id]
	return v, ok
}

// Balances returns the whole read model.
func (p *Projection) Balances() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.balances))
	for k, v := range p.balances {
		out[k] = v
	}
	return out
}

// Checkpoint is the log position the projection has consumed.
func (p *Projection) Checkpoint() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.checkpoint
}

// EventsSeen counts applications (including replays of the same log).
func (p *Projection) EventsSeen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eventsSeen
}

// Rebuild discards the read model and replays the whole log, which is the
// standard answer to "the projection had a bug".
func (p *Projection) Rebuild(store *EventStore) int {
	p.mu.Lock()
	p.balances = map[string]int{}
	p.checkpoint = 0
	p.mu.Unlock()
	return p.CatchUp(store)
}

// ----------------------------------------------------------------- snapshot --

// Snapshot is a cached aggregate state plus the stream version it corresponds
// to. It is purely an optimisation: deleting every snapshot must not change any
// observable behaviour.
type Snapshot struct {
	Stream  string
	Version int
	State   Account
}

// SnapshotStore persists snapshots.
type SnapshotStore struct {
	mu   sync.Mutex
	snap map[string]Snapshot
}

// NewSnapshotStore creates an empty snapshot store.
func NewSnapshotStore() *SnapshotStore { return &SnapshotStore{snap: map[string]Snapshot{}} }

// Save records a snapshot.
func (s *SnapshotStore) Save(a *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap[a.ID] = Snapshot{Stream: a.ID, Version: a.Version, State: *a}
}

// Load returns the snapshot for a stream, if any.
func (s *SnapshotStore) Load(stream string) (Snapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, ok := s.snap[stream]
	return sn, ok
}

// LoadAccountFast rebuilds an aggregate from a snapshot plus the events after
// it — identical in result to a full replay, cheaper in practice.
func LoadAccountFast(store *EventStore, snaps *SnapshotStore, id string) *Account {
	sn, ok := snaps.Load(id)
	var a *Account
	if ok {
		state := sn.State
		a = &state
	} else {
		a = &Account{ID: id}
	}
	for _, e := range store.Load(id) {
		if e.Version > a.Version {
			a.apply(e)
		}
	}
	return a
}
