package clocks

import (
	"testing"
	"time"
)

// A Lamport clock respects causality, and that is all it does: the two
// concurrent events end up ordered, but the order is arbitrary.
func TestLamportRespectsCausality(t *testing.T) {
	a, b := NewLamport(), NewLamport()

	_ = a.Tick()       // a1
	msg := a.Send()    // a2, sent to b
	_ = b.Observe(msg) // b1
	b1 := b.Tick()     // b2

	// The receive is after the send in Lamport time.
	if b.Now() < msg {
		t.Fatalf("b.Now()=%d < received timestamp %d", b.Now(), msg)
	}
	if b1 <= msg {
		t.Fatalf("an event after the receive must be later than the send: %d <= %d", b1, msg)
	}

	// Two concurrent events still get an arbitrary total order — that is the
	// limitation, not a bug.
	c := NewLamport()
	_ = c.Tick()
	if c.Now() == a.Now() {
		t.Fatal("concurrent events from different nodes collided in value")
	}
}

// Vector clocks answer the question Lamport clocks cannot: did these two events
// happen concurrently?
func TestVectorClockDetectsConcurrency(t *testing.T) {
	a, b := NewVector("A"), NewVector("B")

	a1 := a.Tick() // A: {A:1}
	b1 := b.Tick() // B: {B:1}
	if rel := CompareVectors(a1, b1); rel != Concurrent {
		t.Fatalf("independent events must be concurrent, got %v", rel)
	}

	// B learns about A, so everything after is causally after A's event.
	b2 := b.Observe(a1)
	if rel := CompareVectors(a1, b2); rel != Before {
		t.Fatalf("after observing, B must be after A: %v", rel)
	}
	if rel := CompareVectors(b2, b1); rel != After {
		t.Fatalf("B's later event must be after its earlier one: %v", rel)
	}
	t.Logf("a1=%s b1=%s b2=%s", Format(a1), Format(b1), Format(b2))

	// A pair of replicas that each saw a different user edit: true concurrency.
	a2 := a.Tick()
	if rel := CompareVectors(a2, b2); rel != Concurrent {
		t.Fatalf("expected concurrency, got %v", rel)
	}
}

func TestVectorClockEqualState(t *testing.T) {
	a := map[string]uint64{"X": 3, "Y": 1}
	b := map[string]uint64{"X": 3, "Y": 1}
	if rel := CompareVectors(a, b); rel != Equal {
		t.Fatalf("identical clocks must compare equal, got %v", rel)
	}
	c := map[string]uint64{"X": 3}
	if rel := CompareVectors(a, c); rel != After {
		t.Fatalf("a superset must be after, got %v", rel)
	}
}

// An HLC stays monotonic even when the wall clock steps backwards.
func TestHLCSurvivesBackwardsClock(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := base
	h := NewHLC(time.Second)
	h.SetNow(func() time.Time { return clock })

	first := h.Now()
	// NTP steps the clock back an hour.
	clock = base.Add(-time.Hour)
	second := h.Now()
	third := h.Now()

	if second.Compare(first) <= 0 {
		t.Fatalf("HLC went backwards: %s -> %s", first, second)
	}
	if third.Compare(second) <= 0 {
		t.Fatalf("HLC not monotonic: %s -> %s", second, third)
	}
	t.Logf("wall clock jumped -1h; HLC still produced %s, %s, %s", first, second, third)

	// Once real time catches up, HLC resumes tracking it.
	clock = base.Add(2 * time.Hour)
	later := h.Now()
	if later.Physical <= third.Physical {
		t.Fatalf("HLC should resume tracking wall time: %s vs %s", later, third)
	}
}

// A message stamped by a node whose clock is wildly ahead must be rejected, not
// absorbed: absorbing it would drag this node's clock into the future.
func TestHLCRejectsExcessiveSkew(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	h := NewHLC(500 * time.Millisecond)
	h.SetNow(func() time.Time { return base })

	// 5 seconds ahead: far beyond the 500ms tolerance.
	remote := Timestamp{Physical: base.Add(5 * time.Second).UnixMilli(), Logical: 0}
	if _, err := h.Update(remote, remote.Physical); err == nil {
		t.Fatal("expected the clock skew check to reject a 5s-ahead timestamp")
	}

	// 100ms ahead: acceptable, and it pulls us forward.
	remote = Timestamp{Physical: base.Add(100 * time.Millisecond).UnixMilli(), Logical: 7}
	got, err := h.Update(remote, remote.Physical)
	if err != nil {
		t.Fatalf("100ms skew should be tolerated: %v", err)
	}
	if got.Compare(remote) <= 0 {
		t.Fatalf("our timestamp after absorbing must exceed the remote one: %s vs %s", got, remote)
	}
}

// Snowflake ids are unique, sortable, and refuse to be generated after a
// backwards clock — the safe failure mode.
func TestSnowflakeUniquenessAndRollback(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := base
	s, err := NewSnowflake(7)
	if err != nil {
		t.Fatal(err)
	}
	s.SetNow(func() time.Time { return clock })

	seen := map[uint64]bool{}
	var last uint64
	for i := 0; i < 5000; i++ {
		id, err := s.Next()
		if err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		}
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
		if id <= last {
			t.Fatalf("ids must increase: %d then %d", last, id)
		}
		last = id
		if i%100 == 99 {
			clock = clock.Add(time.Millisecond)
		}
	}

	// Two nodes in the same millisecond never collide.
	other, _ := NewSnowflake(8)
	other.SetNow(func() time.Time { return clock })
	for i := 0; i < 100; i++ {
		a, _ := s.Next()
		b, _ := other.Next()
		if a == b {
			t.Fatal("ids from different nodes collided")
		}
	}

	// The clock jumps back: we must refuse rather than risk duplicates.
	clock = clock.Add(-time.Second)
	if _, err := s.Next(); err == nil {
		t.Fatal("expected an error after the clock moved backwards")
	}

	ms, node, _ := DecodeSnowflake(last)
	if node != 7 {
		t.Fatalf("decoded node = %d, want 7", node)
	}
	t.Logf("last id decodes to ms=%d node=%d", ms, node)
}
