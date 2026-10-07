package windowing

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func TestTumblingAssignsOneWindowPerEvent(t *testing.T) {
	a := Tumbling{Size: 10 * time.Second}
	for _, tc := range []struct {
		sec       int
		wantStart int
	}{
		{0, 0}, {9, 0}, {10, 10}, {19, 10}, {25, 20},
	} {
		ws := a.WindowsOf(Event{EventTime: at(tc.sec)})
		if len(ws) != 1 {
			t.Fatalf("event at %ds landed in %d windows", tc.sec, len(ws))
		}
		if got := int(ws[0].Start.Sub(t0).Seconds()); got != tc.wantStart {
			t.Fatalf("event at %ds -> window starting %ds, want %ds", tc.sec, got, tc.wantStart)
		}
	}
}

func TestSlidingAssignsOverlappingWindows(t *testing.T) {
	a := Sliding{Size: 10 * time.Second, Slide: 5 * time.Second}
	ws := a.WindowsOf(Event{EventTime: at(12)})
	if len(ws) != 2 {
		t.Fatalf("event at 12s belongs to 2 sliding windows, got %d: %v", len(ws), ws)
	}
	// One event counted in two windows is exactly why sliding windows cost more.
	t.Logf("event at 12s -> %v", ws)
}

// The watermark is a promise: results fire only after it passes the window end.
func TestWatermarkDrivesEmission(t *testing.T) {
	p := NewPipeline(10*time.Second, 2*time.Second, 0)

	p.Process(Event{Key: "a", Value: 1, EventTime: at(1)})
	p.Process(Event{Key: "a", Value: 2, EventTime: at(3)})
	if len(p.Emitted()) != 0 {
		t.Fatalf("nothing should fire before the watermark passes the window: %v", p.Emitted())
	}

	// An event at 11s pushes the watermark to 9s: still inside the first window.
	p.Process(Event{Key: "a", Value: 1, EventTime: at(11)})
	if len(p.Emitted()) != 0 {
		t.Fatalf("watermark 9s must not close the [0,10) window: %v", p.Emitted())
	}

	// An event at 13s pushes the watermark to 11s > 10s: fire.
	p.Process(Event{Key: "a", Value: 1, EventTime: at(13)})
	emits := p.Emitted()
	if len(emits) != 1 {
		t.Fatalf("expected exactly one emission, got %v", emits)
	}
	if emits[0].Sum != 3 || emits[0].Count != 2 {
		t.Fatalf("window aggregate = %+v, want sum=3 count=2", emits[0])
	}
	if v, ok := p.Sink.Get(fmt.Sprintf("%s|a", emits[0].Window)); !ok || v != 3 {
		t.Fatalf("sink does not hold the window result: %v %v", v, ok)
	}
	t.Logf("emitted %+v after watermark reached %s", emits[0], p.Watermark.Watermark().Format("15:04:05"))
}

// A late-but-tolerated event updates the already-emitted result. The sink is
// upserted, so a downstream reader sees a corrected number rather than a
// duplicate row.
func TestLateEventWithinAllowedLatenessUpdatesResult(t *testing.T) {
	p := NewPipeline(10*time.Second, 2*time.Second, 5*time.Second)

	p.Process(Event{Key: "a", Value: 1, EventTime: at(1)})
	// 14s pushes the watermark to 12s: past the window end (10s) but still
	// inside the lateness allowance (10s + 5s), so the state is retained.
	p.Process(Event{Key: "a", Value: 1, EventTime: at(14)})
	first := p.Emitted()
	if len(first) != 1 || first[0].Sum != 1 {
		t.Fatalf("first emission = %v", first)
	}

	// An event at 4s arrives now: late, but within the 5s allowance.
	p.Process(Event{Key: "a", Value: 10, EventTime: at(4)})
	emits := p.Emitted()
	if len(emits) != 2 {
		t.Fatalf("expected a re-emission, got %v", emits)
	}
	last := emits[len(emits)-1]
	if last.Sum != 11 || last.Revision < 2 {
		t.Fatalf("late event should have updated the result: %+v", last)
	}
	if v, _ := p.Sink.Get(fmt.Sprintf("%s|a", last.Window)); v != 11 {
		t.Fatalf("sink was not upserted: %v", v)
	}
	t.Logf("result corrected from 1 to 11 after a late event (revision %d)", last.Revision)
}

// Past the allowed lateness the state is gone, so the event cannot be folded in.
// It goes to a side output rather than being silently dropped.
func TestTooLateEventGoesToSideOutput(t *testing.T) {
	p := NewPipeline(10*time.Second, 1*time.Second, 2*time.Second)

	p.Process(Event{Key: "a", Value: 1, EventTime: at(1)})
	p.Process(Event{Key: "a", Value: 1, EventTime: at(30)}) // watermark -> 29s: window closed
	if len(p.Emitted()) == 0 {
		t.Fatal("window should have fired")
	}
	// 5s of lateness is more than the 2s allowance: unrecoverable.
	p.Process(Event{Key: "a", Value: 99, EventTime: at(2)})

	if got := len(p.TooLate()); got != 1 {
		t.Fatalf("expected 1 dead-lettered event, got %d (%v)", got, p.TooLate())
	}
	for _, e := range p.Emitted() {
		if e.Sum == 100 {
			t.Fatalf("a too-late event was folded into a result: %+v", e)
		}
	}
	t.Logf("dead letter: %v", p.TooLate()[0])
}

// Out-of-order events inside the disorder bound produce exactly the same result
// as in-order events. That reproducibility is the whole point of event time.
func TestOutOfOrderWithinBoundIsEquivalentToInOrder(t *testing.T) {
	inOrder := NewPipeline(10*time.Second, 3*time.Second, 0)
	for _, s := range []int{1, 2, 3, 4, 5} {
		inOrder.Process(Event{Key: "a", Value: float64(s), EventTime: at(s)})
	}
	inOrder.AdvanceWatermark(at(20))

	shuffled := NewPipeline(10*time.Second, 3*time.Second, 0)
	for _, s := range []int{5, 1, 4, 2, 3} { // same events, different arrival order
		shuffled.Process(Event{Key: "a", Value: float64(s), EventTime: at(s)})
	}
	shuffled.AdvanceWatermark(at(20))

	if !reflect.DeepEqual(inOrder.Emitted(), shuffled.Emitted()) {
		t.Fatalf("out-of-order delivery changed the result:\n in-order: %v\n shuffled: %v",
			inOrder.Emitted(), shuffled.Emitted())
	}
	t.Logf("both produced %v", inOrder.Emitted())
}

// Replay: at-least-once delivery means the sink may be written twice, but an
// idempotent upsert makes the *result* identical. This is what "exactly-once
// semantics" actually means in practice — exactly-once *effect*, not
// exactly-once delivery.
func TestReplayIsIdempotentAtTheSink(t *testing.T) {
	sink := NewSink()
	events := []Event{
		{Key: "a", Value: 1, EventTime: at(1)},
		{Key: "b", Value: 2, EventTime: at(2)},
		{Key: "a", Value: 3, EventTime: at(3)},
	}

	p := NewPipeline(10*time.Second, 2*time.Second, 0)
	p.Sink = sink
	for _, e := range events {
		p.Process(e)
	}
	p.AdvanceWatermark(at(30))
	before := sink.Snapshot()
	attemptsBefore := sink.Attempts

	// A crash after the sink write but before the offset commit restarts the
	// job from the previous checkpoint and re-reads the same records.
	replay := NewPipeline(10*time.Second, 2*time.Second, 0)
	replay.Sink = sink
	for _, e := range events {
		replay.Process(e)
	}
	replay.AdvanceWatermark(at(30))

	after := sink.Snapshot()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("replay changed the sink state:\n before: %v\n after:  %v", before, after)
	}
	if sink.Attempts <= attemptsBefore {
		t.Fatal("expected the physical writes to happen again")
	}
	t.Logf("physical writes %d -> %d, but the visible state is identical (%d rows)",
		attemptsBefore, sink.Attempts, sink.Logical)
}

func TestCheckpointCapturesOperatorState(t *testing.T) {
	p := NewPipeline(10*time.Second, 2*time.Second, 0)
	p.Process(Event{Key: "a", Value: 5, EventTime: at(1)})
	st := p.Checkpoint()
	if len(st.Panes) != 1 {
		t.Fatalf("checkpoint should hold the open pane: %+v", st)
	}
	if v := st.Panes[fmt.Sprintf("%d|a", at(0).UnixNano())]; v != 5 {
		t.Fatalf("pane sum not checkpointed: %+v", st.Panes)
	}
	t.Logf("checkpoint: watermark=%s panes=%v", st.Watermark, st.Panes)
}

func TestSlidingPipelineCountsAnEventTwice(t *testing.T) {
	p := NewSlidingPipeline(10*time.Second, 5*time.Second, 1*time.Second, 0)
	p.Process(Event{Key: "a", Value: 1, EventTime: at(12)})
	p.AdvanceWatermark(at(40))

	emits := p.Emitted()
	if len(emits) != 2 {
		t.Fatalf("an event at 12s should appear in 2 sliding windows, got %v", emits)
	}
	for _, e := range emits {
		if e.Sum != 1 {
			t.Fatalf("unexpected aggregate: %+v", e)
		}
	}
	t.Logf("sliding windows emitted %v", emits)
}
