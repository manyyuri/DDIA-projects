// Package windowing implements DDIA §11.3-11.5: event-time processing.
//
// The three confusions this package exists to clear up:
//
//  1. Processing time != event time. A stream processor's "now" says nothing
//     about when the events it is receiving actually happened. Windowing on
//     event time is what makes results reproducible on replay.
//  2. You cannot know an event is the last one for a window. A watermark is a
//     *heuristic* ("I believe nothing older than T will arrive"), and its
//     lateness setting is a trade-off between completeness and latency.
//  3. Late data is normal. The choices are: drop it, update the result
//     (allowed lateness), or route it to a side output for human attention.
//
// Exactly-once is handled at the sink, not the source: an idempotent upsert
// keyed by (window, key) makes replay produce the same *result*, even though
// the write happens again.
package windowing

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Event is one observation with an event-time stamp.
type Event struct {
	Key       string
	Value     float64
	EventTime time.Time
}

func (e Event) String() string {
	return fmt.Sprintf("%s@%s=%.1f", e.Key, e.EventTime.Format("15:04:05"), e.Value)
}

// Window is a half-open time interval [Start, End).
type Window struct {
	Start time.Time
	End   time.Time
}

func (w Window) String() string {
	return fmt.Sprintf("[%s,%s)", w.Start.Format("15:04:05"), w.End.Format("15:04:05"))
}

// ------------------------------------------------------------- assigners --

// Tumbling assigns each event to exactly one fixed-size window.
type Tumbling struct{ Size time.Duration }

// WindowsOf implements the assignment.
func (t Tumbling) WindowsOf(e Event) []Window {
	start := e.EventTime.Truncate(t.Size)
	return []Window{{Start: start, End: start.Add(t.Size)}}
}

// Sliding assigns each event to every window of length Size that contains it,
// sliding every Slide. One event therefore appears in several windows — which
// is why sliding windows are more expensive than they look.
type Sliding struct {
	Size  time.Duration
	Slide time.Duration
}

// WindowsOf implements the assignment.
func (s Sliding) WindowsOf(e Event) []Window {
	if s.Slide <= 0 {
		s.Slide = s.Size
	}
	var out []Window
	// The last window that may contain e starts at or before e.EventTime.
	latest := e.EventTime.Truncate(s.Slide)
	for start := latest; start.Add(s.Size).After(e.EventTime); start = start.Add(-s.Slide) {
		if !start.After(e.EventTime) {
			out = append(out, Window{Start: start, End: start.Add(s.Size)})
		}
		if len(out) > 1024 {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// ------------------------------------------------------------ watermark --

// WatermarkGenerator advances event time by tracking the largest timestamp seen
// and subtracting a bound on out-of-orderness. The more disorder you expect,
// the later every result is emitted.
type WatermarkGenerator struct {
	MaxOutOfOrderness time.Duration
	maxSeen           time.Time
	idle              bool
}

// Observe records an event's timestamp.
func (g *WatermarkGenerator) Observe(e Event) {
	if e.EventTime.After(g.maxSeen) {
		g.maxSeen = e.EventTime
	}
	g.idle = false
}

// Watermark returns the current watermark: "no event older than this should
// still arrive".
func (g *WatermarkGenerator) Watermark() time.Time {
	if g.maxSeen.IsZero() {
		return time.Time{}
	}
	return g.maxSeen.Add(-g.MaxOutOfOrderness)
}

// ------------------------------------------------------------------ sink --

// Sink is an idempotent upsert sink. Writing the same (window, key) twice
// produces the same state, which is what turns at-least-once delivery into
// exactly-once *results*.
type Sink struct {
	mu       sync.Mutex
	state    map[string]float64
	Attempts int // physical writes
	Logical  int // distinct keys written
}

// NewSink creates an empty sink.
func NewSink() *Sink { return &Sink{state: map[string]float64{}} }

// Upsert writes a value under a key. Idempotent by construction.
func (s *Sink) Upsert(key string, value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Attempts++
	if _, exists := s.state[key]; !exists {
		s.Logical++
	}
	s.state[key] = value
}

// Get reads a value back.
func (s *Sink) Get(key string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.state[key]
	return v, ok
}

// Snapshot copies the sink state (for replay comparisons).
func (s *Sink) Snapshot() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]float64, len(s.state))
	for k, v := range s.state {
		out[k] = v
	}
	return out
}

// ---------------------------------------------------------------- pipeline --

// Emit is one result handed to the sink (or retracted and re-emitted).
type Emit struct {
	Window   Window
	Key      string
	Sum      float64
	Count    int
	Revision int // >1 means a late event changed an already-emitted result
}

type paneKey struct {
	windowStart int64
	key         string
}

type pane struct {
	window   Window
	sum      float64
	count    int
	emitted  bool
	dirty    bool // a late event changed an already-emitted result
	revision int
}

// Pipeline is a keyed windowed aggregation over event time.
type Pipeline struct {
	Assigner        func(Event) []Window
	AllowedLateness time.Duration
	Sink            *Sink
	Watermark       WatermarkGenerator

	mu         sync.Mutex
	panes      map[paneKey]*pane
	watermark  time.Time
	SideOutput []Event // events that arrived too late to be included
	Emits      []Emit
	maxSeen    time.Time
}

// NewPipeline builds a tumbling-window pipeline.
func NewPipeline(size, maxOutOfOrderness, allowedLateness time.Duration) *Pipeline {
	t := Tumbling{Size: size}
	return &Pipeline{
		Assigner:        t.WindowsOf,
		AllowedLateness: allowedLateness,
		Sink:            NewSink(),
		Watermark:       WatermarkGenerator{MaxOutOfOrderness: maxOutOfOrderness},
		panes:           map[paneKey]*pane{},
	}
}

// NewSlidingPipeline builds a sliding-window pipeline.
func NewSlidingPipeline(size, slide, maxOutOfOrderness, allowedLateness time.Duration) *Pipeline {
	s := Sliding{Size: size, Slide: slide}
	return &Pipeline{
		Assigner:        s.WindowsOf,
		AllowedLateness: allowedLateness,
		Sink:            NewSink(),
		Watermark:       WatermarkGenerator{MaxOutOfOrderness: maxOutOfOrderness},
		panes:           map[paneKey]*pane{},
	}
}

// Process ingests one event and fires whatever windows have become due.
func (p *Pipeline) Process(e Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e.EventTime.After(p.maxSeen) {
		p.maxSeen = e.EventTime
	}
	p.Watermark.Observe(e)
	p.watermark = p.Watermark.Watermark()

	for _, w := range p.Assigner(e) {
		// Lateness is measured against the watermark, not against the event:
		// once the watermark passes windowEnd + allowedLateness the state is
		// gone and the event is unrecoverable. It goes to a side output rather
		// than silently skewing a result somebody already read.
		if !p.watermark.IsZero() && p.watermark.After(w.End.Add(p.AllowedLateness)) {
			p.SideOutput = append(p.SideOutput, e)
			continue
		}
		pk := paneKey{windowStart: w.Start.UnixNano(), key: e.Key}
		pn := p.panes[pk]
		if pn == nil {
			pn = &pane{window: w}
			p.panes[pk] = pn
		}
		if pn.emitted {
			pn.dirty = true // the aggregate changed after we published it
		}
		pn.sum += e.Value
		pn.count++
	}
	p.fireDueLocked()
}

// AdvanceWatermark lets a test push the watermark forward without events
// (a silent input, or the end of a batch).
func (p *Pipeline) AdvanceWatermark(t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t.After(p.maxSeen) {
		p.maxSeen = t
	}
	p.Watermark.Observe(Event{EventTime: t})
	p.watermark = p.Watermark.Watermark()
	p.fireDueLocked()
}

func (p *Pipeline) fireDueLocked() {
	keys := make([]paneKey, 0, len(p.panes))
	for k := range p.panes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].windowStart != keys[j].windowStart {
			return keys[i].windowStart < keys[j].windowStart
		}
		return keys[i].key < keys[j].key
	})

	for _, k := range keys {
		pn := p.panes[k]
		if p.watermark.IsZero() || !p.watermark.After(pn.window.End) {
			continue
		}
		// A window is final once the watermark passes its end plus the allowed
		// lateness: after that its state is discarded, and late events for it
		// become dead letters.
		final := p.watermark.After(pn.window.End.Add(p.AllowedLateness))

		switch {
		case !pn.emitted:
			pn.revision = 1
			p.EmitLocked(Emit{Window: pn.window, Key: k.key, Sum: pn.sum, Count: pn.count, Revision: 1})
			pn.emitted, pn.dirty = true, false
		case pn.dirty:
			// Late data changed the aggregate after we published it: publish a
			// correction. A real system also emits a retraction of the old value
			// (DDIA §11.3), so downstream consumers can subtract it.
			pn.revision++
			p.EmitLocked(Emit{Window: pn.window, Key: k.key, Sum: pn.sum, Count: pn.count, Revision: pn.revision})
			pn.dirty = false
		}
		if final {
			delete(p.panes, k)
		}
	}
}

func (p *Pipeline) key(k paneKey) string { return k.key }

func (p *Pipeline) EmitLocked(e Emit) {
	p.Emits = append(p.Emits, e)
	if p.Sink != nil {
		p.Sink.Upsert(fmt.Sprintf("%s|%s", e.Window, e.Key), e.Sum)
	}
}

// Emitted returns the emission log.
func (p *Pipeline) Emitted() []Emit {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Emit(nil), p.Emits...)
}

// TooLate returns the dead-lettered events.
func (p *Pipeline) TooLate() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Event(nil), p.SideOutput...)
}

// State returns the live pane state (this is what a checkpoint would persist).
type State struct {
	Watermark time.Time
	MaxSeen   time.Time
	Panes     map[string]float64
}

// Checkpoint captures the operator state so it can be restored on replay.
func (p *Pipeline) Checkpoint() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := State{Watermark: p.watermark, MaxSeen: p.maxSeen, Panes: map[string]float64{}}
	for k, pn := range p.panes {
		st.Panes[fmt.Sprintf("%d|%s", k.windowStart, k.key)] = pn.sum
	}
	return st
}
