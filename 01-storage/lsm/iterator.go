package lsm

import (
	"container/heap"
	"io"
)

// iter is the common contract for every record source in the engine. Having
// one interface for memtables and SSTables is what lets compaction be a pure
// k-way merge, and what gives LSM engines free range scans (the property
// bitcask cannot offer).
type iter interface {
	valid() bool
	entry() entry
	next()
	seek(target string)
	err() error
	close() error
}

// ------------------------------------------------------------- memtable iter --

type memIter struct {
	m    *memTable
	n    *slNode
	done bool
}

func (it *memIter) valid() bool { return !it.done && it.n != nil }
func (it *memIter) entry() entry {
	return entry{key: it.n.key, value: it.n.value, typ: it.n.typ}
}
func (it *memIter) next() {
	it.m.mu.RLock()
	n := it.n
	if n != nil {
		it.n = n.next[0]
	}
	it.m.mu.RUnlock()
	if it.n == nil {
		it.done = true
	}
}
func (it *memIter) seek(target string) {
	it.m.mu.RLock()
	defer it.m.mu.RUnlock()
	x := it.m.head
	for i := it.m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < target {
			x = x.next[i]
		}
	}
	it.n = x.next[0]
	it.done = it.n == nil
}
func (it *memIter) err() error   { return nil }
func (it *memIter) close() error { return nil }

func (m *memTable) iter() *memIter {
	m.mu.RLock()
	first := m.head.next[0]
	m.mu.RUnlock()
	return &memIter{m: m, n: first, done: first == nil}
}

// --------------------------------------------------------------- table iter --

type tableIter struct {
	sr   *seqReader
	cur  entry
	has  bool
	done bool
	errv error
}

func (it *tableIter) valid() bool  { return it.has }
func (it *tableIter) entry() entry { return it.cur }
func (it *tableIter) err() error   { return it.errv }
func (it *tableIter) close() error { return nil }

func (it *tableIter) advance() {
	if it.done {
		it.has = false
		return
	}
	e, err := it.sr.readEntry()
	if err != nil {
		if err != io.EOF {
			it.errv = err
		}
		it.done, it.has = true, false
		return
	}
	it.cur, it.has = e, true
}

func (it *tableIter) next() { it.advance() }

func (it *tableIter) seek(target string) {
	for it.valid() && it.cur.key < target {
		it.advance()
	}
}

// --------------------------------------------------------------- merge iter --

// rankedIter tags a source with its rank. Rank matters only for equal keys:
// lower rank means "newer", so the merge iterator keeps the youngest value and
// silently shadows the rest. This is how LSMs do update and delete.
type rankedIter struct {
	iter
	rank int
}

type srcHeap []*rankedIter

func (h srcHeap) Len() int      { return len(h) }
func (h srcHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h srcHeap) Less(i, j int) bool {
	a, b := h[i].entry().key, h[j].entry().key
	if a != b {
		return a < b
	}
	return h[i].rank < h[j].rank
}
func (h *srcHeap) Push(x any) { *h = append(*h, x.(*rankedIter)) }
func (h *srcHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

type mergeIter struct {
	src  []*rankedIter
	h    srcHeap
	cur  entry
	has  bool
	errv error
}

// newMergeIter merges sources that must be given newest-first.
func newMergeIter(iters ...iter) *mergeIter {
	mi := &mergeIter{}
	for i, it := range iters {
		if it == nil {
			continue
		}
		ri := &rankedIter{iter: it, rank: i}
		mi.src = append(mi.src, ri)
		if ri.valid() {
			mi.h = append(mi.h, ri)
		}
	}
	heap.Init(&mi.h)
	mi.advance()
	return mi
}

func (mi *mergeIter) advance() {
	if mi.h.Len() == 0 {
		mi.has = false
		return
	}
	top := heap.Pop(&mi.h).(*rankedIter)
	key := top.entry().key
	mi.cur, mi.has = top.entry(), true
	top.next()
	if err := top.err(); err != nil {
		mi.errv = err
	}
	if top.valid() {
		heap.Push(&mi.h, top)
	}
	// Drop every older record with the same key: it is shadowed.
	for mi.h.Len() > 0 && mi.h[0].entry().key == key {
		dup := heap.Pop(&mi.h).(*rankedIter)
		dup.next()
		if err := dup.err(); err != nil {
			mi.errv = err
		}
		if dup.valid() {
			heap.Push(&mi.h, dup)
		}
	}
}

func (mi *mergeIter) valid() bool  { return mi.has }
func (mi *mergeIter) entry() entry { return mi.cur }
func (mi *mergeIter) next()        { mi.advance() }
func (mi *mergeIter) err() error   { return mi.errv }

func (mi *mergeIter) seek(target string) {
	mi.h = mi.h[:0]
	for _, r := range mi.src {
		r.seek(target)
		if r.valid() {
			mi.h = append(mi.h, r)
		}
	}
	heap.Init(&mi.h)
	mi.advance()
}

func (mi *mergeIter) close() error {
	var first error
	for _, r := range mi.src {
		if err := r.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// ------------------------------------------------------------------ user iter --

// Iterator is the public, user-facing cursor. It yields live keys in sorted
// order and hides deletes by filtering winning tombstones.
type Iterator struct {
	db   *DB
	mi   *mergeIter
	key  string
	val  []byte
	ok   bool
	done bool
}

func (it *Iterator) advance() {
	for it.mi.valid() {
		e := it.mi.entry()
		it.mi.next()
		if e.isDelete() {
			continue // deleted key: skip, do not surface it
		}
		it.key, it.val, it.ok = e.key, clone(e.value), true
		return
	}
	it.ok, it.done = false, true
}

// Valid reports whether the cursor is on a live key.
func (it *Iterator) Valid() bool { return it.ok }

// Key returns the current key.
func (it *Iterator) Key() string { return it.key }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.val }

// Next moves to the next live key.
func (it *Iterator) Next() { it.advance() }

// Seek repositions the cursor at the first live key >= target.
func (it *Iterator) Seek(target string) {
	it.mi.seek(target)
	it.done = false
	it.advance()
}

// Err returns the first error seen while scanning.
func (it *Iterator) Err() error { return it.mi.err() }

// Close releases the snapshot. While an Iterator is open, writers block, so
// close it as soon as you are done.
func (it *Iterator) Close() {
	_ = it.mi.close()
	it.db.mu.RUnlock()
}
