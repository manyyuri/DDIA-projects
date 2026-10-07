// Package mvcc implements DDIA §7 from the inside: multi-version concurrency
// control with snapshot isolation.
//
// The idea that makes MVCC work: a write never overwrites anything. It appends
// a new version stamped with the timestamp at which it committed, and marks the
// previous version as ending at that timestamp. Readers therefore never block
// writers and writers never block readers — a read just picks, for each key,
// the newest version whose lifetime covers its snapshot timestamp.
//
// Snapshot isolation is *not* serializability, and the gap has a name:
// write skew. Two transactions can each read a set of rows, each write based on
// what it read, and commit — as long as they did not write the *same* row.
// The classic example (DDIA §7.2.3) is two doctors both going off call.
//
//	Snapshot     two txns may read the same data and write disjoint keys
//	Serializable adds validation of the read set (this package uses OCC-style
//	             read validation, which is more conservative than Postgres' SSI)
package mvcc

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	// ErrConflict is returned when commit validation fails. The application is
	// expected to retry the whole transaction.
	ErrConflict = errors.New("mvcc: serialization conflict, retry the transaction")
	// ErrTxnDone is returned when using a committed or rolled-back transaction.
	ErrTxnDone = errors.New("mvcc: transaction already finished")
	// ErrNotFound means the key has no visible version in this snapshot.
	ErrNotFound = errors.New("mvcc: key not found")
)

// Isolation is the guarantee a transaction asks for.
type Isolation int

const (
	// Snapshot gives repeatable reads plus write-write conflict detection.
	Snapshot Isolation = iota
	// Serializable additionally validates the read set at commit time.
	Serializable
)

func (i Isolation) String() string {
	if i == Serializable {
		return "serializable"
	}
	return "snapshot"
}

// version is one committed value with the interval during which it is visible.
//
//	visible to a snapshot at ts  <=>  startTS <= ts < endTS (endTS==0 means "now")
type version struct {
	value   string
	startTS uint64
	endTS   uint64 // 0 = still the newest
	deleted bool
}

// visibleAt reports whether this version is part of the snapshot at ts.
func (v version) visibleAt(ts uint64) bool {
	if v.startTS > ts {
		return false // written after our snapshot
	}
	return v.endTS == 0 || v.endTS > ts
}

func (v version) String() string {
	tag := v.value
	if v.deleted {
		tag = "<del>"
	}
	end := "∞"
	if v.endTS != 0 {
		end = fmt.Sprint(v.endTS)
	}
	return fmt.Sprintf("%s[%d..%s)", tag, v.startTS, end)
}

// DB is an in-memory MVCC store.
type DB struct {
	mu       sync.Mutex
	versions map[string][]version // newest first
	nextTS   uint64
	active   map[*Txn]bool

	stats struct {
		commits, aborts, conflicts, gcRemoved int
	}
}

// New creates an empty database.
func New() *DB {
	return &DB{versions: map[string][]version{}, active: map[*Txn]bool{}}
}

// Txn is a transaction: a snapshot timestamp, a read set and a write set.
type Txn struct {
	db     *DB
	iso    Isolation
	readTS uint64

	writes map[string]writeOp
	reads  map[string]bool

	done       bool
	snapshotOf uint64
}

type writeOp struct {
	value   string
	deleted bool
}

// Begin starts a transaction. Its snapshot sees every write that committed
// before this call.
func (db *DB) Begin(iso Isolation) *Txn {
	db.mu.Lock()
	defer db.mu.Unlock()
	t := &Txn{
		db:     db,
		iso:    iso,
		readTS: db.nextTS,
		writes: map[string]writeOp{},
		reads:  map[string]bool{},
	}
	db.active[t] = true
	return t
}

// ReadTS exposes the snapshot timestamp.
func (t *Txn) ReadTS() uint64 { return t.readTS }

// Isolation returns the level the transaction was started with.
func (t *Txn) Isolation() Isolation { return t.iso }

// Get returns the value visible to this transaction's snapshot.
func (t *Txn) Get(key string) (string, error) {
	t.mustBeOpen()
	// A transaction always sees its own writes.
	if w, ok := t.writes[key]; ok {
		if w.deleted {
			return "", ErrNotFound
		}
		return w.value, nil
	}
	t.reads[key] = true // recorded for Serializable validation
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	v, ok := t.db.lookupLocked(key, t.readTS)
	if !ok || v.deleted {
		return "", ErrNotFound
	}
	return v.value, nil
}

// Put stages a write. Nothing becomes visible until Commit.
func (t *Txn) Put(key, value string) error {
	t.mustBeOpen()
	t.writes[key] = writeOp{value: value}
	return nil
}

// Delete stages a tombstone.
func (t *Txn) Delete(key string) error {
	t.mustBeOpen()
	t.writes[key] = writeOp{deleted: true}
	return nil
}

// Scan returns the live keys in sorted order as of the snapshot.
func (t *Txn) Scan() map[string]string {
	t.mustBeOpen()
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	out := map[string]string{}
	for k := range t.db.versions {
		v, ok := t.db.lookupLocked(k, t.readTS)
		if ok && !v.deleted {
			out[k] = v.value
		}
	}
	for k, w := range t.writes {
		if w.deleted {
			delete(out, k)
		} else {
			out[k] = w.value
		}
	}
	return out
}

// Commit validates and applies the write set.
func (t *Txn) Commit() error {
	t.mustBeOpen()
	db := t.db
	db.mu.Lock()
	defer db.mu.Unlock()

	// Serializable: validate the *read* set first. This is the check that
	// catches write skew — a key we read changed under us, so our decision was
	// based on data that no longer holds.
	if t.iso == Serializable {
		for k := range t.reads {
			if _, wrote := t.writes[k]; wrote {
				continue // we are about to overwrite it anyway
			}
			if db.changedAfterLocked(k, t.readTS) {
				db.stats.aborts++
				db.stats.conflicts++
				t.finishLocked()
				return fmt.Errorf("%w: read set key %q changed after snapshot %d", ErrConflict, k, t.readTS)
			}
		}
	}

	// Both levels: first-committer-wins on the write set. Without this, snapshot
	// isolation would allow lost updates.
	for k := range t.writes {
		if db.changedAfterLocked(k, t.readTS) {
			db.stats.aborts++
			db.stats.conflicts++
			t.finishLocked()
			return fmt.Errorf("%w: key %q was modified by a concurrent transaction", ErrConflict, k)
		}
	}

	db.nextTS++
	commitTS := db.nextTS

	// Deterministic order keeps reproducing tests stable.
	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		w := t.writes[k]
		vs := db.versions[k]
		if len(vs) > 0 && vs[0].endTS == 0 {
			vs[0].endTS = commitTS // the previous version stops being visible now
		}
		nv := version{value: w.value, startTS: commitTS, deleted: w.deleted}
		db.versions[k] = append([]version{nv}, vs...)
	}
	db.stats.commits++
	t.finishLocked()
	return nil
}

// Rollback discards the write set.
func (t *Txn) Rollback() {
	t.mustBeOpen()
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.finishLocked()
}

func (t *Txn) finishLocked() {
	t.done = true
	delete(t.db.active, t)
}

func (t *Txn) mustBeOpen() {
	if t.done {
		panic("mvcc: transaction used after Commit/Rollback")
	}
}

func (db *DB) lookupLocked(key string, ts uint64) (version, bool) {
	for _, v := range db.versions[key] {
		if v.visibleAt(ts) {
			return v, true
		}
	}
	return version{}, false
}

// changedAfterLocked reports whether key gained a new version after ts.
func (db *DB) changedAfterLocked(key string, ts uint64) bool {
	for _, v := range db.versions[key] {
		if v.startTS > ts {
			return true
		}
	}
	return false
}

// History returns a key's version chain, oldest first, for inspection.
func (db *DB) History(key string) []string {
	db.mu.Lock()
	defer db.mu.Unlock()
	vs := db.versions[key]
	out := make([]string, 0, len(vs))
	for i := len(vs) - 1; i >= 0; i-- {
		out = append(out, vs[i].String())
	}
	return out
}

// LiveVersions counts the versions currently retained.
func (db *DB) LiveVersions() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	n := 0
	for _, vs := range db.versions {
		n += len(vs)
	}
	return n
}

// GC discards versions that no active transaction can still need. This is the
// maintenance job that keeps MVCC from growing forever: you can only delete a
// version once no snapshot can see it.
func (db *DB) GC() int {
	db.mu.Lock()
	defer db.mu.Unlock()

	// The oldest snapshot still in flight pins history.
	minTS := db.nextTS
	for t := range db.active {
		if t.readTS < minTS {
			minTS = t.readTS
		}
	}
	removed := 0
	for k, vs := range db.versions {
		kept := make([]version, 0, len(vs))
		for _, v := range vs {
			if v.endTS != 0 && v.endTS <= minTS {
				removed++ // superseded before every live snapshot: unreachable
				continue
			}
			kept = append(kept, v)
		}
		if len(kept) == 0 {
			delete(db.versions, k)
			continue
		}
		db.versions[k] = kept
	}
	db.stats.gcRemoved += removed
	return removed
}

// Stats is a snapshot of transaction counters.
type Stats struct {
	Commits     int
	Aborts      int
	Conflicts   int
	GCRemoved   int
	ActiveTxns  int
	LiveVersion int
}

// Stats returns the counters.
func (db *DB) Stats() Stats {
	db.mu.Lock()
	defer db.mu.Unlock()
	live := 0
	for _, vs := range db.versions {
		live += len(vs)
	}
	return Stats{
		Commits:     db.stats.commits,
		Aborts:      db.stats.aborts,
		Conflicts:   db.stats.conflicts,
		GCRemoved:   db.stats.gcRemoved,
		ActiveTxns:  len(db.active),
		LiveVersion: live,
	}
}
