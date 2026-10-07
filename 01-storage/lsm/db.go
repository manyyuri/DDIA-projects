package lsm

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const manifestName = "MANIFEST"

// Options configures the engine.
type Options struct {
	Dir string
	// MemTableSize is the flush trigger, in bytes.
	MemTableSize int64
	// LevelCount is the number of levels, L0 included.
	LevelCount int
	// L0CompactionTrigger is how many L0 tables may pile up before compaction.
	L0CompactionTrigger int
	// LevelSizeBase/LevelSizeRatio: level i+1 may hold ratio times level i.
	LevelSizeBase  int64
	LevelSizeRatio int
	// TableSize is the target SSTable size produced by compaction.
	TableSize int64
	// SyncOnWrite fsyncs the WAL on every write.
	SyncOnWrite bool
}

func (o Options) withDefaults() Options {
	if o.MemTableSize <= 0 {
		o.MemTableSize = 4 << 20
	}
	if o.LevelCount <= 1 {
		o.LevelCount = 7
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = 4
	}
	if o.LevelSizeBase <= 0 {
		o.LevelSizeBase = 8 << 20
	}
	if o.LevelSizeRatio <= 1 {
		o.LevelSizeRatio = 10
	}
	if o.TableSize <= 0 {
		o.TableSize = 2 << 20
	}
	return o
}

// Stats exposes the amplification factors that the whole chapter is about.
type Stats struct {
	Keys              int
	MemTableBytes     int64
	LevelTables       []int
	LevelBytes        []int64
	WALBytes          int64
	DiskBytes         int64
	Flushes           int64
	Compactions       int64
	BytesWritten      int64 // includes WAL + SSTable writes
	BloomFilterSkips  int64
	TablesTouchedRead int64
}

type DB struct {
	opts Options

	mu     sync.RWMutex // guards everything below
	mem    *memTable
	imm    *memTable // being flushed; reads must check it too
	wal    *wal
	walGen uint64
	levels [][]*table // levels[0] == L0, overlapping; levels[1..] sorted, non-overlapping
	nextID uint64
	closed bool

	wmu sync.Mutex // serialises writers

	cFlushes, cCompactions, cBytesWritten, cBloomSkips, cTableReads int64
}

// Open opens or creates a database, then rebuilds state: manifest -> tables,
// WAL -> memtable. Everything after that point is derived data.
func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		opts:   opts,
		mem:    newMemTable(),
		levels: make([][]*table, opts.LevelCount),
		nextID: 1,
	}
	if err := db.loadManifest(); err != nil {
		return nil, err
	}

	// Replay every WAL in generation order. Replays are idempotent, which is
	// what makes "crash during recovery" harmless.
	wals, err := filepath.Glob(filepath.Join(opts.Dir, "wal-*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(wals)
	for _, p := range wals {
		if err := replayWAL(p, func(e entry) {
			db.mem.put(e.key, e.value, e.typ)
		}); err != nil {
			db.closeTables()
			return nil, err
		}
	}
	if bytes, _ := db.mem.size(); bytes > 0 {
		// Make the replayed data durable in an SSTable *before* dropping the
		// WALs, otherwise a crash in between loses committed writes.
		if err := db.flushLocked(); err != nil {
			db.closeTables()
			return nil, err
		}
	}
	for _, p := range wals {
		_ = os.Remove(p)
	}
	if err := db.openNewWAL(); err != nil {
		db.closeTables()
		return nil, err
	}
	return db, nil
}

// ---------------------------------------------------------------- WAL wiring --

func (db *DB) walPath(gen uint64) string {
	return filepath.Join(db.opts.Dir, fmt.Sprintf("wal-%06d.log", gen))
}

func (db *DB) openNewWAL() error {
	db.walGen++
	w, err := openWAL(db.walPath(db.walGen))
	if err != nil {
		return err
	}
	db.wal = w
	return nil
}

// -------------------------------------------------------------------- writes --

// Put stores value under key.
func (db *DB) Put(key, value []byte) error {
	return db.write(entry{key: string(key), value: value, typ: typePut})
}

// Delete writes a tombstone for key. A tombstone, not a removal: the value
// being deleted may still live in lower levels.
func (db *DB) Delete(key []byte) error {
	return db.write(entry{key: string(key), typ: typeDelete})
}

func (db *DB) write(e entry) error {
	db.wmu.Lock()
	defer db.wmu.Unlock()

	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	w := db.wal
	db.mu.RUnlock()

	// 1. Durability first: the WAL record must land before the memtable change
	//    is visible, otherwise a crash loses an acknowledged write.
	if err := w.append(e); err != nil {
		return err
	}
	if db.opts.SyncOnWrite {
		if err := w.sync(); err != nil {
			return err
		}
	}

	// 2. Then the in-memory index.
	db.mu.Lock()
	db.mem.put(e.key, e.value, e.typ)
	bytes, _ := db.mem.size()
	needFlush := bytes >= db.opts.MemTableSize
	db.mu.Unlock()

	if needFlush {
		return db.rotateAndFlush()
	}
	return nil
}

func (db *DB) rotateAndFlush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.imm != nil {
		return nil // somebody already rotated
	}
	if bytes, _ := db.mem.size(); bytes < db.opts.MemTableSize {
		return nil
	}
	oldWAL := db.wal
	db.imm = db.mem
	db.mem = newMemTable()
	if err := db.openNewWAL(); err != nil {
		return err
	}
	// Flush the immutable memtable, then the old WAL can go.
	if err := db.flushImmLocked(); err != nil {
		return err
	}
	_ = oldWAL.close()
	_ = os.Remove(oldWAL.path)
	return nil
}

// flushLocked writes db.mem out to L0. Caller holds db.mu.
func (db *DB) flushLocked() error {
	if bytes, _ := db.mem.size(); bytes == 0 {
		return nil
	}
	db.imm = db.mem
	db.mem = newMemTable()
	return db.flushImmLocked()
}

func (db *DB) flushImmLocked() error {
	if db.imm == nil {
		return nil
	}
	code, count := db.imm.size()
	if count == 0 {
		db.imm = nil
		return nil
	}
	t, err := db.writeTableLocked(db.imm.iter())
	if err != nil {
		return err
	}
	_ = code
	db.levels[0] = append(db.levels[0], t)
	db.imm = nil
	db.cFlushes++
	if err := db.saveManifestLocked(); err != nil {
		return err
	}
	return db.maybeCompactLocked()
}

// writeTableLocked creates a new SSTable file from a sorted iterator.
func (db *DB) writeTableLocked(it iter) (*table, error) {
	id := db.nextID
	db.nextID++
	path := db.tablePath(id)
	t, err := writeTable(path, id, it)
	if err != nil {
		return nil, err
	}
	db.cBytesWritten += t.size
	return t, nil
}

func (db *DB) tablePath(id uint64) string {
	return filepath.Join(db.opts.Dir, fmt.Sprintf("t-%06d.sst", id))
}

// --------------------------------------------------------------------- reads --

// Get returns the newest visible value for key.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	return db.getLocked(string(key))
}

func (db *DB) getLocked(key string) ([]byte, error) {
	e, found, err := db.lookupLocked(key)
	if err != nil {
		return nil, err
	}
	if !found || e.isDelete() {
		return nil, ErrNotFound
	}
	return clone(e.value), nil
}

func (db *DB) lookupLocked(key string) (entry, bool, error) {
	// 1. Newest data is in memory.
	if e, ok := db.mem.get(key); ok {
		return e, true, nil
	}
	if db.imm != nil {
		if e, ok := db.imm.get(key); ok {
			return e, true, nil
		}
	}
	// 2. L0 tables overlap, so they must be searched newest-id first.
	for i := len(db.levels[0]) - 1; i >= 0; i-- {
		t := db.levels[0][i]
		if !t.bloom.mayContain(key) {
			db.cBloomSkips++
			continue
		}
		db.cTableReads++
		e, ok, err := t.get(key)
		if err != nil {
			return entry{}, false, err
		}
		if ok {
			return e, true, nil
		}
	}
	// 3. L1+ are sorted and non-overlapping: one binary search finds the only
	//    table that could hold the key.
	for lvl := 1; lvl < len(db.levels); lvl++ {
		t := findTable(db.levels[lvl], key)
		if t == nil || !t.bloom.mayContain(key) {
			if t != nil {
				db.cBloomSkips++
			}
			continue
		}
		db.cTableReads++
		e, ok, err := t.get(key)
		if err != nil {
			return entry{}, false, err
		}
		if ok {
			return e, true, nil
		}
	}
	return entry{}, false, nil
}

// findTable binary searches a non-overlapping level for the table that may
// contain key.
func findTable(level []*table, key string) *table {
	i := sort.Search(len(level), func(i int) bool { return level[i].maxKey >= key })
	if i < len(level) && level[i].minKey <= key {
		return level[i]
	}
	return nil
}

// NewIterator returns a sorted cursor over a consistent snapshot. Sources are
// layered newest-first, which is what makes shadowing correct.
func (db *DB) NewIterator() *Iterator {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return &Iterator{db: db, done: true, mi: newMergeIter()}
	}
	// The iterator holds the read lock until Close, so the level layout it
	// captured stays valid even if compaction runs.
	sources := []iter{db.mem.iter()}
	if db.imm != nil {
		sources = append(sources, db.imm.iter())
	}
	for i := len(db.levels[0]) - 1; i >= 0; i-- {
		sources = append(sources, db.levels[0][i].iterAll())
	}
	for lvl := 1; lvl < len(db.levels); lvl++ {
		for _, t := range db.levels[lvl] {
			sources = append(sources, t.iterAll())
		}
	}
	it := &Iterator{db: db, mi: newMergeIter(sources...)}
	it.advance()
	return it
}

// ------------------------------------------------------------------ compactions --

// Compact forces one round of levelled compaction, mainly for experiments.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.maybeCompactLocked()
}

// Flush forces the active memtable out to L0.
func (db *DB) Flush() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	old := db.wal
	if err := db.flushLocked(); err != nil {
		return err
	}
	_ = old.close()
	_ = os.Remove(old.path)
	return db.openNewWAL()
}

func (db *DB) maybeCompactLocked() error {
	for round := 0; round < 16; round++ {
		if len(db.levels[0]) >= db.opts.L0CompactionTrigger {
			if err := db.compactLevelLocked(0); err != nil {
				return err
			}
			continue
		}
		didSomething := false
		for lvl := 1; lvl < len(db.levels)-1; lvl++ {
			if db.levelBytesLocked(lvl) > db.levelBudget(lvl) {
				if err := db.compactLevelLocked(lvl); err != nil {
					return err
				}
				didSomething = true
				break
			}
		}
		if !didSomething {
			return nil
		}
	}
	return nil
}

func (db *DB) levelBudget(lvl int) int64 {
	budget := db.opts.LevelSizeBase
	for i := 1; i < lvl; i++ {
		budget *= int64(db.opts.LevelSizeRatio)
	}
	return budget
}

func (db *DB) levelBytesLocked(lvl int) int64 {
	var total int64
	for _, t := range db.levels[lvl] {
		total += t.size
	}
	return total
}

// compactLevelLocked merges every table in `level` with the overlapping tables
// in `level+1` into fresh tables in `level+1`.
//
// Merging a whole level (rather than a key range) keeps the code obvious; real
// engines merge only the overlapping key range to bound write amplification.
func (db *DB) compactLevelLocked(level int) error {
	if level+1 >= len(db.levels) || len(db.levels[level]) == 0 {
		return nil
	}
	inputs := append([]*table(nil), db.levels[level]...)

	var overlap, keep []*table
	for _, t := range db.levels[level+1] {
		if overlapsAny(inputs, t) {
			overlap = append(overlap, t)
		} else {
			keep = append(keep, t)
		}
	}

	// Newest-first ordering: higher level = newer data, and inside a level,
	// higher id = newer.
	sortTablesNewestFirst(inputs)
	sortTablesNewestFirst(overlap)

	sources := make([]iter, 0, len(inputs)+len(overlap))
	for _, t := range inputs {
		sources = append(sources, t.iterAll())
	}
	for _, t := range overlap {
		sources = append(sources, t.iterAll())
	}
	mi := newMergeIter(sources...)
	defer mi.close()

	// Only the last level may drop tombstones: anywhere else an older value
	// could still be sitting below, and dropping the tombstone would resurrect it.
	dropTombstones := level+1 == len(db.levels)-1

	var outputs []*table
	buf := newSpillWriter(db)
	for mi.valid() {
		e := mi.entry()
		mi.next()
		if mi.err() != nil {
			return mi.err()
		}
		if e.isDelete() && dropTombstones {
			continue
		}
		if err := buf.add(e); err != nil {
			return err
		}
	}
	last, err := buf.finish()
	if err != nil {
		return err
	}
	outputs = buf.tables
	if last != nil {
		outputs = append(outputs, last)
	}

	for _, t := range inputs {
		_ = t.remove()
	}
	for _, t := range overlap {
		_ = t.remove()
	}

	db.levels[level] = nil
	db.levels[level+1] = append(keep, outputs...)
	sortTablesByMinKey(db.levels[level+1])
	db.cCompactions++
	return db.saveManifestLocked()
}

// spillWriter accumulates merged records into SSTables of roughly
// opts.TableSize, which is what keeps level-1+ tables non-overlapping.
type spillWriter struct {
	db     *DB
	tables []*table
	cur    []entry
	curLen int64
}

func newSpillWriter(db *DB) *spillWriter { return &spillWriter{db: db} }

func (s *spillWriter) add(e entry) error {
	s.cur = append(s.cur, entry{key: e.key, value: e.value, typ: e.typ})
	s.curLen += int64(len(e.key) + len(e.value) + 16)
	if s.curLen >= s.db.opts.TableSize {
		_, err := s.flush()
		return err
	}
	return nil
}

func (s *spillWriter) flush() (*table, error) {
	if len(s.cur) == 0 {
		return nil, nil
	}
	t, err := s.db.writeTableLocked(&sliceIter{items: s.cur})
	if err != nil {
		return nil, err
	}
	s.tables = append(s.tables, t)
	s.cur = nil
	s.curLen = 0
	return t, nil
}

func (s *spillWriter) finish() (*table, error) {
	return s.flush()
}

// sliceIter is a trivial sorted iterator over an in-memory slice.
type sliceIter struct {
	items []entry
	i     int
}

func (it *sliceIter) valid() bool  { return it.i < len(it.items) }
func (it *sliceIter) entry() entry { return it.items[it.i] }
func (it *sliceIter) next()        { it.i++ }
func (it *sliceIter) err() error   { return nil }
func (it *sliceIter) close() error { return nil }
func (it *sliceIter) seek(t string) {
	for it.i < len(it.items) && it.items[it.i].key < t {
		it.i++
	}
}

func overlapsAny(inputs []*table, t *table) bool {
	for _, in := range inputs {
		if in.minKey <= t.maxKey && in.maxKey >= t.minKey {
			return true
		}
	}
	return false
}

func sortTablesNewestFirst(ts []*table) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].id > ts[j].id })
}

func sortTablesByMinKey(ts []*table) {
	sort.Slice(ts, func(i, j int) bool {
		if ts[i].minKey != ts[j].minKey {
			return ts[i].minKey < ts[j].minKey
		}
		return ts[i].id < ts[j].id
	})
}

// ------------------------------------------------------------------ manifest --

// The MANIFEST is the source of truth for "which SSTables are part of this
// database". It is replaced atomically (write temp, fsync, rename), so a crash
// leaves either the old or the new table set, never a mixture.
func (db *DB) saveManifestLocked() error {
	tmp, err := os.CreateTemp(db.opts.Dir, manifestName+".tmp")
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(tmp)
	for lvl, ts := range db.levels {
		for _, t := range ts {
			fmt.Fprintf(bw, "%d %d\n", lvl, t.id)
		}
	}
	if err := bw.Flush(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(db.opts.Dir, manifestName)); err != nil {
		return err
	}
	return syncDir(db.opts.Dir)
}

func (db *DB) loadManifest() error {
	path := filepath.Join(db.opts.Dir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	known := map[uint64]bool{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var lvl int
		var id uint64
		if _, err := fmt.Sscanf(line, "%d %d", &lvl, &id); err != nil {
			continue
		}
		if lvl < 0 || lvl >= len(db.levels) {
			continue
		}
		t, err := openTable(db.tablePath(id), id, 0)
		if err != nil {
			return fmt.Errorf("open table %d: %w", id, err)
		}
		db.levels[lvl] = append(db.levels[lvl], t)
		known[id] = true
		if id >= db.nextID {
			db.nextID = id + 1
		}
	}
	for lvl := 1; lvl < len(db.levels); lvl++ {
		sortTablesByMinKey(db.levels[lvl])
	}
	// Any .sst not named by the manifest was written by a flush/compaction that
	// never committed: it is garbage, not data.
	orphans, err := filepath.Glob(filepath.Join(db.opts.Dir, "t-*.sst"))
	if err != nil {
		return err
	}
	for _, p := range orphans {
		base := filepath.Base(p)
		id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(base, "t-"), ".sst"), 10, 64)
		if err != nil {
			continue
		}
		if !known[id] {
			_ = os.Remove(p)
		}
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// -------------------------------------------------------------------- closing --

func (db *DB) closeTables() {
	for _, ts := range db.levels {
		for _, t := range ts {
			_ = t.close()
		}
	}
}

// Close flushes the memtable and releases resources.
func (db *DB) Close() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	old := db.wal
	if err := db.flushLocked(); err != nil {
		db.closeTables()
		return err
	}
	_ = old.close()
	_ = os.Remove(old.path)
	db.closeTables()
	return nil
}

// Stats returns amplification-relevant counters.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	bytes, count := db.mem.size()
	st := Stats{
		Keys:              count,
		MemTableBytes:     bytes,
		WALBytes:          db.wal.size,
		Flushes:           db.cFlushes,
		Compactions:       db.cCompactions,
		BytesWritten:      db.cBytesWritten,
		BloomFilterSkips:  db.cBloomSkips,
		TablesTouchedRead: db.cTableReads,
	}
	for lvl := range db.levels {
		var lvlBytes int64
		for _, t := range db.levels[lvl] {
			lvlBytes += t.size
		}
		st.LevelTables = append(st.LevelTables, len(db.levels[lvl]))
		st.LevelBytes = append(st.LevelBytes, lvlBytes)
		st.DiskBytes += lvlBytes
	}
	return st
}
