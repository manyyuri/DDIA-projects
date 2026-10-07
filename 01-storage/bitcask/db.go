// Package bitcask implements the storage engine from DDIA §3.1 ("Hash Indexes"):
//
//	log file (source of truth)  +  in-memory hash map (rebuildable cache)
//
// Writes only ever append; the hash map maps a key to the byte offset of its
// newest record. Reads are one hash lookup plus one pread. Deletes append a
// tombstone. Space is reclaimed by a background-style merge pass.
//
// The design is deliberately small so that the *trade-offs* are visible:
//   - O(1) point reads, no read amplification  <=>  the keydir must fit in RAM
//   - sequential, cheap writes                 <=>  space + write amplification
//     (every update rewrites the whole value)
//   - Range scans are expensive (hash order is not key order)
package bitcask

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const defaultMaxSegmentSize = 64 << 20 // 64 MiB

// Options configures a DB. The zero value (plus Dir) is a sensible default.
type Options struct {
	// Dir is the directory holding segment files.
	Dir string
	// MaxSegmentSize rotates the active segment once it grows past this.
	MaxSegmentSize int64
	// SyncOnWrite fsyncs after every write. Durability vs throughput: with it
	// off (the default) a power loss can lose the last few writes, but the log
	// is still internally consistent because records are checksummed.
	SyncOnWrite bool
	// SyncInterval fsyncs periodically instead of on every write.
	SyncInterval time.Duration
	// VerifyOnRead checksums values read from disk. Off by default: pure CPU
	// tax on the read path, and corruption is normally caught at startup.
	VerifyOnRead bool
	// MaxOpenFiles bounds the segment fd cache.
	MaxOpenFiles int
}

func (o Options) withDefaults() Options {
	if o.MaxSegmentSize <= 0 {
		o.MaxSegmentSize = defaultMaxSegmentSize
	}
	if o.MaxOpenFiles <= 0 {
		o.MaxOpenFiles = 64
	}
	return o
}

// entry is a pointer into the log: where a key's newest value lives.
type entry struct {
	fileID uint32
	offset int64
	size   int64
}

// keydir is the in-memory hash index. It is the only mutable shared state on
// the read path, hence the RWMutex.
type keydir struct {
	mu sync.RWMutex
	m  map[string]entry
}

func (k *keydir) get(key string) (entry, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	e, ok := k.m[key]
	return e, ok
}

func (k *keydir) set(key string, e entry) {
	k.mu.Lock()
	k.m[key] = e
	k.mu.Unlock()
}

func (k *keydir) del(key string) {
	k.mu.Lock()
	delete(k.m, key)
	k.mu.Unlock()
}

func (k *keydir) len() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return len(k.m)
}

func (k *keydir) keys() []string {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make([]string, 0, len(k.m))
	for s := range k.m {
		out = append(out, s)
	}
	return out
}

// Stats is a snapshot of engine counters, used by the experiment scripts.
type Stats struct {
	Keys           int
	Segments       int
	DiskBytes      int64
	Writes         int64
	Reads          int64
	Deletes        int64
	Merges         int64
	Recovered      int64 // records replayed on open
	TruncatedBytes int64 // bytes discarded because of torn/corrupt tail
}

// DB is a Bitcask-style key/value store. It is safe for concurrent use.
type DB struct {
	opts   Options
	keydir *keydir
	files  *fileCache

	wmu sync.Mutex // serialises writers; Bitcask has exactly one writer

	mu       sync.RWMutex // guards segments
	segments []*segment

	closed atomic.Bool
	done   chan struct{}
	wg     sync.WaitGroup

	cWrites, cReads, cDeletes, cMerges, cRecovered, cTruncated atomic.Int64
}

// Open opens (or creates) a database in opts.Dir and replays the log to
// rebuild the keydir.
func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{
		opts:   opts,
		keydir: &keydir{m: make(map[string]entry)},
		files:  newFileCache(opts.Dir, opts.MaxOpenFiles),
		done:   make(chan struct{}),
	}
	segs, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		s := &segment{id: 1, path: filepath.Join(opts.Dir, segmentName(1))}
		f, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		_ = f.Close()
		segs = []*segment{s}
	}
	for i, s := range segs {
		last := i == len(segs)-1
		if err := db.recoverSegment(s, last); err != nil {
			_ = db.files.closeAll()
			return nil, err
		}
	}
	db.segments = segs

	active := segs[len(segs)-1]
	if active.size >= opts.MaxSegmentSize {
		if err := db.rotateLocked(); err != nil {
			_ = db.files.closeAll()
			return nil, err
		}
	}
	if opts.SyncInterval > 0 {
		db.wg.Add(1)
		go db.syncLoop()
	}
	return db, nil
}

// recoverSegment replays one segment from byte 0, rebuilding keydir entries.
//
// A short/corrupt record in the *final* segment is the expected shape of a
// crash mid-append, so the file is truncated there and the process starts.
// The same damage in an older segment means real data loss, so Open fails.
func (db *DB) recoverSegment(s *segment, last bool) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<16)
	var off int64
	for {
		rec, err := readRecordFrom(r, true)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // clean end of log
			}
			tornTail := errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errGarbageLength)
			if !tornTail {
				// The record framing was intact but its checksum did not match.
				// That is silent corruption (bit rot), not an interrupted append:
				// dropping it would silently destroy committed data, so refuse.
				return fmt.Errorf("%w: segment %010d damaged at offset %d", ErrCorrupt, s.id, off)
			}
			if !last {
				return fmt.Errorf("%w: torn write in sealed segment %010d at offset %d", ErrCorrupt, s.id, off)
			}
			// Torn tail: drop it. The write never completed, so dropping it is
			// exactly the semantics of "the last write did not happen".
			cw, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
			if err != nil {
				return err
			}
			defer cw.Close()
			if err := cw.Truncate(off); err != nil {
				return err
			}
			db.cTruncated.Add(s.size - off)
			break
		}
		off += rec.size
		db.cRecovered.Add(1)
		if rec.typ == recordDelete {
			db.keydir.del(string(rec.key))
			continue
		}
		db.keydir.set(string(rec.key), entry{fileID: s.id, offset: off - rec.size, size: rec.size})
	}
	s.size = off
	return nil
}

// Put stores value under key, overwriting any previous value.
func (db *DB) Put(key, value []byte) error {
	return db.append(recordPut, key, value)
}

// Delete removes key. It appends a tombstone rather than touching older
// records, which is what makes appends-only writes crash safe.
func (db *DB) Delete(key []byte) error {
	return db.append(recordDelete, key, nil)
}

func (db *DB) append(typ recordType, key, value []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}
	buf, err := encodeRecord(typ, key, value)
	if err != nil {
		return err
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}
	return db.appendLocked(typ, key, buf)
}

// appendLocked writes a pre-encoded record and updates the index. The caller
// must hold wmu.
func (db *DB) appendLocked(typ recordType, key, buf []byte) error {
	active := db.activeSegment()
	offset := active.size
	f, err := db.files.get(active.id)
	if err != nil {
		return err
	}
	if _, err := f.WriteAt(buf, offset); err != nil {
		return err
	}
	active.size += int64(len(buf))

	if typ == recordDelete {
		db.keydir.del(string(key))
		db.cDeletes.Add(1)
	} else {
		db.keydir.set(string(key), entry{fileID: active.id, offset: offset, size: int64(len(buf))})
		db.cWrites.Add(1)
	}
	if db.opts.SyncOnWrite {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if active.size >= db.opts.MaxSegmentSize {
		return db.rotateLocked()
	}
	return nil
}

// Get returns the value stored under key, or ErrKeyNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	// Holding the keydir read lock across the pread is what makes Merge's
	// index swap safe: no reader can be looking at an offset in a file that
	// Merge is about to delete.
	db.keydir.mu.RLock()
	defer db.keydir.mu.RUnlock()
	e, ok := db.keydir.m[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	db.cReads.Add(1)
	return db.readValue(e, key)
}

// readValue fetches the record described by e and returns its value.
func (db *DB) readValue(e entry, key []byte) ([]byte, error) {
	f, err := db.files.get(e.fileID)
	if err != nil {
		return nil, err
	}
	rec, err := readRecordAt(f, e.offset, db.opts.VerifyOnRead)
	if err != nil {
		return nil, err
	}
	if rec.typ != recordPut || string(rec.key) != string(key) {
		return nil, ErrCorrupt
	}
	out := make([]byte, len(rec.value))
	copy(out, rec.value)
	return out, nil
}

// Has reports whether key exists.
func (db *DB) Has(key []byte) bool {
	_, err := db.Get(key)
	return err == nil
}

// Len returns the number of live keys.
func (db *DB) Len() int { return db.keydir.len() }

// Keys returns all live keys in unspecified (hash) order. Bitcask cannot
// serve range scans: hashes destroy the ordering that a B+ tree or SSTable
// preserves. This is the engine's defining limitation (DDIA §3.1).
func (db *DB) Keys() []string { return db.keydir.keys() }

// Sync flushes all open segment handles to stable storage.
func (db *DB) Sync() error { return db.files.syncAll() }

func (db *DB) syncLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-db.done:
			return
		case <-t.C:
			_ = db.files.syncAll()
		}
	}
}

// Close syncs and releases the database. It is safe to call more than once.
func (db *DB) Close() error {
	if db.closed.CompareAndSwap(false, true) {
		close(db.done)
	}
	db.wg.Wait()
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if err := db.files.syncAll(); err != nil {
		_ = db.files.closeAll()
		return err
	}
	return db.files.closeAll()
}

func (db *DB) activeSegment() *segment {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.segments[len(db.segments)-1]
}

func (db *DB) segmentList() []*segment {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]*segment, len(db.segments))
	copy(out, db.segments)
	return out
}

// rotateLocked starts a new active segment. Caller holds wmu.
func (db *DB) rotateLocked() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	next := db.segments[len(db.segments)-1].id + 1
	s := &segment{id: next, path: filepath.Join(db.opts.Dir, segmentName(next))}
	f, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil { // make the new file's existence durable
		_ = f.Close()
		return err
	}
	_ = f.Close()
	db.segments = append(db.segments, s)
	return nil
}

// DiskBytes reports the total size of all segment files.
func (db *DB) DiskBytes() (int64, error) {
	var total int64
	for _, s := range db.segmentList() {
		info, err := os.Stat(s.path)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// Stats returns a snapshot of counters.
func (db *DB) Stats() Stats {
	disk, _ := db.DiskBytes()
	return Stats{
		Keys:           db.keydir.len(),
		Segments:       len(db.segmentList()),
		DiskBytes:      disk,
		Writes:         db.cWrites.Load(),
		Reads:          db.cReads.Load(),
		Deletes:        db.cDeletes.Load(),
		Merges:         db.cMerges.Load(),
		Recovered:      db.cRecovered.Load(),
		TruncatedBytes: db.cTruncated.Load(),
	}
}
