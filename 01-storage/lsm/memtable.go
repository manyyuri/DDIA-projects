// Package lsm implements the storage engine from DDIA §3.2 ("SSTables and
// LSM-Trees"): a write-optimised, sorted-string-table engine.
//
// Anatomy:
//
//	write path:  WAL (crash safety) -> memtable (skiplist, sorted) -> flush -> L0 SSTables
//	             -> background compaction merges L0 into L1, L1 into L2, ...
//	read  path:  memtable -> immutable memtable -> L0 (newest first) -> L1..Ln (binary search)
//
// Compared with bitcask (see ../bitcask), the same workload trades differently:
//
//	bitcask        O(1) point reads    but no range scans, keydir must fit in RAM
//	LSM tree       range scans free    but read amplification (check N tables) and
//	               disk-based index    write amplification (rewrite on every compaction)
//
// Bloom filters and block indexes are what keep the read amplification tolerable.
package lsm

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"sync"
)

// entryType distinguishes a value from a tombstone. Tombstones must be written
// (not just "not written") so that they can shadow older values living in
// lower levels.
type entryType uint8

const (
	typePut entryType = iota
	typeDelete
)

// entry is one logical record flowing through the whole engine.
type entry struct {
	key   string
	value []byte
	typ   entryType
}

func (e entry) isDelete() bool { return e.typ == typeDelete }

// ---------------------------------------------------------------- memtable --

const (
	maxSkiplistLevel = 12
	skiplistP        = 0.25
)

type slNode struct {
	key   string
	value []byte
	typ   entryType
	next  []*slNode
}

// memTable is a sorted map backed by a skiplist. A sorted structure (rather
// than a hash map) is what makes an LSM tree able to serve range scans and to
// flush itself out to disk in key order.
type memTable struct {
	mu    sync.RWMutex
	head  *slNode
	level int
	rnd   *rand.Rand
	bytes int64
	count int
}

func newMemTable() *memTable {
	return &memTable{
		head:  &slNode{next: make([]*slNode, maxSkiplistLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(0x5eed)),
	}
}

func (m *memTable) randomLevel() int {
	lvl := 1
	for lvl < maxSkiplistLevel && m.rnd.Float64() < skiplistP {
		lvl++
	}
	return lvl
}

// put inserts or replaces key.
func (m *memTable) put(key string, value []byte, typ entryType) {
	m.mu.Lock()
	defer m.mu.Unlock()

	update := make([]*slNode, maxSkiplistLevel)
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
		update[i] = x
	}
	if n := x.next[0]; n != nil && n.key == key {
		// Same key: overwrite in place. The old value is garbage in this
		// memtable, but the *older* value may still live in an SSTable below
		// — which is why an overwrite is not the same as "gone".
		m.bytes += int64(len(value) - len(n.value))
		n.value = clone(value)
		n.typ = typ
		return
	}
	lvl := m.randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}
	n := &slNode{key: key, value: clone(value), typ: typ, next: make([]*slNode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	m.bytes += int64(len(key)+len(value)) + 24
	m.count++
}

func (m *memTable) get(key string) (entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && x.next[i].key < key {
			x = x.next[i]
		}
	}
	n := x.next[0]
	if n != nil && n.key == key {
		return entry{key: n.key, value: n.value, typ: n.typ}, true
	}
	return entry{}, false
}

func (m *memTable) size() (int64, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bytes, m.count
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// ---------------------------------------------------------------- write-ahead log --

// WAL record layout is intentionally the same shape as bitcask's: a checksum
// covering the rest of the record, so recovery can tell a torn append from
// silent corruption.
//
//	+-------+------+--------+--------+-----+-------+
//	| crc32 | type | keyLen | valLen | key | value |
//	+-------+------+--------+--------+-----+-------+
const walHeaderSize = 13

var errTornWrite = io.ErrUnexpectedEOF

func encodeWAL(e entry) []byte {
	val := e.value
	if e.typ == typeDelete {
		val = nil
	}
	buf := make([]byte, walHeaderSize+len(e.key)+len(val))
	buf[4] = byte(e.typ)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(e.key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(val)))
	copy(buf[walHeaderSize:], e.key)
	copy(buf[walHeaderSize+len(e.key):], val)
	binary.LittleEndian.PutUint32(buf[:4], crc32.ChecksumIEEE(buf[4:]))
	return buf
}

func decodeWAL(r io.Reader) (entry, int64, error) {
	var hdr [walHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return entry{}, 0, err
	}
	typ := entryType(hdr[4])
	keyLen := binary.LittleEndian.Uint32(hdr[5:9])
	valLen := binary.LittleEndian.Uint32(hdr[9:13])
	if keyLen > 1<<16 || valLen > 1<<30 || typ > typeDelete {
		return entry{}, 0, errCorrupt
	}
	body := make([]byte, int(keyLen)+int(valLen))
	if _, err := io.ReadFull(r, body); err != nil {
		return entry{}, 0, errTornWrite
	}
	want := binary.LittleEndian.Uint32(hdr[:4])
	chk := crc32.NewIEEE()
	_, _ = chk.Write(hdr[4:])
	_, _ = chk.Write(body)
	if chk.Sum32() != want {
		return entry{}, 0, errCorrupt
	}
	return entry{
		key:   string(body[:keyLen]),
		value: clone(body[keyLen:]),
		typ:   typ,
	}, int64(walHeaderSize) + int64(len(body)), nil
}

type wal struct {
	mu   sync.Mutex
	f    *os.File
	path string
	size int64
}

func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &wal{f: f, path: path, size: info.Size()}, nil
}

func (w *wal) append(e entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	buf := encodeWAL(e)
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	w.size += int64(len(buf))
	return nil
}

func (w *wal) sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// replayWAL feeds every intact record into fn. A torn tail is dropped; a
// checksum failure with intact framing is reported, because that is silent
// corruption rather than an interrupted append.
func replayWAL(path string, fn func(entry)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	for {
		e, _, err := decodeWAL(f)
		if err != nil {
			if err == io.EOF || err == errTornWrite {
				return nil
			}
			return err
		}
		fn(e)
	}
}
