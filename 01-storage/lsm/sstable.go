package lsm

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

var (
	errCorrupt = errors.New("lsm: corrupt file")
	ErrClosed  = errors.New("lsm: database is closed")
	// ErrNotFound is returned by Get when the key does not exist (or was
	// deleted).
	ErrNotFound = errors.New("lsm: key not found")
)

// ------------------------------------------------------------------- bloom --

// bloomFilter is a standard bitset bloom filter: k hash functions derived by
// double hashing from two independent hashes. It is the main defence against
// read amplification: a negative answer means "no such key in this table",
// which lets Get skip opening/scanning tables entirely.
type bloomFilter struct {
	bits []uint64
	n    uint64 // number of bits
	k    uint8
}

func newBloom(keys int, bitsPerKey int) *bloomFilter {
	if keys < 1 {
		keys = 1
	}
	n := uint64(keys * bitsPerKey)
	if n < 64 {
		n = 64
	}
	n = (n + 63) / 64 * 64
	return &bloomFilter{bits: make([]uint64, n/64), n: n, k: 7}
}

func bloomHashes(key string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	h1 := h.Sum64()
	// A cheap second hash (mix the first one) avoids hashing the key twice.
	h2 := h1>>33 | h1<<31
	return h1, h2
}

func (b *bloomFilter) add(key string) {
	h1, h2 := bloomHashes(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.n
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

func (b *bloomFilter) mayContain(key string) bool {
	h1, h2 := bloomHashes(key)
	for i := uint8(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % b.n
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

func (b *bloomFilter) encode() []byte {
	buf := make([]byte, 0, 8*(len(b.bits)+2))
	buf = binary.AppendUvarint(buf, b.n)
	buf = append(buf, b.k)
	for _, w := range b.bits {
		buf = binary.LittleEndian.AppendUint64(buf, w)
	}
	return buf
}

func decodeBloom(data []byte) (*bloomFilter, error) {
	n, m := binary.Uvarint(data)
	if m <= 0 || len(data) < m+1 {
		return nil, errCorrupt
	}
	k := data[m]
	words := (n + 63) / 64
	off := m + 1
	if uint64(len(data)-off) < words*8 {
		return nil, errCorrupt
	}
	bits := make([]uint64, words)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(data[off+8*i:])
	}
	return &bloomFilter{bits: bits, n: n, k: k}, nil
}

// ---------------------------------------------------------------- SSTable --

const (
	tableMagic      = "DIALSM01"
	footerSize      = 8 + 8*5 // magic + 5 uint64 fields
	indexInterval   = 16      // sparse index: one entry per N data records
	bloomBitsPerKey = 10
)

type indexEntry struct {
	key    string
	offset int64
}

// table is an immutable, sorted, on-disk file plus its in-memory sparse index
// and bloom filter. Immutability is what makes the read path lock-free.
type table struct {
	id     uint64
	path   string
	f      *os.File
	index  []indexEntry
	bloom  *bloomFilter
	minKey string
	maxKey string
	count  int
	size   int64 // bytes in the data region (= indexOffset)
}

// writeTable streams a sorted iterator into a new SSTable file.
func writeTable(path string, id uint64, it iter) (*table, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var (
		off     int64
		index   []indexEntry
		keys    []string
		minKey  string
		maxKey  string
		count   int
		dataBuf []byte
	)
	for ; it.valid(); it.next() {
		e := it.entry()
		if count == 0 {
			minKey = e.key
			index = append(index, indexEntry{key: e.key, offset: off})
		} else if count%indexInterval == 0 {
			index = append(index, indexEntry{key: e.key, offset: off})
		}
		maxKey = e.key
		keys = append(keys, e.key)
		dataBuf = dataBuf[:0]
		dataBuf = binary.AppendUvarint(dataBuf, uint64(len(e.key)))
		dataBuf = append(dataBuf, e.key...)
		dataBuf = binary.AppendUvarint(dataBuf, uint64(len(e.value)))
		dataBuf = append(dataBuf, e.value...)
		dataBuf = append(dataBuf, byte(e.typ))
		if _, err := f.WriteAt(dataBuf, off); err != nil {
			return nil, err
		}
		off += int64(len(dataBuf))
		count++
	}
	if it.err() != nil {
		return nil, it.err()
	}
	dataEnd := off

	// Index region: min/max keys, then the sparse index.
	var idx []byte
	idx = binary.AppendUvarint(idx, uint64(len(minKey)))
	idx = append(idx, minKey...)
	idx = binary.AppendUvarint(idx, uint64(len(maxKey)))
	idx = append(idx, maxKey...)
	idx = binary.AppendUvarint(idx, uint64(len(index)))
	for _, ie := range index {
		idx = binary.AppendUvarint(idx, uint64(len(ie.key)))
		idx = append(idx, ie.key...)
		idx = binary.AppendUvarint(idx, uint64(ie.offset))
	}
	indexOff := off
	if _, err := f.WriteAt(idx, indexOff); err != nil {
		return nil, err
	}
	off += int64(len(idx))

	bf := newBloom(len(keys), bloomBitsPerKey)
	for _, k := range keys {
		bf.add(k)
	}
	bloomBytes := bf.encode()
	bloomOff := off
	if _, err := f.WriteAt(bloomBytes, bloomOff); err != nil {
		return nil, err
	}
	off += int64(len(bloomBytes))

	footer := make([]byte, footerSize)
	copy(footer[0:8], tableMagic)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(indexOff))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(len(idx)))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[32:40], uint64(len(bloomBytes)))
	binary.LittleEndian.PutUint64(footer[40:48], uint64(count))
	if _, err := f.WriteAt(footer, off); err != nil {
		return nil, err
	}
	if err := f.Sync(); err != nil {
		return nil, err
	}
	return openTable(path, id, dataEnd)
}

// openTable loads the footer, the sparse index and the bloom filter. The data
// region itself is only read on demand.
func openTable(path string, id uint64, dataEnd int64) (*table, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if info.Size() < footerSize {
		_ = f.Close()
		return nil, errCorrupt
	}
	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, info.Size()-footerSize); err != nil {
		_ = f.Close()
		return nil, err
	}
	if string(footer[0:8]) != tableMagic {
		_ = f.Close()
		return nil, errCorrupt
	}
	indexOff := int64(binary.LittleEndian.Uint64(footer[8:16]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[16:24]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[24:32]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[32:40]))
	count := int(binary.LittleEndian.Uint64(footer[40:48]))

	idxBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(idxBuf, indexOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	minKey, maxKey, index, err := parseIndex(idxBuf)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, bloomOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	bf, err := decodeBloom(bloomBuf)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if dataEnd == 0 {
		dataEnd = indexOff
	}
	return &table{
		id: id, path: path, f: f, index: index, bloom: bf,
		minKey: minKey, maxKey: maxKey, count: count, size: dataEnd,
	}, nil
}

func parseIndex(buf []byte) (string, string, []indexEntry, error) {
	pos := 0
	readBytes := func() (string, error) {
		n, m := binary.Uvarint(buf[pos:])
		if m <= 0 {
			return "", errCorrupt
		}
		pos += m
		if pos+int(n) > len(buf) {
			return "", errCorrupt
		}
		s := string(buf[pos : pos+int(n)])
		pos += int(n)
		return s, nil
	}
	minKey, err := readBytes()
	if err != nil {
		return "", "", nil, err
	}
	maxKey, err := readBytes()
	if err != nil {
		return "", "", nil, err
	}
	n, m := binary.Uvarint(buf[pos:])
	if m <= 0 {
		return "", "", nil, errCorrupt
	}
	pos += m
	index := make([]indexEntry, 0, n)
	for i := uint64(0); i < n; i++ {
		key, err := readBytes()
		if err != nil {
			return "", "", nil, err
		}
		off, m := binary.Uvarint(buf[pos:])
		if m <= 0 {
			return "", "", nil, errCorrupt
		}
		pos += m
		index = append(index, indexEntry{key: key, offset: int64(off)})
	}
	return minKey, maxKey, index, nil
}

func (t *table) close() error { return t.f.Close() }

func (t *table) remove() error {
	_ = t.f.Close()
	return os.Remove(t.path)
}

// get looks up key: bloom filter, then binary search on the sparse index, then
// a short forward scan inside one data block.
func (t *table) get(key string) (entry, bool, error) {
	if t.count == 0 || key < t.minKey || key > t.maxKey {
		return entry{}, false, nil
	}
	if !t.bloom.mayContain(key) {
		return entry{}, false, nil
	}
	i := sort.Search(len(t.index), func(i int) bool { return t.index[i].key > key }) - 1
	if i < 0 {
		i = 0
	}
	sr := newSeqReader(t.f, t.index[i].offset, t.size)
	for {
		e, err := sr.readEntry()
		if err != nil {
			if err == io.EOF {
				return entry{}, false, nil
			}
			return entry{}, false, err
		}
		switch {
		case e.key == key:
			return e, true, nil
		case e.key > key:
			return entry{}, false, nil
		}
	}
}

// iterAll returns an iterator over the whole data region, primed on the first
// record (a cursor that has not been primed reports valid()==false).
func (t *table) iterAll() *tableIter {
	it := &tableIter{sr: newSeqReader(t.f, 0, t.size)}
	it.advance()
	return it
}

// iterFrom seeks to the first key >= target using the sparse index.
func (t *table) iterFrom(target string) *tableIter {
	if t.count == 0 || target > t.maxKey {
		return &tableIter{sr: newSeqReader(t.f, t.size, t.size), done: true}
	}
	i := sort.Search(len(t.index), func(i int) bool { return t.index[i].key >= target }) - 1
	if i < 0 {
		i = 0
	}
	ti := &tableIter{sr: newSeqReader(t.f, t.index[i].offset, t.size)}
	ti.advance()
	ti.seek(target)
	return ti
}

// ---------------------------------------------------------------- seq reader --

// seqReader gives buffered sequential reads starting at a byte offset, which
// is what SSTable scans need (records are variable length, so seeking to a
// record requires scanning from the last checkpoint anyway).
type seqReader struct {
	f    *os.File
	off  int64 // next byte to be read from f
	stop int64 // end of the region
	buf  []byte
	pos  int
	end  int
}

func newSeqReader(f *os.File, off, stop int64) *seqReader {
	return &seqReader{f: f, off: off, stop: stop, buf: make([]byte, 1<<16)}
}

func (s *seqReader) fill() error {
	if s.off >= s.stop {
		return io.EOF
	}
	n := int64(len(s.buf))
	if remaining := s.stop - s.off; remaining < n {
		n = remaining
	}
	got, err := s.f.ReadAt(s.buf[:n], s.off)
	if got > 0 {
		s.pos, s.end = 0, got
		s.off += int64(got)
	}
	if err != nil && got == 0 {
		return err
	}
	return nil
}

func (s *seqReader) readByte() (byte, error) {
	if s.pos >= s.end {
		if err := s.fill(); err != nil {
			return 0, err
		}
	}
	b := s.buf[s.pos]
	s.pos++
	return b, nil
}

func (s *seqReader) readFull(p []byte) error {
	for i := range p {
		b, err := s.readByte()
		if err != nil {
			return err
		}
		p[i] = b
	}
	return nil
}

func (s *seqReader) readUvarint() (uint64, error) {
	var v uint64
	var shift uint
	for i := 0; i < 10; i++ {
		b, err := s.readByte()
		if err != nil {
			return 0, err
		}
		v |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return v, nil
		}
		shift += 7
	}
	return 0, errCorrupt
}

func (s *seqReader) readEntry() (entry, error) {
	klen, err := s.readUvarint()
	if err != nil {
		return entry{}, err
	}
	if klen > 1<<16 {
		return entry{}, errCorrupt
	}
	key := make([]byte, klen)
	if err := s.readFull(key); err != nil {
		return entry{}, err
	}
	vlen, err := s.readUvarint()
	if err != nil {
		return entry{}, err
	}
	if vlen > 1<<30 {
		return entry{}, errCorrupt
	}
	val := make([]byte, vlen)
	if err := s.readFull(val); err != nil {
		return entry{}, err
	}
	typ, err := s.readByte()
	if err != nil {
		return entry{}, err
	}
	if typ > byte(typeDelete) {
		return entry{}, errCorrupt
	}
	return entry{key: string(key), value: val, typ: entryType(typ)}, nil
}
