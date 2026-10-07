package bitcask

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// On-disk record layout (little-endian):
//
//	+-------+------+--------+--------+-----+-------+
//	| crc32 | type | keyLen | valLen | key | value |
//	| 4 B   | 1 B  | 4 B    | 4 B    | k B | v B   |
//	+-------+------+--------+--------+-----+-------+
//
// The checksum covers everything *after* the checksum field, so a torn write
// or a single bit flip is detected while scanning the log on startup.
//
// This is the whole point of DDIA §3.1's "hash index" engine: the log is the
// source of truth, and the in-memory map is only a cache that can be rebuilt.
const (
	headerSize   = 13
	maxKeySize   = 1 << 16
	maxValueSize = 1 << 30 // bounds the allocation while decoding corrupt data
)

var (
	ErrKeyNotFound = errors.New("bitcask: key not found")
	ErrClosed      = errors.New("bitcask: database is closed")
	ErrCorrupt     = errors.New("bitcask: corrupt record")
	ErrKeyTooLarge = errors.New("bitcask: key too large")
	ErrValueTooBig = errors.New("bitcask: value too large")

	// errGarbageLength means the length prefixes are implausible: almost always
	// leftover bytes from a partial page flush, i.e. a torn write. It is
	// deliberately distinct from ErrCorrupt so recovery can tell the two apart.
	errGarbageLength = errors.New("bitcask: implausible record length")
)

type recordType uint8

const (
	recordPut    recordType = 0x00
	recordDelete recordType = 0x01
)

// record is one decoded entry from the log.
type record struct {
	typ   recordType
	key   []byte
	value []byte
	size  int64 // total bytes occupied on disk, header included
}

// encodeRecord serialises one record into a freshly allocated buffer.
func encodeRecord(typ recordType, key, value []byte) ([]byte, error) {
	if len(key) > maxKeySize {
		return nil, ErrKeyTooLarge
	}
	if len(value) > maxValueSize {
		return nil, ErrValueTooBig
	}
	buf := make([]byte, headerSize+len(key)+len(value))
	buf[4] = byte(typ)
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(value)))
	copy(buf[headerSize:], key)
	copy(buf[headerSize+len(key):], value)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))
	return buf, nil
}

// decodeRecord parses a complete record out of buf. verify==false skips the
// CRC check, which is the fast path for point reads on the hot path.
func decodeRecord(buf []byte, verify bool) (record, error) {
	if len(buf) < headerSize {
		return record{}, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(buf[0:4])
	typ := recordType(buf[4])
	keyLen := binary.LittleEndian.Uint32(buf[5:9])
	valLen := binary.LittleEndian.Uint32(buf[9:13])
	if keyLen > maxKeySize || valLen > maxValueSize {
		return record{}, errGarbageLength
	}
	total := int64(headerSize) + int64(keyLen) + int64(valLen)
	if int64(len(buf)) < total {
		return record{}, ErrCorrupt
	}
	if typ != recordPut && typ != recordDelete {
		return record{}, ErrCorrupt
	}
	if verify && crc32.ChecksumIEEE(buf[4:total]) != want {
		return record{}, ErrCorrupt
	}
	return record{
		typ:   typ,
		key:   buf[headerSize : headerSize+int(keyLen)],
		value: buf[headerSize+int(keyLen) : total],
		size:  total,
	}, nil
}

// readRecordAt reads and decodes the record stored at offset in f.
func readRecordAt(r io.ReaderAt, offset int64, verify bool) (record, error) {
	var hdr [headerSize]byte
	if _, err := r.ReadAt(hdr[:], offset); err != nil {
		return record{}, err
	}
	keyLen := binary.LittleEndian.Uint32(hdr[5:9])
	valLen := binary.LittleEndian.Uint32(hdr[9:13])
	if keyLen > maxKeySize || valLen > maxValueSize {
		return record{}, errGarbageLength
	}
	body := make([]byte, int(keyLen)+int(valLen))
	if _, err := r.ReadAt(body, offset+headerSize); err != nil {
		return record{}, err
	}
	// Reassemble so decodeRecord sees the exact same byte layout it expects.
	buf := append(hdr[:headerSize:headerSize], body...)
	return decodeRecord(buf, verify)
}

// readRecordFrom streams one record from r. It is used by recovery, which
// walks whole segments sequentially instead of seeking.
func readRecordFrom(r io.Reader, verify bool) (record, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return record{}, err // io.EOF on a clean boundary
	}
	keyLen := binary.LittleEndian.Uint32(hdr[5:9])
	valLen := binary.LittleEndian.Uint32(hdr[9:13])
	if keyLen > maxKeySize || valLen > maxValueSize {
		return record{}, errGarbageLength
	}
	buf := make([]byte, headerSize+int(keyLen)+int(valLen))
	copy(buf, hdr[:])
	if _, err := io.ReadFull(r, buf[headerSize:]); err != nil {
		return record{}, err // io.ErrUnexpectedEOF => torn write
	}
	return decodeRecord(buf, verify)
}
