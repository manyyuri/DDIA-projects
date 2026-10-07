package bitcask

import (
	"os"
)

// Merge compacts the log: every live key is rewritten once into a fresh
// segment, tombstones and shadowed values are dropped, and the old segments
// are deleted.
//
// Crash safety is the interesting part. The merged segment gets the highest
// id, and recovery replays segments in id order, taking the last write per
// key. So if we crash half way through a merge, the partially written segment
// is still correct: keys written into it win, keys not yet reached still
// resolve to the old segments. Merging is therefore idempotent and restartable.
//
// Cost: writes block for the duration (a real Bitcask keeps serving writes and
// writes a hint file to make startup cheaper). This is the honest trade-off
// between simple correctness and availability.
func (db *DB) Merge() error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed.Load() {
		return ErrClosed
	}

	src := db.segmentList()
	if len(src) <= 1 {
		return nil // a single segment is already as compact as it gets
	}

	// Snapshot the live index. Writers are blocked by wmu, so it cannot change
	// underneath us while we copy values out of the old segments.
	type live struct {
		key string
		e   entry
	}
	db.keydir.mu.RLock()
	snapshot := make([]live, 0, len(db.keydir.m))
	for k, e := range db.keydir.m {
		snapshot = append(snapshot, live{key: k, e: e})
	}
	db.keydir.mu.RUnlock()

	id := nextSegmentID(src)
	path := segmentName(id)
	if err := cleanStaleMerge(db.opts.Dir, id); err != nil {
		return err
	}
	f, err := os.OpenFile(db.opts.Dir+string(os.PathSeparator)+path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	newMap := make(map[string]entry, len(snapshot))
	var off int64
	for _, item := range snapshot {
		value, err := db.readValue(item.e, []byte(item.key))
		if err != nil {
			return err
		}
		buf, err := encodeRecord(recordPut, []byte(item.key), value)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(buf, off); err != nil {
			return err
		}
		newMap[item.key] = entry{fileID: id, offset: off, size: int64(len(buf))}
		off += int64(len(buf))
	}
	if err := f.Sync(); err != nil {
		return err
	}

	merged := &segment{id: id, path: db.opts.Dir + string(os.PathSeparator) + path, size: off}

	// Swap the index and the segment list atomically with respect to readers.
	db.keydir.mu.Lock()
	db.mu.Lock()
	old := db.segments
	db.segments = []*segment{merged}
	db.keydir.m = newMap
	db.mu.Unlock()
	db.keydir.mu.Unlock()

	for _, s := range old {
		db.files.remove(s.id)
		_ = os.Remove(s.path)
	}
	db.cMerges.Add(1)
	return nil
}

func nextSegmentID(segs []*segment) uint32 {
	var max uint32
	for _, s := range segs {
		if s.id > max {
			max = s.id
		}
	}
	return max + 1
}

// cleanStaleMerge removes a leftover file from an interrupted previous merge
// so that O_EXCL can be used.
func cleanStaleMerge(dir string, id uint32) error {
	err := os.Remove(dir + string(os.PathSeparator) + segmentName(id))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
