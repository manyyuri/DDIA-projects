package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const segmentExt = ".data"

// segment is one append-only log file. Ids increase monotonically, and the
// segment with the highest id is the only one that accepts appends.
type segment struct {
	id   uint32
	path string
	size int64 // bytes of valid records
}

func segmentName(id uint32) string {
	return fmt.Sprintf("%010d%s", id, segmentExt)
}

func parseSegmentID(name string) (uint32, bool) {
	if !strings.HasSuffix(name, segmentExt) {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(name, segmentExt), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// fileCache keeps a bounded number of segment file descriptors open. Without
// it a merge over thousands of segments would exhaust the process fd limit.
type fileCache struct {
	dir     string
	maxOpen int

	mu    sync.Mutex
	open  map[uint32]*cacheEntry
	clock uint64
}

type cacheEntry struct {
	f    *os.File
	used uint64
}

func newFileCache(dir string, maxOpen int) *fileCache {
	if maxOpen < 1 {
		maxOpen = 1
	}
	return &fileCache{dir: dir, maxOpen: maxOpen, open: make(map[uint32]*cacheEntry)}
}

func (c *fileCache) get(id uint32) (*os.File, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock++
	if e, ok := c.open[id]; ok {
		e.used = c.clock
		return e.f, nil
	}
	if len(c.open) >= c.maxOpen {
		c.evictLocked()
	}
	f, err := os.OpenFile(filepath.Join(c.dir, segmentName(id)), os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	c.open[id] = &cacheEntry{f: f, used: c.clock}
	return f, nil
}

func (c *fileCache) evictLocked() {
	var oldestID uint32
	var oldestUsed uint64 = ^uint64(0)
	for id, e := range c.open {
		if e.used < oldestUsed {
			oldestID, oldestUsed = id, e.used
		}
	}
	if oldestUsed != ^uint64(0) {
		_ = c.open[oldestID].f.Close()
		delete(c.open, oldestID)
	}
}

// remove closes and forgets a handle (used before deleting a merged segment).
func (c *fileCache) remove(id uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.open[id]; ok {
		_ = e.f.Close()
		delete(c.open, id)
	}
}

func (c *fileCache) syncAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.open {
		if err := e.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (c *fileCache) closeAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for id, e := range c.open {
		if err := e.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.open, id)
	}
	return firstErr
}

// listSegments returns every segment file in dir, ordered by id.
func listSegments(dir string) ([]*segment, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []*segment
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		id, ok := parseSegmentID(de.Name())
		if !ok {
			continue
		}
		info, err := de.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, &segment{id: id, path: filepath.Join(dir, de.Name()), size: info.Size()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].id < segs[j].id })
	return segs, nil
}
