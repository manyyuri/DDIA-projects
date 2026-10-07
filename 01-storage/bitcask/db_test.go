package bitcask

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func tempDB(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	opts.Dir = dir
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, dir
}

func TestPutGetDelete(t *testing.T) {
	db, _ := tempDB(t, Options{})

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("hello"))
	if err != nil || string(got) != "world" {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Overwrite: the old value is shadowed but still on disk.
	if err := db.Put([]byte("hello"), []byte("there")); err != nil {
		t.Fatal(err)
	}
	got, _ = db.Get([]byte("hello"))
	if string(got) != "there" {
		t.Fatalf("after overwrite Get = %q", got)
	}

	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("hello")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("after delete: %v", err)
	}
	if db.Len() != 0 {
		t.Fatalf("Len = %d, want 0", db.Len())
	}
	if err := db.Delete([]byte("hello")); err != nil {
		t.Fatalf("deleting a missing key must be a no-op: %v", err)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		_ = db.Put([]byte("k"+strconv.Itoa(i)), []byte("v"+strconv.Itoa(i)))
	}
	for i := 0; i < 250; i++ {
		_ = db.Delete([]byte("k" + strconv.Itoa(i)))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if db2.Len() != 250 {
		t.Fatalf("Len after recovery = %d, want 250", db2.Len())
	}
	if _, err := db2.Get([]byte("k10")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("deleted key resurrected: %v", err)
	}
	got, err := db2.Get([]byte("k499"))
	if err != nil || string(got) != "v499" {
		t.Fatalf("Get(k499) = %q, %v", got, err)
	}
	if st := db2.Stats(); st.Recovered != 750 {
		t.Fatalf("Recovered = %d, want 750", st.Recovered)
	}
}

// A crash in the middle of an append leaves a half-written record. Recovery
// must drop exactly that record and keep everything before it.
func TestTornWriteRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("value-payload"))
	}
	_ = db.Sync()
	path := filepath.Join(dir, segmentName(1))
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Simulate a torn write: a valid record header with a truncated body.
	buf, _ := encodeRecord(recordPut, []byte("torn"), []byte("this body is cut off"))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(buf[:len(buf)-5]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open after torn write: %v", err)
	}
	defer db2.Close()

	if db2.Len() != 10 {
		t.Fatalf("Len = %d, want 10 intact records", db2.Len())
	}
	if err := db2.Put([]byte("after"), []byte("ok")); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() <= before.Size() {
		t.Fatalf("recovered file should have been truncated then appended to: %d -> %d",
			before.Size(), after.Size())
	}
}

// A flipped bit inside a value must be caught by the CRC.
func TestCorruptedRecordIsDetected(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	_ = db.Put([]byte("key"), []byte("0123456789"))
	_ = db.Sync()
	_ = db.Close()

	path := filepath.Join(dir, segmentName(1))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff // flip a bit in the value
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("Open must refuse a final segment whose only record fails its CRC")
	} else if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestSegmentRotationAndMergeReclaimsSpace(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	// The same 20 keys rewritten 200 times: 4000 records, 20 live.
	payload := make([]byte, 100)
	for i := range payload {
		payload[i] = 'x'
	}
	for round := 0; round < 200; round++ {
		for k := 0; k < 20; k++ {
			if err := db.Put([]byte(fmt.Sprintf("key-%03d", k)), payload); err != nil {
				t.Fatal(err)
			}
		}
	}
	st := db.Stats()
	if st.Segments < 3 {
		t.Fatalf("expected rotation, got %d segments", st.Segments)
	}
	before := st.DiskBytes

	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after, err := db.DiskBytes()
	if err != nil {
		t.Fatal(err)
	}
	if after >= before/2 {
		t.Fatalf("merge should shrink the log: %d -> %d", before, after)
	}
	if db.Len() != 20 {
		t.Fatalf("Len after merge = %d", db.Len())
	}
	for k := 0; k < 20; k++ {
		key := fmt.Sprintf("key-%03d", k)
		got, err := db.Get([]byte(key))
		if err != nil || len(got) != 100 {
			t.Fatalf("Get(%s) after merge: len=%d err=%v", key, len(got), err)
		}
	}
	if err := db.Put([]byte("post-merge"), []byte("v")); err != nil {
		t.Fatalf("write after merge: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The merged state must survive a restart.
	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if db2.Len() != 21 {
		t.Fatalf("Len after reopen = %d, want 21", db2.Len())
	}
}

// Merge must be safe to interrupt: a partially written merged segment still
// resolves to correct values because recovery replays it last.
func TestMergeIsCrashSafe(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 2 << 10})
	if err != nil {
		t.Fatal(err)
	}
	value := make([]byte, 80)
	for i := 0; i < 40; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%02d", i)), append([]byte("v"), value...))
	}
	_ = db.Sync()
	// Craft the state an interrupted merge leaves behind: a highest-id segment
	// containing only some of the live keys.
	merged := filepath.Join(dir, segmentName(999))
	f, err := os.OpenFile(merged, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	buf, _ := encodeRecord(recordPut, []byte("k03"), []byte("newer"))
	if _, err := f.Write(buf); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	_ = db.Close()

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if got, _ := db2.Get([]byte("k03")); string(got) != "newer" {
		t.Fatalf("k03 = %q, want newer (highest id wins)", got)
	}
	if got, err := db2.Get([]byte("k20")); err != nil || len(got) != 81 {
		t.Fatalf("k20 should still resolve from an older segment: %v", err)
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	db, _ := tempDB(t, Options{})
	var wg sync.WaitGroup
	const writers, readers, ops = 4, 4, 500

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i%50)
				if err := db.Put([]byte(k), []byte("v")); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				_, err := db.Get([]byte(fmt.Sprintf("w%d-k%d", r, i%50)))
				if err != nil && !errors.Is(err, ErrKeyNotFound) {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(r)
	}
	wg.Wait()

	if db.Len() != writers*50 {
		t.Fatalf("Len = %d, want %d", db.Len(), writers*50)
	}
}

func TestWriteAmplificationIsVisible(t *testing.T) {
	// This test exists to make the engine's core weakness measurable: 1000
	// updates of one key write 1000 full records to disk.
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	value := make([]byte, 256)
	var logical int64
	for i := 0; i < 1000; i++ {
		rand.Read(value)
		if err := db.Put([]byte("hot"), value); err != nil {
			t.Fatal(err)
		}
		logical += int64(len(value))
	}
	disk, _ := db.DiskBytes()
	if disk <= logical {
		t.Fatalf("expected write amplification: logical=%d disk=%d", logical, disk)
	}
	t.Logf("logical=%d disk=%d amplification=%.2fx", logical, disk, float64(disk)/float64(logical))
}
