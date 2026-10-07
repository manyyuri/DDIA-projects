package lsm

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func openDB(t *testing.T, opts Options) *DB {
	t.Helper()
	opts.Dir = t.TempDir()
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPutGetDelete(t *testing.T) {
	db := openDB(t, Options{})
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := db.Put([]byte(k), []byte("v"+k)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		got, err := db.Get([]byte(k))
		if err != nil || string(got) != "v"+k {
			t.Fatalf("Get(%s) = %q, %v", k, got, err)
		}
	}
	if err := db.Delete([]byte("key-050")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("key-050")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if _, err := db.Get([]byte("nope")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// Overwrites and deletes must shadow older values even after those values have
// moved to disk and been merged down into lower levels.
func TestShadowingAcrossLevels(t *testing.T) {
	db := openDB(t, Options{MemTableSize: 512, L0CompactionTrigger: 2, TableSize: 1024})

	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old")); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		if err := db.Delete([]byte(fmt.Sprintf("k%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("k%03d", i)
		got, err := db.Get([]byte(key))
		if i < 100 {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s should be deleted, got %q %v", key, got, err)
			}
			continue
		}
		if err != nil || string(got) != "new" {
			t.Fatalf("%s = %q, %v; want new", key, got, err)
		}
	}
}

func TestIterateSortedAndSeek(t *testing.T) {
	db := openDB(t, Options{MemTableSize: 700})
	keys := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("key-%04d", i)
		keys = append(keys, k)
		if err := db.Put([]byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(keys)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator()
	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, it.Key())
	}
	if it.Err() != nil {
		t.Fatal(it.Err())
	}
	it.Close()
	if len(got) != len(keys) {
		t.Fatalf("scanned %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Fatalf("scan not sorted at %d: %s != %s", i, got[i], keys[i])
		}
	}

	// Range scan — the thing bitcask cannot do.
	it = db.NewIterator()
	it.Seek("key-0250")
	var ranged []string
	for ; it.Valid() && len(ranged) < 10; it.Next() {
		ranged = append(ranged, it.Key())
	}
	it.Close()
	if ranged[0] != "key-0250" || ranged[9] != "key-0259" {
		t.Fatalf("bad range scan: %v", ranged)
	}
}

func TestDurabilityViaWALAndReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
	}
	_ = db.Delete([]byte("k0001"))
	// Simulate a crash: do NOT call Close (which flushes), just drop the handle.
	_ = db.wal.sync()

	db2, err := Open(Options{Dir: dir, MemTableSize: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k0001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k0001 should stay deleted after WAL replay: %v", err)
	}
	if got, err := db2.Get([]byte("k0299")); err != nil || string(got) != "v" {
		t.Fatalf("k0299 lost after replay: %q %v", got, err)
	}
	// Recovery must have written the replayed data to an SSTable.
	if st := db2.Stats(); len(st.LevelTables) == 0 || st.LevelTables[0] == 0 {
		t.Fatalf("expected an L0 table after recovery, got %+v", st.LevelTables)
	}
}

func TestTornWALTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("good"), []byte("value"))
	_ = db.wal.sync()
	walPath := db.wal.path
	_ = db.wal.close()

	// Append a half record, exactly like a crash mid-append.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	half := encodeWAL(entry{key: "torn", value: []byte("truncated"), typ: typePut})
	if _, err := f.Write(half[:len(half)-6]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open after torn WAL: %v", err)
	}
	defer db2.Close()
	if got, err := db2.Get([]byte("good")); err != nil || string(got) != "value" {
		t.Fatalf("good record lost: %q %v", got, err)
	}
	if _, err := db2.Get([]byte("torn")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("torn record must not be visible: %v", err)
	}
}

// Bloom filters are what keep reads from touching every level. Verify the
// false-positive rate is in the expected ballpark and that skipping happens.
func TestBloomFilterEffectiveness(t *testing.T) {
	const n = 20000
	bf := newBloom(n, bloomBitsPerKey)
	for i := 0; i < n; i++ {
		bf.add(fmt.Sprintf("present-%06d", i))
	}
	fp := 0
	const probes = 20000
	for i := 0; i < probes; i++ {
		key := fmt.Sprintf("absent-%06d", i)
		if bf.mayContain(key) {
			fp++
		}
	}
	rate := float64(fp) / probes
	t.Logf("bloom false positive rate = %.4f (%d bits/key)", rate, bloomBitsPerKey)
	if rate > 0.05 {
		t.Fatalf("false positive rate too high: %.4f", rate)
	}
	// No false negatives, ever.
	for i := 0; i < n; i++ {
		if !bf.mayContain(fmt.Sprintf("present-%06d", i)) {
			t.Fatalf("false negative for present-%06d", i)
		}
	}
}

func TestCompactionBoundsReadAmplification(t *testing.T) {
	db := openDB(t, Options{
		MemTableSize:        4 << 10,
		L0CompactionTrigger: 4,
		TableSize:           8 << 10,
		LevelSizeBase:       16 << 10,
		LevelSizeRatio:      4,
	})
	rng := rand.New(rand.NewSource(42))
	const n = 20000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%06d", rng.Intn(5000)))
		v := make([]byte, 64)
		rng.Read(v)
		if err := db.Put(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	t.Logf("levels tables=%v bytes=%v flushes=%d compactions=%d bloomSkips=%d tableReads=%d",
		st.LevelTables, st.LevelBytes, st.Flushes, st.Compactions, st.BloomFilterSkips, st.TablesTouchedRead)
	if st.LevelTables[0] > 4 && st.Compactions == 0 {
		t.Fatal("L0 should have been compacted away")
	}
	// Everything still readable after all that merging.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", rng.Intn(5000))
		if _, err := db.Get([]byte(key)); err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%s): %v", key, err)
		}
	}
}

func TestCompactionPreservesFullDataset(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir, MemTableSize: 2 << 10, L0CompactionTrigger: 3, TableSize: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{}
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("k-%05d", i)
		v := fmt.Sprintf("value-%d", i)
		want[k] = v
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 { // sprinkle deletes
			dk := fmt.Sprintf("k-%05d", i/2)
			delete(want, dk)
			if err := db.Delete([]byte(dk)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for k, v := range want {
		got, err := db2.Get([]byte(k))
		if err != nil || string(got) != v {
			t.Fatalf("after reopen %s = %q, %v; want %q", k, got, err, v)
		}
	}
}
