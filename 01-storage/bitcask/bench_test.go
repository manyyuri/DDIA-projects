package bitcask

import (
	"fmt"
	"math/rand"
	"testing"
)

func mustOpen(b *testing.B, opts Options) *DB {
	b.Helper()
	db, err := Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkPutSequential(b *testing.B) {
	dir := b.TempDir()
	db := mustOpen(b, Options{Dir: dir})
	key := make([]byte, 16)
	value := make([]byte, 256)
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryPutUint64(key, uint64(i))
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutSequentialSync(b *testing.B) {
	dir := b.TempDir()
	db := mustOpen(b, Options{Dir: dir, SyncOnWrite: true})
	key := make([]byte, 16)
	value := make([]byte, 256)
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binaryPutUint64(key, uint64(i))
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetRandom(b *testing.B) {
	dir := b.TempDir()
	db := mustOpen(b, Options{Dir: dir})
	const n = 100_000
	value := make([]byte, 256)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%08d", i))
		keys[i] = k
		if err := db.Put(k, value); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[rand.Intn(n)]
		if _, err := db.Get(k); err != nil {
			b.Fatal(err)
		}
	}
}

// binaryPutUint64 writes v into the first 8 bytes of buf.
func binaryPutUint64(buf []byte, v uint64) {
	for i := 0; i < 8; i++ {
		buf[i] = byte(v >> (8 * i))
	}
}
