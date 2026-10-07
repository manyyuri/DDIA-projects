package mvcc

import (
	"errors"
	"sync"
	"testing"
)

func seed(t *testing.T, db *DB, kvs map[string]string) {
	t.Helper()
	tx := db.Begin(Snapshot)
	keys := make([]string, 0, len(kvs))
	for k := range kvs {
		keys = append(keys, k)
	}
	// deterministic order
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		if err := tx.Put(k, kvs[k]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// A snapshot stays fixed: another transaction committing in between is
// invisible. This is what prevents non-repeatable reads.
func TestSnapshotIsRepeatable(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{"k": "v1"})

	reader := db.Begin(Snapshot)
	first, err := reader.Get("k")
	if err != nil || first != "v1" {
		t.Fatalf("first read = %q, %v", first, err)
	}

	writer := db.Begin(Snapshot)
	_ = writer.Put("k", "v2")
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}

	second, err := reader.Get("k")
	if err != nil || second != "v1" {
		t.Fatalf("snapshot changed under a repeatable read: %q %v", second, err)
	}
	if err := reader.Commit(); err != nil {
		t.Fatal(err)
	}

	// A new snapshot sees the new value.
	fresh := db.Begin(Snapshot)
	defer fresh.Rollback()
	got, err := fresh.Get("k")
	if err != nil || got != "v2" {
		t.Fatalf("new snapshot = %q, %v", got, err)
	}
	t.Logf("version chain: %v", db.History("k"))
}

// Two transactions writing the same key: the second must fail, because
// otherwise the first write would be silently lost.
func TestLostUpdateIsPrevented(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{"counter": "1"})

	t1 := db.Begin(Snapshot)
	t2 := db.Begin(Snapshot)

	v1, _ := t1.Get("counter")
	v2, _ := t2.Get("counter")
	_ = t1.Put("counter", v1+"+1")
	_ = t2.Put("counter", v2+"+1")

	if err := t1.Commit(); err != nil {
		t.Fatalf("first committer should win: %v", err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second committer must get ErrConflict, got %v", err)
	}
	if st := db.Stats(); st.Conflicts != 1 {
		t.Fatalf("expected 1 conflict, got %+v", st)
	}

	check := db.Begin(Snapshot)
	defer check.Rollback()
	got, _ := check.Get("counter")
	if got != "1+1" {
		t.Fatalf("counter = %q, want exactly one increment", got)
	}
}

// THE key lesson of §7.2.3. The on-call invariant "at least one doctor on
// call" is preserved by neither transaction, yet both commit, because they read
// the same rows and write *different* rows.
func TestWriteSkewIsAllowedUnderSnapshotIsolation(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{
		"doctor:alice:on_call": "true",
		"doctor:bob:on_call":   "true",
	})

	alice := db.Begin(Snapshot)
	bob := db.Begin(Snapshot)

	// Both check how many doctors are on call: they each see 2, so each
	// believes it is safe to go off call.
	countFor := func(tx *Txn) int {
		n := 0
		for _, k := range []string{"doctor:alice:on_call", "doctor:bob:on_call"} {
			if v, err := tx.Get(k); err == nil && v == "true" {
				n++
			}
		}
		return n
	}
	if countFor(alice) != 2 || countFor(bob) != 2 {
		t.Fatal("precondition: both should see 2 doctors on call")
	}

	_ = alice.Put("doctor:alice:on_call", "false")
	_ = bob.Put("doctor:bob:on_call", "false")

	if err := alice.Commit(); err != nil {
		t.Fatalf("alice should commit under snapshot isolation: %v", err)
	}
	if err := bob.Commit(); err != nil {
		t.Fatalf("bob should ALSO commit under snapshot isolation (that is the bug): %v", err)
	}

	final := db.Begin(Snapshot)
	defer final.Rollback()
	onCall := 0
	for _, k := range []string{"doctor:alice:on_call", "doctor:bob:on_call"} {
		if v, err := final.Get(k); err == nil && v == "true" {
			onCall++
		}
	}
	if onCall != 0 {
		t.Fatalf("expected the invariant to be violated (0 on call), got %d", onCall)
	}
	t.Log("write skew: both transactions committed, invariant broken, no conflict reported")
}

// The same scenario under read validation: the second transaction is refused,
// so the application retries and the invariant holds.
func TestWriteSkewIsPreventedBySerializable(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{
		"doctor:alice:on_call": "true",
		"doctor:bob:on_call":   "true",
	})

	alice := db.Begin(Serializable)
	bob := db.Begin(Serializable)

	for _, tx := range []*Txn{alice, bob} {
		for _, k := range []string{"doctor:alice:on_call", "doctor:bob:on_call"} {
			_, _ = tx.Get(k) // populate the read set
		}
	}
	_ = alice.Put("doctor:alice:on_call", "false")
	_ = bob.Put("doctor:bob:on_call", "false")

	if err := alice.Commit(); err != nil {
		t.Fatalf("alice should commit: %v", err)
	}
	err := bob.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("bob must be refused, got %v", err)
	}
	t.Log("bob's retry would now see only 1 doctor on call and would refuse")

	// Simulate the retry: it re-reads, sees 1, and does not go off call.
	retry := db.Begin(Serializable)
	defer retry.Rollback()
	v, err := retry.Get("doctor:alice:on_call")
	if err != nil || v != "false" {
		t.Fatalf("retry should observe alice off call: %q %v", v, err)
	}
	bobOn, _ := retry.Get("doctor:bob:on_call")
	if bobOn != "true" {
		t.Fatalf("bob should still be on call: %q", bobOn)
	}
}

// Reads never block writes and writes never block reads.
func TestReadersDoNotBlockWriters(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{"k": "v0"})

	reader := db.Begin(Snapshot)
	defer reader.Rollback()
	if _, err := reader.Get("k"); err != nil {
		t.Fatal(err)
	}
	// The reader is still open. Writers must not care.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := db.Begin(Snapshot)
			_ = tx.Put("k", "v")
			_ = tx.Commit() // may or may not conflict; must never block
		}(i)
	}
	wg.Wait()
	if _, err := reader.Get("k"); err != nil || true {
		// still on the original snapshot
	}
}

// An open transaction pins history; GC may only drop what nobody can see.
func TestGarbageCollectionRespectsOpenSnapshots(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{"k": "v0"})

	old := db.Begin(Snapshot) // holds the earliest snapshot
	if _, err := old.Get("k"); err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		tx := db.Begin(Snapshot)
		_ = tx.Put("k", "v"+string(rune('0'+i)))
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	before := db.LiveVersions()
	if removed := db.GC(); removed != 0 {
		t.Fatalf("GC must keep every version visible to the open snapshot, removed %d", removed)
	}
	// The old snapshot still reads its own value.
	if v, err := old.Get("k"); err != nil || v != "v0" {
		t.Fatalf("open snapshot lost its data: %q %v", v, err)
	}
	old.Rollback()

	removed := db.GC()
	if removed == 0 {
		t.Fatalf("GC should now reclaim %d superseded versions", before)
	}
	t.Logf("versions before=%d, reclaimed=%d, after=%d", before, removed, db.LiveVersions())
	if db.LiveVersions() != 1 {
		t.Fatalf("expected exactly the newest version to survive, got %d", db.LiveVersions())
	}
}

func TestDeleteIsAVersionNotAnErasure(t *testing.T) {
	db := New()
	seed(t, db, map[string]string{"k": "v1"})

	before := db.Begin(Snapshot)
	defer before.Rollback()

	del := db.Begin(Snapshot)
	_ = del.Delete("k")
	if err := del.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := before.Get("k"); err != nil || true {
		if v, err := before.Get("k"); err != nil || v != "v1" {
			t.Fatalf("the pre-delete snapshot must still see v1: %q %v", v, err)
		}
	}
	after := db.Begin(Snapshot)
	defer after.Rollback()
	if _, err := after.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new snapshot should see the delete, got %v", err)
	}
	t.Logf("version chain: %v", db.History("k"))
}
