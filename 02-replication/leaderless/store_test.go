package leaderless

import (
	"testing"
)

func TestVectorClockComparison(t *testing.T) {
	cases := []struct {
		name string
		a, b VectorClock
		want Relation
	}{
		{"identical", VectorClock{"n1": 1}, VectorClock{"n1": 1}, Equal},
		{"a newer", VectorClock{"n1": 2}, VectorClock{"n1": 1}, After},
		{"a older", VectorClock{"n1": 1}, VectorClock{"n1": 2}, Before},
		{"concurrent", VectorClock{"n1": 1}, VectorClock{"n2": 1}, Concurrent},
		{"concurrent on same node", VectorClock{"n1": 2, "n2": 1}, VectorClock{"n1": 1, "n2": 2}, Concurrent},
		{"causally after mixing", VectorClock{"n1": 2, "n2": 1}, VectorClock{"n1": 1}, After},
	}
	for _, tc := range cases {
		if got := Compare(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: Compare = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func new3(t *testing.T) *Cluster {
	t.Helper()
	return New([]string{"n1", "n2", "n3"}, 2, 2)
}

// W+R>N means the write quorum and the read quorum must intersect, so a read
// always sees the latest write — with no leader and no global clock.
func TestQuorumOverlapSeesLatestWrite(t *testing.T) {
	c := new3(t)
	if !c.QuorumOverlap() {
		t.Fatal("W+R must exceed N for this test")
	}
	if err := c.PutVia("n1", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	values, conflict, err := c.Get("k")
	if err != nil || conflict || len(values) != 1 || values[0] != "v1" {
		t.Fatalf("Get = %v conflict=%v err=%v", values, conflict, err)
	}

	// Now update through a *different* coordinator; the read still sees it.
	if err := c.PutVia("n2", "k", "v2"); err != nil {
		t.Fatal(err)
	}
	values, _, err = c.Get("k")
	if err != nil || len(values) != 1 || values[0] != "v2" {
		t.Fatalf("after update Get = %v err=%v", values, err)
	}
}

// W+R<=N and quorums may not overlap: the read can silently miss a write. No
// error, no conflict — just a stale answer, which is the worst failure mode.
func TestWPlusRNotGreaterThanNCanMissWrites(t *testing.T) {
	c := New([]string{"n1", "n2", "n3"}, 1, 1)
	if c.QuorumOverlap() {
		t.Fatal("precondition: quorums must not be guaranteed to overlap")
	}
	prefs := c.preferenceList("k")
	// Write reaches exactly one replica...
	if err := c.PutVia(prefs[0], "k", "v1"); err != nil {
		t.Fatal(err)
	}
	// ...and a single-replica read from a different one returns "no such key"
	// with no error at all.
	values, _, err := c.GetVia(prefs[1], "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 0 {
		t.Fatalf("expected a silent miss from a non-overlapping quorum, got %v", values)
	}
	// Reading from the replica that got the write does see it.
	values, _, err = c.GetVia(prefs[0], "k")
	if err != nil || len(values) != 1 || values[0] != "v1" {
		t.Fatalf("GetVia(writer) = %v err=%v", values, err)
	}
}

// Siblings: a partition lets two coordinators write without seeing each other,
// producing two versions that neither supersedes. The system must surface both
// and let the application merge them.
func TestConcurrentWritesProduceSiblings(t *testing.T) {
	c := new3(t)
	// The read quorum is the first R replicas in the preference list, so the
	// partition must have split exactly those two.
	prefs := c.preferenceList("cart")
	c.Inject(prefs[0], "cart", "apple", VectorClock{"n1": 1})
	c.Inject(prefs[1], "cart", "banana", VectorClock{"n2": 1})

	values, conflict, err := c.Get("cart")
	if err != nil {
		t.Fatal(err)
	}
	if !conflict || len(values) != 2 {
		t.Fatalf("expected 2 sibling values, got %v (conflict=%v)", values, conflict)
	}
	t.Logf("siblings surfaced to the application: %v", values)

	// A later write observes both siblings and therefore supersedes both.
	if err := c.PutVia("n3", "cart", "cherry"); err != nil {
		t.Fatal(err)
	}
	values, conflict, err = c.Get("cart")
	if err != nil {
		t.Fatal(err)
	}
	if conflict || len(values) != 1 || values[0] != "cherry" {
		t.Fatalf("after the resolving write: %v conflict=%v", values, conflict)
	}
}

// Read repair: the read path is where stale replicas get fixed. A quorum read
// returns the newest version, then writes it back to whoever was behind.
func TestReadRepairHealsStaleReplica(t *testing.T) {
	c := new3(t)
	prefs := c.preferenceList("k")
	// A partition left these two with different, causally ordered versions and
	// no hints parked anywhere.
	c.Inject(prefs[0], "k", "stale", VectorClock{"n1": 1})
	c.Inject(prefs[1], "k", "fresh", VectorClock{"n1": 2})

	values, conflict, err := c.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if conflict || len(values) != 1 || values[0] != "fresh" {
		t.Fatalf("quorum read should resolve to the newest version, got %v conflict=%v", values, conflict)
	}
	if st := c.Stats(); st.ReadRepairs == 0 {
		t.Fatal("expected the stale replica to be repaired on the read path")
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		got := c.Inspect(id, "k")
		if len(got) != 1 || got[0].Value != "fresh" {
			t.Fatalf("%s = %v, want the reparied newest version", id, got)
		}
	}
}

// Hinted handoff: with a down replica, a "sloppy quorum" keeps accepting
// writes and parks them; when the replica returns, the hints are delivered.
func TestHintedHandoffDeliversOnRecovery(t *testing.T) {
	c := new3(t)
	prefs := c.preferenceList("k")
	down := prefs[0]
	c.Fail(down)

	if err := c.PutVia(prefs[1], "k", "v1"); err != nil {
		t.Fatalf("sloppy quorum should still accept the write: %v", err)
	}
	if st := c.Stats(); st.PendingHints == 0 {
		t.Fatal("expected a pending hint for the down replica")
	}

	delivered := c.Recover(down)
	if delivered == 0 {
		t.Fatal("recovery should deliver the hinted write")
	}
	got := c.Inspect(down, "k")
	if len(got) != 1 || got[0].Value != "v1" {
		t.Fatalf("hinted write not delivered: %v", got)
	}
	if st := c.Stats(); st.PendingHints != 0 {
		t.Fatalf("hints should be drained, %d left", st.PendingHints)
	}
}

// Anti-entropy is the background sweep that fixes divergence read repair never
// touched.
func TestAntiEntropyReconcilesSilentDivergence(t *testing.T) {
	c := new3(t)
	// Hand-craft divergence: n1 knows v2, n2 knows v1, n3 knows nothing.
	c.replicas["n1"].data["k"] = []VersionedValue{{Value: "v2", Clock: VectorClock{"n1": 2}}}
	c.replicas["n2"].data["k"] = []VersionedValue{{Value: "v1", Clock: VectorClock{"n1": 1}}}

	moved := c.AntiEntropy()
	if moved == 0 {
		t.Fatal("anti-entropy should have found divergence")
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		got := c.Inspect(id, "k")
		if len(got) != 1 || got[0].Value != "v2" {
			t.Fatalf("%s = %v, want the single newest version", id, got)
		}
	}
}

func TestDeleteUsesTombstone(t *testing.T) {
	c := new3(t)
	if err := c.PutVia("n1", "k", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Del("k"); err != nil {
		t.Fatal(err)
	}
	values, _, err := c.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 0 {
		t.Fatalf("deleted key should read as empty, got %v", values)
	}
	// A tombstone must still exist on disk to shadow the older value.
	seen := false
	for _, id := range []string{"n1", "n2", "n3"} {
		for _, v := range c.Inspect(id, "k") {
			if v.Deleted {
				seen = true
			}
		}
	}
	if !seen {
		t.Fatal("expected a tombstone somewhere (a bare delete would resurrect the value)")
	}
}
