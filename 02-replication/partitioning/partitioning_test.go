package partitioning

import (
	"fmt"
	"testing"
)

// The headline number of §6.2: consistent hashing moves only the keys that
// belonged to the arc the new node takes over (~1/(N+1)), while modulo hashing
// moves almost everything.
func TestKeyMovementOnAddingANode(t *testing.T) {
	keys := SampleKeys(20000)

	// --- modulo hashing ---
	before3 := []string{"n1", "n2", "n3"}
	before4 := []string{"n1", "n2", "n3", "n4"}
	modMoved, modTotal := KeyMovement(keys,
		func(k string) string { return ModuloAssign(before3, k) },
		func(k string) string { return ModuloAssign(before4, k) },
	)
	modFrac := float64(modMoved) / float64(modTotal)

	// --- consistent hashing ---
	ring := NewHashRing(160)
	for _, n := range before3 {
		ring.AddNode(n)
	}
	ring4 := NewHashRing(160)
	for _, n := range before4 {
		ring4.AddNode(n)
	}
	ringMoved, ringTotal := KeyMovement(keys,
		func(k string) string { return ring.Locate(k) },
		func(k string) string { return ring4.Locate(k) },
	)
	ringFrac := float64(ringMoved) / float64(ringTotal)

	t.Logf("adding a 4th node to a 3-node cluster out of %d keys:", ringTotal)
	t.Logf("  modulo hashing:      %5d keys moved (%.1f%%)", modMoved, modFrac*100)
	t.Logf("  consistent hashing:  %5d keys moved (%.1f%%)", ringMoved, ringFrac*100)

	if modFrac < 0.5 {
		t.Fatalf("modulo hashing should have remapped most keys, moved %.1f%%", modFrac*100)
	}
	// Theory: a new node takes 1/4 of the ring, so ~25% of keys move. Allow slack
	// for hash variance.
	if ringFrac > 0.40 {
		t.Fatalf("consistent hashing moved too much (%.1f%%); expected ~25%%", ringFrac*100)
	}
	if ringFrac < 0.10 {
		t.Fatalf("consistent hashing moved too little (%.1f%%)", ringFrac*100)
	}
}

func TestVirtualNodesEvenOutTheLoad(t *testing.T) {
	keys := SampleKeys(50000)
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	var sparse, dense float64
	{
		ring := NewHashRing(1)
		for _, n := range nodes {
			ring.AddNode(n)
		}
		sparse = ring.LoadImbalance(keys)
	}
	{
		ring := NewHashRing(256)
		for _, n := range nodes {
			ring.AddNode(n)
		}
		dense = ring.LoadImbalance(keys)
		dist := ring.Distribution(keys)
		t.Logf("1 vnode/node   -> max/avg load = %.2fx (%v)", sparse, dist)
		dist = ring.Distribution(keys)
		t.Logf("256 vnodes/node -> max/avg load = %.2fx", dense)
		for n, c := range dist {
			t.Logf("   %s: %d keys (%.1f%%)", n, c, float64(c)/float64(len(keys))*100)
		}
	}
	if dense > sparse {
		t.Fatalf("more virtual nodes should balance better: 256 -> %.2f, 1 -> %.2f", dense, sparse)
	}
	if dense > 1.15 {
		t.Fatalf("256 vnodes should be within 15%% of average, got %.2fx", dense)
	}
}

func TestRemovingANodeKeepsOtherAssignments(t *testing.T) {
	keys := SampleKeys(5000)
	ring := NewHashRing(128)
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		ring.AddNode(n)
	}
	before := map[string]string{}
	for _, k := range keys {
		before[k] = ring.Locate(k)
	}
	ring.RemoveNode("n2")

	for _, k := range keys {
		after := ring.Locate(k)
		if before[k] == "n2" {
			if after == "n2" {
				t.Fatalf("key %s still maps to the removed node", k)
			}
			continue
		}
		if after != before[k] {
			t.Fatalf("key %s moved from %s to %s even though its owner is untouched", k, before[k], after)
		}
	}
}

func TestRangeLocateAndSplit(t *testing.T) {
	p := NewRangePartitioner([]string{"n1", "n2"}, 2)
	t.Logf("initial: %s", p.Describe())

	if got := p.Locate("k00000"); got != "n1" {
		t.Fatalf("Locate(k00000) = %s, want n1", got)
	}
	if got := p.Locate("k90000"); got != "n2" {
		t.Fatalf("Locate(k90000) = %s, want n2", got)
	}

	// A hot key range is fixed by splitting, not by moving the hot key.
	if err := p.Split("k02500", "n3"); err != nil {
		t.Fatal(err)
	}
	t.Logf("after split at k02500: %s", p.Describe())
	if got := p.Locate("k01000"); got != "n1" {
		t.Fatalf("k01000 should stay on the left half, got %s", got)
	}
	if got := p.Locate("k05000"); got != "n3" {
		t.Fatalf("k05000 should move to the new node, got %s", got)
	}
	counts := p.Counts()
	if counts["n3"] != 1 || counts["n1"] != 1 || counts["n2"] != 1 {
		t.Fatalf("unexpected range counts: %v", counts)
	}
}

func TestRangeMerge(t *testing.T) {
	p := NewRangePartitioner([]string{"n1", "n2", "n3", "n4"}, 4)
	before := len(p.Ranges)
	if err := p.MergeRange(2); err != nil {
		t.Fatal(err)
	}
	if len(p.Ranges) != before-1 {
		t.Fatalf("merge did not shrink the range set: %s", p.Describe())
	}
	// The whole keyspace must stay covered after merge.
	for _, k := range []string{"k00000", "k30000", "k60000", "k99999"} {
		if p.Locate(k) == "" {
			t.Fatalf("key %s is no longer covered: %s", k, p.Describe())
		}
	}
}

func TestRebalanceMovesMinimalRanges(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	p := NewRangePartitioner(nodes, 12)
	beforeCounts := p.Counts()

	moved := p.AssignRanges([]string{"n1", "n2", "n3", "n4"})
	afterCounts := p.Counts()
	t.Logf("ranges before: %v", beforeCounts)
	t.Logf("ranges after:  %v (moved per node: %v)", afterCounts, moved)

	total := 0
	for _, v := range moved {
		total += v
	}
	// 12 ranges / 4 nodes = 3 each. n4 must get 3; that is the minimum.
	if total != 3 {
		t.Fatalf("expected exactly 3 ranges to move, moved %d", total)
	}
	for n, c := range afterCounts {
		if c != 3 {
			t.Fatalf("node %s has %d ranges, want 3 (%v)", n, c, afterCounts)
		}
	}
}

func TestRebalanceWhenANodeLeaves(t *testing.T) {
	p := NewRangePartitioner([]string{"n1", "n2", "n3"}, 9)
	moved := p.AssignRanges([]string{"n1", "n2"})
	counts := p.Counts()
	t.Logf("after n3 left: %v (moved %v)", counts, moved)
	for n, c := range counts {
		if n == "n3" {
			t.Fatalf("n3 should own nothing: %v", counts)
		}
		if c != 4 && c != 5 {
			t.Fatalf("unbalanced reassignment: %v", counts)
		}
	}
	for _, k := range SampleKeys(200) {
		if p.Locate(k) == "" {
			t.Fatalf("key %s lost its owner after rebalance", k)
		}
	}
}

func TestModuloHashingIsBalancedButFragile(t *testing.T) {
	keys := SampleKeys(20000)
	nodes := []string{"n1", "n2", "n3", "n4"}
	counts := map[string]int{}
	for _, k := range keys {
		counts[ModuloAssign(nodes, k)]++
	}
	for _, n := range nodes {
		share := float64(counts[n]) / float64(len(keys))
		if share < 0.22 || share > 0.28 {
			t.Fatalf("modulo hashing should be near-perfectly balanced, %s got %.1f%%", n, share*100)
		}
	}
	fmt.Println("modulo distribution:", counts)
}
