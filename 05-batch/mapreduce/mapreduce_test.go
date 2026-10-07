package mapreduce

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestWordCountMatchesNaive(t *testing.T) {
	docs := []KV{
		{"doc1", "the quick brown fox"},
		{"doc2", "the lazy dog and the fox"},
		{"doc3", "quick quick quick"},
	}
	res := WordCount(docs)

	// Naive reference implementation.
	want := map[string]int{}
	for _, d := range docs {
		for _, w := range strings.Fields(d.Value) {
			want[w]++
		}
	}
	got := map[string]int{}
	for _, kv := range res.Output {
		var n int
		_, _ = fmt.Sscanf(kv.Value, "%d", &n)
		got[kv.Key] = n
	}
	if len(got) != len(want) {
		t.Fatalf("got %d keys, want %d (%v vs %v)", len(got), len(want), got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %d, want %d", k, got[k], v)
		}
	}
	t.Logf("stats: mapTasks=%d reduceTasks=%d pairs=%d bytes=%d partitions=%v skew=%.2f",
		res.Stats.MapTasks, res.Stats.ReduceTasks, res.Stats.IntermediatePairs,
		res.Stats.BytesShuffled, res.Stats.PartitionSizes, res.Stats.MaxPartitionShare)
}

func TestShuffleSpreadsKeysAcrossReducers(t *testing.T) {
	var docs []KV
	for i := 0; i < 500; i++ {
		docs = append(docs, KV{Key: fmt.Sprint(i), Value: fmt.Sprintf("word%d word%d common", i, i*7)})
	}
	res := WordCount(docs)
	if len(res.Stats.PartitionSizes) != 3 {
		t.Fatalf("expected 3 partitions, got %v", res.Stats.PartitionSizes)
	}
	// "common" appears 500 times and must not dominate a reducer by itself.
	for _, s := range res.Stats.PartitionSizes {
		if s == 0 {
			t.Fatalf("a reducer got no work at all — hashing is broken: %v", res.Stats.PartitionSizes)
		}
	}
	if res.Stats.MaxPartitionShare > 0.6 {
		t.Fatalf("hash partitioning is badly skewed: %v", res.Stats.PartitionSizes)
	}
}

// Data skew: one key with astronomic fan-out is the classic batch-job straggler.
// The framework reports it instead of hiding it.
func TestSkewIsDetected(t *testing.T) {
	inputs := []KV{}
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, KV{Key: "r1", Value: "hotkey"})
	}
	inputs = append(inputs, KV{Key: "r2", Value: "coldkey"})

	res := WordCount(inputs)
	t.Logf("skewed keys: %v", res.Stats.SkewedKeys)
	if len(res.Stats.SkewedKeys) == 0 {
		t.Fatal("expected the hot key to be reported as skewed")
	}
	if res.Stats.MaxPartitionShare < 0.9 {
		t.Fatalf("one reducer should own nearly all the data: %.2f", res.Stats.MaxPartitionShare)
	}
}

func TestReduceSideJoin(t *testing.T) {
	left := []KV{
		EncodeJoinInput("u1", "dept:eng", "alice"),
		EncodeJoinInput("u2", "dept:eng", "bob"),
		EncodeJoinInput("u3", "dept:sales", "carol"),
	}
	right := []KV{
		EncodeJoinInput("d1", "dept:eng", "Engineering"),
		EncodeJoinInput("d2", "dept:sales", "Sales"),
	}
	res := Join(left, right)

	pairs := map[string]bool{}
	for _, kv := range res.Output {
		pairs[kv.Value] = true
	}
	for _, want := range []string{"alice+Engineering", "bob+Engineering", "carol+Sales"} {
		if !pairs[want] {
			t.Fatalf("missing join result %q; got %v", want, pairs)
		}
	}
	// A department with no employees must not produce a row (inner join).
	if len(pairs) != 3 {
		t.Fatalf("expected exactly 3 rows, got %v", pairs)
	}
	t.Logf("shuffled %d pairs to produce %d join rows", res.Stats.IntermediatePairs, len(res.Output))
}

func TestPageRankConvergesAndRanksByIncomingLinks(t *testing.T) {
	// A star: everyone links to "hub", which links back to nobody.
	edges := map[string][]string{
		"a":   {"hub"},
		"b":   {"hub"},
		"c":   {"hub"},
		"hub": {"a"},
	}
	ranks := PageRank(edges, 30, 0.85)
	hub := ranks["hub"]
	for _, n := range []string{"a", "b", "c"} {
		if hub <= ranks[n] {
			t.Fatalf("hub (%.4f) should outrank %s (%.4f)", hub, n, ranks[n])
		}
	}
	sum := 0.0
	for _, r := range ranks {
		sum += r
	}
	if math.Abs(sum-1) > 1e-6 {
		t.Fatalf("ranks must sum to 1, got %.10f", sum)
	}
	t.Logf("ranks: hub=%.4f a=%.4f b=%.4f c=%.4f", hub, ranks["a"], ranks["b"], ranks["c"])
}
