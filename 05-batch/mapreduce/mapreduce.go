// Package mapreduce implements DDIA §10: batch processing as a programming
// model, with the parts that actually matter in practice made visible:
//
//	shuffling      the all-to-all data movement that dominates cost
//	partitioning   which reducer gets a key (hash for load spread, range for order)
//	data skew      one hot key making 99% of the work land on one reducer
//	determinism    retries are safe only if map/reduce are pure functions
//
// The executor is deliberately in-process and single-machine: the lesson is the
// *shape* of the computation (map -> shuffle -> reduce, twice for PageRank and
// joins), not the cluster plumbing.
package mapreduce

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
)

// KV is one record flowing between phases.
type KV struct {
	Key   string
	Value string
}

// MapFunc turns an input record into zero or more intermediate pairs.
type MapFunc func(key, value string) []KV

// ReduceFunc folds all values for one shuffled key into one output record.
type ReduceFunc func(key string, values []string) []KV

// Job describes a batch computation.
type Job struct {
	Name        string
	Inputs      []KV
	Map         MapFunc
	Reduce      ReduceFunc
	NumReducers int
	MapWorkers  int
}

// Stats exposes the numbers that decide whether a batch job scales.
type Stats struct {
	MapTasks          int
	ReduceTasks       int
	IntermediatePairs int
	BytesShuffled     int
	PartitionSizes    []int
	MaxPartitionShare float64  // skew: largest reducer's share of the data
	SkewedKeys        []string // keys whose fan-out exceeds the threshold
}

// Result is the job output plus its statistics.
type Result struct {
	Output []KV
	Stats  Stats
}

// Run executes the job.
func Run(job Job) Result {
	if job.NumReducers <= 0 {
		job.NumReducers = 3
	}
	if job.MapWorkers <= 0 {
		job.MapWorkers = 4
	}

	// ---- map phase (embarrassingly parallel: no shared state) ----
	type mapOut struct {
		inputs []KV
		out    []KV
	}
	chunks := splitInputs(job.Inputs, job.MapWorkers)
	results := make([]mapOut, len(chunks))
	var wg sync.WaitGroup
	for i, chunk := range chunks {
		wg.Add(1)
		go func(i int, chunk []KV) {
			defer wg.Done()
			res := mapOut{inputs: chunk}
			for _, kv := range chunk {
				res.out = append(res.out, job.Map(kv.Key, kv.Value)...)
			}
			results[i] = res
		}(i, chunk)
	}
	wg.Wait()

	stats := Stats{MapTasks: len(chunks), ReduceTasks: job.NumReducers}

	// ---- shuffle: partition by hash so equal keys land together ----
	partitions := make([][]KV, job.NumReducers)
	for _, res := range results {
		for _, kv := range res.out {
			p := partitionOf(kv.Key, job.NumReducers)
			partitions[p] = append(partitions[p], kv)
			stats.IntermediatePairs++
			stats.BytesShuffled += len(kv.Key) + len(kv.Value)
		}
	}
	for _, p := range partitions {
		stats.PartitionSizes = append(stats.PartitionSizes, len(p))
	}
	stats.MaxPartitionShare = maxShare(stats.PartitionSizes)

	// ---- reduce phase ----
	var mu sync.Mutex
	var out []KV
	for i := range partitions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			grouped := groupByKey(partitions[i])
			var local []KV
			for _, g := range grouped {
				if len(g.values) > skewThreshold {
					// A single key with enormous fan-out: this is the classic
					// reducer straggler (DDIA §10.2 "Skew"). The standard fixes
					// are a combiner, salting the key, or a two-stage reduce.
				}
				local = append(local, job.Reduce(g.key, g.values)...)
			}
			mu.Lock()
			out = append(out, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	stats.SkewedKeys = detectSkewedKeys(partitions)
	return Result{Output: out, Stats: stats}
}

type group struct {
	key    string
	values []string
}

func groupByKey(pairs []KV) []group {
	m := map[string][]string{}
	order := []string{}
	for _, kv := range pairs {
		if _, seen := m[kv.Key]; !seen {
			order = append(order, kv.Key)
		}
		m[kv.Key] = append(m[kv.Key], kv.Value)
	}
	sort.Strings(order) // deterministic output, decisive for reproducible retries
	out := make([]group, 0, len(order))
	for _, k := range order {
		out = append(out, group{key: k, values: m[k]})
	}
	return out
}

func partitionOf(key string, n int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

func splitInputs(inputs []KV, workers int) [][]KV {
	if workers > len(inputs) {
		workers = len(inputs)
	}
	if workers < 1 {
		workers = 1
	}
	size := (len(inputs) + workers - 1) / workers
	var out [][]KV
	for i := 0; i < len(inputs); i += size {
		end := i + size
		if end > len(inputs) {
			end = len(inputs)
		}
		out = append(out, inputs[i:end])
	}
	return out
}

func maxShare(sizes []int) float64 {
	total, max := 0, 0
	for _, s := range sizes {
		total += s
		if s > max {
			max = s
		}
	}
	if total == 0 {
		return 0
	}
	return float64(max) / float64(total)
}

const skewThreshold = 100

func detectSkewedKeys(partitions [][]KV) []string {
	fanout := map[string]int{}
	for _, p := range partitions {
		for _, kv := range p {
			fanout[kv.Key]++
		}
	}
	var out []string
	for k, c := range fanout {
		if c > skewThreshold {
			out = append(out, fmt.Sprintf("%s(%d)", k, c))
		}
	}
	sort.Strings(out)
	return out
}

// ------------------------------------------------------------------- jobs --

// WordCount maps each word to 1 and sums.
func WordCount(inputs []KV) Result {
	return Run(Job{
		Name:   "wordcount",
		Inputs: inputs,
		Map: func(_, value string) []KV {
			var out []KV
			for _, w := range strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
				return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
			}) {
				out = append(out, KV{Key: w, Value: "1"})
			}
			return out
		},
		Reduce: func(key string, values []string) []KV {
			return []KV{{Key: key, Value: fmt.Sprint(len(values))}}
		},
	})
}

// Join implements a reduce-side join: both tables are mapped to the join key,
// tagged by origin, and the reducer emits the cartesian product per key.
//
// The cost is the shuffle: the *entire* left table crosses the network. A
// map-side (broadcast) join avoids that when one side is small enough to fit in
// memory — the classic batch-join trade-off.
func Join(left, right []KV) Result {
	inputs := make([]KV, 0, len(left)+len(right))
	for _, kv := range left {
		inputs = append(inputs, KV{Key: kv.Key, Value: "L:" + kv.Value})
	}
	for _, kv := range right {
		inputs = append(inputs, KV{Key: kv.Key, Value: "R:" + kv.Value})
	}
	return Run(Job{
		Name:        "join",
		Inputs:      inputs,
		NumReducers: 4,
		Map: func(key, value string) []KV {
			return []KV{{Key: extractionKey(key, value), Value: value}}
		},
		Reduce: func(key string, values []string) []KV {
			var ls, rs []string
			for _, v := range values {
				switch {
				case strings.HasPrefix(v, "L:"):
					ls = append(ls, strings.TrimPrefix(v, "L:"))
				case strings.HasPrefix(v, "R:"):
					rs = append(rs, strings.TrimPrefix(v, "R:"))
				}
			}
			var out []KV
			for _, l := range ls {
				for _, r := range rs {
					out = append(out, KV{Key: key, Value: l + "+" + r})
				}
			}
			return out
		},
	})
}

// extractionKey decodes the "<rowid>#<joinKey>" input encoding.
func extractionKey(key, value string) string {
	if i := strings.LastIndex(key, "#"); i >= 0 {
		return key[i+1:]
	}
	return key
}

// EncodeJoinInput builds a Join input row.
func EncodeJoinInput(rowID, joinKey, payload string) KV {
	return KV{Key: rowID + "#" + joinKey, Value: payload}
}

// PageRank runs a fixed number of iterations; each iteration is one
// map/shuffle/reduce job, which is exactly how GraphX and friends do it.
func PageRank(edges map[string][]string, iterations int, damping float64) map[string]float64 {
	nodes := map[string]bool{}
	for from, tos := range edges {
		nodes[from] = true
		for _, t := range tos {
			nodes[t] = true
		}
	}
	ids := make([]string, 0, len(nodes))
	for n := range nodes {
		ids = append(ids, n)
	}
	sort.Strings(ids)

	rank := map[string]float64{}
	for _, n := range ids {
		rank[n] = 1 / float64(len(ids))
	}

	for it := 0; it < iterations; it++ {
		inputs := make([]KV, 0, len(edges))
		for _, from := range ids {
			payload := fmt.Sprintf("%s|%s", from, strings.Join(edges[from], ","))
			// Send the rank contribution to every neighbour.
			for _, to := range edges[from] {
				inputs = append(inputs, KV{Key: to, Value: fmt.Sprintf("%f", rank[from]/float64(max(1, len(edges[from]))))})
			}
			inputs = append(inputs, KV{Key: from, Value: payload})
		}
		res := Run(Job{
			Name:        fmt.Sprintf("pagerank-%d", it),
			Inputs:      inputs,
			NumReducers: 3,
			Map:         func(key, value string) []KV { return []KV{{Key: key, Value: value}} },
			Reduce: func(key string, values []string) []KV {
				sum := 0.0
				for _, v := range values {
					if strings.Contains(v, "|") {
						continue // the topology record, not a contribution
					}
					var f float64
					_, _ = fmt.Sscanf(v, "%f", &f)
					sum += f
				}
				next := (1-damping)/float64(len(ids)) + damping*sum
				return []KV{{Key: key, Value: fmt.Sprintf("%.10f", next)}}
			},
		})
		for _, kv := range res.Output {
			var f float64
			_, _ = fmt.Sscanf(kv.Value, "%f", &f)
			rank[kv.Key] = f
		}
	}
	return rank
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
