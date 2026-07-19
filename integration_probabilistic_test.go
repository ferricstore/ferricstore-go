//go:build integration

package ferricstore

import "testing"

func TestIntegrationProbabilisticHelpers(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()

	runID := integrationSuffix("prob")
	prefix := "go-sdk:prob:" + runID + ":"
	defer cleanupPrefix(t, ctx, client, prefix)

	bloom := prefix + "bf"
	requireTrue(t, must[bool](t)(client.Bloom().Reserve(ctx, bloom, 0.01, 100)))
	_ = must[bool](t)(client.Bloom().Add(ctx, bloom, "a"))
	requireLen(t, must[[]bool](t)(client.Bloom().MAdd(ctx, bloom, "b", "c")), 2)
	_ = must[bool](t)(client.Bloom().Exists(ctx, bloom, "a"))
	requireLen(t, must[[]bool](t)(client.Bloom().MExists(ctx, bloom, "a", "z")), 2)
	requirePositive(t, must[int64](t)(client.Bloom().Card(ctx, bloom)))
	requireMap(t, must[map[string]any](t)(client.Bloom().Info(ctx, bloom)))

	cuckoo := prefix + "cf"
	requireTrue(t, must[bool](t)(client.Cuckoo().Reserve(ctx, cuckoo, 100)))
	_ = must[bool](t)(client.Cuckoo().Add(ctx, cuckoo, "a"))
	_ = must[bool](t)(client.Cuckoo().AddNX(ctx, cuckoo, "b"))
	_ = must[bool](t)(client.Cuckoo().Exists(ctx, cuckoo, "a"))
	requireLen(t, must[[]bool](t)(client.Cuckoo().MExists(ctx, cuckoo, "a", "z")), 2)
	requireNonNegative(t, must[int64](t)(client.Cuckoo().Count(ctx, cuckoo, "a")))
	_ = must[bool](t)(client.Cuckoo().Del(ctx, cuckoo, "a"))
	requireMap(t, must[map[string]any](t)(client.Cuckoo().Info(ctx, cuckoo)))

	cmsA := prefix + "{cms}:a"
	cmsB := prefix + "{cms}:b"
	cmsDst := prefix + "{cms}:dst"
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByDim(ctx, cmsA, 20, 4)))
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByDim(ctx, cmsB, 20, 4)))
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByProb(ctx, prefix+"cms-prob", 0.01, 0.01)))
	requireLen(t, must[[]int64](t)(client.CountMinSketch().IncrByMany(ctx, cmsA, CMSIncrement{Item: "a", Count: 2}, CMSIncrement{Item: "b", Count: 3})), 2)
	requireValue(t, must[any](t)(client.CountMinSketch().IncrBy(ctx, cmsB, "a", 1)))
	requireLen(t, must[[]int64](t)(client.CountMinSketch().Query(ctx, cmsA, "a", "b")), 2)
	requireTrue(t, must[bool](t)(client.CountMinSketch().Merge(ctx, cmsDst, CMSMergeOptions{Sources: []string{cmsA, cmsB}})))
	requireMap(t, must[map[string]any](t)(client.CountMinSketch().Info(ctx, cmsDst)))

	topk := prefix + "topk"
	requireTrue(t, must[bool](t)(client.TopK().ReserveWithOptions(ctx, topk, 3, TopKReserveOptions{
		Width: Int64(20),
		Depth: Int64(7),
	})))
	for index, evicted := range must[[]any](t)(client.TopK().Add(ctx, topk, "a", "b", "a")) {
		if evicted != nil {
			t.Fatalf("TOPK.ADD eviction %d = %#v, want nil while filling the sketch", index, evicted)
		}
	}
	for index, evicted := range must[[]any](t)(client.TopK().IncrBy(ctx, topk, TopKIncrement{Item: "c", Count: 2})) {
		if evicted != nil {
			t.Fatalf("TOPK.INCRBY eviction %d = %#v, want nil while filling the sketch", index, evicted)
		}
	}
	query := must[[]bool](t)(client.TopK().Query(ctx, topk, "a", "z"))
	if !query[0] || query[1] {
		t.Fatalf("TOPK.QUERY = %#v, want [true false]", query)
	}
	items := must[[]any](t)(client.TopK().List(ctx, topk))
	entries := must[[]TopKEntry](t)(client.TopK().ListWithCount(ctx, topk))
	if len(items) == 0 || len(entries) != len(items) {
		t.Fatalf("TOPK list lengths = items:%d entries:%d", len(items), len(entries))
	}
	for index, entry := range entries {
		if _, ok := entry.Item.(string); !ok || entry.Count <= 0 {
			t.Fatalf("TOPK entry %d = %#v, want decoded string and positive count", index, entry)
		}
	}
	counts := must[[]int64](t)(client.TopK().Count(ctx, topk, "a", "z"))
	if counts[0] <= 0 || counts[1] != 0 {
		t.Fatalf("TOPK.COUNT = %#v, want positive count and zero", counts)
	}
	info := must[map[string]any](t)(client.TopK().Info(ctx, topk))
	for field, want := range map[string]int64{"k": 3, "width": 20, "depth": 7} {
		got, err := responseInt64(info[field], nil)
		if err != nil || got != want {
			t.Fatalf("TOPK.INFO %s = %#v, %v; want %d", field, info[field], err, want)
		}
	}

	tdigest := prefix + "{tdigest}:main"
	tdigestSrc := prefix + "{tdigest}:src"
	requireTrue(t, must[bool](t)(client.TDigest().Create(ctx, tdigest, nil)))
	requireTrue(t, must[bool](t)(client.TDigest().Add(ctx, tdigest, 1, 2, 3, 4)))
	requireLen(t, must[[]float64](t)(client.TDigest().Quantile(ctx, tdigest, 0.5)), 1)
	requireLen(t, must[[]float64](t)(client.TDigest().CDF(ctx, tdigest, 2)), 1)
	requireLen(t, must[[]int64](t)(client.TDigest().Rank(ctx, tdigest, 2)), 1)
	requireLen(t, must[[]int64](t)(client.TDigest().RevRank(ctx, tdigest, 2)), 1)
	requireLen(t, must[[]float64](t)(client.TDigest().ByRank(ctx, tdigest, 1)), 1)
	requireLen(t, must[[]float64](t)(client.TDigest().ByRevRank(ctx, tdigest, 1)), 1)
	requireNonNegativeFloat(t, must[float64](t)(client.TDigest().TrimmedMean(ctx, tdigest, 0.1, 0.9)))
	requireNonNegativeFloat(t, must[float64](t)(client.TDigest().Min(ctx, tdigest)))
	requireNonNegativeFloat(t, must[float64](t)(client.TDigest().Max(ctx, tdigest)))
	requireMap(t, must[map[string]any](t)(client.TDigest().Info(ctx, tdigest)))
	requireTrue(t, must[bool](t)(client.TDigest().Create(ctx, tdigestSrc, nil)))
	requireTrue(t, must[bool](t)(client.TDigest().Add(ctx, tdigestSrc, 5, 6)))
	requireTrue(t, must[bool](t)(client.TDigest().Merge(ctx, prefix+"{tdigest}:dst", TDigestMergeOptions{Sources: []string{tdigest, tdigestSrc}, Override: true})))
	requireTrue(t, must[bool](t)(client.TDigest().Reset(ctx, tdigest)))
}
