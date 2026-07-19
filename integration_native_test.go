//go:build integration

package ferricstore

import "testing"

func TestIntegrationNativeHelpersAndDiagnostics(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("native")
	prefix := "go-sdk:native:" + runID + ":"
	key := prefix + "cas"
	lockKey := prefix + "lock"
	rateKey := prefix + "rate"
	cacheKey := prefix + "cache"
	errorKey := prefix + "cache-error"
	defer cleanupPrefix(t, ctx, client, prefix)

	requireString(t, must[string](t)(client.Ping(ctx)), "PONG")
	requireString(t, must[string](t)(client.Echo(ctx, "hello")), "hello")
	if err := client.ClientSetName(ctx, "go-sdk-"+runID); err != nil {
		t.Fatal(err)
	}
	requireMap(t, must[map[string]any](t)(client.ClientInfo(ctx)))

	encodedOld := must[any](t)(client.Codec().Encode("old"))
	pipeline := must[[]any](t)(client.Pipeline(ctx, [][]any{
		{"SET", key, encodedOld},
		{"GET", key},
	}))
	requireString(t, must[any](t)(client.Codec().Decode(pipeline[1])), "old")
	requireTrue(t, must[bool](t)(client.CAS(ctx, key, "old", "new", nil)))
	requireString(t, must[any](t)(client.KV().Get(ctx, key)), "new")

	requireTrue(t, must[bool](t)(client.Lock(ctx, lockKey, "owner-a", 30_000)))
	requireInt64(t, must[int64](t)(client.ExtendLock(ctx, lockKey, "owner-a", 30_000)), 1)
	requireInt64(t, must[int64](t)(client.Unlock(ctx, lockKey, "owner-a")), 1)

	rate := must[RateLimitResult](t)(client.RateLimitAdd(ctx, rateKey, 60_000, 5, 2))
	if rate.Count < 1 || rate.Remaining < 0 {
		t.Fatalf("unexpected rate limit result: %#v", rate)
	}
	if info := must[KeyInfo](t)(client.KeyInfo(ctx, key)); len(info.Raw) == 0 {
		t.Fatalf("empty key info: %#v", info)
	}

	first := must[FetchOrComputeResult](t)(client.FetchOrCompute(ctx, cacheKey, 60_000, "integration"))
	if first.Status == "" || first.Status == "hit" {
		t.Fatalf("expected compute response, got %#v", first)
	}
	requireTrue(t, must[bool](t)(client.FetchOrComputeResult(ctx, cacheKey, first.OwnershipToken, map[string]any{"computed": true}, 60_000)))
	if cached := must[FetchOrComputeResult](t)(client.FetchOrCompute(ctx, cacheKey, 60_000, "")); cached.Status != "hit" {
		t.Fatalf("expected cache hit, got %#v", cached)
	}
	failed := must[FetchOrComputeResult](t)(client.FetchOrCompute(ctx, errorKey, 60_000, "integration"))
	requireTrue(t, must[bool](t)(client.FetchOrComputeError(ctx, errorKey, failed.OwnershipToken, "boom")))

	requireMap(t, must[map[string]any](t)(client.ServerInfo(ctx, "server")))
	requirePositive(t, must[int64](t)(client.CommandCount(ctx)))
	if commands := must[[]string](t)(client.CommandList(ctx)); !containsFold(commands, "get") {
		t.Fatalf("COMMAND LIST did not include GET")
	}
	requireValue(t, must[any](t)(client.CommandInfo(ctx, "get")))
	requireValue(t, must[any](t)(client.CommandDocs(ctx, "get")))
	requireValue(t, must[any](t)(client.CommandGetKeys(ctx, "GET", key)))
	requireMap(t, must[map[string]any](t)(client.ClusterHealth(ctx)))
	requireMap(t, must[map[string]any](t)(client.ClusterStats(ctx)))
	requireNonNegative(t, must[int64](t)(client.ClusterKeySlot(ctx, key)))
	requireValue(t, must[any](t)(client.ClusterSlots(ctx)))
	requireMap(t, must[map[string]any](t)(client.ClusterStatus(ctx)))
	requireValue(t, must[any](t)(client.ClusterRole(ctx)))
	requireValue(t, must[any](t)(client.FerricStoreConfig(ctx, "GET", "*")))
	requireValue(t, must[string](t)(client.FerricStoreMetricsText(ctx)))
	requireMap(t, must[map[string]any](t)(client.FerricStoreHotness(ctx)))
	requireValue(t, must[any](t)(client.FerricStoreDoctor(ctx, "CHECK", "SCOPE", "BITCASK")))
}
