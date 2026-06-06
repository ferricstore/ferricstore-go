//go:build integration

package ferricstore

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type claimedFlow struct {
	id           string
	partitionKey string
	job          ClaimedItem
}

func TestIntegrationKVAndFlowRoundTrip(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	requireString(t, must[string](t)(client.Ping(ctx)), "PONG")

	runID := integrationSuffix("smoke")
	key := "go-sdk:kv:" + runID
	id := "go-sdk:flow:" + runID
	typeName := "go-sdk-integration"
	partition := id + ":partition"
	defer cleanupPrefix(t, ctx, client, "go-sdk:kv:"+runID)

	if err := client.KV().Set(ctx, key, map[string]any{"plan": "pro"}); err != nil {
		t.Fatal(err)
	}
	profile, ok := must[any](t)(client.KV().Get(ctx, key)).(map[string]any)
	if !ok || profile["plan"] != "pro" {
		t.Fatalf("unexpected profile: %#v", profile)
	}

	now := time.Now().UnixMilli()
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           id,
		Type:         typeName,
		State:        "queued",
		PartitionKey: partition,
		Payload:      map[string]any{"step": 1},
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
	}))

	job := claimOne(t, ctx, client, typeName, "queued", partition, "go-sdk-worker", now+1, 30_000)
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{
		ID:           job.ID,
		LeaseToken:   job.LeaseToken,
		FencingToken: job.FencingToken,
		PartitionKey: job.PartitionKey,
		Result:       map[string]any{"ok": true},
	}))

	record := must[*FlowRecord](t)(client.Get(ctx, id, partition, nil, nil))
	if record == nil || record.State != "completed" {
		t.Fatalf("expected completed flow, got %#v", record)
	}
}

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
	requireTrue(t, must[bool](t)(client.FetchOrComputeResult(ctx, cacheKey, map[string]any{"computed": true}, 60_000)))
	if cached := must[FetchOrComputeResult](t)(client.FetchOrCompute(ctx, cacheKey, 60_000, "")); cached.Status != "hit" {
		t.Fatalf("expected cache hit, got %#v", cached)
	}
	_ = must[FetchOrComputeResult](t)(client.FetchOrCompute(ctx, errorKey, 60_000, "integration"))
	requireTrue(t, must[bool](t)(client.FetchOrComputeError(ctx, errorKey, "boom")))

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
	requireMap(t, must[map[string]any](t)(client.FerricStoreMetrics(ctx)))
	requireMap(t, must[map[string]any](t)(client.FerricStoreHotness(ctx)))
	requireValue(t, must[any](t)(client.FerricStoreDoctor(ctx, "CHECK", "SCOPE", "BITCASK")))
}

func TestIntegrationTypedStoreFamilies(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(StringCodec{})
	defer client.Close()

	runID := integrationSuffix("store")
	prefix := "go-sdk:store:" + runID + ":"
	defer cleanupPrefix(t, ctx, client, prefix)

	assertStringCommands(t, ctx, client, prefix)
	assertHashCommands(t, ctx, client, prefix)
	assertListSetSortedSetCommands(t, ctx, client, prefix)
	assertStreamBitmapHllGeoCommands(t, ctx, client, prefix, runID)
}

func TestIntegrationProbabilisticHelpersExceptJSON(t *testing.T) {
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

	cmsA := prefix + "cms-a"
	cmsB := prefix + "cms-b"
	cmsDst := prefix + "cms-dst"
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByDim(ctx, cmsA, 20, 4)))
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByDim(ctx, cmsB, 20, 4)))
	requireTrue(t, must[bool](t)(client.CountMinSketch().InitByProb(ctx, prefix+"cms-prob", 0.01, 0.01)))
	requireLen(t, must[[]int64](t)(client.CountMinSketch().IncrByMany(ctx, cmsA, CMSIncrement{Item: "a", Count: 2}, CMSIncrement{Item: "b", Count: 3})), 2)
	requireValue(t, must[any](t)(client.CountMinSketch().IncrBy(ctx, cmsB, "a", 1)))
	requireLen(t, must[[]int64](t)(client.CountMinSketch().Query(ctx, cmsA, "a", "b")), 2)
	requireTrue(t, must[bool](t)(client.CountMinSketch().Merge(ctx, cmsDst, CMSMergeOptions{Sources: []string{cmsA, cmsB}})))
	requireMap(t, must[map[string]any](t)(client.CountMinSketch().Info(ctx, cmsDst)))

	topk := prefix + "topk"
	requireTrue(t, must[bool](t)(client.TopK().Reserve(ctx, topk, 3)))
	requireLen(t, must[[]any](t)(client.TopK().Add(ctx, topk, "a", "b", "a")), 3)
	requireLen(t, must[[]any](t)(client.TopK().IncrBy(ctx, topk, TopKIncrement{Item: "c", Count: 2})), 1)
	requireLen(t, must[[]bool](t)(client.TopK().Query(ctx, topk, "a", "z")), 2)
	requireValue(t, must[any](t)(client.TopK().List(ctx, topk, true)))
	requireLen(t, must[[]int64](t)(client.TopK().Count(ctx, topk, "a", "z")), 2)
	requireMap(t, must[map[string]any](t)(client.TopK().Info(ctx, topk)))

	tdigest := prefix + "tdigest"
	tdigestSrc := prefix + "tdigest-src"
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
	requireTrue(t, must[bool](t)(client.TDigest().Merge(ctx, prefix+"tdigest-dst", TDigestMergeOptions{Sources: []string{tdigest, tdigestSrc}, Override: true})))
	requireTrue(t, must[bool](t)(client.TDigest().Reset(ctx, tdigest)))
}

func TestIntegrationFlowStateMachineRepairAndIndexes(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("flow")
	typeName := "go-sdk-flow-" + runID
	now := time.Now().UnixMilli()

	requireValue(t, must[any](t)(client.InstallPolicy(ctx, typeName, &RetryPolicy{MaxRetries: 2, Backoff: "fixed", BaseMS: 10, MaxMS: 100, ExhaustedTo: "failed"}, nil)))
	requireValue(t, must[any](t)(client.InstallPolicy(ctx, typeName, nil, map[string]RetryPolicy{
		"queued": {MaxRetries: 1, Backoff: "fixed", BaseMS: 10, MaxMS: 100, ExhaustedTo: "failed"},
	})))
	requireMap(t, must[map[string]any](t)(client.PolicyGet(ctx, typeName, "")))
	requireMap(t, must[map[string]any](t)(client.PolicyGet(ctx, typeName, "queued")))

	valueResponse := must[any](t)(client.ValuePut(ctx, map[string]any{"shared": true}, ValuePutOptions{PartitionKey: "go-sdk:value:" + runID, TTLMS: Int64(60_000)}))
	valueRef := asString(responseField(valueResponse, "ref"))
	if valueRef == "" {
		t.Fatalf("FLOW.VALUE.PUT did not return ref: %#v", valueResponse)
	}
	requireLen(t, must[[]any](t)(client.ValueMGet(ctx, []string{valueRef}, nil)), 1)

	signalID := "go-sdk:signal:" + runID
	signalPartition := signalID + ":partition"
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: signalID, Type: typeName, State: "created", PartitionKey: signalPartition, Payload: map[string]any{"step": "created"}, Idempotent: Bool(true)}))
	requireValue(t, must[any](t)(client.Signal(ctx, SignalOptions{ID: signalID, Signal: "approve", PartitionKey: signalPartition, IfStates: []string{"created"}, TransitionTo: "approved"})))
	requireValue(t, must[any](t)(client.FlowSignal(ctx, SignalOptions{ID: signalID, Signal: "ship", PartitionKey: signalPartition, IfStates: []string{"approved"}, TransitionTo: "shipped"})))
	if record := must[*FlowRecord](t)(client.Get(ctx, signalID, signalPartition, nil, nil)); record == nil || record.State != "shipped" {
		t.Fatalf("signal flow = %#v", record)
	}

	assertBatchFlowCommands(t, ctx, client, typeName, runID, now)
	assertSingleMutationCommands(t, ctx, client, typeName, runID, now)
	assertManyMutationCommands(t, ctx, client, typeName, runID, now)
	assertRepairIndexAndRewindCommands(t, ctx, client, typeName, runID, now)

	requireLenAtLeast(t, must[[]FlowRecord](t)(client.List(ctx, typeName, ReadOptions{Count: Int(100)})), 1)
	requireMap(t, must[map[string]any](t)(client.Info(ctx, typeName, "", nil, nil)))
	requireLenAtLeast(t, must[[]any](t)(client.History(ctx, HistoryOptions{ID: signalID, PartitionKey: signalPartition, Count: 5})), 1)
	requireMap(t, must[map[string]any](t)(client.RetentionCleanup(ctx, RetentionCleanupOptions{Limit: Int(10)})))
}

func TestIntegrationQueueAndWorkflowWrappers(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("wrappers")
	queueType := "go-sdk-queue-" + runID
	queueID := "go-sdk:queue:" + runID
	queuePartition := queueID + ":partition"
	queue := NewQueueClient(client).Queue(queueType)

	_ = must[*FlowRecord](t)(queue.Enqueue(ctx, queueID, map[string]any{"step": "queued"}, CreateOptions{PartitionKey: queuePartition, Idempotent: Bool(true)}))
	queueResult := must[QueueWorkerResult](t)(queue.Worker("go-sdk-queue-worker", func(_ context.Context, job FlowRecord) error {
		payload, ok := job.Payload.(map[string]any)
		if !ok || payload["step"] != "queued" {
			return fmt.Errorf("unexpected queue payload: %#v", job.Payload)
		}
		return nil
	}, WorkerOptions{BatchSize: 1, ClaimPayload: true}).RunOnce(ctx))
	if queueResult.Claimed != 1 || queueResult.Completed != 1 || queueResult.Retried != 0 || queueResult.Failed != 0 {
		t.Fatalf("unexpected queue result: %#v", queueResult)
	}

	workflowType := "go-sdk-workflow-" + runID
	workflowID := "go-sdk:workflow:" + runID
	workflowPartition := workflowID + ":partition"
	workflow := NewWorkflowClient(client).Workflow(workflowType, "received")
	workflow.State("received", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionTo("validated", map[string]any{"validated": true}), nil
	})
	workflow.State("validated", func(_ context.Context, ctx WorkflowContext) (Outcome, error) {
		return CompleteWith(map[string]any{"id": ctx.ID(), "done": true}), nil
	})

	_ = must[*FlowRecord](t)(workflow.Start(ctx, workflowID, map[string]any{"order": runID}, CreateOptions{PartitionKey: workflowPartition, Idempotent: Bool(true)}))
	first := must[WorkflowWorkerResult](t)(workflow.Worker("go-sdk-workflow-worker", []string{"received"}, WorkerOptions{BatchSize: 1, ClaimPayload: true}).RunOnce(ctx))
	if first.Claimed != 1 || first.Applied != 1 {
		t.Fatalf("unexpected first workflow result: %#v", first)
	}
	second := must[WorkflowWorkerResult](t)(workflow.Worker("go-sdk-workflow-worker", []string{"validated"}, WorkerOptions{BatchSize: 1, ClaimPayload: true}).RunOnce(ctx))
	if second.Claimed != 1 || second.Applied != 1 {
		t.Fatalf("unexpected second workflow result: %#v", second)
	}
	if record := must[*FlowRecord](t)(client.Get(ctx, workflowID, workflowPartition, nil, nil)); record == nil || record.State != "completed" {
		t.Fatalf("expected completed workflow, got %#v", record)
	}
}

func assertStringCommands(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()

	key := prefix + "string"
	requireOKResponse(t, must[any](t)(client.KV().SetWithOptions(ctx, key, "abc", SetOptions{PXMilliseconds: Int64(60_000)})))
	requireString(t, must[any](t)(client.KV().Get(ctx, key)), "abc")
	requireInt64(t, must[int64](t)(client.KV().Exists(ctx, key)), 1)
	values := must[[]any](t)(client.KV().MGet(ctx, key, prefix+"missing"))
	if len(values) != 2 || values[0] != "abc" || values[1] != nil {
		t.Fatalf("MGET = %#v", values)
	}
	if err := client.KV().MSet(ctx, map[string]any{prefix + "string2": "2", prefix + "string3": "3"}); err != nil {
		t.Fatal(err)
	}
	requireTrue(t, must[bool](t)(client.KV().MSetNX(ctx, map[string]any{prefix + "nx1": "1", prefix + "nx2": "2"})))
	requireInt64(t, must[int64](t)(client.KV().Incr(ctx, prefix+"counter")), 1)
	requireInt64(t, must[int64](t)(client.KV().IncrBy(ctx, prefix+"counter", 4)), 5)
	requireInt64(t, must[int64](t)(client.KV().Decr(ctx, prefix+"counter")), 4)
	requireInt64(t, must[int64](t)(client.KV().DecrBy(ctx, prefix+"counter", 2)), 2)
	requireNonNegativeFloat(t, must[float64](t)(client.KV().IncrByFloat(ctx, prefix+"float", 1.5)))
	requireInt64(t, must[int64](t)(client.KV().Append(ctx, prefix+"append", "abc")), 3)
	requireInt64(t, must[int64](t)(client.KV().StrLen(ctx, prefix+"append")), 3)
	requireString(t, must[any](t)(client.KV().GetSet(ctx, prefix+"append", "xyz")), "abc")
	requireString(t, must[any](t)(client.KV().GetRange(ctx, prefix+"append", 0, 1)), "xy")
	requireInt64(t, must[int64](t)(client.KV().SetRange(ctx, prefix+"append", 1, "Q")), 3)
	requireString(t, must[any](t)(client.KV().GetEX(ctx, prefix+"append", GetEXOptions{PXMilliseconds: Int64(60_000)})), "xQz")
	requireNonNegative(t, must[int64](t)(client.KV().TTL(ctx, prefix+"append")))
	requireNonNegative(t, must[int64](t)(client.KV().PTTL(ctx, prefix+"append")))
	requireTrue(t, must[bool](t)(client.KV().Persist(ctx, prefix+"append")))
	requireTrue(t, must[bool](t)(client.KV().Expire(ctx, prefix+"append", 60)))
	requireTrue(t, must[bool](t)(client.KV().PExpire(ctx, prefix+"append", 60_000)))
	requireTrue(t, must[bool](t)(client.KV().ExpireAt(ctx, prefix+"append", time.Now().Unix()+60)))
	requireTrue(t, must[bool](t)(client.KV().PExpireAt(ctx, prefix+"append", time.Now().UnixMilli()+60_000)))
	requireNonNegative(t, must[int64](t)(client.KV().ExpireTime(ctx, prefix+"append")))
	requireNonNegative(t, must[int64](t)(client.KV().PExpireTime(ctx, prefix+"append")))
	requireString(t, must[string](t)(client.Type(ctx, prefix+"append")), "string")
	requireTrue(t, must[bool](t)(client.KV().SetNX(ctx, prefix+"setnx", "1")))
	if err := client.KV().SetEX(ctx, prefix+"setex", 60, "1"); err != nil {
		t.Fatal(err)
	}
	if err := client.KV().PSetEX(ctx, prefix+"psetex", 60_000, "1"); err != nil {
		t.Fatal(err)
	}
	requireTrue(t, must[bool](t)(client.Copy(ctx, key, prefix+"copy", true)))
	if err := client.Rename(ctx, prefix+"copy", prefix+"renamed"); err != nil {
		t.Fatal(err)
	}
	requireTrue(t, must[bool](t)(client.RenameNX(ctx, prefix+"renamed", prefix+"renamed-nx")))
	requireValue(t, must[string](t)(client.RandomKey(ctx)))
	requireLenAtLeast(t, must[[]string](t)(client.Keys(ctx, prefix+"*")), 1)
	requireValue(t, must[any](t)(client.Scan(ctx, 0, prefix+"*", Int(10))))
	requirePositive(t, must[int64](t)(client.DBSize(ctx)))
	requireValue(t, must[any](t)(client.Object(ctx, "ENCODING", key)))
	requireValue(t, must[any](t)(client.ObjectHelp(ctx)))
	requireNonNegative(t, must[int64](t)(client.ObjectRefCount(ctx, key)))
	requireInt64(t, must[int64](t)(client.Wait(ctx, 0, 1)), 0)
	requireValue(t, must[any](t)(client.WaitAOF(ctx, 0, 0, 1)))
	requireValue(t, must[any](t)(client.Memory(ctx, "USAGE", key)))
	requireString(t, must[any](t)(client.KV().GetDel(ctx, prefix+"setnx")), "1")
	requireNonNegative(t, must[int64](t)(client.Unlink(ctx, prefix+"nx1")))
}

func assertHashCommands(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()

	key := prefix + "hash"
	requirePositive(t, must[int64](t)(client.Hash().Set(ctx, key, "field", "value")))
	requirePositive(t, must[int64](t)(client.Hash().Set(ctx, key, "count", "1")))
	requireString(t, must[any](t)(client.Hash().Get(ctx, key, "field")), "value")
	values := must[[]any](t)(client.Hash().MGet(ctx, key, "field", "none"))
	if len(values) != 2 || values[0] != "value" || values[1] != nil {
		t.Fatalf("HMGET = %#v", values)
	}
	if all := must[map[string]any](t)(client.Hash().GetAll(ctx, key)); all["field"] != "value" {
		t.Fatalf("HGETALL = %#v", all)
	}
	requireTrue(t, must[bool](t)(client.Hash().Exists(ctx, key, "field")))
	if keys := must[[]string](t)(client.Hash().Keys(ctx, key)); !contains(keys, "field") {
		t.Fatalf("HKEYS = %#v", keys)
	}
	requireLenAtLeast(t, must[[]any](t)(client.Hash().Values(ctx, key)), 1)
	requirePositive(t, must[int64](t)(client.Hash().Len(ctx, key)))
	requireInt64(t, must[int64](t)(client.Hash().IncrBy(ctx, key, "count", 2)), 3)
	requireNonNegativeFloat(t, must[float64](t)(client.Hash().IncrByFloat(ctx, key, "float", 1.25)))
	requireTrue(t, must[bool](t)(client.Hash().SetNX(ctx, key, "new", "item")))
	requireInt64(t, must[int64](t)(client.Hash().StrLen(ctx, key, "field")), 5)
	requireValue(t, must[any](t)(client.Hash().RandField(ctx, key, Int(1), true)))
	requireValue(t, must[any](t)(client.Hash().Scan(ctx, key, 0, "", Int(10))))
	requireValue(t, must[any](t)(client.Hash().Expire(ctx, key, 60, "field")))
	requireValue(t, must[any](t)(client.Hash().TTL(ctx, key, "field")))
	requireValue(t, must[any](t)(client.Hash().Persist(ctx, key, "field")))
	requireValue(t, must[any](t)(client.Hash().PExpire(ctx, key, 60_000, "field")))
	requireValue(t, must[any](t)(client.Hash().PTTL(ctx, key, "field")))
	requireValue(t, must[any](t)(client.Hash().ExpireTime(ctx, key, "field")))
	requireValue(t, must[any](t)(client.Hash().PExpireTime(ctx, key, "field")))
	got := must[[]any](t)(client.Hash().GetEX(ctx, key, []string{"field"}, HashGetEXOptions{PXMilliseconds: Int64(60_000)}))
	if len(got) != 1 || got[0] != "value" {
		t.Fatalf("HGETEX = %#v", got)
	}
	requireTrue(t, must[bool](t)(client.Hash().SetEX(ctx, key, map[string]any{"temp": "1"}, HashSetEXOptions{EXSeconds: Int64(60)})))
	got = must[[]any](t)(client.Hash().GetDel(ctx, key, "temp"))
	if len(got) != 1 || got[0] != "1" {
		t.Fatalf("HGETDEL = %#v", got)
	}
	requireInt64(t, must[int64](t)(client.Hash().Del(ctx, key, "new")), 1)
}

func assertListSetSortedSetCommands(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()

	listKey := prefix + "list"
	listDst := prefix + "list-dst"
	requireInt64(t, must[int64](t)(client.ListStore().LPush(ctx, listKey, "b", "a")), 2)
	requireInt64(t, must[int64](t)(client.ListStore().RPush(ctx, listKey, "c")), 3)
	requireLenAtLeast(t, must[[]any](t)(client.ListStore().Range(ctx, listKey, 0, -1)), 1)
	requireInt64(t, must[int64](t)(client.ListStore().Len(ctx, listKey)), 3)
	requireString(t, must[any](t)(client.ListStore().Index(ctx, listKey, 0)), "a")
	if err := client.ListStore().Set(ctx, listKey, 1, "bb"); err != nil {
		t.Fatal(err)
	}
	requireInt64(t, must[int64](t)(client.ListStore().Rem(ctx, listKey, 0, "bb")), 1)
	if err := client.ListStore().Trim(ctx, listKey, 0, 1); err != nil {
		t.Fatal(err)
	}
	requireValue(t, must[any](t)(client.ListStore().Pos(ctx, listKey, "a", nil, nil, nil)))
	requireNonNegative(t, must[int64](t)(client.ListStore().Insert(ctx, listKey, false, "a", "aa")))
	requireValue(t, must[any](t)(client.ListStore().Move(ctx, listKey, listDst, "LEFT", "RIGHT")))
	requireValue(t, must[any](t)(client.ListStore().RPopLPush(ctx, listDst, listKey)))
	requirePositive(t, must[int64](t)(client.ListStore().LPushX(ctx, listKey, "left")))
	requirePositive(t, must[int64](t)(client.ListStore().RPushX(ctx, listKey, "right")))
	requireValue(t, must[any](t)(client.ListStore().BLPop(ctx, 1, listKey)))
	requirePositive(t, must[int64](t)(client.ListStore().RPush(ctx, listKey, "block")))
	requireValue(t, must[any](t)(client.ListStore().BRPop(ctx, 1, listKey)))
	requirePositive(t, must[int64](t)(client.ListStore().RPush(ctx, listKey, "move")))
	requireValue(t, must[any](t)(client.ListStore().BLMove(ctx, listKey, listDst, "LEFT", "RIGHT", 1)))
	requirePositive(t, must[int64](t)(client.ListStore().RPush(ctx, listKey, "mpop")))
	requireValue(t, must[any](t)(client.ListStore().BLMPop(ctx, 1, []string{listKey}, "LEFT", Int(1))))

	setA := prefix + "set-a"
	setB := prefix + "set-b"
	requireInt64(t, must[int64](t)(client.SetStore().Add(ctx, setA, "a", "b")), 2)
	requireInt64(t, must[int64](t)(client.SetStore().Add(ctx, setB, "b", "c")), 2)
	requireLenAtLeast(t, must[[]any](t)(client.SetStore().Members(ctx, setA)), 1)
	requireTrue(t, must[bool](t)(client.SetStore().IsMember(ctx, setA, "a")))
	if values := must[[]bool](t)(client.SetStore().MIsMember(ctx, setA, "a", "z")); len(values) != 2 || !values[0] || values[1] {
		t.Fatalf("SMISMEMBER = %#v", values)
	}
	requireInt64(t, must[int64](t)(client.SetStore().Card(ctx, setA)), 2)
	requireLenAtLeast(t, must[[]any](t)(client.SetStore().RandMember(ctx, setA, Int(1))), 1)
	requireLen(t, must[[]any](t)(client.SetStore().Diff(ctx, setA, setB)), 1)
	requireLen(t, must[[]any](t)(client.SetStore().Inter(ctx, setA, setB)), 1)
	requireLenAtLeast(t, must[[]any](t)(client.SetStore().Union(ctx, setA, setB)), 1)
	requireNonNegative(t, must[int64](t)(client.SetStore().DiffStore(ctx, prefix+"sdiff", setA, setB)))
	requireNonNegative(t, must[int64](t)(client.SetStore().InterStore(ctx, prefix+"sinter", setA, setB)))
	requireNonNegative(t, must[int64](t)(client.SetStore().UnionStore(ctx, prefix+"sunion", setA, setB)))
	requireNonNegative(t, must[int64](t)(client.SetStore().InterCard(ctx, []string{setA, setB}, Int64(10))))
	_ = must[bool](t)(client.SetStore().Move(ctx, setA, setB, "a"))
	requireValue(t, must[any](t)(client.SetStore().Scan(ctx, setB, 0, "", Int(10))))
	requireValue(t, must[[]any](t)(client.SetStore().Pop(ctx, setB, Int(1))))
	requireNonNegative(t, must[int64](t)(client.SetStore().Remove(ctx, setA, "b")))

	zset := prefix + "zset"
	requireInt64(t, must[int64](t)(client.SortedSet().Add(ctx, zset,
		ZAddMember{Score: 1, Member: "a"},
		ZAddMember{Score: 2, Member: "b"},
		ZAddMember{Score: 3, Member: "c"},
	)), 3)
	requireNonNegativeFloat(t, must[float64](t)(client.SortedSet().Score(ctx, zset, "a")))
	requireInt64(t, must[int64](t)(client.SortedSet().Rank(ctx, zset, "a")), 0)
	requireInt64(t, must[int64](t)(client.SortedSet().RevRank(ctx, zset, "c")), 0)
	requireLenAtLeast(t, must[[]any](t)(client.SortedSet().Range(ctx, zset, 0, -1)), 1)
	requireLenAtLeast(t, must[[]any](t)(client.SortedSet().RevRange(ctx, zset, 0, -1)), 1)
	requireInt64(t, must[int64](t)(client.SortedSet().Card(ctx, zset)), 3)
	requireNonNegativeFloat(t, must[float64](t)(client.SortedSet().IncrBy(ctx, zset, 1, "a")))
	requirePositive(t, must[int64](t)(client.SortedSet().Count(ctx, zset, "-inf", "+inf")))
	requireValue(t, must[any](t)(client.SortedSet().RandMember(ctx, zset, Int(1), true)))
	requireLen(t, must[[]float64](t)(client.SortedSet().MScore(ctx, zset, "a", "none")), 2)
	requireValue(t, must[any](t)(client.SortedSet().RangeByScore(ctx, zset, "-inf", "+inf", false, nil, nil)))
	requireValue(t, must[any](t)(client.SortedSet().RevRangeByScore(ctx, zset, "+inf", "-inf", false, nil, nil)))
	requireValue(t, must[any](t)(client.SortedSet().Scan(ctx, zset, 0, "", Int(10))))
	requireInt64(t, must[int64](t)(client.SortedSet().Rem(ctx, zset, "b")), 1)
	requireValue(t, must[any](t)(client.SortedSet().PopMin(ctx, zset, Int(1))))
	requireValue(t, must[any](t)(client.SortedSet().PopMax(ctx, zset, Int(1))))
}

func assertStreamBitmapHllGeoCommands(t *testing.T, ctx context.Context, client *Client, prefix, runID string) {
	t.Helper()

	stream := prefix + "stream"
	streamID := must[string](t)(client.Stream().Add(ctx, stream, "*", map[string]any{"field": "value"}))
	requirePositive(t, must[int64](t)(client.Stream().Len(ctx, stream)))
	requireValue(t, must[any](t)(client.Stream().Range(ctx, stream, "-", "+", Int(10))))
	requireValue(t, must[any](t)(client.Stream().RevRange(ctx, stream, "+", "-", Int(10))))
	requireValue(t, must[any](t)(client.Stream().Read(ctx, StreamReadOptions{Count: Int(1), Streams: []StreamRef{{Key: stream, ID: "0-0"}}})))
	requireValue(t, must[any](t)(client.Stream().Info(ctx, stream)))
	group := "group-" + runID
	if err := client.Stream().GroupCreate(ctx, stream, group, "0", false); err != nil {
		t.Fatal(err)
	}
	requireValue(t, must[any](t)(client.Stream().ReadGroup(ctx, StreamReadGroupOptions{Group: group, Consumer: "consumer", Count: Int(1), Streams: []StreamRef{{Key: stream, ID: ">"}}})))
	requireNonNegative(t, must[int64](t)(client.Stream().Ack(ctx, stream, group, streamID)))
	requireNonNegative(t, must[int64](t)(client.Stream().Trim(ctx, stream, true, "10", nil)))
	requireNonNegative(t, must[int64](t)(client.Stream().Del(ctx, stream, streamID)))

	bitmap := prefix + "bitmap"
	requireInt64(t, must[int64](t)(client.Bitmap().SetBit(ctx, bitmap, 7, 1)), 0)
	requireInt64(t, must[int64](t)(client.Bitmap().GetBit(ctx, bitmap, 7)), 1)
	requirePositive(t, must[int64](t)(client.Bitmap().Count(ctx, bitmap)))
	requireNonNegative(t, must[int64](t)(client.Bitmap().Pos(ctx, bitmap, 1, nil, nil)))
	requireNonNegative(t, must[int64](t)(client.Bitmap().Op(ctx, "OR", prefix+"bitmap-out", bitmap)))

	hll := prefix + "hll"
	requireNonNegative(t, must[int64](t)(client.HyperLogLog().Add(ctx, hll, "a", "b")))
	requirePositive(t, must[int64](t)(client.HyperLogLog().Count(ctx, hll)))
	if err := client.HyperLogLog().Merge(ctx, prefix+"hll-dst", hll); err != nil {
		t.Fatal(err)
	}

	geo := prefix + "geo"
	requireInt64(t, must[int64](t)(client.Geo().Add(ctx, geo, 13.361389, 38.115556, "palermo")), 1)
	requireInt64(t, must[int64](t)(client.Geo().Add(ctx, geo, 15.087269, 37.502669, "catania")), 1)
	requireValue(t, must[any](t)(client.Geo().Pos(ctx, geo, "palermo")))
	requireValue(t, must[any](t)(client.Geo().Distance(ctx, geo, "palermo", "catania", "km")))
	requireLen(t, must[[]string](t)(client.Geo().Hash(ctx, geo, "palermo")), 1)
	requireValue(t, must[any](t)(client.Geo().Search(ctx, geo, GeoSearchOptions{FromMember: "palermo", ByRadius: &GeoRadius{Radius: 200, Unit: "km"}})))
	requireNonNegative(t, must[int64](t)(client.Geo().SearchStore(ctx, prefix+"geo-dst", geo, GeoSearchOptions{FromMember: "palermo", ByRadius: &GeoRadius{Radius: 200, Unit: "km"}}, true)))
}

func assertBatchFlowCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	partition := "go-sdk:batch:" + runID + ":partition"
	_ = must[[]FlowRecord](t)(client.CreateMany(ctx, CreateManyOptions{
		PartitionKey: partition,
		Type:         typeName,
		State:        "batch",
		RunAtMS:      now,
		NowMS:        now,
		Idempotent:   Bool(true),
		Items: []CreateItem{
			{ID: "go-sdk:batch:" + runID + ":a", Payload: map[string]any{"n": 1}},
			{ID: "go-sdk:batch:" + runID + ":b", Payload: map[string]any{"n": 2}},
		},
	}))
	jobs := claimMany(t, ctx, client, typeName, "batch", partition, "go-sdk-batch-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.CompleteMany(ctx, CompleteManyOptions{PartitionKey: partition, Items: jobs, Result: map[string]any{"batch": true}}))
}

func assertSingleMutationCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	transition := createAndClaim(t, ctx, client, typeName, runID, "transition", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.ExtendLease(ctx, transition.id, transition.job.LeaseToken, transition.job.FencingToken, 30_000, transition.partitionKey))
	_ = must[*FlowRecord](t)(client.Transition(ctx, TransitionOptions{ID: transition.id, FromState: "queued", ToState: "ready", LeaseToken: transition.job.LeaseToken, FencingToken: transition.job.FencingToken, PartitionKey: transition.partitionKey, Payload: map[string]any{"step": "ready"}}))
	ready := claimOne(t, ctx, client, typeName, "ready", transition.partitionKey, "go-sdk-ready-worker", now+1, 30_000)
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{ID: ready.ID, LeaseToken: ready.LeaseToken, FencingToken: ready.FencingToken, PartitionKey: ready.PartitionKey, Result: map[string]any{"ok": true}}))

	retry := createAndClaim(t, ctx, client, typeName, runID, "retry", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.Retry(ctx, RetryOptions{ID: retry.id, LeaseToken: retry.job.LeaseToken, FencingToken: retry.job.FencingToken, PartitionKey: retry.partitionKey, Error: map[string]any{"retry": true}, RunAtMS: now, NowMS: now}))
	retried := claimOne(t, ctx, client, typeName, "queued", retry.partitionKey, "go-sdk-retry-worker", now+1, 30_000)
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{ID: retried.ID, LeaseToken: retried.LeaseToken, FencingToken: retried.FencingToken, PartitionKey: retried.PartitionKey}))

	failed := createAndClaim(t, ctx, client, typeName, runID, "fail", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.Fail(ctx, FailOptions{ID: failed.id, LeaseToken: failed.job.LeaseToken, FencingToken: failed.job.FencingToken, PartitionKey: failed.partitionKey, Error: map[string]any{"failed": true}}))
	if record := must[*FlowRecord](t)(client.Get(ctx, failed.id, failed.partitionKey, nil, nil)); record == nil || record.State != "failed" {
		t.Fatalf("failed record = %#v", record)
	}
	if failures := must[[]FlowRecord](t)(client.Failures(ctx, typeName, ReadOptions{Count: Int(20)})); !hasRecordID(failures, failed.id) {
		t.Fatalf("FLOW.FAILURES = %#v", failures)
	}

	cancelled := createAndClaim(t, ctx, client, typeName, runID, "cancel", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.Cancel(ctx, CancelOptions{ID: cancelled.id, LeaseToken: cancelled.job.LeaseToken, FencingToken: cancelled.job.FencingToken, PartitionKey: cancelled.partitionKey, Reason: map[string]any{"cancelled": true}}))
	if record := must[*FlowRecord](t)(client.Get(ctx, cancelled.id, cancelled.partitionKey, nil, nil)); record == nil || record.State != "cancelled" {
		t.Fatalf("cancelled record = %#v", record)
	}
	if terminals := must[[]FlowRecord](t)(client.Terminals(ctx, typeName, ReadOptions{Count: Int(50)})); !hasRecordID(terminals, cancelled.id) {
		t.Fatalf("FLOW.TERMINALS = %#v", terminals)
	}
}

func assertManyMutationCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	transitionPartition := "go-sdk:many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, transitionPartition, "many-transition", runID, "many", now)
	manyJobs := claimMany(t, ctx, client, typeName, "many-transition", transitionPartition, "go-sdk-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.TransitionMany(ctx, TransitionManyOptions{PartitionKey: transitionPartition, FromState: "many-transition", ToState: "many-complete", Items: fencedItems(manyJobs), NowMS: now}))
	completeJobs := claimMany(t, ctx, client, typeName, "many-complete", transitionPartition, "go-sdk-many-worker", now+1, 2)
	_ = must[[]FlowRecord](t)(client.CompleteMany(ctx, CompleteManyOptions{PartitionKey: transitionPartition, Items: completeJobs, Result: map[string]any{"ok": true}}))

	retryPartition := "go-sdk:retry-many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, retryPartition, "retry-many", runID, "retry-many", now)
	retryJobs := claimMany(t, ctx, client, typeName, "retry-many", retryPartition, "go-sdk-retry-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.RetryMany(ctx, RetryManyOptions{PartitionKey: retryPartition, Items: retryJobs, Error: map[string]any{"retry": "many"}, RunAtMS: now, NowMS: now}))
	retryAgain := claimMany(t, ctx, client, typeName, "retry-many", retryPartition, "go-sdk-retry-many-worker", now+1, 2)
	_ = must[[]FlowRecord](t)(client.FailMany(ctx, FailManyOptions{PartitionKey: retryPartition, Items: retryAgain, Error: map[string]any{"done": true}}))

	cancelPartition := "go-sdk:cancel-many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, cancelPartition, "cancel-many", runID, "cancel-many", now)
	cancelJobs := claimMany(t, ctx, client, typeName, "cancel-many", cancelPartition, "go-sdk-cancel-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.CancelMany(ctx, CancelManyOptions{PartitionKey: cancelPartition, Items: fencedItems(cancelJobs), Reason: map[string]any{"cancel": "many"}}))
}

func assertRepairIndexAndRewindCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	reclaimID := "go-sdk:reclaim:" + runID
	reclaimPartition := reclaimID + ":partition"
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: reclaimID, Type: typeName, State: "reclaim", PartitionKey: reclaimPartition, RunAtMS: 1_000, NowMS: 1_000}))
	_ = claimOne(t, ctx, client, typeName, "reclaim", reclaimPartition, "go-sdk-reclaim-initial", 1_000, 10)
	reclaimed := must[[]ClaimedItem](t)(client.ReclaimJobs(ctx, ReclaimOptions{Type: typeName, Worker: "go-sdk-reclaim-worker", PartitionKey: reclaimPartition, LeaseMS: 30_000, Limit: 1, NowMS: 2_000}))
	requireLen(t, reclaimed, 1)
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{ID: reclaimed[0].ID, LeaseToken: reclaimed[0].LeaseToken, FencingToken: reclaimed[0].FencingToken, PartitionKey: reclaimed[0].PartitionKey}))

	stuckID := "go-sdk:stuck:" + runID
	stuckPartition := stuckID + ":partition"
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: stuckID, Type: typeName, State: "stuck", PartitionKey: stuckPartition, RunAtMS: 1_000, NowMS: 1_000}))
	stuck := claimOne(t, ctx, client, typeName, "stuck", stuckPartition, "go-sdk-stuck-worker", 1_000, 60_000)
	if stuckRecords := must[[]FlowRecord](t)(client.Stuck(ctx, typeName, stuckPartition, Int(10), Int64(1), Int64(120_000))); !hasRecordID(stuckRecords, stuckID) {
		t.Fatalf("FLOW.STUCK = %#v", stuckRecords)
	}
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{ID: stuck.ID, LeaseToken: stuck.LeaseToken, FencingToken: stuck.FencingToken, PartitionKey: stuck.PartitionKey}))

	parentID := "go-sdk:parent:" + runID
	parentPartition := parentID + ":partition"
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: parentID, Type: typeName, State: "dispatch", PartitionKey: parentPartition, RootFlowID: "root:" + runID, CorrelationID: "corr:" + runID, Idempotent: Bool(true)}))
	parent := must[*FlowRecord](t)(client.Get(ctx, parentID, parentPartition, nil, nil))
	_ = must[any](t)(client.SpawnChildren(ctx, SpawnChildrenOptions{
		ParentID:     parentID,
		PartitionKey: parentPartition,
		FencingToken: &parent.FencingToken,
		GroupID:      "fanout",
		Wait:         "any",
		WaitState:    "waiting_children",
		Success:      "children_done",
		Failure:      "children_failed",
		FromState:    "dispatch",
		Children:     []ChildSpec{{ID: "go-sdk:child:" + runID + ":a", Type: typeName, Payload: map[string]any{"child": "a"}}, {ID: "go-sdk:child:" + runID + ":b", Type: typeName, Payload: map[string]any{"child": "b"}}},
	}))
	if records := must[[]FlowRecord](t)(client.ByParent(ctx, parentID, ReadOptions{Count: Int(20)})); !hasRecordPrefix(records, "go-sdk:child:"+runID+":") {
		t.Fatalf("FLOW.BY_PARENT = %#v", records)
	}
	if records := must[[]FlowRecord](t)(client.ByRoot(ctx, "root:"+runID, ReadOptions{Count: Int(20)})); !hasRecordID(records, parentID) {
		t.Fatalf("FLOW.BY_ROOT = %#v", records)
	}
	if records := must[[]FlowRecord](t)(client.ByCorrelation(ctx, "corr:"+runID, ReadOptions{Count: Int(20)})); !hasRecordID(records, parentID) {
		t.Fatalf("FLOW.BY_CORRELATION = %#v", records)
	}

	rewind := createAndClaim(t, ctx, client, typeName, runID, "rewind", "queued", now, 30_000)
	history := must[[]any](t)(client.History(ctx, HistoryOptions{ID: rewind.id, PartitionKey: rewind.partitionKey, Count: 10}))
	requireLenAtLeast(t, history, 1)
	createdEventID := eventID(history[0])
	_ = must[*FlowRecord](t)(client.Complete(ctx, CompleteOptions{ID: rewind.id, LeaseToken: rewind.job.LeaseToken, FencingToken: rewind.job.FencingToken, PartitionKey: rewind.partitionKey}))
	if rewound := must[*FlowRecord](t)(client.Rewind(ctx, RewindOptions{ID: rewind.id, PartitionKey: rewind.partitionKey, ExpectState: "completed", ToEvent: createdEventID, ReturnRecord: true})); rewound == nil || rewound.State != "queued" {
		t.Fatalf("FLOW.REWIND = %#v", rewound)
	}
}

func createManyState(t *testing.T, ctx context.Context, client *Client, typeName, partition, state, runID, name string, now int64) {
	t.Helper()
	_ = must[[]FlowRecord](t)(client.CreateMany(ctx, CreateManyOptions{
		PartitionKey: partition,
		Type:         typeName,
		State:        state,
		RunAtMS:      now,
		NowMS:        now,
		Items: []CreateItem{
			{ID: "go-sdk:" + name + ":" + runID + ":a"},
			{ID: "go-sdk:" + name + ":" + runID + ":b"},
		},
	}))
}

func createAndClaim(t *testing.T, ctx context.Context, client *Client, typeName, runID, name, state string, now, leaseMS int64) claimedFlow {
	t.Helper()
	id := "go-sdk:" + name + ":" + runID
	partition := id + ":partition"
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: id, Type: typeName, State: state, PartitionKey: partition, Payload: map[string]any{"name": name}, RunAtMS: now, NowMS: now, Idempotent: Bool(true)}))
	return claimedFlow{id: id, partitionKey: partition, job: claimOne(t, ctx, client, typeName, state, partition, "go-sdk-"+name+"-worker", now, leaseMS)}
}

func claimOne(t *testing.T, ctx context.Context, client *Client, typeName, state, partition, worker string, now, leaseMS int64) ClaimedItem {
	t.Helper()
	jobs := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{Type: typeName, State: state, Worker: worker, PartitionKey: partition, LeaseMS: leaseMS, Limit: 1, NowMS: now}))
	requireLen(t, jobs, 1)
	return jobs[0]
}

func claimMany(t *testing.T, ctx context.Context, client *Client, typeName, state, partition, worker string, now int64, limit int) []ClaimedItem {
	t.Helper()
	jobs := must[[]ClaimedItem](t)(client.ClaimJobs(ctx, ClaimDueOptions{Type: typeName, State: state, Worker: worker, PartitionKey: partition, LeaseMS: 30_000, Limit: limit, NowMS: now}))
	requireLen(t, jobs, limit)
	return jobs
}

func fencedItems(items []ClaimedItem) []FencedItem {
	out := make([]FencedItem, 0, len(items))
	for _, item := range items {
		out = append(out, FencedItem{ID: item.ID, LeaseToken: item.LeaseToken, FencingToken: item.FencingToken, PartitionKey: item.PartitionKey})
	}
	return out
}

func eventID(event any) string {
	if items, ok := event.([]any); ok && len(items) > 0 {
		return asString(items[0])
	}
	id := responseField(event, "event_id")
	if id == nil {
		id = responseField(event, "id")
	}
	return asString(id)
}

func responseField(value any, name string) any {
	mapping, err := respMap(value)
	if err != nil {
		return nil
	}
	return mapping[name]
}

func integrationContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func integrationClient(codec Codec) *Client {
	addr := os.Getenv("FERRICSTORE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	return NewClient(addr, WithCodec(codec))
}

func integrationSuffix(name string) string {
	return fmt.Sprintf("%s:%d", name, time.Now().UnixNano())
}

func cleanupPrefix(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()
	keys, err := client.Keys(ctx, prefix+"*")
	if err != nil || len(keys) == 0 {
		return
	}
	if _, err := client.Delete(ctx, keys...); err != nil {
		t.Fatalf("cleanup %s: %v", prefix, err)
	}
}

func must[T any](t *testing.T) func(T, error) T {
	t.Helper()
	return func(value T, err error) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
}

func requireValue(t *testing.T, value any) {
	t.Helper()
	switch v := value.(type) {
	case nil:
		t.Fatalf("expected value, got %#v", value)
	case string:
		if v == "" {
			t.Fatalf("expected value, got %#v", value)
		}
	case []byte:
		if len(v) == 0 {
			t.Fatalf("expected value, got %#v", value)
		}
	}
}

func requireMap(t *testing.T, value map[string]any) {
	t.Helper()
	if len(value) == 0 {
		t.Fatalf("expected non-empty map, got %#v", value)
	}
}

func requireTrue(t *testing.T, value bool) {
	t.Helper()
	if !value {
		t.Fatal("expected true")
	}
}

func requireString(t *testing.T, value any, want string) {
	t.Helper()
	if asString(value) != want {
		t.Fatalf("expected %q, got %#v", want, value)
	}
}

func requireInt64(t *testing.T, value, want int64) {
	t.Helper()
	if value != want {
		t.Fatalf("expected %d, got %d", want, value)
	}
}

func requirePositive(t *testing.T, value int64) {
	t.Helper()
	if value < 1 {
		t.Fatalf("expected positive integer, got %d", value)
	}
}

func requireNonNegative(t *testing.T, value int64) {
	t.Helper()
	if value < 0 {
		t.Fatalf("expected non-negative integer, got %d", value)
	}
}

func requireNonNegativeFloat(t *testing.T, value float64) {
	t.Helper()
	if value < 0 {
		t.Fatalf("expected non-negative float, got %f", value)
	}
}

func requireOKResponse(t *testing.T, value any) {
	t.Helper()
	if !isOK(value) && asInt64(value) != 1 && !asBool(value) {
		t.Fatalf("expected OK response, got %#v", value)
	}
}

func requireLen[T any](t *testing.T, values []T, want int) {
	t.Helper()
	if len(values) != want {
		t.Fatalf("expected %d items, got %d: %#v", want, len(values), values)
	}
}

func requireLenAtLeast[T any](t *testing.T, values []T, want int) {
	t.Helper()
	if len(values) < want {
		t.Fatalf("expected at least %d items, got %d: %#v", want, len(values), values)
	}
}

func hasRecordID(records []FlowRecord, id string) bool {
	for _, record := range records {
		if record.ID == id {
			return true
		}
	}
	return false
}

func hasRecordPrefix(records []FlowRecord, prefix string) bool {
	for _, record := range records {
		if strings.HasPrefix(record.ID, prefix) {
			return true
		}
	}
	return false
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func containsFold(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
