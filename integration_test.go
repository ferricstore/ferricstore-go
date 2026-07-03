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

func assertStringCommands(t *testing.T, ctx context.Context, client *Client, prefix string) {
	t.Helper()

	key := prefix + "string"
	requireOKResponse(t, must[any](t)(client.KV().SetWithOptions(ctx, key, "abc", SetOptions{PXMilliseconds: Int64(60_000)})))
	requireString(t, must[any](t)(client.KV().Get(ctx, key)), "abc")
	requireInt64(t, must[int64](t)(client.KV().Exists(ctx, key)), 1)
	values := must[[]any](t)(client.KV().MGet(ctx, key, prefix+"missing"))
	if len(values) != 2 || values[0] != "abc" || values[1] != "" {
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
	if len(values) != 2 || values[0] != "value" || values[1] != "" {
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
	_, err := client.Hash().PExpireTime(ctx, key, "field")
	requireCommandError(t, err)
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

	popKey := prefix + "list-pop"
	requireInt64(t, must[int64](t)(client.ListStore().LPush(ctx, popKey, "left")), 1)
	requireInt64(t, must[int64](t)(client.ListStore().RPush(ctx, popKey, "right")), 2)
	requireString(t, must[any](t)(client.ListStore().LPop(ctx, popKey)), "left")
	requireString(t, must[any](t)(client.ListStore().RPop(ctx, popKey)), "right")

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
	requireNonNegative(t, must[int64](t)(client.Geo().SearchStore(ctx, prefix+"geo-dst", geo, GeoSearchOptions{FromMember: "palermo", ByRadius: &GeoRadius{Radius: 200, Unit: "km"}}, false)))
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
	requireLen(t, jobs, 2)
}

func assertSingleMutationCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	transition := createAndClaim(t, ctx, client, typeName, runID, "transition", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.ExtendLease(ctx, transition.id, transition.job.LeaseToken, transition.job.FencingToken, 30_000, transition.partitionKey))
	_ = must[*FlowRecord](t)(client.Transition(ctx, TransitionOptions{ID: transition.id, FromState: transition.job.State, ToState: "ready", LeaseToken: transition.job.LeaseToken, FencingToken: transition.job.FencingToken, PartitionKey: transition.partitionKey, Payload: map[string]any{"step": "ready"}, RunAtMS: now, NowMS: now}))
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
	_ = must[[]FlowRecord](t)(client.Failures(ctx, typeName, ReadOptions{Count: Int(20)}))

	cancelled := createAndClaim(t, ctx, client, typeName, runID, "cancel", "queued", now, 30_000)
	_ = must[*FlowRecord](t)(client.Cancel(ctx, CancelOptions{ID: cancelled.id, LeaseToken: cancelled.job.LeaseToken, FencingToken: cancelled.job.FencingToken, PartitionKey: cancelled.partitionKey, Reason: map[string]any{"cancelled": true}}))
	if record := must[*FlowRecord](t)(client.Get(ctx, cancelled.id, cancelled.partitionKey, nil, nil)); record == nil || record.State != "cancelled" {
		t.Fatalf("cancelled record = %#v", record)
	}
	_ = must[[]FlowRecord](t)(client.Terminals(ctx, typeName, ReadOptions{Count: Int(50)}))
}

func assertManyMutationCommands(t *testing.T, ctx context.Context, client *Client, typeName, runID string, now int64) {
	t.Helper()

	transitionPartition := "go-sdk:many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, transitionPartition, "many-transition", runID, "many", now)
	manyJobs := claimMany(t, ctx, client, typeName, "many-transition", transitionPartition, "go-sdk-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.TransitionMany(ctx, TransitionManyOptions{PartitionKey: transitionPartition, FromState: manyJobs[0].State, ToState: "many-complete", Items: fencedItems(manyJobs), NowMS: now}))
	completeJobs := claimMany(t, ctx, client, typeName, "many-complete", transitionPartition, "go-sdk-many-worker", now+1, 2)
	requireLen(t, completeJobs, 2)

	retryPartition := "go-sdk:retry-many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, retryPartition, "retry-many", runID, "retry-many", now)
	retryJobs := claimMany(t, ctx, client, typeName, "retry-many", retryPartition, "go-sdk-retry-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.RetryMany(ctx, RetryManyOptions{PartitionKey: retryPartition, Items: retryJobs, Error: map[string]any{"retry": "many"}, RunAtMS: now, NowMS: now}))
	retryAgain := claimMany(t, ctx, client, typeName, "retry-many", retryPartition, "go-sdk-retry-many-worker", now+1, 2)
	_ = must[[]FlowRecord](t)(client.FailMany(ctx, FailManyOptions{PartitionKey: retryPartition, Items: retryAgain, Error: map[string]any{"done": true}}))

	completePartition := "go-sdk:complete-many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, completePartition, "complete-many", runID, "complete-many", now)
	manyCompleteJobs := claimMany(t, ctx, client, typeName, "complete-many", completePartition, "go-sdk-complete-many-worker", now, 2)
	_ = must[[]FlowRecord](t)(client.CompleteMany(ctx, CompleteManyOptions{PartitionKey: completePartition, Items: manyCompleteJobs, Result: map[string]any{"done": true}}))

	cancelPartition := "go-sdk:cancel-many:" + runID + ":partition"
	createManyState(t, ctx, client, typeName, cancelPartition, "cancel-many", runID, "cancel-many", now)
	cancelJobs := claimMany(t, ctx, client, typeName, "cancel-many", cancelPartition, "go-sdk-cancel-many-worker", now, 2)
	_, err := client.CancelMany(ctx, CancelManyOptions{PartitionKey: cancelPartition, Items: fencedItems(cancelJobs), Reason: map[string]any{"cancelled": true}})
	requireCommandError(t, err)
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
	_ = must[[]FlowRecord](t)(client.ByParent(ctx, parentID, ReadOptions{Count: Int(20)}))
	_ = must[[]FlowRecord](t)(client.ByRoot(ctx, "root:"+runID, ReadOptions{Count: Int(20)}))
	_ = must[[]FlowRecord](t)(client.ByCorrelation(ctx, "corr:"+runID, ReadOptions{Count: Int(20)}))

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
	mapping, err := nativeMap(value)
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
		addr = "127.0.0.1:6388"
	}
	return newIntegrationTrackedClient(addr, codec)
}

func integrationDirectClient(codec Codec) *Client {
	addr := os.Getenv("FERRICSTORE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6388"
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

func requireCommandError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected command error")
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

func containsRuleForUser(values []string, username string) bool {
	prefix := "user " + username + " "
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
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
