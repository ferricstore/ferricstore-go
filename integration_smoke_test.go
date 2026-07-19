//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

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
		MaxActiveMS:  int64(60_000),
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

	record := must[*FlowRecord](t)(client.Get(ctx, id, partition, nil))
	if record == nil || record.State != "completed" || record.MaxActiveMS != 60_000 {
		t.Fatalf("expected completed flow, got %#v", record)
	}
}
