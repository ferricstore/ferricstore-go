//go:build integration

package ferricstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestIntegrationKVAndFlowRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr := os.Getenv("FERRICSTORE_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := NewClient(addr, WithCodec(JSONCodec{}))
	defer client.Close()

	if _, err := client.Command(ctx, "PING"); err != nil {
		t.Fatal(err)
	}

	runID := fmt.Sprintf("go-sdk-integration:%d", time.Now().UnixNano())
	if err := client.KV().Set(ctx, runID+":profile", map[string]any{"plan": "pro"}); err != nil {
		t.Fatal(err)
	}
	value, err := client.KV().Get(ctx, runID+":profile")
	if err != nil {
		t.Fatal(err)
	}
	profile, ok := value.(map[string]any)
	if !ok || profile["plan"] != "pro" {
		t.Fatalf("unexpected profile: %#v", value)
	}

	partition := runID + ":partition"
	_, err = client.Create(ctx, CreateOptions{
		ID:           runID + ":flow",
		Type:         "go-sdk-integration",
		State:        "queued",
		PartitionKey: partition,
		Payload:      map[string]any{"step": 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	jobs, err := client.ClaimDue(ctx, ClaimDueOptions{
		Type:         "go-sdk-integration",
		State:        "queued",
		Worker:       "go-sdk-worker",
		PartitionKey: partition,
		Limit:        1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 claimed job, got %d", len(jobs))
	}

	_, err = client.Complete(ctx, CompleteOptions{
		ID:           jobs[0].ID,
		LeaseToken:   jobs[0].LeaseToken,
		FencingToken: jobs[0].FencingToken,
		PartitionKey: jobs[0].PartitionKey,
		Result:       map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatal(err)
	}
}
