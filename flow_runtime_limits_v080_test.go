package ferricstore

import (
	"context"
	"testing"
)

func TestV080FlowRuntimeDefaultsAreNotSDKHardLimits(t *testing.T) {
	if err := validateFlowBatchSize(1_001); err != nil {
		t.Fatalf("configured mutation batch above default was rejected locally: %v", err)
	}

	claimExec := &fakeExecutor{value: []any{}}
	if _, err := NewClientWithExecutor(claimExec).ClaimDue(context.Background(), ClaimDueOptions{
		Type: "work", Worker: "worker", Limit: 1_001,
	}); err != nil {
		t.Fatalf("configured claim limit above default was rejected locally: %v", err)
	}
	if len(claimExec.calls) != 1 {
		t.Fatalf("claim request did not reach transport: %#v", claimExec.calls)
	}

	reclaimExec := &fakeExecutor{value: []any{}}
	if _, err := NewClientWithExecutor(reclaimExec).Reclaim(context.Background(), ReclaimOptions{
		Type: "work", Worker: "worker", Limit: 1_001,
	}); err != nil {
		t.Fatalf("configured reclaim limit above default was rejected locally: %v", err)
	}
	if len(reclaimExec.calls) != 1 {
		t.Fatalf("reclaim request did not reach transport: %#v", reclaimExec.calls)
	}

	fireExec := &fakeExecutor{value: map[string]any{
		"claimed": int64(0), "fired": int64(0), "skipped": int64(0), "errors": []any{},
	}}
	if _, err := NewClientWithExecutor(fireExec).ScheduleFireDueWithOptions(
		context.Background(), ScheduleFireDueOptions{Limit: Int(1_001)},
	); err != nil {
		t.Fatalf("configured scheduler claim limit above default was rejected locally: %v", err)
	}
	if len(fireExec.calls) != 1 {
		t.Fatalf("scheduler request did not reach transport: %#v", fireExec.calls)
	}
}

func TestV080FlowMutationBatchKeepsServerHardCeiling(t *testing.T) {
	if err := validateFlowBatchSize(100_000); err != nil {
		t.Fatalf("server hard mutation batch ceiling was rejected: %v", err)
	}
	if err := validateFlowBatchSize(100_001); err == nil {
		t.Fatal("mutation batch above the server hard ceiling was accepted")
	}
}
