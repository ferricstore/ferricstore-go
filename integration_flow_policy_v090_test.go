//go:build integration

package ferricstore

import (
	"errors"
	"testing"
	"time"
)

func TestIntegrationV090FlowPolicyPatchReplacementAndGenerationCAS(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()
	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("policy-cas")
	flowType := "go-sdk-policy-cas-" + runID
	initial := must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		Replace:           Bool(true),
		IndexedAttributes: []string{"tenant"},
		Retry:             &RetryPolicy{MaxRetries: 2},
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
		},
	}))
	if initial.Generation <= 0 {
		t.Fatalf("initial generation = %d", initial.Generation)
	}

	patched := must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		ExpectedGeneration: Int64(initial.Generation),
		IndexedStateMeta:   "attempt",
	}))
	if patched.Generation <= initial.Generation {
		t.Fatalf("patch generation = %d, initial = %d", patched.Generation, initial.Generation)
	}
	if asInt64(patched.Retry["max_retries"]) != 2 ||
		len(patched.IndexedAttributes) != 1 || patched.IndexedAttributes[0] != "tenant" ||
		patched.IndexedStateMeta != "attempt" || patched.States["queued"].Mode != FlowStateModeFIFO {
		t.Fatalf("deep patch dropped omitted policy fields: %+v", patched)
	}

	replaced := must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		Replace:            Bool(true),
		ExpectedGeneration: Int64(patched.Generation),
		IndexedStateMeta:   "version",
	}))
	if replaced.Generation <= patched.Generation {
		t.Fatalf("replacement generation = %d, patch = %d", replaced.Generation, patched.Generation)
	}
	if len(replaced.States) != 0 || len(replaced.IndexedAttributes) != 0 ||
		asInt64(replaced.Retry["max_retries"]) == 2 || replaced.IndexedStateMeta != "version" {
		t.Fatalf("replacement retained omitted policy fields: %+v", replaced)
	}

	_, err := client.SetPolicy(ctx, flowType, PolicyOptions{
		ExpectedGeneration: Int64(patched.Generation),
		IndexedStateMeta:   "stale-write",
	})
	var stale *StalePolicyGenerationError
	if !errors.Is(err, ErrStalePolicyGeneration) || !errors.As(err, &stale) {
		t.Fatalf("stale CAS error = %T %v", err, err)
	}
	read := must[PolicySnapshot](t)(client.PolicyGet(ctx, flowType, ""))
	if read.Generation != replaced.Generation || read.IndexedStateMeta != "version" {
		t.Fatalf("stale CAS mutated policy: read=%+v replaced=%+v", read, replaced)
	}
}

func TestIntegrationV090FIFOClaimsOneHeadPerPartition(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()
	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("fifo-partitions")
	flowType := "go-sdk-fifo-partitions-" + runID
	partitions := []string{"tenant:fifo:a:" + runID, "tenant:fifo:b:" + runID}
	now := time.Now().UnixMilli()
	must[PolicySnapshot](t)(client.SetPolicy(ctx, flowType, PolicyOptions{
		Replace: Bool(true),
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
		},
	}))

	wantHeads := make(map[string]string, len(partitions))
	wantSeconds := make(map[string]string, len(partitions))
	for partitionIndex, partition := range partitions {
		first := "z-first-" + runID + "-" + string(rune('a'+partitionIndex))
		second := "a-second-" + runID + "-" + string(rune('a'+partitionIndex))
		wantHeads[partition] = first
		wantSeconds[partition] = second
		for offset, id := range []string{first, second} {
			must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
				ID: id, Type: flowType, State: "queued", PartitionKey: partition,
				NowMS: now + int64(partitionIndex*10+offset), RunAtMS: now + 100,
				ReturnRecord: true,
			}))
		}
	}

	heads := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type: flowType, State: "queued", Worker: "go-sdk-fifo-partition-worker",
		PartitionKeys: partitions, Limit: 4, NowMS: now + 100,
	}))
	if len(heads) != len(partitions) {
		t.Fatalf("FIFO cross-partition claims = %d, want %d: %#v", len(heads), len(partitions), heads)
	}
	for _, head := range heads {
		if head.ID != wantHeads[head.PartitionKey] {
			t.Fatalf("partition %q head = %q, want %q", head.PartitionKey, head.ID, wantHeads[head.PartitionKey])
		}
	}
	blocked := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type: flowType, State: "queued", Worker: "go-sdk-fifo-partition-worker",
		PartitionKeys: partitions, Limit: 4, NowMS: now + 101,
	}))
	if len(blocked) != 0 {
		t.Fatalf("FIFO claimed behind active heads: %#v", blocked)
	}
	for _, head := range heads {
		must[any](t)(client.Complete(ctx, CompleteOptions{
			ID: head.ID, LeaseToken: head.LeaseToken, FencingToken: head.FencingToken,
			PartitionKey: head.PartitionKey,
		}))
	}
	seconds := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type: flowType, State: "queued", Worker: "go-sdk-fifo-partition-worker",
		PartitionKeys: partitions, Limit: 4, NowMS: now + 102,
	}))
	if len(seconds) != len(partitions) {
		t.Fatalf("second FIFO cross-partition claims = %d, want %d: %#v", len(seconds), len(partitions), seconds)
	}
	for _, second := range seconds {
		if second.ID != wantSeconds[second.PartitionKey] {
			t.Fatalf("partition %q second = %q, want %q", second.PartitionKey, second.ID, wantSeconds[second.PartitionKey])
		}
	}
}
