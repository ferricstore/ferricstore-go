//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

func TestIntegrationFlowFIFOStatePolicy(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("fifo")
	typeName := "go-sdk-fifo-" + runID
	partition := "tenant:fifo:" + runID
	transitionPartition := "tenant:fifo-transition:" + runID
	now := time.Now().UnixMilli()

	parallelType := typeName + ":default"
	parallelPartition := "tenant:fifo-default:" + runID
	for idx, id := range []string{"z-default-parallel:" + runID, "a-default-parallel:" + runID} {
		requireValue(t, must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
			ID:           id,
			Type:         parallelType,
			State:        "queued",
			PartitionKey: parallelPartition,
			Priority:     Int64(1),
			NowMS:        now + int64(idx),
			RunAtMS:      now + 5,
			ReturnRecord: true,
		})))
	}
	parallelClaims := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:         parallelType,
		State:        "queued",
		Worker:       "go-sdk-default-worker",
		PartitionKey: parallelPartition,
		Priority:     Int64(1),
		Limit:        10,
		NowMS:        now + 5,
	}))
	requireLen(t, parallelClaims, 2)

	installed := must[PolicySnapshot](t)(client.SetPolicy(ctx, typeName, PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{
			"queued":    {Mode: FlowStateModeFIFO},
			"ready":     {Mode: FlowStateModeParallel},
			"fifo_gate": {Mode: FlowStateModeFIFO},
		},
	}))
	if installed.Generation <= 0 {
		t.Fatalf("installed FIFO policy generation = %d", installed.Generation)
	}
	queuedPolicy := must[PolicySnapshot](t)(client.PolicyGet(ctx, typeName, "queued"))
	if queuedPolicy.Mode != FlowStateModeFIFO {
		t.Fatalf("expected queued state to be fifo, got %#v", queuedPolicy)
	}

	_, err := client.Create(ctx, CreateOptions{
		ID:           "fifo-priority:" + runID,
		Type:         typeName,
		State:        "queued",
		PartitionKey: partition,
		Priority:     Int64(1),
		NowMS:        now,
		RunAtMS:      now,
	})
	requireCommandError(t, err)

	_, err = client.Create(ctx, CreateOptions{
		ID:      "fifo-implicit-partition:" + runID,
		Type:    typeName,
		State:   "queued",
		NowMS:   now,
		RunAtMS: now,
	})
	requireCommandError(t, err)

	firstID := "z-fifo-first:" + runID
	secondID := "a-fifo-second:" + runID
	for idx, id := range []string{firstID, secondID} {
		requireValue(t, must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
			ID:           id,
			Type:         typeName,
			State:        "queued",
			PartitionKey: partition,
			Payload:      map[string]any{"id": id},
			NowMS:        now + int64(idx),
			RunAtMS:      now + 10,
			ReturnRecord: true,
		})))
	}

	claimed := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:          typeName,
		State:         "queued",
		Worker:        "go-sdk-fifo-worker",
		PartitionKeys: []string{partition, "tenant:ignored:" + runID},
		Limit:         10,
		NowMS:         now + 10,
	}))
	requireLen(t, claimed, 1)
	first := claimed[0]
	if first.ID != firstID || first.PartitionKey != partition || first.LeaseToken == "" || first.FencingToken == 0 {
		t.Fatalf("unexpected first fifo claim: %#v", first)
	}

	blocked := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:          typeName,
		State:         "queued",
		Worker:        "go-sdk-fifo-worker",
		PartitionKeys: []string{partition},
		Limit:         10,
		NowMS:         now + 11,
	}))
	requireLen(t, blocked, 0)

	_, err = client.Transition(ctx, TransitionOptions{
		ID:           first.ID,
		FromState:    first.State,
		ToState:      "ready",
		LeaseToken:   first.LeaseToken,
		FencingToken: first.FencingToken,
		PartitionKey: first.PartitionKey,
		NowMS:        now + 20,
		RunAtMS:      now + 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	next := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:         typeName,
		State:        "queued",
		Worker:       "go-sdk-fifo-worker",
		PartitionKey: partition,
		Limit:        10,
		NowMS:        now + 21,
	}))
	requireLen(t, next, 1)
	if next[0].ID != secondID || next[0].PartitionKey != partition || next[0].LeaseToken == "" || next[0].FencingToken == 0 {
		t.Fatalf("unexpected second fifo claim: %#v", next[0])
	}

	transitionID := "fifo-transition:" + runID
	requireValue(t, must[*FlowRecord](t)(client.Create(ctx, CreateOptions{
		ID:           transitionID,
		Type:         typeName,
		State:        "ready",
		PartitionKey: transitionPartition,
		NowMS:        now + 30,
		RunAtMS:      now + 30,
		ReturnRecord: true,
	})))
	ready := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:         typeName,
		State:        "ready",
		Worker:       "go-sdk-ready-worker",
		PartitionKey: transitionPartition,
		Limit:        1,
		NowMS:        now + 31,
	}))
	requireLen(t, ready, 1)

	_, err = client.Transition(ctx, TransitionOptions{
		ID:           ready[0].ID,
		FromState:    ready[0].State,
		ToState:      "fifo_gate",
		LeaseToken:   ready[0].LeaseToken,
		FencingToken: ready[0].FencingToken,
		PartitionKey: ready[0].PartitionKey,
		Priority:     Int64(1),
		NowMS:        now + 32,
		RunAtMS:      now + 32,
	})
	requireCommandError(t, err)

	_, err = client.Transition(ctx, TransitionOptions{
		ID:           ready[0].ID,
		FromState:    ready[0].State,
		ToState:      "fifo_gate",
		LeaseToken:   ready[0].LeaseToken,
		FencingToken: ready[0].FencingToken,
		NowMS:        now + 33,
		RunAtMS:      now + 33,
	})
	requireCommandError(t, err)

	_, err = client.Transition(ctx, TransitionOptions{
		ID:           ready[0].ID,
		FromState:    ready[0].State,
		ToState:      "fifo_gate",
		LeaseToken:   ready[0].LeaseToken,
		FencingToken: ready[0].FencingToken,
		PartitionKey: ready[0].PartitionKey,
		NowMS:        now + 34,
		RunAtMS:      now + 34,
	})
	if err != nil {
		t.Fatal(err)
	}

	gated := must[[]FlowRecord](t)(client.ClaimDue(ctx, ClaimDueOptions{
		Type:         typeName,
		State:        "fifo_gate",
		Worker:       "go-sdk-gate-worker",
		PartitionKey: transitionPartition,
		Limit:        1,
		NowMS:        now + 35,
	}))
	requireLen(t, gated, 1)
	if gated[0].ID != transitionID || gated[0].PartitionKey != transitionPartition || gated[0].LeaseToken == "" || gated[0].FencingToken == 0 {
		t.Fatalf("unexpected fifo transition claim: %#v", gated[0])
	}
}
