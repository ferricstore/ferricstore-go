//go:build integration

package ferricstore

import (
	"testing"
	"time"
)

func TestIntegrationFlowStateMachineRepairAndIndexes(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("flow")
	typeName := "go-sdk-flow-" + runID
	now := time.Now().UnixMilli()

	valueResponse := must[any](t)(client.ValuePut(ctx, map[string]any{"shared": true}, ValuePutOptions{PartitionKey: "go-sdk:value:" + runID, TTLMS: Int64(60_000)}))
	valueRef := asString(responseField(valueResponse, "ref"))
	if valueRef == "" {
		t.Fatalf("FLOW.VALUE.PUT did not return ref: %#v", valueResponse)
	}
	requireLen(t, must[[]any](t)(client.ValueMGet(ctx, []string{valueRef}, nil)), 1)
	pipelineValues := must[[]any](t)(client.Pipeline(ctx, [][]any{{"FLOW.VALUE.MGET", valueRef}}))
	if len(pipelineValues) != 1 {
		t.Fatalf("pipelined FLOW.VALUE.MGET returned %d results, expected 1", len(pipelineValues))
	}
	pipelineItems, ok := pipelineValues[0].([]any)
	if !ok {
		t.Fatalf("pipelined FLOW.VALUE.MGET result = %T, expected []any", pipelineValues[0])
	}
	requireLen(t, pipelineItems, 1)

	signalID := "go-sdk:signal:" + runID
	signalPartition := signalID + ":partition"
	policy := PolicyOptions{
		IndexedAttributes: []string{"search_marker"},
		IndexedStateMeta:  "version",
		MaxActiveMS:       FlowMaxActiveInfinity,
		Retry:             &RetryPolicy{MaxRetries: 3, Backoff: "fixed", BaseMS: 10, MaxMS: 100, ExhaustedTo: "failed"},
		States: map[string]RetryPolicy{
			"queued": {MaxRetries: 1, Backoff: "fixed", BaseMS: 10, MaxMS: 100},
		},
	}
	installedPolicy := must[PolicySnapshot](t)(client.SetPolicy(ctx, typeName, policy))
	if installedPolicy.Type != typeName || installedPolicy.Generation <= 0 {
		t.Fatalf("installed policy snapshot = %+v", installedPolicy)
	}
	readPolicy := must[PolicySnapshot](t)(client.PolicyGet(ctx, typeName, ""))
	if readPolicy.Type != typeName || readPolicy.Generation != installedPolicy.Generation {
		t.Fatalf("read policy snapshot = %+v, installed = %+v", readPolicy, installedPolicy)
	}
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: signalID, Type: typeName, State: "created", PartitionKey: signalPartition, Payload: map[string]any{"step": "created"}, Idempotent: Bool(true)}))
	namedValue := must[any](t)(client.PutValue(ctx, "result", map[string]any{"named": true}, ValuePutOptions{
		OwnerFlowID: signalID, PartitionKey: signalPartition,
	}))
	namedRef := asString(responseField(namedValue, "ref"))
	if namedRef == "" {
		t.Fatalf("named FLOW.VALUE.PUT did not return ref: %#v", namedValue)
	}
	requireLen(t, must[[]any](t)(client.ValueMGet(ctx, []string{namedRef}, nil)), 1)
	requireValue(t, must[any](t)(client.Signal(ctx, SignalOptions{ID: signalID, Signal: "approve", PartitionKey: signalPartition, IfStates: []string{"created"}, TransitionTo: "approved"})))
	requireValue(t, must[any](t)(client.FlowSignal(ctx, SignalOptions{ID: signalID, Signal: "ship", PartitionKey: signalPartition, IfStates: []string{"approved"}, TransitionTo: "shipped"})))
	if record := must[*FlowRecord](t)(client.Get(ctx, signalID, signalPartition, nil)); record == nil || record.State != "shipped" {
		t.Fatalf("signal flow = %#v", record)
	}

	assertBatchFlowCommands(t, ctx, client, typeName, runID, now)
	assertSingleMutationCommands(t, ctx, client, typeName, runID, now)
	assertSearchCommands(t, ctx, client, typeName, runID, now)
	assertFusedWorkflowCommands(t, ctx, client, typeName, runID, now)
	assertManyMutationCommands(t, ctx, client, typeName, runID, now)
	assertRepairIndexAndRewindCommands(t, ctx, client, typeName, runID, now)

	requireValue(t, waitForFlowQueryRecord(t, ctx, signalID, func() ([]FlowRecord, error) {
		return client.List(ctx, typeName, ReadOptions{
			PartitionKey: signalPartition,
			State:        "shipped",
			Count:        Int(100),
		})
	}))
	requireMap(t, must[map[string]any](t)(client.Info(ctx, typeName, "", nil, nil)))
	_ = must[[]any](t)(client.History(ctx, HistoryOptions{ID: signalID, PartitionKey: signalPartition, Count: 5, IncludeCold: Bool(false), ConsistentProjection: Bool(true)}))
	requireMap(t, must[map[string]any](t)(client.RetentionCleanup(ctx, RetentionCleanupOptions{Limit: Int(10)})))
}
