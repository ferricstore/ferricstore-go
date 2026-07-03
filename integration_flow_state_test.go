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
	_, err := client.ValueMGet(ctx, []string{valueRef}, nil)
	requireCommandError(t, err)

	signalID := "go-sdk:signal:" + runID
	signalPartition := signalID + ":partition"
	requireValue(t, must[any](t)(client.InstallPolicy(ctx, typeName, &RetryPolicy{MaxRetries: 3, Backoff: "fixed", BaseMS: 10, MaxMS: 100, ExhaustedTo: "failed"}, map[string]RetryPolicy{
		"queued": {MaxRetries: 1, Backoff: "fixed", BaseMS: 10, MaxMS: 100},
	})))
	requireMap(t, must[map[string]any](t)(client.PolicyGet(ctx, typeName, "")))
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

	requireValue(t, must[[]FlowRecord](t)(client.List(ctx, typeName, ReadOptions{Count: Int(100)})))
	requireMap(t, must[map[string]any](t)(client.Info(ctx, typeName, "", nil, nil)))
	requireLenAtLeast(t, must[[]any](t)(client.History(ctx, HistoryOptions{ID: signalID, PartitionKey: signalPartition, Count: 5})), 1)
	requireMap(t, must[map[string]any](t)(client.RetentionCleanup(ctx, RetentionCleanupOptions{Limit: Int(10)})))
}
