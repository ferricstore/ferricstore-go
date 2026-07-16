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
	if _, err := client.ValueMGet(ctx, []string{valueRef}, nil); err != nil {
		if !isUnsupportedValueMGetCommand(err) {
			t.Fatal(err)
		}
		skipIntegrationCommandCoverage("server image does not expose FLOW.VALUE.MGET", "FLOW.VALUE.MGET")
	}

	signalID := "go-sdk:signal:" + runID
	signalPartition := signalID + ":partition"
	policy := PolicyOptions{
		IndexedAttributes: []string{"search_marker"},
		IndexedStateMeta:  "version",
		Retry:             &RetryPolicy{MaxRetries: 3, Backoff: "fixed", BaseMS: 10, MaxMS: 100, ExhaustedTo: "failed"},
		States: map[string]RetryPolicy{
			"queued": {MaxRetries: 1, Backoff: "fixed", BaseMS: 10, MaxMS: 100},
		},
	}
	if value, err := client.SetPolicy(ctx, typeName, policy); err != nil {
		if !isUnsupportedNativePolicyOption(err) {
			t.Fatal(err)
		}
		t.Logf("server image does not support full native FLOW.POLICY.SET policy options: %v", err)
		skipIntegrationCommandCoverage("server image does not support indexed Flow policy options", "FLOW.SEARCH")
		value, fallbackErr := client.SetPolicy(ctx, typeName, PolicyOptions{
			IndexedStateMeta: "version",
		})
		if fallbackErr != nil {
			if !isUnsupportedNativePolicyOption(fallbackErr) {
				t.Fatal(fallbackErr)
			}
			t.Logf("server image does not support indexed Flow state meta policy option: %v", fallbackErr)
			var basicErr error
			value, basicErr = client.SetPolicy(ctx, typeName, PolicyOptions{
				Retry:  policy.Retry,
				States: policy.States,
			})
			if basicErr != nil {
				if !isUnsupportedNativePolicyOption(basicErr) {
					t.Fatal(basicErr)
				}
				t.Logf("server image does not support native FLOW.POLICY.SET retry options: %v", basicErr)
				skipIntegrationCommandCoverage("server image does not support native Flow policy options", "FLOW.POLICY.SET")
				value = nil
			}
		}
		if value != nil {
			requireValue(t, value)
		}
	} else {
		requireValue(t, value)
	}
	requireMap(t, must[map[string]any](t)(client.PolicyGet(ctx, typeName, "")))
	_ = must[*FlowRecord](t)(client.Create(ctx, CreateOptions{ID: signalID, Type: typeName, State: "created", PartitionKey: signalPartition, Payload: map[string]any{"step": "created"}, Idempotent: Bool(true)}))
	requireValue(t, must[any](t)(client.Signal(ctx, SignalOptions{ID: signalID, Signal: "approve", PartitionKey: signalPartition, IfStates: []string{"created"}, TransitionTo: "approved"})))
	requireValue(t, must[any](t)(client.FlowSignal(ctx, SignalOptions{ID: signalID, Signal: "ship", PartitionKey: signalPartition, IfStates: []string{"approved"}, TransitionTo: "shipped"})))
	if record := must[*FlowRecord](t)(client.Get(ctx, signalID, signalPartition, nil, nil)); record == nil || record.State != "shipped" {
		t.Fatalf("signal flow = %#v", record)
	}

	assertBatchFlowCommands(t, ctx, client, typeName, runID, now)
	assertSingleMutationCommands(t, ctx, client, typeName, runID, now)
	assertSearchCommands(t, ctx, client, typeName, runID, now)
	assertFusedWorkflowCommands(t, ctx, client, typeName, runID, now)
	assertManyMutationCommands(t, ctx, client, typeName, runID, now)
	assertRepairIndexAndRewindCommands(t, ctx, client, typeName, runID, now)

	requireValue(t, must[[]FlowRecord](t)(client.List(ctx, typeName, ReadOptions{Count: Int(100)})))
	requireMap(t, must[map[string]any](t)(client.Info(ctx, typeName, "", nil, nil)))
	requireLenAtLeast(t, must[[]any](t)(client.History(ctx, HistoryOptions{ID: signalID, PartitionKey: signalPartition, Count: 5})), 1)
	requireMap(t, must[map[string]any](t)(client.RetentionCleanup(ctx, RetentionCleanupOptions{Limit: Int(10)})))
}
