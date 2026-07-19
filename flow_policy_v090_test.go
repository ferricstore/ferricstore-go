package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func policySnapshotResponse(flowType string, generation int64, overrides map[string]any) map[string]any {
	response := map[string]any{
		"type":               flowType,
		"generation":         generation,
		"version":            nil,
		"max_active_ms":      nil,
		"retry":              map[string]any{},
		"retention":          map[string]any{},
		"indexed_attributes": []any{},
		"indexed_state_meta": nil,
		"governance":         nil,
		"states":             map[string]any{},
	}
	for key, value := range overrides {
		response[key] = value
	}
	return response
}

func TestV090PolicyOptionsEncodeReplaceAndExpectedGeneration(t *testing.T) {
	exec := &fakeExecutor{value: policySnapshotResponse("order", 8, map[string]any{
		"states": map[string]any{
			"queued": map[string]any{"mode": []byte("fifo")},
		},
	})}
	client := NewClientWithExecutor(exec)

	snapshot, err := client.SetPolicy(context.Background(), "order", PolicyOptions{
		Replace:            Bool(false),
		ExpectedGeneration: Int64(7),
		StatePolicies: map[string]FlowStatePolicy{
			"queued": {Mode: FlowStateModeFIFO},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Type != "order" || snapshot.Generation != 8 {
		t.Fatalf("policy snapshot = %+v", snapshot)
	}
	if got := snapshot.States["queued"].Mode; got != FlowStateModeFIFO {
		t.Fatalf("queued mode = %q, want FIFO", got)
	}
	if !containsSubsequence(exec.calls[0], []any{"REPLACE", "false", "EXPECTED_GENERATION", int64(7)}) {
		t.Fatalf("policy CAS options missing from %#v", exec.calls[0])
	}
}

func TestV090DirectPolicyUpdateDefaultsToPatch(t *testing.T) {
	exec := &fakeExecutor{value: policySnapshotResponse("order", 1, nil)}
	if _, err := NewClientWithExecutor(exec).SetPolicy(
		context.Background(), "order", PolicyOptions{},
	); err != nil {
		t.Fatal(err)
	}
	if indexOf(exec.calls[0], "REPLACE") >= 0 {
		t.Fatalf("direct policy update forced replacement: %#v", exec.calls[0])
	}
}

func TestV090WorkflowInstallDefaultsToReplacementAndAllowsPatchOverride(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		policySnapshotResponse("order", 1, nil),
		policySnapshotResponse("order", 2, nil),
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "queued")

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{Replace: Bool(false)}); err != nil {
		t.Fatal(err)
	}
	if !containsSubsequence(exec.calls[0], []any{"REPLACE", "true"}) {
		t.Fatalf("workflow install did not default to replacement: %#v", exec.calls[0])
	}
	if !containsSubsequence(exec.calls[1], []any{"REPLACE", "false"}) {
		t.Fatalf("workflow install ignored explicit patch mode: %#v", exec.calls[1])
	}
}

func TestV090PolicyGenerationValidationFailsBeforeTransport(t *testing.T) {
	for _, generation := range []int64{-1, maxFlowPolicyGeneration + 1} {
		exec := &fakeExecutor{value: policySnapshotResponse("order", 1, nil)}
		_, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "order", PolicyOptions{
			ExpectedGeneration: Int64(generation),
		})
		if err == nil || !strings.Contains(err.Error(), "generation") {
			t.Fatalf("generation %d error = %v", generation, err)
		}
		if len(exec.calls) != 0 {
			t.Fatalf("invalid generation %d reached transport: %#v", generation, exec.calls)
		}
	}

	exec := &fakeExecutor{value: policySnapshotResponse("order", maxFlowPolicyGeneration, nil)}
	if _, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "order", PolicyOptions{
		ExpectedGeneration: Int64(maxFlowPolicyGeneration),
	}); err != nil {
		t.Fatalf("maximum safe generation rejected: %v", err)
	}
}

func TestV090PolicyResponsesAreTypedAndFailClosed(t *testing.T) {
	exec := &fakeExecutor{value: map[interface{}]interface{}{
		"type":               []byte("order"),
		"state":              []byte("queued"),
		"generation":         int64(11),
		"mode":               []byte("fifo"),
		"indexed_attributes": []any{[]byte("tenant")},
		"retry": map[interface{}]interface{}{
			"exhausted_to": []byte("failed"),
		},
	}}
	snapshot, err := NewClientWithExecutor(exec).PolicyGet(context.Background(), "order", "queued")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Type != "order" || snapshot.State != "queued" || snapshot.Generation != 11 || snapshot.Mode != FlowStateModeFIFO {
		t.Fatalf("decoded policy snapshot = %+v", snapshot)
	}
	if !reflect.DeepEqual(snapshot.IndexedAttributes, []string{"tenant"}) || asString(snapshot.Retry["exhausted_to"]) != "failed" {
		t.Fatalf("decoded policy fields = %+v", snapshot)
	}

	for _, response := range []any{
		[]byte("OK"),
		map[string]any{"type": "order"},
		map[string]any{"type": "other", "generation": int64(1)},
		map[string]any{"type": "order", "generation": maxFlowPolicyGeneration + 1},
	} {
		exec := &fakeExecutor{value: response}
		if _, err := NewClientWithExecutor(exec).PolicyGet(context.Background(), "order", ""); err == nil {
			t.Fatalf("malformed policy response %#v was accepted", response)
		}
	}
}

func TestV090StalePolicyGenerationHasDedicatedErrorAndNoClientRetry(t *testing.T) {
	cause := NativeError{Status: 1, Value: map[string]any{
		"code": "error", "message": "ERR stale flow policy generation",
		"retryable": false, "safe_to_retry": false,
	}}
	exec := &fakeExecutor{err: cause}
	_, err := NewClientWithExecutor(exec).SetPolicy(context.Background(), "order", PolicyOptions{
		ExpectedGeneration: Int64(7),
	})
	if err == nil {
		t.Fatal("stale CAS succeeded")
	}
	if !errors.Is(err, ErrStalePolicyGeneration) {
		t.Fatalf("stale error = %T %v; sentinel not exposed", err, err)
	}
	var stale *StalePolicyGenerationError
	if !errors.As(err, &stale) {
		t.Fatalf("stale error = %T %v; typed error not exposed", err, err)
	}
	var nativeCause NativeError
	if stale.FlowType != "order" || stale.ExpectedGeneration != 7 || !errors.As(stale, &nativeCause) {
		t.Fatalf("stale error fields = %+v", stale)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("stale CAS was retried %d times", len(exec.calls))
	}
}

func TestV090WorkflowValidatesFIFOEntryBeforeTransport(t *testing.T) {
	workflow := NewWorkflowClient(NewClientWithExecutor(&fakeExecutor{})).Workflow("order", "queued")
	workflow.State("queued", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	}, FlowStatePolicy{Mode: FlowStateModeFIFO})

	for _, opt := range []CreateOptions{
		{},
		{PartitionKey: "tenant:1", Priority: Int64(1)},
	} {
		if _, err := workflow.Start(context.Background(), "flow-1", nil, opt); err == nil {
			t.Fatalf("invalid FIFO start %+v succeeded", opt)
		}
	}
}

func TestV090WorkflowRejectsTransitionIntoFIFOWithoutPartition(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "created")
	workflow.statePolicies["queued"] = FlowStatePolicy{Mode: FlowStateModeFIFO}
	worker := workflow.Worker("worker", []string{"created"}, WorkerOptions{})
	job := FlowRecord{ID: "flow-1", State: "created", LeaseToken: "lease", FencingToken: 1}

	err := worker.apply(context.Background(), job, "created", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionTo("queued", nil), nil
	}, ErrorPolicyRetry)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "partition") {
		t.Fatalf("missing-partition FIFO transition error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid FIFO transition reached transport: %#v", exec.calls)
	}
}
