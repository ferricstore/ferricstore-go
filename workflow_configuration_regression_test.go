package ferricstore

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestWorkflowWorkerValidatesEveryHandlerBeforeClaiming(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	})

	_, err := workflow.Worker("worker-1", []string{"ready", "missing"}, WorkerOptions{}).
		RunOnce(context.Background())
	if err == nil {
		t.Fatal("workflow worker accepted a state without a handler")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid workflow configuration issued claims: %#v", exec.calls)
	}
}

func TestWorkflowWorkerRejectsEmptyStateSetBeforeClaiming(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")

	result, err := workflow.Worker("worker-1", nil, WorkerOptions{}).
		RunOnce(context.Background())
	if err == nil {
		t.Fatal("workflow worker accepted an empty state set")
	}
	if result != (WorkflowWorkerResult{}) {
		t.Fatalf("empty workflow configuration returned result %+v", result)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("empty workflow configuration issued claims: %#v", exec.calls)
	}
}

func TestWorkflowWorkerSortsAutomaticallySelectedStates(t *testing.T) {
	workflow := NewWorkflowClient(NewClientWithExecutor(&fakeExecutor{})).Workflow("order", "ready")
	handler := func(context.Context, WorkflowContext) (Outcome, error) { return CompleteWith(nil), nil }
	workflow.State("zeta", handler).State("alpha", handler).State("middle", handler)

	want := []string{"alpha", "middle", "zeta"}
	for iteration := 0; iteration < 20; iteration++ {
		got := workflow.Worker("worker-1", nil, WorkerOptions{}).Options.States
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("automatic workflow states = %#v, want %#v", got, want)
		}
	}
}

func TestWorkflowWorkerRejectsDuplicateStatesBeforeClaiming(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	})

	_, err := workflow.Worker("worker-1", []string{"ready", "ready"}, WorkerOptions{}).
		RunOnce(context.Background())
	if err == nil {
		t.Fatal("workflow worker accepted a duplicate state")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("duplicate workflow states issued claims: %#v", exec.calls)
	}
}

func TestWorkflowRemembersInstalledFIFOPoliciesForWorkerValidation(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		policySnapshotResponse("order", 1, map[string]any{"states": map[string]any{
			"ready": map[string]any{"mode": "fifo"},
		}}),
		[]any{map[string]any{
			"id": "flow-1", "type": "order", "state": "created",
			"partition_key": "tenant:1", "lease_token": "lease-1", "fencing_token": int64(9),
		}},
		[]byte("OK"),
	}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "created")
	workflow.State("created", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionResult{ToState: "ready", Priority: Int64(1)}, nil
	})

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{"ready": {Mode: FlowStateModeFIFO}},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := workflow.Worker("worker-1", []string{"created"}, WorkerOptions{BatchSize: 1}).
		RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "priority is not supported for fifo state") {
		t.Fatalf("worker FIFO validation error = %v", err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("FIFO policy and claim calls = %#v; priority transition reached transport", exec.calls)
	}
}

func TestWorkflowInstallPolicyRejectsConflictingStatePolicySources(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	}, FlowStatePolicy{Mode: FlowStateModeFIFO})

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{"ready": {Mode: FlowStateModeParallel}},
	}); err == nil {
		t.Fatal("workflow silently overwrote a conflicting explicit state policy")
	}
	if len(exec.calls) != 0 {
		t.Fatalf("conflicting workflow policy reached transport: %#v", exec.calls)
	}
}

func TestWorkflowInstallPolicyAllowsEquivalentStatePolicySources(t *testing.T) {
	exec := &fakeExecutor{value: policySnapshotResponse("order", 1, map[string]any{"states": map[string]any{
		"ready": map[string]any{"mode": "fifo", "retry": map[string]any{"max_retries": int64(2)}},
	}})}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	policy := FlowStatePolicy{Mode: FlowStateModeFIFO, Retry: &RetryPolicy{MaxRetries: 2}}
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	}, policy)

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{"ready": policy},
	}); err != nil {
		t.Fatal(err)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("equivalent workflow policy calls = %#v; want one", exec.calls)
	}
}

func TestWorkflowRejectsDuplicateHandlerRegistrationBeforeIO(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	first := func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith("first"), nil
	}
	second := func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith("second"), nil
	}
	workflow.State("ready", first).State("ready", second)

	_, err := workflow.Worker("worker-1", nil, WorkerOptions{}).RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "duplicate workflow state") {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("duplicate workflow registration reached transport: %#v", exec.calls)
	}
}

func TestWorkflowRejectsMultiplePoliciesForOneStateBeforeIO(t *testing.T) {
	exec := &fakeExecutor{value: []byte("OK")}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	}, FlowStatePolicy{Mode: FlowStateModeFIFO}, FlowStatePolicy{Mode: FlowStateModeParallel})

	_, err := workflow.InstallPolicy(context.Background(), PolicyOptions{})
	if err == nil || !strings.Contains(err.Error(), "at most one state policy") {
		t.Fatalf("multiple policy error = %v", err)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("invalid workflow policy reached transport: %#v", exec.calls)
	}
}

func TestWorkflowSnapshotsRegisteredStatePolicy(t *testing.T) {
	exec := &fakeExecutor{value: policySnapshotResponse("order", 1, map[string]any{"states": map[string]any{
		"ready": map[string]any{"mode": "fifo", "retry": map[string]any{"max_retries": int64(2)}},
	}})}
	retry := &RetryPolicy{MaxRetries: 2}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	}, FlowStatePolicy{Mode: FlowStateModeFIFO, Retry: retry})

	retry.MaxRetries = 9
	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{}); err != nil {
		t.Fatal(err)
	}
	if !containsSubsequence(exec.calls[0], []any{"STATE", "ready", "MODE", "FIFO", "MAX_RETRIES", 2}) {
		t.Fatalf("workflow policy was not snapshotted: %#v", exec.calls[0])
	}
}

func TestWorkflowSnapshotsSuccessfullyInstalledPolicy(t *testing.T) {
	exec := &fakeExecutor{value: policySnapshotResponse("order", 1, map[string]any{"states": map[string]any{
		"ready": map[string]any{"mode": "fifo", "retry": map[string]any{"max_retries": int64(2)}},
	}})}
	retry := &RetryPolicy{MaxRetries: 2}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{"ready": {Mode: FlowStateModeFIFO, Retry: retry}},
	}); err != nil {
		t.Fatal(err)
	}

	retry.MaxRetries = 9
	if got := workflow.statePolicies["ready"].Retry.MaxRetries; got != 2 {
		t.Fatalf("installed workflow retry policy = %d, want snapshotted 2", got)
	}
}
