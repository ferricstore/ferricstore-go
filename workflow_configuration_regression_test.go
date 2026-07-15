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
		[]byte("OK"),
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
