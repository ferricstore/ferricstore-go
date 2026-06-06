package ferricstore

import (
	"context"
	"errors"
	"testing"
)

func TestWorkflowWorkerTransitionsState(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "validate")
	workflow.State("validate", func(_ context.Context, ctx WorkflowContext) (Outcome, error) {
		if ctx.ID() != "flow-1" || ctx.State() != "validate" {
			t.Fatalf("unexpected context: %+v", ctx.Job)
		}
		return TransitionTo("charge", []byte("next")), nil
	})

	result, err := workflow.Worker("worker-1", []string{"validate"}, WorkerOptions{BatchSize: 1}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and transition calls, got %d", len(exec.calls))
	}
	if exec.calls[1][0] != "FLOW.TRANSITION" {
		t.Fatalf("expected FLOW.TRANSITION, got %#v", exec.calls[1])
	}
}

func TestWorkflowWorkerFailPolicyFailsJob(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "validate")
	workflow.State("validate", func(context.Context, WorkflowContext) (Outcome, error) {
		return nil, errors.New("bad input")
	})

	result, err := workflow.Worker("worker-1", []string{"validate"}, WorkerOptions{
		BatchSize:   1,
		ErrorPolicy: ErrorPolicyFail,
	}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Applied != 1 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and fail calls, got %d", len(exec.calls))
	}
	if exec.calls[1][0] != "FLOW.FAIL" {
		t.Fatalf("expected FLOW.FAIL, got %#v", exec.calls[1])
	}
}
