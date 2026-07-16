package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
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

func TestWorkflowWorkerAcceptsPointerOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		outcome Outcome
		command string
	}{
		{name: "transition", outcome: &TransitionResult{ToState: "next"}, command: "FLOW.TRANSITION"},
		{name: "complete", outcome: &CompleteResult{Result: "done"}, command: "FLOW.COMPLETE"},
		{name: "retry", outcome: &RetryResult{Error: "retry"}, command: "FLOW.RETRY"},
		{name: "fail", outcome: &FailResult{Error: "failed"}, command: "FLOW.FAIL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []byte("OK")}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
			worker := workflow.Worker("worker-1", []string{"ready"}, WorkerOptions{})
			job := FlowRecord{
				ID: "flow-1", State: "ready", PartitionKey: "tenant:1",
				LeaseToken: "lease-1", FencingToken: 9,
			}
			err := worker.apply(context.Background(), job, "ready", func(context.Context, WorkflowContext) (Outcome, error) {
				return test.outcome, nil
			})
			if err != nil {
				t.Fatalf("pointer outcome failed: %v", err)
			}
			if len(exec.calls) != 1 || asString(exec.calls[0][0]) != test.command {
				t.Fatalf("pointer outcome calls = %#v; want %s", exec.calls, test.command)
			}
		})
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

func TestWorkflowWorkerAppliesFailPolicyToHandlerPanic(t *testing.T) {
	claimed := []any{
		map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "validate",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		},
	}
	exec := &fakeExecutor{values: []any{claimed, []byte("OK")}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "validate")
	workflow.State("validate", func(context.Context, WorkflowContext) (Outcome, error) {
		panic(errWorkerHandlerPanic)
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
	if len(exec.calls) != 2 || exec.calls[1][0] != "FLOW.FAIL" {
		t.Fatalf("panic did not follow fail policy: %#v", exec.calls)
	}
	if got := asString(exec.calls[1][indexOf(exec.calls[1], "ERROR")+1]); !strings.Contains(got, errWorkerHandlerPanic.Error()) {
		t.Fatalf("fail error = %q; want panic cause", got)
	}
}

func TestWorkflowInstallsFIFOStatePolicyAndRejectsPriorityTransition(t *testing.T) {
	exec := &fakeExecutor{values: []any{
		[]byte("OK"),
		[]any{map[string]any{
			"id":            "flow-1",
			"type":          "order",
			"state":         "created",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(9),
		}},
	}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "created")
	workflow.State("created", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionResult{ToState: "ready", Priority: Int64(1)}, nil
	})
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(map[string]any{"ok": true}), nil
	}, FlowStatePolicy{Mode: FlowStateMode("fifo")})

	if _, err := workflow.InstallPolicy(context.Background(), PolicyOptions{}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(exec.calls[0], []any{"FLOW.POLICY.SET", "order", "STATE", "ready", "MODE", "FIFO"}) {
		t.Fatalf("unexpected workflow policy call: %#v", exec.calls[0])
	}

	_, err := workflow.Worker("worker-1", []string{"created"}, WorkerOptions{BatchSize: 1}).RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "priority is not supported for fifo state") {
		t.Fatalf("expected fifo priority error, got %v", err)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected policy and claim calls only, got %#v", exec.calls)
	}
}

func TestWorkflowWorkerValidatesOptionsBeforeClaiming(t *testing.T) {
	tests := []struct {
		name string
		opts WorkerOptions
	}{
		{name: "negative batch", opts: WorkerOptions{BatchSize: -1}},
		{name: "negative concurrency", opts: WorkerOptions{Concurrency: -1}},
		{name: "negative lease", opts: WorkerOptions{LeaseMS: -1}},
		{name: "invalid error policy", opts: WorkerOptions{ErrorPolicy: ErrorPolicy(99)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
			workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
				return CompleteWith(nil), nil
			})

			if _, err := workflow.Worker("worker-1", []string{"ready"}, test.opts).RunOnce(context.Background()); err == nil {
				t.Fatal("invalid workflow worker options were accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid options reached executor: %#v", exec.calls)
			}
		})
	}
}
