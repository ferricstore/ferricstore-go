package ferricstore

import (
	"context"
	"errors"
	"testing"
)

func TestQueueWorkerCompletesSuccessfulJobs(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "job-1",
			"type":          "email",
			"state":         "queued",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(1),
		},
	}}
	client := NewClientWithExecutor(exec)
	queue := NewQueueClient(client).Queue("email")

	result, err := queue.Worker("worker-1", func(context.Context, FlowRecord) error {
		return nil
	}, WorkerOptions{BatchSize: 1, State: "queued", PartitionKey: "tenant:1"}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 1 || result.Completed != 1 || result.Retried != 0 || result.Failed != 0 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and complete calls, got %d", len(exec.calls))
	}
	if exec.calls[1][0] != "FLOW.COMPLETE" {
		t.Fatalf("expected FLOW.COMPLETE, got %#v", exec.calls[1])
	}
}

func TestQueueWorkerReturnPolicyDoesNotRetry(t *testing.T) {
	exec := &fakeExecutor{value: []any{
		map[string]any{
			"id":            "job-1",
			"type":          "email",
			"state":         "queued",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(1),
		},
	}}
	client := NewClientWithExecutor(exec)
	queue := NewQueueClient(client).Queue("email")
	handlerErr := errors.New("handler failed")

	result, err := queue.Worker("worker-1", func(context.Context, FlowRecord) error {
		return handlerErr
	}, WorkerOptions{BatchSize: 1, ErrorPolicy: ErrorPolicyReturn}).RunOnce(context.Background())

	if !errors.Is(err, handlerErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if result.Claimed != 1 || result.Completed != 0 || result.Retried != 0 || result.Failed != 0 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 1 {
		t.Fatalf("expected only claim call, got %d", len(exec.calls))
	}
}
