package ferricstore

import (
	"context"
	"testing"
	"time"
)

func TestQueueWorkerStartStopJoin(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)
	worker := NewQueueClient(client).Queue("email").Worker("worker-1", func(context.Context, FlowRecord) error {
		return nil
	}, WorkerOptions{BatchSize: 1})

	handle := worker.Start(context.Background(), time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	handle.Stop()
	result, err := handle.Join()

	if err != nil {
		t.Fatal(err)
	}
	if result.ClaimCalls == 0 {
		t.Fatalf("expected worker to poll at least once: %+v", result)
	}
}

func TestWorkflowWorkerRunForeverStopsOnContext(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)
	workflow := NewWorkflowClient(client).Workflow("order", "created")
	workflow.State("created", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith("ok"), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := workflow.Worker("worker-1", nil, WorkerOptions{BatchSize: 1}).RunForever(ctx, time.Millisecond)

	if err != nil {
		t.Fatal(err)
	}
	if result.ClaimCalls != 0 {
		t.Fatalf("expected no polling after canceled context: %+v", result)
	}
}
