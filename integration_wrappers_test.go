//go:build integration

package ferricstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestIntegrationQueueAndWorkflowWrappers(t *testing.T) {
	ctx, cancel := integrationContext(t)
	defer cancel()

	client := integrationClient(JSONCodec{})
	defer client.Close()

	runID := integrationSuffix("wrappers")
	now := time.Now().UnixMilli()
	queueType := "go-sdk-queue-" + runID
	queueID := "go-sdk:queue:" + runID
	queuePartition := queueID + ":partition"
	queue := NewQueueClient(client).Queue(queueType)

	_ = must[*FlowRecord](t)(queue.Enqueue(ctx, queueID, map[string]any{"step": "queued"}, CreateOptions{PartitionKey: queuePartition, Idempotent: Bool(true), NowMS: now, RunAtMS: now}))
	queueResult := must[QueueWorkerResult](t)(queue.Worker("go-sdk-queue-worker", func(_ context.Context, job FlowRecord) error {
		payload, ok := job.Payload.(map[string]any)
		if !ok || payload["step"] != "queued" {
			return fmt.Errorf("unexpected queue payload: %#v", job.Payload)
		}
		return nil
	}, WorkerOptions{BatchSize: 1, ClaimPayload: true, NowMS: now + 1, PartitionKey: queuePartition}).RunOnce(ctx))
	if queueResult.Claimed != 1 || queueResult.Completed != 1 || queueResult.Retried != 0 || queueResult.Failed != 0 {
		t.Fatalf("unexpected queue result: %#v", queueResult)
	}

	workflowType := "go-sdk-workflow-" + runID
	workflowID := "go-sdk:workflow:" + runID
	workflowPartition := workflowID + ":partition"
	workflow := NewWorkflowClient(client).Workflow(workflowType, "received")
	workflow.State("received", func(context.Context, WorkflowContext) (Outcome, error) {
		return TransitionResult{ToState: "validated", Payload: map[string]any{"validated": true}, RunAtMS: now + 1}, nil
	})
	workflow.State("validated", func(_ context.Context, ctx WorkflowContext) (Outcome, error) {
		return CompleteWith(map[string]any{"id": ctx.ID(), "done": true}), nil
	})

	_ = must[*FlowRecord](t)(workflow.Start(ctx, workflowID, map[string]any{"order": runID}, CreateOptions{PartitionKey: workflowPartition, Idempotent: Bool(true), NowMS: now, RunAtMS: now}))
	first := must[WorkflowWorkerResult](t)(workflow.Worker("go-sdk-workflow-worker", []string{"received"}, WorkerOptions{BatchSize: 1, ClaimPayload: true, NowMS: now + 1, PartitionKey: workflowPartition}).RunOnce(ctx))
	if first.Claimed != 1 || first.Applied != 1 {
		t.Fatalf("unexpected first workflow result: %#v", first)
	}
	second := must[WorkflowWorkerResult](t)(workflow.Worker("go-sdk-workflow-worker", []string{"validated"}, WorkerOptions{BatchSize: 1, ClaimPayload: true, NowMS: now + 2, PartitionKey: workflowPartition}).RunOnce(ctx))
	if second.Claimed != 1 || second.Applied != 1 {
		t.Fatalf("unexpected second workflow result: %#v", second)
	}
	if record := must[*FlowRecord](t)(client.Get(ctx, workflowID, workflowPartition, nil, nil)); record == nil || record.State != "completed" {
		t.Fatalf("expected completed workflow, got %#v", record)
	}
}
