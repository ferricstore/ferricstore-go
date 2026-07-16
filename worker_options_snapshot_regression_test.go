package ferricstore

import (
	"context"
	"reflect"
	"testing"
)

func TestQueueWorkerSnapshotsMutableOptions(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	states := []string{"ready"}
	partitions := []string{"tenant:1"}
	reclaimExpired := true
	reclaimRatio := int64(25)
	worker := NewQueueClient(NewClientWithExecutor(exec)).Queue("order").Worker(
		"worker-1", func(context.Context, FlowRecord) error { return nil }, WorkerOptions{
			States: states, PartitionKeys: partitions,
			ReclaimExpired: &reclaimExpired, ReclaimRatio: &reclaimRatio,
		},
	)

	states[0] = "mutated"
	partitions[0] = "mutated"
	reclaimExpired = false
	reclaimRatio = 99
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertWorkerClaimSnapshot(t, exec.calls[0])
}

func TestWorkflowWorkerSnapshotsMutableOptions(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	workflow := NewWorkflowClient(NewClientWithExecutor(exec)).Workflow("order", "ready")
	workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
		return CompleteWith(nil), nil
	})
	states := []string{"ready"}
	partitions := []string{"tenant:1"}
	reclaimExpired := true
	reclaimRatio := int64(25)
	worker := workflow.Worker("worker-1", states, WorkerOptions{
		PartitionKeys: partitions, ReclaimExpired: &reclaimExpired, ReclaimRatio: &reclaimRatio,
	})

	states[0] = "mutated"
	partitions[0] = "mutated"
	reclaimExpired = false
	reclaimRatio = 99
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	assertWorkerClaimSnapshot(t, exec.calls[0])
}

func assertWorkerClaimSnapshot(t *testing.T, command []any) {
	t.Helper()
	stateIndex := indexOf(command, "STATE")
	if stateIndex < 0 || stateIndex+1 >= len(command) || command[stateIndex+1] != "ready" {
		t.Fatalf("claim states were not snapshotted: %#v", command)
	}
	partitionIndex := indexOf(command, "PARTITIONS")
	if partitionIndex < 0 || !reflect.DeepEqual(command[partitionIndex+2:partitionIndex+3], []any{"tenant:1"}) {
		t.Fatalf("claim partitions were not snapshotted: %#v", command)
	}
	if got := command[indexOf(command, "RECLAIM_EXPIRED")+1]; got != "true" {
		t.Fatalf("claim reclaim_expired = %#v, want true", got)
	}
	if got := command[indexOf(command, "RECLAIM_RATIO")+1]; got != int64(25) {
		t.Fatalf("claim reclaim_ratio = %#v, want 25", got)
	}
}
