package ferricstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

type queueLeaseTimingExecutor struct {
	completed chan struct{}
	once      sync.Once
}

func (e *queueLeaseTimingExecutor) Do(_ context.Context, args ...any) (any, error) {
	switch commandName(args) {
	case "FLOW.CLAIM_DUE":
		return []any{
			map[string]any{
				"id": "fast", "type": "email", "state": "queued",
				"partition_key": "tenant:1", "lease_token": "lease-fast", "fencing_token": int64(1),
			},
			map[string]any{
				"id": "slow", "type": "email", "state": "queued",
				"partition_key": "tenant:1", "lease_token": "lease-slow", "fencing_token": int64(2),
			},
		}, nil
	case "FLOW.COMPLETE":
		e.once.Do(func() { close(e.completed) })
		return []byte("OK"), nil
	default:
		return nil, errors.New("unexpected queue command")
	}
}

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

func TestQueueCompletionBatchCapacityIsBoundedByAvailableJobs(t *testing.T) {
	successes := make(chan ClaimedItem, 1)
	successes <- ClaimedItem{ID: "one"}
	close(successes)

	batch, open := nextQueueCompletionBatch(successes, 100_000)
	if open || len(batch) != 1 {
		t.Fatalf("completion batch = %#v, open=%t; want one final item", batch, open)
	}
	if cap(batch) > cap(successes) {
		t.Fatalf("completion batch capacity = %d for %d queued jobs", cap(batch), cap(successes))
	}
}

func TestQueueWorkerBatchesSuccessfulCompletions(t *testing.T) {
	claimed := []any{
		map[string]any{
			"id":            "job-1",
			"type":          "email",
			"state":         "queued",
			"partition_key": "tenant:1",
			"lease_token":   "lease-1",
			"fencing_token": int64(1),
		},
		map[string]any{
			"id":            "job-2",
			"type":          "email",
			"state":         "queued",
			"partition_key": "tenant:1",
			"lease_token":   "lease-2",
			"fencing_token": int64(2),
		},
	}
	exec := &fakeExecutor{values: []any{claimed, []byte("OK")}}
	client := NewClientWithExecutor(exec)
	queue := NewQueueClient(client).Queue("email")

	result, err := queue.Worker("worker-1", func(context.Context, FlowRecord) error {
		return nil
	}, WorkerOptions{BatchSize: 2, State: "queued", PartitionKey: "tenant:1", Concurrency: 2}).RunOnce(context.Background())

	if err != nil {
		t.Fatal(err)
	}
	if result.Claimed != 2 || result.Completed != 2 || result.Retried != 0 || result.Failed != 0 {
		t.Fatalf("unexpected worker result: %+v", result)
	}
	if len(exec.calls) != 2 {
		t.Fatalf("expected claim and complete_many calls, got %d", len(exec.calls))
	}
	got := exec.calls[1]
	if !containsSubsequence(got, []any{"FLOW.COMPLETE_MANY", "MIXED"}) {
		t.Fatalf("expected complete_many command, got %#v", got)
	}
	if indexOf(got, "RESULT") >= 0 {
		t.Fatalf("queue worker should not write a default result payload, got %#v", got)
	}
	if !containsSubsequence(got, []any{"job-1", "tenant:1", "lease-1", int64(1)}) ||
		!containsSubsequence(got, []any{"job-2", "tenant:1", "lease-2", int64(2)}) {
		t.Fatalf("expected both complete_many items, got %#v", got)
	}
}

func TestQueueWorkerRequiresHandler(t *testing.T) {
	exec := &fakeExecutor{value: []any{}}
	client := NewClientWithExecutor(exec)
	queue := NewQueueClient(client).Queue("email")

	result, err := queue.Worker("worker-1", nil, WorkerOptions{BatchSize: 1}).RunOnce(context.Background())

	if err == nil {
		t.Fatal("expected missing handler error")
	}
	if result != (QueueWorkerResult{}) {
		t.Fatalf("expected empty result before claim, got %+v", result)
	}
	if len(exec.calls) != 0 {
		t.Fatalf("expected no claim call without handler, got %#v", exec.calls)
	}
}

func TestQueueWorkerRejectsInvalidExecutionOptionsBeforeClaim(t *testing.T) {
	tests := []struct {
		name string
		opt  WorkerOptions
	}{
		{name: "negative batch size", opt: WorkerOptions{BatchSize: -1}},
		{name: "negative concurrency", opt: WorkerOptions{Concurrency: -1}},
		{name: "negative lease", opt: WorkerOptions{LeaseMS: -1}},
		{name: "unknown error policy", opt: WorkerOptions{ErrorPolicy: ErrorPolicy(99)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &fakeExecutor{value: []any{}}
			worker := NewQueueClient(NewClientWithExecutor(exec)).Queue("email").Worker(
				"worker-1", func(context.Context, FlowRecord) error { return nil }, test.opt,
			)
			if _, err := worker.RunOnce(context.Background()); err == nil {
				t.Fatal("invalid worker options were accepted")
			}
			if len(exec.calls) != 0 {
				t.Fatalf("invalid worker options reached claim transport: %#v", exec.calls)
			}
		})
	}
}

func TestQueuePolicyHelpersUseQueueType(t *testing.T) {
	exec := &fakeExecutor{values: []any{[]byte("OK"), []byte("OK")}}
	client := NewClientWithExecutor(exec)
	queueClient := NewQueueClient(client)
	queue := queueClient.Queue("email")

	if _, err := queue.InstallPolicy(context.Background(), PolicyOptions{
		StatePolicies: map[string]FlowStatePolicy{"queued": {Mode: FlowStateModeFIFO}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queueClient.InstallPolicy(context.Background(), "sms", PolicyOptions{Retry: &RetryPolicy{MaxRetries: 2}}); err != nil {
		t.Fatal(err)
	}

	if !containsSubsequence(exec.calls[0], []any{"FLOW.POLICY.SET", "email", "STATE", "queued", "MODE", "FIFO"}) {
		t.Fatalf("unexpected queue policy call: %#v", exec.calls[0])
	}
	if !reflect.DeepEqual(exec.calls[1], []any{"FLOW.POLICY.SET", "sms", "MAX_RETRIES", 2}) {
		t.Fatalf("unexpected queue client policy call: %#v", exec.calls[1])
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

func TestQueueWorkerCompletesSuccessfulSiblingsBeforeReturningError(t *testing.T) {
	claimed := []any{
		map[string]any{
			"id": "job-ok", "type": "email", "state": "queued",
			"partition_key": "tenant:1", "lease_token": "lease-ok", "fencing_token": int64(1),
		},
		map[string]any{
			"id": "job-failed", "type": "email", "state": "queued",
			"partition_key": "tenant:1", "lease_token": "lease-failed", "fencing_token": int64(2),
		},
	}
	exec := &fakeExecutor{values: []any{claimed, []byte("OK")}}
	client := NewClientWithExecutor(exec)
	wantErr := errors.New("job failed")
	queue := NewQueueClient(client).Queue("email")

	result, err := queue.Worker("worker-1", func(_ context.Context, job FlowRecord) error {
		if job.ID == "job-failed" {
			return wantErr
		}
		return nil
	}, WorkerOptions{BatchSize: 2, Concurrency: 2, ErrorPolicy: ErrorPolicyReturn}).RunOnce(context.Background())

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected handler error, got %v", err)
	}
	if result.Claimed != 2 || result.Completed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if len(exec.calls) != 2 || exec.calls[1][0] != "FLOW.COMPLETE" {
		t.Fatalf("successful sibling was not completed: %#v", exec.calls)
	}
	if !containsSubsequence(exec.calls[1], []any{"FLOW.COMPLETE", "job-ok", "lease-ok"}) {
		t.Fatalf("wrong job completed: %#v", exec.calls[1])
	}
}

func TestQueueWorkerCompletesFastJobBeforeSlowPeerFinishes(t *testing.T) {
	exec := &queueLeaseTimingExecutor{completed: make(chan struct{})}
	queue := NewQueueClient(NewClientWithExecutor(exec)).Queue("email")
	slowEntered := make(chan struct{})
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
	t.Cleanup(release)

	done := make(chan error, 1)
	go func() {
		_, err := queue.Worker("worker-1", func(_ context.Context, job FlowRecord) error {
			if job.ID == "slow" {
				close(slowEntered)
				<-releaseSlow
			}
			return nil
		}, WorkerOptions{BatchSize: 2, Concurrency: 2}).RunOnce(context.Background())
		done <- err
	}()

	select {
	case <-slowEntered:
	case <-time.After(time.Second):
		t.Fatal("slow handler did not start")
	}
	select {
	case <-exec.completed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("fast job was not completed while its lease was still independent of the slow peer")
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
