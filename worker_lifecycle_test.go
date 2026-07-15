package ferricstore

import (
	"context"
	"sync"
	"testing"
	"time"
)

type cancellationBlockingExecutor struct {
	started chan struct{}
	once    sync.Once
}

func (e *cancellationBlockingExecutor) Do(ctx context.Context, _ ...any) (any, error) {
	e.once.Do(func() { close(e.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

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

func TestWorkerHandlesTreatStopDuringPollAsGraceful(t *testing.T) {
	tests := []struct {
		name  string
		start func(*Client) (func(), func() error)
	}{
		{
			name: "queue",
			start: func(client *Client) (func(), func() error) {
				handle := NewQueueClient(client).Queue("email").Worker(
					"worker-1", func(context.Context, FlowRecord) error { return nil }, WorkerOptions{},
				).Start(context.Background(), time.Hour)
				return handle.Stop, func() error { _, err := handle.Join(); return err }
			},
		},
		{
			name: "workflow",
			start: func(client *Client) (func(), func() error) {
				workflow := NewWorkflowClient(client).Workflow("order", "ready")
				workflow.State("ready", func(context.Context, WorkflowContext) (Outcome, error) {
					return CompleteWith(nil), nil
				})
				handle := workflow.Worker("worker-1", []string{"ready"}, WorkerOptions{}).
					Start(context.Background(), time.Hour)
				return handle.Stop, func() error { _, err := handle.Join(); return err }
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &cancellationBlockingExecutor{started: make(chan struct{})}
			stop, join := test.start(NewClientWithExecutor(exec))
			select {
			case <-exec.started:
			case <-time.After(time.Second):
				t.Fatal("worker did not start its poll")
			}
			stop()
			if err := join(); err != nil {
				t.Fatalf("Stop followed by Join returned %v; want graceful shutdown", err)
			}
		})
	}
}

func TestWorkerLifecycleAcceptsNilContext(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Client) error
	}{
		{
			name: "queue RunForever",
			run: func(client *Client) error {
				worker := NewQueueClient(client).Queue("email").Worker("worker-1", nil, WorkerOptions{})
				_, err := worker.RunForever(nil, time.Millisecond)
				return err
			},
		},
		{
			name: "queue Start",
			run: func(client *Client) error {
				worker := NewQueueClient(client).Queue("email").Worker("worker-1", nil, WorkerOptions{})
				_, err := worker.Start(nil, time.Millisecond).Join()
				return err
			},
		},
		{
			name: "workflow RunForever",
			run: func(client *Client) error {
				workflow := NewWorkflowClient(client).Workflow("order", "ready")
				_, err := workflow.Worker("worker-1", nil, WorkerOptions{}).RunForever(nil, time.Millisecond)
				return err
			},
		},
		{
			name: "workflow Start",
			run: func(client *Client) error {
				workflow := NewWorkflowClient(client).Workflow("order", "ready")
				_, err := workflow.Worker("worker-1", nil, WorkerOptions{}).Start(nil, time.Millisecond).Join()
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run(NewClientWithExecutor(&fakeExecutor{value: []any{}}))
			if err == nil {
				t.Fatal("expected worker configuration error")
			}
		})
	}
}
