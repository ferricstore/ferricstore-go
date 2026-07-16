package ferricstore

import (
	"context"
	"errors"
	"testing"
)

var errWorkerHandlerPanic = errors.New("handler exploded")

func TestQueueHandlerPanicBecomesTypedError(t *testing.T) {
	err := invokeQueueHandler(func(context.Context, FlowRecord) error {
		panic(errWorkerHandlerPanic)
	}, context.Background(), FlowRecord{ID: "job"})
	var panicErr *HandlerPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("queue panic error = %T %v; want *HandlerPanicError", err, err)
	}
	if panicErr.Handler != "queue" || panicErr.Value != errWorkerHandlerPanic {
		t.Fatalf("queue panic metadata = %#v", panicErr)
	}
	if !errors.Is(err, errWorkerHandlerPanic) {
		t.Fatalf("queue panic did not retain its error cause: %v", err)
	}
}

func TestWorkflowHandlerPanicBecomesTypedError(t *testing.T) {
	outcome, err := invokeWorkflowHandler(func(context.Context, WorkflowContext) (Outcome, error) {
		panic("workflow exploded")
	}, context.Background(), WorkflowContext{})
	if outcome != nil {
		t.Fatalf("workflow panic outcome = %#v; want nil", outcome)
	}
	var panicErr *HandlerPanicError
	if !errors.As(err, &panicErr) || panicErr.Handler != "workflow" || panicErr.Value != "workflow exploded" {
		t.Fatalf("workflow panic error = %#v, %v", panicErr, err)
	}
}

func TestHandlerRecoveryFastPathDoesNotAllocate(t *testing.T) {
	allocations := testing.AllocsPerRun(1000, func() {
		if err := invokeQueueHandler(noopQueueHandler, context.Background(), FlowRecord{}); err != nil {
			panic(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("normal handler recovery path allocations = %.0f; want 0", allocations)
	}
}

func noopQueueHandler(context.Context, FlowRecord) error { return nil }
