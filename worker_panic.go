package ferricstore

import (
	"context"
	"fmt"
)

// HandlerPanicError reports a panic recovered at a queue or workflow handler
// boundary. Workers apply ErrorPolicy to it exactly as they do to an error
// returned by the handler.
type HandlerPanicError struct {
	Handler string
	Value   any
}

func (e *HandlerPanicError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("ferricstore %s handler panic: %v", e.Handler, e.Value)
}

func (e *HandlerPanicError) Unwrap() error {
	if e == nil {
		return nil
	}
	err, _ := e.Value.(error)
	return err
}

func invokeQueueHandler(handler QueueHandler, ctx context.Context, job FlowRecord) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &HandlerPanicError{Handler: "queue", Value: value}
		}
	}()
	return handler(ctx, job)
}

func invokeWorkflowHandler(
	handler WorkflowHandler,
	ctx context.Context,
	workflowContext WorkflowContext,
) (outcome Outcome, err error) {
	defer func() {
		if value := recover(); value != nil {
			outcome = nil
			err = &HandlerPanicError{Handler: "workflow", Value: value}
		}
	}()
	return handler(ctx, workflowContext)
}
