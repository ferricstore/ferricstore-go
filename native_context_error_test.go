package ferricstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

type expiredDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx expiredDeadlineContext) Deadline() (time.Time, bool) {
	return ctx.deadline, true
}

func TestNativeWriteErrorUsesExpiredContextDeadlineBeforeTimerNotification(t *testing.T) {
	ctx := expiredDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Millisecond),
	}
	err := nativeWriteContextError(ctx, errors.New("write: i/o timeout"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write error = %v, want context deadline", err)
	}
}
