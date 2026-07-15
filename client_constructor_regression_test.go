package ferricstore

import (
	"context"
	"errors"
	"testing"
)

var errTypedNilExecutorCalled = errors.New("typed-nil executor was called")

type typedNilClientExecutor struct{}

func (*typedNilClientExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errTypedNilExecutorCalled
}

func TestClientConstructorRejectsTypedNilExecutor(t *testing.T) {
	var exec *typedNilClientExecutor
	client := NewClientWithExecutor(exec)

	_, err := client.Ping(context.Background())
	if !errors.Is(err, errClientExecutorRequired) {
		t.Fatalf("typed-nil executor error = %v; want %v", err, errClientExecutorRequired)
	}
	if errors.Is(err, errTypedNilExecutorCalled) {
		t.Fatal("client dispatched through a typed-nil executor")
	}
}
