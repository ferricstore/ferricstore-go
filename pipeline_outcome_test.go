package ferricstore

import (
	"context"
	"errors"
	"testing"
)

type completeOutcomePipelineExecutor struct {
	err error
}

func (*completeOutcomePipelineExecutor) Do(context.Context, ...any) (any, error) {
	return nil, errors.New("unexpected direct Do call")
}

func (e *completeOutcomePipelineExecutor) Pipeline(context.Context, [][]any) ([]any, error) {
	return []any{[]byte("first"), e.err, []byte("third")}, e.err
}

func TestPipelineResultValuesPreservesEveryOutcome(t *testing.T) {
	wantErr := errors.New("middle command failed")
	values, err := pipelineResultValues([]pipelineItemResult{
		{value: "first"},
		{err: wantErr},
		{value: "third"},
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("pipeline aggregate error = %v; want %v", err, wantErr)
	}
	if len(values) != 3 || values[0] != "first" || values[2] != "third" {
		t.Fatalf("pipeline outcomes = %#v", values)
	}
	if itemErr, ok := values[1].(error); !ok || !errors.Is(itemErr, wantErr) {
		t.Fatalf("pipeline failed item = %#v; want %v", values[1], wantErr)
	}
}

func TestClientPipelinePreservesCompleteOutcomesWithAggregateError(t *testing.T) {
	wantErr := errors.New("middle command failed")
	client := NewClientWithExecutor(&completeOutcomePipelineExecutor{err: wantErr})

	values, err := client.Pipeline(context.Background(), [][]any{
		{"GET", "first"},
		{"GET", "second"},
		{"GET", "third"},
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("pipeline aggregate error = %v; want %v", err, wantErr)
	}
	if len(values) != 3 || string(values[0].([]byte)) != "first" || string(values[2].([]byte)) != "third" {
		t.Fatalf("pipeline outcomes = %#v; want complete result slice", values)
	}
	if itemErr, ok := values[1].(error); !ok || !errors.Is(itemErr, wantErr) {
		t.Fatalf("failed pipeline item = %#v; want %v", values[1], wantErr)
	}
}
