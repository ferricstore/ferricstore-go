package ferricstore

import (
	"context"
	"testing"
)

type topKCapacityExecutor struct {
	response any
	argsLen  int
	argsCap  int
}

func (e *topKCapacityExecutor) Do(_ context.Context, args ...any) (any, error) {
	e.argsLen = len(args)
	e.argsCap = cap(args)
	return e.response, nil
}

func TestTopKBatchCommandsPreallocateExactArgumentCapacity(t *testing.T) {
	const batchSize = 100
	items := make([]any, batchSize)
	increments := make([]TopKIncrement, batchSize)
	arrayResponse := make([]any, batchSize)
	for index := range items {
		items[index] = "item"
		increments[index] = TopKIncrement{Item: "item", Count: 1}
		arrayResponse[index] = int64(0)
	}

	tests := []struct {
		name     string
		response any
		call     func(*TopKStore) error
	}{
		{
			name:     "ADD",
			response: make([]any, batchSize),
			call: func(store *TopKStore) error {
				_, err := store.Add(context.Background(), "topk", items...)
				return err
			},
		},
		{
			name:     "INCRBY",
			response: make([]any, batchSize),
			call: func(store *TopKStore) error {
				_, err := store.IncrBy(context.Background(), "topk", increments...)
				return err
			},
		},
		{
			name:     "QUERY",
			response: arrayResponse,
			call: func(store *TopKStore) error {
				_, err := store.Query(context.Background(), "topk", items...)
				return err
			},
		},
		{
			name:     "COUNT",
			response: arrayResponse,
			call: func(store *TopKStore) error {
				_, err := store.Count(context.Background(), "topk", items...)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := &topKCapacityExecutor{response: test.response}
			if err := test.call(NewClientWithExecutor(exec).TopK()); err != nil {
				t.Fatal(err)
			}
			if exec.argsCap != exec.argsLen {
				t.Fatalf("command argument capacity = %d for length %d; want exact preallocation", exec.argsCap, exec.argsLen)
			}
		})
	}
}
